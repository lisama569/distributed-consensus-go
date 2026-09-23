package consensus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/lisama569/distributed-consensus-go/pkg/cluster"
)

// Message kinds exchanged over the simulated network.
const (
	kindRequestVote   = "raft.requestVote"
	kindAppendEntries = "raft.appendEntries"
)

// ErrNotLeader is returned by Propose when the node is not leader or has
// just lost leadership.
var ErrNotLeader = errors.New("consensus: not the leader")

// Config controls a Raft node's local timers.
type Config struct {
	ElectionTimeoutMin int // milliseconds
	ElectionTimeoutMax int // milliseconds
	HeartbeatInterval  int // milliseconds
	TickInterval       int // milliseconds
}

// DefaultConfig returns timer values usable for local simulation.
func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin: 150,
		ElectionTimeoutMax: 300,
		HeartbeatInterval:  50,
		TickInterval:       10,
	}
}

type proposalResult struct {
	resp []byte
	err  error
}

// RaftNode is one member of the simulated Raft cluster. All mutable state
// is guarded by mu; every state transition is made under that single lock,
// which is what makes the safety arguments easy to reason about.
type RaftNode struct {
	id    string
	peers []string
	net   cluster.Network
	pers  Persister
	cfg   Config
	apply func(Apply) []byte
	logf  func(format string, args ...any)

	mu       sync.Mutex
	role     Role
	term     int
	votedFor string
	log      []LogEntry // log[0] is a synthetic empty entry at index 0
	commitIx int
	applied  int
	leaderID string

	nextIndex  map[string]int
	matchIndex map[string]int

	proposals map[int]chan proposalResult

	deadline     time.Time
	lastBeatSent time.Time
	wg           sync.WaitGroup
}

// NewRaftNode creates a node. applyf is called serially for every
// committed entry and returns the state machine's opaque response.
func NewRaftNode(id string, peers []string, net cluster.Network, pers Persister, cfg Config, applyf func(Apply) []byte) *RaftNode {
	if cfg.TickInterval <= 0 {
		cfg = DefaultConfig()
	}
	if pers == nil {
		pers = NewMemPersister()
	}
	allPeers := make([]string, 0, len(peers))
	for _, p := range peers {
		if p != id {
			allPeers = append(allPeers, p)
		}
	}
	return &RaftNode{
		id:         id,
		peers:      allPeers,
		net:        net,
		pers:       pers,
		cfg:        cfg,
		apply:      applyf,
		logf:       func(string, ...any) {},
		log:        []LogEntry{{}},
		proposals:  map[int]chan proposalResult{},
		nextIndex:  map[string]int{},
		matchIndex: map[string]int{},
	}
}

// WithLogger installs a decision-log sink and returns the node.
func (r *RaftNode) WithLogger(f func(format string, args ...any)) *RaftNode {
	r.logf = f
	return r
}

// Run restores durable state, wires the network handler and runs the
// timer loop until ctx is canceled.
func (r *RaftNode) Run(ctx context.Context) {
	r.mu.Lock()
	if term, voted, err := r.pers.LoadState(); err == nil {
		r.term = term
		r.votedFor = voted
	}
	if entries, err := r.pers.LoadLog(); err == nil && len(entries) > 0 {
		r.log = append([]LogEntry{{}}, entries...)
	}
	if ci, err := r.pers.LoadCommitIndex(); err == nil {
		r.commitIx = ci
	}
	// Replay committed entries into the freshly built state machine so a
	// restarted node serves the same lock/token state without waiting for
	// a leader to re-push everything.
	for r.applied < r.commitIx {
		r.applied++
		r.apply(Apply{Entry: r.log[r.applied]})
	}
	r.role = Follower
	r.resetElectionLocked()
	startTerm, startLogLen, startCommit := r.term, r.lastIndex(), r.commitIx
	r.mu.Unlock()

	r.net.Register(r.id, r.handle)
	r.logf("[%s] started term=%d logLen=%d commitIndex=%d role=follower",
		r.id, startTerm, startLogLen, startCommit)

	ticker := time.NewTicker(time.Duration(r.cfg.TickInterval) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.wg.Wait()
			r.net.DropNode(r.id)
			return
		case now := <-ticker.C:
			r.tick(now)
		}
	}
}

func (r *RaftNode) tick(now time.Time) {
	r.mu.Lock()
	switch r.role {
	case Follower, Candidate:
		if now.After(r.deadline) {
			r.startElectionLocked()
		}
	case Leader:
		if now.Sub(r.lastBeatSent) >= time.Duration(r.cfg.HeartbeatInterval)*time.Millisecond {
			r.lastBeatSent = now
			r.broadcastAppendLocked()
		}
	}
	r.mu.Unlock()
}

// ---------- leader election ----------

func (r *RaftNode) resetElectionLocked() {
	d := r.cfg.ElectionTimeoutMin + rand.Intn(r.cfg.ElectionTimeoutMax-r.cfg.ElectionTimeoutMin+1)
	r.deadline = time.Now().Add(time.Duration(d) * time.Millisecond)
}

func (r *RaftNode) startElectionLocked() {
	r.role = Candidate
	r.term++
	r.votedFor = r.id
	if err := r.pers.SaveState(r.term, r.id); err != nil {
		r.logf("[%s] persist term failed: %v", r.id, err)
	}
	r.resetElectionLocked()
	term := r.term
	args := VoteArgs{
		Term:         term,
		CandidateID:  r.id,
		LastLogIndex: r.lastIndex(),
		LastLogTerm:  r.lastTerm(),
	}
	r.logf("[%s] election timeout -> candidate term=%d lastLog=(%d,%d)",
		r.id, term, args.LastLogIndex, args.LastLogTerm)

	granted := 1 // self vote
	finished := false
	for _, p := range r.peers {
		peer := p
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			reply := r.sendVote(peer, args)
			if reply == nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if reply.Term > r.term {
				r.becomeFollowerLocked(reply.Term)
				return
			}
			if r.role != Candidate || r.term != term {
				return
			}
			if reply.VoteGranted {
				granted++
				if !finished && granted*2 > len(r.peers)+1 {
					finished = true
					r.becomeLeaderLocked()
				}
			}
		}()
	}
}

func (r *RaftNode) becomeLeaderLocked() {
	r.role = Leader
	r.leaderID = r.id
	next := r.lastIndex() + 1
	for _, p := range r.peers {
		r.nextIndex[p] = next
		r.matchIndex[p] = 0
	}
	r.lastBeatSent = time.Time{} // send heartbeat immediately
	r.logf("[%s] elected leader term=%d lastIndex=%d (majority votes)",
		r.id, r.term, r.lastIndex())
	r.broadcastAppendLocked()
}

func (r *RaftNode) becomeFollowerLocked(term int) {
	if term > r.term {
		r.term = term
		r.votedFor = ""
		if err := r.pers.SaveState(term, ""); err != nil {
			r.logf("[%s] persist term failed: %v", r.id, err)
		}
	}
	r.role = Follower
	r.leaderID = ""
	r.resetElectionLocked()
	r.failProposalsLocked(ErrNotLeader)
}

// ---------- log replication ----------

func (r *RaftNode) broadcastAppendLocked() {
	for _, p := range r.peers {
		peer := p
		r.replicateToLocked(peer)
	}
}

func (r *RaftNode) replicateToLocked(peer string) {
	ni := r.nextIndex[peer]
	prev := ni - 1
	if prev < 0 || prev >= len(r.log) {
		r.logf("[%s] replicate bug: nextIndex=%d logLen=%d", r.id, ni, len(r.log))
		return
	}
	args := AppendArgs{
		Term:         r.term,
		LeaderID:     r.id,
		PrevLogIndex: prev,
		PrevLogTerm:  r.log[prev].Term,
		Entries:      append([]LogEntry(nil), r.log[ni:]...),
		LeaderCommit: r.commitIx,
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		reply := r.sendAppend(peer, args)
		if reply == nil {
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if reply.Term > r.term || (reply.Term == r.term && r.role != Leader) {
			if reply.Term > r.term {
				r.becomeFollowerLocked(reply.Term)
			}
			return
		}
		if r.term != args.Term {
			return
		}
		if reply.Success {
			acked := args.PrevLogIndex + len(args.Entries)
			if acked > r.matchIndex[peer] {
				r.matchIndex[peer] = acked
				r.nextIndex[peer] = acked + 1
			}
			r.advanceCommitLocked()
		} else {
			cf := reply.ConflictIndex
			if cf < 1 {
				cf = 1
			}
			if cf < r.nextIndex[peer] {
				r.nextIndex[peer] = cf
			}
		}
	}()
}

func (r *RaftNode) advanceCommitLocked() {
	for n := r.lastIndex(); n > r.commitIx; n-- {
		if r.log[n].Term != r.term {
			continue // Raft: leader only commits entries from its own term directly
		}
		votes := 1 // self
		for _, p := range r.peers {
			if r.matchIndex[p] >= n {
				votes++
			}
		}
		if votes*2 > len(r.peers)+1 {
			r.commitIx = n
			if err := r.pers.SaveCommitIndex(n); err != nil {
				r.logf("[%s] persist commit failed: %v", r.id, err)
			}
			r.applyCommittedLocked()
			break
		}
	}
}

func (r *RaftNode) applyCommittedLocked() {
	for r.applied < r.commitIx {
		r.applied++
		entry := r.log[r.applied]
		resp := r.apply(Apply{Entry: entry})
		if ch, ok := r.proposals[entry.Index]; ok {
			delete(r.proposals, entry.Index)
			select {
			case ch <- proposalResult{resp: resp}:
			default:
			}
		}
	}
}

func (r *RaftNode) failProposalsLocked(err error) {
	for ix, ch := range r.proposals {
		delete(r.proposals, ix)
		select {
		case ch <- proposalResult{err: err}:
		default:
		}
	}
}

// Propose appends cmd to the leader's log and blocks until it is committed
// and applied.
func (r *RaftNode) Propose(ctx context.Context, cmd []byte) ([]byte, error) {
	r.mu.Lock()
	if r.role != Leader {
		r.mu.Unlock()
		return nil, ErrNotLeader
	}
	entry := LogEntry{Term: r.term, Index: r.lastIndex() + 1, Command: cmd}
	if err := r.pers.AppendLog([]LogEntry{entry}); err != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("persist log: %w", err)
	}
	r.log = append(r.log, entry)
	r.matchIndex[r.id] = entry.Index
	ch := make(chan proposalResult, 1)
	r.proposals[entry.Index] = ch
	r.broadcastAppendLocked()
	r.mu.Unlock()

	select {
	case res := <-ch:
		return res.resp, res.err
	case <-ctx.Done():
		r.mu.Lock()
		delete(r.proposals, entry.Index)
		r.mu.Unlock()
		return nil, ctx.Err()
	}
}

// ---------- RPC plumbing ----------

func (r *RaftNode) sendVote(peer string, args VoteArgs) *VoteReply {
	body, _ := json.Marshal(args)
	out := r.net.Send(cluster.Message{From: r.id, To: peer, Kind: kindRequestVote, Payload: body})
	if out == nil {
		return nil
	}
	var reply VoteReply
	if err := json.Unmarshal(out.Payload, &reply); err != nil {
		return nil
	}
	return &reply
}

func (r *RaftNode) sendAppend(peer string, args AppendArgs) *AppendReply {
	body, _ := json.Marshal(args)
	out := r.net.Send(cluster.Message{From: r.id, To: peer, Kind: kindAppendEntries, Payload: body})
	if out == nil {
		return nil
	}
	var reply AppendReply
	if err := json.Unmarshal(out.Payload, &reply); err != nil {
		return nil
	}
	return &reply
}

func (r *RaftNode) handle(msg cluster.Message) *cluster.Message {
	switch msg.Kind {
	case kindRequestVote:
		var args VoteArgs
		if json.Unmarshal(msg.Payload, &args) != nil {
			return nil
		}
		reply := r.handleVote(args)
		body, _ := json.Marshal(reply)
		return &cluster.Message{From: r.id, To: msg.From, Kind: msg.Kind, Payload: body}
	case kindAppendEntries:
		var args AppendArgs
		if json.Unmarshal(msg.Payload, &args) != nil {
			return nil
		}
		reply := r.handleAppend(args)
		body, _ := json.Marshal(reply)
		return &cluster.Message{From: r.id, To: msg.From, Kind: msg.Kind, Payload: body}
	}
	return nil
}

func (r *RaftNode) handleVote(args VoteArgs) VoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	if args.Term < r.term {
		return VoteReply{Term: r.term, VoteGranted: false}
	}
	if args.Term > r.term {
		r.becomeFollowerLocked(args.Term)
	}
	upToDate := args.LastLogTerm > r.lastTerm() ||
		(args.LastLogTerm == r.lastTerm() && args.LastLogIndex >= r.lastIndex())
	canGrant := (r.votedFor == "" || r.votedFor == args.CandidateID) && upToDate
	if canGrant {
		r.votedFor = args.CandidateID
		if err := r.pers.SaveState(r.term, args.CandidateID); err != nil {
			r.logf("[%s] persist vote failed: %v", r.id, err)
		}
		r.resetElectionLocked()
		r.logf("[%s] grant vote to %s term=%d (candidate log %d/%d up-to-date vs %d/%d)",
			r.id, args.CandidateID, args.Term,
			args.LastLogTerm, args.LastLogIndex, r.lastTerm(), r.lastIndex())
	}
	return VoteReply{Term: r.term, VoteGranted: canGrant}
}

func (r *RaftNode) handleAppend(args AppendArgs) AppendReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	if args.Term < r.term {
		return AppendReply{Term: r.term, Success: false, ConflictIndex: r.lastIndex() + 1}
	}
	if args.Term > r.term {
		r.becomeFollowerLocked(args.Term)
	}
	if r.role == Candidate {
		r.role = Follower
	}
	r.leaderID = args.LeaderID
	r.resetElectionLocked()

	reply := AppendReply{Term: r.term}
	if args.PrevLogIndex > r.lastIndex() {
		reply.Success = false
		reply.ConflictIndex = r.lastIndex() + 1
		return reply
	}
	if r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := r.log[args.PrevLogIndex].Term
		ix := args.PrevLogIndex
		for ix > 0 && r.log[ix-1].Term == conflictTerm {
			ix--
		}
		reply.Success = false
		reply.ConflictIndex = ix
		return reply
	}

	// Merge entries, deleting any conflict, persisting as we go.
	for i, e := range args.Entries {
		ix := args.PrevLogIndex + 1 + i
		if ix < len(r.log) {
			if r.log[ix].Term == e.Term && string(r.log[ix].Command) == string(e.Command) {
				continue
			}
			r.log = r.log[:ix]
			if err := r.pers.TruncateSuffix(ix); err != nil {
				r.logf("[%s] persist truncate failed: %v", r.id, err)
			}
		}
		r.log = append(r.log, e)
		if err := r.pers.AppendLog([]LogEntry{e}); err != nil {
			r.logf("[%s] persist append failed: %v", r.id, err)
		}
	}

	if args.LeaderCommit > r.commitIx {
		r.commitIx = min(args.LeaderCommit, r.lastIndex())
		if err := r.pers.SaveCommitIndex(r.commitIx); err != nil {
			r.logf("[%s] persist commit failed: %v", r.id, err)
		}
		r.applyCommittedLocked()
	}
	reply.Success = true
	return reply
}

// ---------- small accessors ----------

func (r *RaftNode) lastIndex() int { return r.log[len(r.log)-1].Index }
func (r *RaftNode) lastTerm() int  { return r.log[len(r.log)-1].Term }

// ID returns the node id.
func (r *RaftNode) ID() string { return r.id }

// Term returns the current term (snapshot, for logging/tests).
func (r *RaftNode) Term() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.term
}

// IsLeader reports whether the node currently holds leadership.
func (r *RaftNode) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role == Leader
}

// Role returns the current role.
func (r *RaftNode) Role() Role {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role
}

// AppliedIndex returns the index of the last applied log entry.
func (r *RaftNode) AppliedIndex() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.applied
}

// CommitIndex returns the current commit index.
func (r *RaftNode) CommitIndex() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitIx
}

// LastIndex returns the last index in the local log.
func (r *RaftNode) LastIndex() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastIndex()
}

// LeaderHint returns the id of the most recently observed leader.
func (r *RaftNode) LeaderHint() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}
