package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/lisama569/distributed-consensus-go/pkg/cluster"
	"github.com/lisama569/distributed-consensus-go/pkg/consensus"
	"github.com/lisama569/distributed-consensus-go/pkg/coordinator"
)

// NodeHandle bundles everything the simulation owns for one node.
type NodeHandle struct {
	ID     string
	Raft   *consensus.RaftNode
	Mach   *coordinator.Machine
	Pers   consensus.Persister
	dir    string
	cancel context.CancelFunc
	done   chan struct{} // closed when Run() has fully returned
}

// Cluster is an in-process multi-node coordination cluster.
type Cluster struct {
	Net   *cluster.LocalNetwork
	nodes map[string]*NodeHandle
	ids   []string
	cfg   consensus.Config
	root  string
	logf  func(format string, args ...any)

	mu sync.Mutex
}

// Logger lets callers inject a sink for lifecycle/decision logs.
type Logger func(format string, args ...any)

// NewCluster creates node identities and storage, but starts nothing.
// Nodes must be started with Start/StartAll.
func NewCluster(ids []string, dataRoot string, cfg consensus.Config, logf Logger) *Cluster {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	c := &Cluster{
		Net:   cluster.NewLocalNetwork(),
		nodes: map[string]*NodeHandle{},
		ids:   append([]string(nil), ids...),
		cfg:   cfg,
		root:  dataRoot,
		logf:  logf,
	}
	for _, id := range ids {
		c.nodes[id] = &NodeHandle{
			ID:   id,
			dir:  filepath.Join(dataRoot, id),
			Mach: coordinator.NewMachine(),
		}
	}
	return c
}

// StartAll launches every node.
func (c *Cluster) StartAll() {
	for _, id := range c.ids {
		c.Start(id)
	}
}

// Start launches one node, building a fresh Raft instance around its
// durable directory and the (rebuilt) state machine. Restarted nodes
// replay their committed log from disk inside RaftNode.Run.
func (c *Cluster) Start(id string) {
	h, ok := c.nodes[id]
	if !ok {
		c.logf("cluster: unknown node %q", id)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = make(chan struct{})
	h.Mach = coordinator.NewMachine()
	h.Pers = consensus.NewFilePersister(h.dir)
	h.Raft = consensus.NewRaftNode(id, c.peerIDs(id), c.Net, h.Pers, c.cfg,
		func(a consensus.Apply) []byte {
			var cmd coordinator.Command
			if err := json.Unmarshal(a.Entry.Command, &cmd); err != nil {
				return []byte(`{"ok":false,"reason":"decode_error"}`)
			}
			resp := h.Mach.Apply(cmd)
			b, _ := json.Marshal(resp)
			return b
		}).WithLogger(func(format string, args ...any) {
		c.logf("%s", fmt.Sprintf(format, args...))
	})
	go func() {
		defer close(h.done)
		h.Raft.Run(ctx)
	}()
}

func (c *Cluster) peerIDs(id string) []string {
	out := make([]string, 0, len(c.ids))
	for _, x := range c.ids {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}

// Stop cancels the node's loop and disconnects it from the network. Its
// durable directory is untouched, so a later Start performs a real
// restart with term/log/commit index restored from disk.
func (c *Cluster) Stop(id string) {
	if h, ok := c.nodes[id]; ok && h.cancel != nil {
		h.cancel()
		<-h.done // wait until all RPC goroutines finished and writes stopped
		h.cancel = nil
		c.logf("[%s] stopped (durable state retained at %s)", id, h.dir)
	}
}

// StopAll stops every node.
func (c *Cluster) StopAll() {
	for _, id := range c.ids {
		c.Stop(id)
	}
}

// Restart performs Stop followed by Start with durable state reloaded.
func (c *Cluster) Restart(id string) {
	c.Stop(id)
	time.Sleep(20 * time.Millisecond)
	c.Start(id)
}

// Partition isolates members from all non-members.
func (c *Cluster) Partition(members []string) {
	set := map[string]bool{}
	for _, m := range members {
		set[m] = true
	}
	c.Net.Partition(set)
	c.logf("[cluster] partition installed: %s vs the rest", joinIDs(members))
}

func joinIDs(ids []string) string {
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += ","
		}
		out += id
	}
	if out == "" {
		return "(empty)"
	}
	return out
}

// Heal removes all network partitions.
func (c *Cluster) Heal() {
	c.Net.Heal()
	c.logf("[cluster] partition healed")
}

// WaitForLeader blocks until exactly one node reports leadership or the
// timeout elapses. Note: brief windows during elections may report zero
// leaders; two simultaneous leaders cannot exist (they would be in
// different terms and a stale leader cannot commit).
func (c *Cluster) WaitForLeader(timeout time.Duration) (string, int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaderID string
		var leaderTerm int
		count := 0
		for _, id := range c.ids {
			h := c.nodes[id]
			if h.Raft != nil && h.Raft.IsLeader() {
				count++
				leaderID = id
				leaderTerm = h.Raft.Term()
			}
		}
		if count == 1 {
			return leaderID, leaderTerm, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", 0, fmt.Errorf("sim: no stable leader within %s", timeout)
}

// Leader returns the current leader id ("" if none) and its term.
func (c *Cluster) Leader() (string, int) {
	for _, id := range c.ids {
		h := c.nodes[id]
		if h.Raft != nil && h.Raft.IsLeader() {
			return id, h.Raft.Term()
		}
	}
	return "", 0
}

// Submit finds the current leader, stamps the command with leader time,
// and proposes it through Raft. Callers set RequestID for idempotency.
// ErrNotLeader results (leadership changed mid-call) are retried on the
// new leader. When allow is non-empty, only leaders in allow are used,
// which lets a test deliberately talk to one partition side.
func (c *Cluster) Submit(ctx context.Context, cmd coordinator.Command, allow ...string) (coordinator.Response, string, error) {
	allowed := map[string]bool{}
	for _, id := range allow {
		allowed[id] = true
	}
	deadline := time.Now().Add(2 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	var lastErr error
	for time.Now().Before(deadline) {
		leaderID := c.leaderIn(allowed)
		if leaderID == "" {
			lastErr = fmt.Errorf("sim: no leader available in candidate set")
			time.Sleep(15 * time.Millisecond)
			continue
		}
		h := c.nodes[leaderID]
		cmd.NowMS = time.Now().UnixMilli()
		body, _ := json.Marshal(cmd)
		out, err := h.Raft.Propose(ctx, body)
		if err != nil {
			lastErr = err
			time.Sleep(15 * time.Millisecond)
			continue
		}
		var resp coordinator.Response
		if err := json.Unmarshal(out, &resp); err != nil {
			return coordinator.Response{}, leaderID, fmt.Errorf("decode response: %w", err)
		}
		return resp, leaderID, nil
	}
	return coordinator.Response{}, "", lastErr
}

func (c *Cluster) leaderIn(allowed map[string]bool) string {
	for _, id := range c.ids {
		if len(allowed) > 0 && !allowed[id] {
			continue
		}
		h := c.nodes[id]
		if h.Raft != nil && h.Raft.IsLeader() {
			return id
		}
	}
	return ""
}

// SnapshotLock returns a node's local view of a resource lock.
func (c *Cluster) SnapshotLock(node, resource string) (coordinator.Lock, bool) {
	h, ok := c.nodes[node]
	if !ok {
		return coordinator.Lock{}, false
	}
	return h.Mach.SnapshotLock(resource)
}

// IDs returns the configured node ids.
func (c *Cluster) IDs() []string { return append([]string(nil), c.ids...) }

// Node returns the handle for id.
func (c *Cluster) Node(id string) *NodeHandle { return c.nodes[id] }
