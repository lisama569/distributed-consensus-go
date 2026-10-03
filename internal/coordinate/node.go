package coordinate

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// 默认计时参数：心跳 << 选举超时，避免不必要的抢主。
const (
	defaultHeartbeat   = 40 * time.Millisecond
	defaultElectionMin = 150 * time.Millisecond
	defaultElectionMax = 300 * time.Millisecond
	defaultRPCTimeout  = 120 * time.Millisecond
)

// NodeConfig 是单个 Raft 节点的配置。
type NodeConfig struct {
	ID          int
	Peers       []int // 含自身
	Transport   Transport
	Log         LogStore
	State       StateStore
	Clock       Clock
	ElectionMin time.Duration
	ElectionMax time.Duration
	Heartbeat   time.Duration
	RPCTimeout  time.Duration
	Logger      *log.Logger
	// Seed 非 0 时选举随机源使用固定种子（确定性测试可复现选举时序）。
	Seed int64
}

// event 是节点串行事件循环处理的所有输入。
type event struct {
	msg  *Message
	resp chan<- Message

	sub  *submitReq
	read *readReq
	hist *histReq
}

type submitReq struct {
	op Op
	rc chan submitOutcome
}

type submitOutcome struct {
	val any
	err error
}

type readReq struct {
	resource string
	rc       chan readOutcome
}

type readOutcome struct {
	state LockState
	found bool
	err   error
}

// histReq 是一条只读历史查询：在事件循环内对历史投影执行 fn 并回传结果。
// 历史是已提交日志的确定性投影，任意节点（含跟随者）都可安全服务。
type histReq struct {
	fn func(h *Historian) (any, error)
	rc chan histOutcome
}

type histOutcome struct {
	val any
	err error
}

// NodeStatus 是节点当前的协调角色快照。
type NodeStatus struct {
	ID     int
	Role   Role
	Term   int64
	Leader int
}

// Node 是一个最小 Raft 节点：选举 + 日志复制 + 线性一致提交。
// 所有可变状态只在 run 循环所在的 goroutine 中修改，
// RPC 线程通过 eventCh 投递请求，因此不需要给 Raft 状态加锁。
type Node struct {
	cfg NodeConfig

	// 持久状态（仅事件循环读写）
	term     int64
	votedFor int
	logs     LogStore
	state    StateStore

	// 易失状态
	role        Role
	leaderID    int
	commitIndex int64
	lastApplied int64
	nextIndex   map[int]int64
	matchIndex  map[int]int64
	votes       map[int]bool

	fsm FSM

	// 原子快照：仅供事件循环写、其他 goroutine 读，避免数据竞争。
	atomicTerm   atomic.Int64
	atomicRole   atomic.Int32
	atomicLeader atomic.Int32

	eventCh chan event
	stopCh  chan struct{}
	doneCh  chan struct{}
	started bool

	// 等待某个日志下标提交的提议。
	waiters map[int64][]chan submitOutcome

	rng *rand.Rand
	wg  sync.WaitGroup
}

// NewNode 按配置构造节点（不启动）。
func NewNode(cfg NodeConfig) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	}
	if cfg.Clock == nil {
		cfg.Clock = wallClock{}
	}
	if cfg.Log == nil {
		cfg.Log = newMemLogStore()
	}
	if cfg.State == nil {
		cfg.State = newMemStateStore()
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = defaultHeartbeat
	}
	if cfg.ElectionMin == 0 {
		cfg.ElectionMin = defaultElectionMin
	}
	if cfg.ElectionMax == 0 {
		cfg.ElectionMax = defaultElectionMax
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = defaultRPCTimeout
	}
	seed := int64(cfg.ID)*1000003 + time.Now().UnixNano()
	if cfg.Seed != 0 {
		seed = cfg.Seed + int64(cfg.ID)*1000003
	}
	return &Node{
		cfg:        cfg,
		votedFor:   -1,
		logs:       cfg.Log,
		state:      cfg.State,
		leaderID:   -1,
		nextIndex:  map[int]int64{},
		matchIndex: map[int]int64{},
		votes:      map[int]bool{},
		eventCh:    make(chan event, 1024),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		waiters:    map[int64][]chan submitOutcome{},
		rng:        rand.New(rand.NewSource(seed)),
	}, nil
}

// Start 从持久状态恢复并启动后台循环。
func (n *Node) Start(ctx context.Context) error {
	st, err := n.state.Load()
	if err != nil {
		return fmt.Errorf("load persistent state: %w", err)
	}
	n.term = st.CurrentTerm
	n.votedFor = st.VotedFor

	// 重放已有日志重建 FSM，使重启节点立刻具备正确的锁/令牌状态。
	n.fsm = NewFSM()
	for _, e := range n.logs.EntriesFrom(1) {
		now := n.cfg.Clock.Now()
		if _, perr := n.fsm.Apply(e.Op, now); perr != nil {
			n.cfg.Logger.Printf("[node %d] replay entry %d term %d: %v", n.cfg.ID, e.Index, e.Term, perr)
		}
		n.lastApplied = e.Index
	}
	n.commitIndex = n.lastApplied
	n.publishSnapshot()
	lastIndex := n.logs.LastIndex()

	n.started = true
	n.cfg.Logger.Printf("[node %d] started term=%d role=%s lastIndex=%d",
		n.cfg.ID, n.term, Follower, lastIndex)
	n.wg.Add(1)
	go n.run()
	return nil
}

// Stop 停止节点后台循环。
func (n *Node) Stop() error {
	if !n.started {
		return nil
	}
	select {
	case <-n.stopCh:
	default:
		close(n.stopCh)
	}
	n.wg.Wait()
	n.started = false
	return nil
}

// dispatch 供传输层调用：投递一条消息并同步等待响应。
func (n *Node) dispatch(ctx context.Context, msg Message) Message {
	resp := make(chan Message, 1)
	e := event{msg: &msg, resp: resp}
	select {
	case n.eventCh <- e:
	case <-n.stopCh:
		return Message{Term: n.atomicTerm.Load(), Type: responseType(msg.Type)}
	case <-ctx.Done():
		return Message{Term: n.atomicTerm.Load()}
	}
	select {
	case r := <-resp:
		return r
	case <-n.stopCh:
		return Message{Term: n.atomicTerm.Load(), Type: responseType(msg.Type)}
	case <-ctx.Done():
		return Message{Term: n.atomicTerm.Load()}
	}
}

// Submit 由领导者把一个锁操作复制到多数派并应用，返回结论。
func (n *Node) Submit(ctx context.Context, op Op) (any, error) {
	rc := make(chan submitOutcome, 1)
	select {
	case n.eventCh <- event{sub: &submitReq{op: op, rc: rc}}:
	case <-n.stopCh:
		return nil, ErrStopped
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case out := <-rc:
		return out.val, out.err
	case <-n.stopCh:
		return nil, ErrStopped
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ReadState 返回资源在已提交状态上的锁状态（仅领导者可服务）。
func (n *Node) ReadState(resource string) (LockState, bool, error) {
	rc := make(chan readOutcome, 1)
	select {
	case n.eventCh <- event{read: &readReq{resource: resource, rc: rc}}:
	case <-n.stopCh:
		return LockState{}, false, ErrStopped
	}
	out := <-rc
	return out.state, out.found, out.err
}

// queryHistory 在事件循环内执行只读历史查询（任意节点可服务）。
func (n *Node) queryHistory(fn func(h *Historian) (any, error)) (any, error) {
	rc := make(chan histOutcome, 1)
	select {
	case n.eventCh <- event{hist: &histReq{fn: fn, rc: rc}}:
	case <-n.stopCh:
		return nil, ErrStopped
	}
	out := <-rc
	return out.val, out.err
}

// OwnershipHistory 返回某资源从首次加锁至今的完整归属段（按开始位置升序）。
func (n *Node) OwnershipHistory(resource string) ([]OwnershipSpan, error) {
	val, err := n.queryHistory(func(h *Historian) (any, error) {
		return h.Spans(resource), nil
	})
	if err != nil {
		return nil, err
	}
	return val.([]OwnershipSpan), nil
}

// HistoryEvents 返回某资源的全部审计事件（每次提交的结论与原因）。
func (n *Node) HistoryEvents(resource string) ([]HistoryEvent, error) {
	val, err := n.queryHistory(func(h *Historian) (any, error) {
		return h.Events(resource), nil
	})
	if err != nil {
		return nil, err
	}
	return val.([]HistoryEvent), nil
}

// StateAt 重建某资源在历史位置 seq 的持有者/令牌/到期情况。
func (n *Node) StateAt(resource string, seq int64) (HistoricalState, error) {
	val, err := n.queryHistory(func(h *Historian) (any, error) {
		return h.StateAt(resource, seq)
	})
	if err != nil {
		return HistoricalState{}, err
	}
	return val.(HistoricalState), nil
}

// ValidateAt 判定携带 token 的提交放在历史位置 seq 是否通过。
func (n *Node) ValidateAt(resource string, seq int64, owner string, token int64) (ValidateResult, error) {
	val, err := n.queryHistory(func(h *Historian) (any, error) {
		return h.ValidateAt(resource, seq, owner, token)
	})
	if err != nil {
		return ValidateResult{}, err
	}
	return val.(ValidateResult), nil
}

// ExportHistory 导出该节点已投影的全部历史（RunID 由调用方填写）。
func (n *Node) ExportHistory() (HistoryExport, error) {
	val, err := n.queryHistory(func(h *Historian) (any, error) {
		return h.Export(), nil
	})
	if err != nil {
		return HistoryExport{}, err
	}
	return val.(HistoryExport), nil
}

// Term 返回当前已知最大任期（任期只会单调递增）。
func (n *Node) Term() int64 { return n.atomicTerm.Load() }

// Status 返回当前角色/任期/领导者快照（无锁、可被任意 goroutine 调用）。
func (n *Node) Status() NodeStatus {
	return NodeStatus{
		ID:     n.cfg.ID,
		Role:   Role(n.atomicRole.Load()),
		Term:   n.atomicTerm.Load(),
		Leader: int(n.atomicLeader.Load()) - 1,
	}
}

// Role 返回当前角色。
func (n *Node) Role() Role { return Role(n.atomicRole.Load()) }

// LeaderID 返回当前已知领导者，未知为 -1。
func (n *Node) LeaderID() int { return int(n.atomicLeader.Load()) - 1 }

// HandleMessage 是传输层回调的外部封装（内部走 dispatch）。
func (n *Node) HandleMessage(msg Message) Message {
	return n.dispatch(context.Background(), msg)
}

func responseType(t MsgType) MsgType {
	switch t {
	case MsgVote:
		return MsgVoteResp
	case MsgAppend:
		return MsgAppendResp
	default:
		return t
	}
}

// ---------------------------------------------------------------------------
// 事件循环
// ---------------------------------------------------------------------------

func (n *Node) run() {
	defer n.wg.Done()
	var (
		electionC  <-chan time.Time
		heartbeatC <-chan time.Time
	)
	resetElection := func() {
		electionC = n.cfg.Clock.After(n.randomElectionTimeout())
	}
	resetElection()

	lastRole := n.role
	for {
		// 仅在角色发生转换的那一拍重新武装定时器：
		// 否则每拍都丢弃旧定时器会把心跳无限推迟。
		if n.role != lastRole {
			if n.role == Leader {
				heartbeatC = n.cfg.Clock.After(0) // 上任即刻广播
			} else {
				heartbeatC = nil
				resetElection()
			}
			lastRole = n.role
		}
		select {
		case <-n.stopCh:
			close(n.doneCh)
			return
		case e := <-n.eventCh:
			n.handleEvent(e, resetElection)
		case <-electionC:
			if n.role != Leader {
				n.startElection()
			}
			resetElection()
		case <-heartbeatC:
			if n.role == Leader {
				n.broadcastAppend()
				heartbeatC = n.cfg.Clock.After(n.cfg.Heartbeat)
			}
		}
	}
}

func (n *Node) handleEvent(e event, resetElection func()) {
	switch {
	case e.msg != nil:
		reply := n.handleRPC(*e.msg, resetElection)
		if e.resp != nil {
			e.resp <- reply
		}
	case e.sub != nil:
		n.handleSubmit(e.sub)
	case e.read != nil:
		n.handleRead(e.read)
	case e.hist != nil:
		val, err := e.hist.fn(n.fsm.History())
		e.hist.rc <- histOutcome{val: val, err: err}
	}
}

func (n *Node) randomElectionTimeout() time.Duration {
	span := int64(n.cfg.ElectionMax - n.cfg.ElectionMin)
	if span <= 0 {
		return n.cfg.ElectionMin
	}
	return n.cfg.ElectionMin + time.Duration(n.rng.Int63n(span))
}

// publishSnapshot 把当前角色/任期/领导者写入原子字段，供无锁读取。
// 领导者 id 加 1 存储，以区分"未知(-1)"与"node 0"（atomic 零值）。
func (n *Node) publishSnapshot() {
	n.atomicTerm.Store(n.term)
	n.atomicRole.Store(int32(n.role))
	n.atomicLeader.Store(int32(n.leaderID) + 1)
}

// becomeFollower 在看到更大任期时回退角色并重置投票。
func (n *Node) becomeFollower(term int64, leader int) {
	first := term != n.term || n.role != Follower
	if term > n.term {
		n.term = term
		n.votedFor = -1
		n.persistState()
	}
	n.role = Follower
	n.leaderID = leader
	n.publishSnapshot()
	if first {
		n.cfg.Logger.Printf("[node %d] -> follower term=%d leader=%d", n.cfg.ID, term, leader)
	}
}

func (n *Node) persistState() {
	if err := n.state.Save(PersistentState{CurrentTerm: n.term, VotedFor: n.votedFor}); err != nil {
		n.cfg.Logger.Printf("[node %d] persist term: %v", n.cfg.ID, err)
	}
}

// ---------------------------------------------------------------------------
// 选举
// ---------------------------------------------------------------------------

func (n *Node) startElection() {
	n.term++
	n.role = Candidate
	n.votedFor = n.cfg.ID
	n.votes = map[int]bool{n.cfg.ID: true}
	n.persistState()
	n.publishSnapshot()
	n.cfg.Logger.Printf("[node %d] -> candidate term=%d (election timeout, seeking majority)",
		n.cfg.ID, n.term)

	// 单节点集群：自投即多数派，立即上任（没有投票响应会再触发检查）。
	if len(n.votes) > len(n.cfg.Peers)/2 {
		n.becomeLeader()
		return
	}

	req := Message{
		Type:         MsgVote,
		Term:         n.term,
		From:         n.cfg.ID,
		LastLogIndex: n.logs.LastIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}
	n.broadcast(req)
}

func (n *Node) lastLogTerm() int64 {
	t, _ := n.logs.TermAt(n.logs.LastIndex())
	return t
}

func (n *Node) broadcast(msg Message) {
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		go func(peer int) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
			defer cancel()
			reply, err := n.cfg.Transport.Send(ctx, peer, msg)
			if err != nil {
				return
			}
			n.eventCh <- event{msg: &reply}
		}(p)
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.cfg.ID
	last := n.logs.LastIndex()
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.cfg.Logger.Printf("[node %d] -> LEADER term=%d (majority votes=%d)",
		n.cfg.ID, n.term, len(n.votes))
	n.publishSnapshot()

	// 上任先提交一条 no-op：在它被多数派确认前，本任期不对外提供读，
	// 从而不会把旧领导者可能尚未提交的状态当成新结论。
	noop := LogEntry{Index: last + 1, Term: n.term, Op: Op{
		Kind: OpNoop, Term: n.term, Seq: last + 1, At: n.cfg.Clock.Now(),
	}}
	n.logs.Append([]LogEntry{noop})
	n.registerWaiter(noop.Index, nil)
	// 单节点集群没有复制响应可触发提交推进，本地追加后即尝试提交；
	// 多节点下此处多数派不成立，行为不变。
	n.advanceCommitIndex()
	n.broadcastAppend()
}

// ---------------------------------------------------------------------------
// RPC 处理
// ---------------------------------------------------------------------------

func (n *Node) handleRPC(msg Message, resetElection func()) Message {
	// 任何 RPC 携带更大任期都必须立刻认输，这是"分区恢复后不出现双主"的关键。
	if msg.Term > n.term {
		n.becomeFollower(msg.Term, -1)
	}

	switch msg.Type {
	case MsgVote:
		return n.handleVote(msg, resetElection)
	case MsgVoteResp:
		n.handleVoteResp(msg)
		return Message{}
	case MsgAppend:
		return n.handleAppend(msg, resetElection)
	case MsgAppendResp:
		n.handleAppendResp(msg)
		return Message{}
	}
	return Message{}
}

func (n *Node) handleVote(msg Message, resetElection func()) Message {
	reply := Message{Type: MsgVoteResp, Term: n.term, From: n.cfg.ID, To: msg.From}
	if msg.Term < n.term {
		reply.Granted = false
		return reply
	}
	canVote := n.votedFor == -1 || n.votedFor == msg.From
	logUpToDate := msg.LastLogTerm > n.lastLogTerm() ||
		(msg.LastLogTerm == n.lastLogTerm() && msg.LastLogIndex >= n.logs.LastIndex())
	if canVote && logUpToDate {
		n.votedFor = msg.From
		n.persistState()
		reply.Granted = true
		resetElection() // 已承诺投票，本任期选举计时重新开始。
		n.cfg.Logger.Printf("[node %d] granted vote term=%d to node %d", n.cfg.ID, n.term, msg.From)
	} else {
		n.cfg.Logger.Printf("[node %d] rejected vote term=%d from node %d (votedFor=%d upToDate=%v)",
			n.cfg.ID, n.term, msg.From, n.votedFor, logUpToDate)
	}
	return reply
}

func (n *Node) handleVoteResp(msg Message) {
	if n.role != Candidate || msg.Term != n.term {
		return
	}
	if msg.Granted {
		n.votes[msg.From] = true
		if len(n.votes) > len(n.cfg.Peers)/2 {
			n.becomeLeader()
		}
	}
}

func (n *Node) handleAppend(msg Message, resetElection func()) Message {
	reply := Message{Type: MsgAppendResp, Term: n.term, From: n.cfg.ID, To: msg.From}
	if msg.Term < n.term {
		reply.Success = false
		return reply
	}
	// 合法领导者的心跳/复制：跟随它，并重置选举定时器。
	n.becomeFollower(msg.Term, msg.From)
	resetElection()

	// 一致性检查：prevLogIndex 处的任期必须匹配。
	pt, ok := n.logs.TermAt(msg.PrevLogIndex)
	if !ok || pt != msg.PrevLogTerm {
		reply.Success = false
		return reply
	}

	// 合并领导者带来的条目（标准 Raft 5.3）：
	// 下标相同而任期不同 -> 删除该下标及之后的全部条目；
	// 下标相同且任期相同 -> 保留并跳过；下标不存在 -> 追加。
	i := int64(0)
	for ; i < int64(len(msg.Entries)); i++ {
		idx := msg.PrevLogIndex + 1 + i
		e := msg.Entries[i]
		e.Index = idx
		msg.Entries[i] = e
		existing, exists := n.logs.Get(idx)
		if !exists {
			break // 从这里开始都是新条目
		}
		if existing.Term != e.Term {
			n.logs.TruncateFrom(idx)
			break
		}
		// 完全相同，继续下一条。
	}
	if i < int64(len(msg.Entries)) {
		n.logs.Append(append([]LogEntry(nil), msg.Entries[i:]...))
	}

	reply.Success = true
	reply.MatchIndex = msg.PrevLogIndex + int64(len(msg.Entries))

	if msg.LeaderCommit > n.commitIndex {
		n.commitIndex = min64(msg.LeaderCommit, n.logs.LastIndex())
		n.applyCommitted()
	}
	return reply
}

func (n *Node) handleAppendResp(msg Message) {
	if n.role != Leader {
		return
	}
	if msg.Term > n.term {
		n.becomeFollower(msg.Term, -1)
		return
	}
	peer := msg.From
	if msg.Success {
		n.matchIndex[peer] = max64(n.matchIndex[peer], msg.MatchIndex)
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitIndex()
	} else {
		// 仅记录需要回退；真正的重发统一由心跳节拍驱动，
		// 避免"失败即立刻再发"形成 goroutine 风暴。
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

// advanceCommitIndex 推进领导者提交点：只有"本任期条目"被多数派复制后才提交，
// 防止通过间接提交旧任期日志选出与原领导者冲突的结论（Raft Figure 8）。
func (n *Node) advanceCommitIndex() {
	for cand := n.commitIndex + 1; cand <= n.logs.LastIndex(); cand++ {
		termAt, ok := n.logs.TermAt(cand)
		if !ok || termAt != n.term {
			continue
		}
		repl := 1 // 自己
		for _, p := range n.cfg.Peers {
			if p != n.cfg.ID && n.matchIndex[p] >= cand {
				repl++
			}
		}
		if repl > len(n.cfg.Peers)/2 {
			n.commitIndex = cand
		}
	}
	n.applyCommitted()
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e, ok := n.logs.Get(n.lastApplied)
		if !ok {
			continue
		}
		val, err := n.fsm.Apply(e.Op, n.cfg.Clock.Now())
		n.notifyWaiters(e.Index, val, err)
	}
}

// ---------------------------------------------------------------------------
// 复制
// ---------------------------------------------------------------------------

func (n *Node) broadcastAppend() {
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.sendAppend(p)
	}
}

func (n *Node) sendAppend(peer int) {
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	prev := next - 1
	prevTerm, _ := n.logs.TermAt(prev)
	entries := n.logs.EntriesFrom(next)
	msg := Message{
		Type:         MsgAppend,
		Term:         n.term,
		From:         n.cfg.ID,
		To:           peer,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      append([]LogEntry(nil), entries...),
		LeaderCommit: n.commitIndex,
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
		defer cancel()
		reply, err := n.cfg.Transport.Send(ctx, peer, msg)
		if err != nil {
			return
		}
		n.eventCh <- event{msg: &reply}
	}()
}

// ---------------------------------------------------------------------------
// 客户端操作
// ---------------------------------------------------------------------------

func (n *Node) registerWaiter(index int64, rc chan submitOutcome) {
	if rc != nil {
		n.waiters[index] = append(n.waiters[index], rc)
	}
}

func (n *Node) notifyWaiters(index int64, val any, err error) {
	for _, rc := range n.waiters[index] {
		rc <- submitOutcome{val: val, err: err}
	}
	delete(n.waiters, index)
}

func (n *Node) handleSubmit(req *submitReq) {
	if n.role != Leader {
		req.rc <- submitOutcome{err: fmt.Errorf("%w: leader is node %d (term %d)",
			ErrNotLeader, n.leaderID, n.term)}
		return
	}
	op := req.op
	op.Term = n.term
	idx := n.logs.LastIndex() + 1
	// 领导者在追加前盖章日志下标与时间：历史投影只依赖这两个字段，
	// 从而同一份日志在任意节点、任意次重放后得到完全一致的历史。
	op.Seq = idx
	op.At = n.cfg.Clock.Now()
	// 必须先登记 waiter 再追加/广播：否则快速返回的复制响应可能在
	// 登记之前就推进提交并 notifyWaiters，导致本次提交永久等待。
	n.registerWaiter(idx, req.rc)
	n.logs.Append([]LogEntry{{Index: idx, Term: n.term, Op: op}})
	// 单节点集群：本地追加即构成多数派，立即尝试推进提交。
	n.advanceCommitIndex()
	n.broadcastAppend()
}

func (n *Node) handleRead(req *readReq) {
	if n.role != Leader {
		req.rc <- readOutcome{err: fmt.Errorf("%w: leader is node %d", ErrNotLeader, n.leaderID)}
		return
	}
	st, found := n.fsm.Lookup(req.resource)
	req.rc <- readOutcome{state: st, found: found}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// wallClock 是默认真实时钟。
type wallClock struct{}

func (wallClock) Now() time.Time                         { return time.Now() }
func (wallClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (wallClock) Sleep(d time.Duration)                  { time.Sleep(d) }
