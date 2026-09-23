package coordinate

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SimNetwork 是所有节点共享的内存网络，可注入分区与节点宕机。
type SimNetwork struct {
	mu      sync.Mutex
	nodes   map[int]*Node
	down    map[int]bool
	blocked func(from, to int) bool
	delay   time.Duration
	clock   Clock
}

// NewSimNetwork 创建空的模拟网络。
func NewSimNetwork(clock Clock) *SimNetwork {
	if clock == nil {
		clock = wallClock{}
	}
	return &SimNetwork{nodes: map[int]*Node{}, down: map[int]bool{}, clock: clock}
}

// Register 把节点挂到网络上。
func (n *SimNetwork) Register(node *Node) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[node.cfg.ID] = node
}

// Send 把消息投递到目标节点并同步返回响应。
// 以下情况返回 ErrUnreachable：目标宕机、任一端宕机、链路被分区策略阻断。
func (n *SimNetwork) Send(ctx context.Context, from, to int, msg Message) (Message, error) {
	n.mu.Lock()
	target, ok := n.nodes[to]
	fromDown := n.down[from]
	toDown := n.down[to]
	blocked := n.blocked
	delay := n.delay
	n.mu.Unlock()

	if !ok || fromDown || toDown || (blocked != nil && (blocked(from, to) || blocked(to, from))) {
		return Message{}, ErrUnreachable
	}

	if delay > 0 {
		select {
		case <-n.clock.After(delay):
		case <-ctx.Done():
			return Message{}, ErrTimeout
		}
	}

	// 节点 HandleMessage 把请求投递到其串行事件循环并取回响应，
	// 这里不持有任何网络锁，避免跨节点调用死锁。
	return target.dispatch(ctx, msg), nil
}

// SetDown 模拟节点进程停止（丢弃全部消息）。
func (n *SimNetwork) SetDown(id int, down bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = down
}

// IsDown 返回节点是否处于宕机状态。
func (n *SimNetwork) IsDown(id int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.down[id]
}

// Partition 安装分区策略：blocked 返回 true 的链路双向丢弃消息。
func (n *SimNetwork) Partition(blocked func(from, to int) bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = blocked
}

// Heal 清除分区策略。
func (n *SimNetwork) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = nil
}

// SetDelay 设置每条消息的固定投递延迟。
func (n *SimNetwork) SetDelay(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay = d
}

// Cluster 是一整套本机模拟集群：节点 + 网络 + 可重启。
type Cluster struct {
	size  int
	net   *SimNetwork
	nodes []*Node
	opts  clusterSettings

	// mem 模式下保存存储实例，使"重启"仍能取回同一份内存状态。
	stores map[int]*memStores
}

type memStores struct {
	logs  LogStore
	state StateStore
}

type clusterSettings struct {
	electionMin time.Duration
	electionMax time.Duration
	heartbeat   time.Duration
	rpcTimeout  time.Duration
	dataDir     string // 非空时使用磁盘持久化（演示真实重启）
	logger      *log.Logger
}

// ClusterOption 配置集群参数。
type ClusterOption func(*clusterSettings)

// WithElectionTimeouts 设置选举随机超时区间。
func WithElectionTimeouts(min, max time.Duration) ClusterOption {
	return func(s *clusterSettings) { s.electionMin, s.electionMax = min, max }
}

// WithHeartbeat 设置领导者心跳间隔。
func WithHeartbeat(d time.Duration) ClusterOption {
	return func(s *clusterSettings) { s.heartbeat = d }
}

// WithRPCTimeout 设置单次 RPC 超时。
func WithRPCTimeout(d time.Duration) ClusterOption {
	return func(s *clusterSettings) { s.rpcTimeout = d }
}

// WithDataDir 启用磁盘持久化，使节点 Restart 等价于真实进程重启。
func WithDataDir(dir string) ClusterOption {
	return func(s *clusterSettings) { s.dataDir = dir }
}

// WithLogger 设置统一日志输出。
func WithLogger(l *log.Logger) ClusterOption {
	return func(s *clusterSettings) { s.logger = l }
}

// NewCluster 创建 n 节点集群（不启动）。
func NewCluster(n int, opts ...ClusterOption) (*Cluster, error) {
	if n < 1 || n%2 == 0 {
		return nil, fmt.Errorf("cluster size must be odd and >= 1, got %d", n)
	}
	s := clusterSettings{
		electionMin: defaultElectionMin,
		electionMax: defaultElectionMax,
		heartbeat:   defaultHeartbeat,
		rpcTimeout:  defaultRPCTimeout,
		logger:      log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds),
	}
	for _, o := range opts {
		o(&s)
	}
	c := &Cluster{
		size:   n,
		net:    NewSimNetwork(wallClock{}),
		nodes:  make([]*Node, n),
		opts:   s,
		stores: map[int]*memStores{},
	}
	peers := make([]int, n)
	for i := 0; i < n; i++ {
		peers[i] = i
	}
	for i := 0; i < n; i++ {
		node, err := c.buildNode(i, peers)
		if err != nil {
			return nil, err
		}
		c.nodes[i] = node
		c.net.Register(node)
	}
	return c, nil
}

func (c *Cluster) buildNode(id int, peers []int) (*Node, error) {
	cfg := NodeConfig{
		ID:          id,
		Peers:       peers,
		Transport:   newSimTransport(id, c.net),
		Clock:       wallClock{},
		ElectionMin: c.opts.electionMin,
		ElectionMax: c.opts.electionMax,
		Heartbeat:   c.opts.heartbeat,
		RPCTimeout:  c.opts.rpcTimeout,
		Logger: log.New(c.opts.logger.Writer(),
			fmt.Sprintf("[node %d] ", id), log.LstdFlags|log.Lmicroseconds),
	}
	if c.opts.dataDir != "" {
		fs, err := OpenFileStore(filepath.Join(c.opts.dataDir, "cluster"), id)
		if err != nil {
			return nil, fmt.Errorf("open store for node %d: %w", id, err)
		}
		cfg.Log = fs
		cfg.State = fs
	} else if ms, ok := c.stores[id]; ok {
		cfg.Log, cfg.State = ms.logs, ms.state
	} else {
		logs, state := newMemLogStore(), newMemStateStore()
		cfg.Log, cfg.State = logs, state
		c.stores[id] = &memStores{logs: logs, state: state}
	}
	return NewNode(cfg)
}

// Start 启动全部节点并发起选举。
func (c *Cluster) Start(ctx context.Context) error {
	for _, node := range c.nodes {
		if err := node.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Stop 停止全部节点。
func (c *Cluster) Stop() error {
	for _, node := range c.nodes {
		if err := node.Stop(); err != nil {
			return err
		}
	}
	return nil
}

// Node 返回第 i 个节点。
func (c *Cluster) Node(i int) *Node {
	if i < 0 || i >= c.size {
		return nil
	}
	return c.nodes[i]
}

// Network 暴露模拟网络用于故障注入。
func (c *Cluster) Network() *SimNetwork { return c.net }

// Size 返回节点数。
func (c *Cluster) Size() int { return c.size }

// Restart 停止并以相同持久身份重新启动某节点：
// 磁盘模式下任期/投票/日志全部来自文件，等价于真实进程重启且不回退。
func (c *Cluster) Restart(ctx context.Context, id int) error {
	if id < 0 || id >= c.size {
		return fmt.Errorf("unknown node %d", id)
	}
	if err := c.nodes[id].Stop(); err != nil {
		return err
	}
	peers := make([]int, c.size)
	for i := range peers {
		peers[i] = i
	}
	node, err := c.buildNode(id, peers)
	if err != nil {
		return err
	}
	c.nodes[id] = node
	c.net.Register(node)
	c.net.SetDown(id, false)
	return node.Start(ctx)
}

// LeaderID 返回"可达且自认领导者"的节点 id。
// 被显式宕机的节点会被跳过；被分区的旧主可能仍自认 leader，
// 因此若存在多个候选，优先返回任期最大的一个（多数派一侧的有效领导者）。
func (c *Cluster) LeaderID() int {
	id := -1
	var maxTerm int64
	for _, node := range c.nodes {
		if c.net.IsDown(node.cfg.ID) {
			continue
		}
		if node.Role() == Leader && node.Term() >= maxTerm {
			id = node.cfg.ID
			maxTerm = node.Term()
		}
	}
	return id
}

// WaitForLeader 轮询等待选出唯一领导者。
func (c *Cluster) WaitForLeader(timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := 0
		id := -1
		for _, node := range c.nodes {
			if node.Role() == Leader {
				leaders++
				id = node.cfg.ID
			}
		}
		if leaders == 1 {
			return id, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return -1, fmt.Errorf("%w: no leader within %s", ErrTimeout, timeout)
}
