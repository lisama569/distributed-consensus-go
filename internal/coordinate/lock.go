package coordinate

import (
	"context"
	"errors"
	"time"
)

// LockService 在 Raft 集群之上提供租约锁与栅栏令牌语义。
// 所有锁操作都是一条被复制到多数派的日志，结论由状态机唯一产生。
type LockService struct {
	cluster    *Cluster
	ttl        time.Duration
	retryEvery time.Duration
	// attemptTimeout 是单次向某节点提交的上限。
	// 被分区的旧主仍自认 leader 但凑不齐多数派，提交会挂住，
	// 到点即放弃并改投其他候选，不必等整体 ctx 超时。
	attemptTimeout time.Duration
}

// NewLockService 基于集群创建锁服务。
func NewLockService(cluster *Cluster, defaultTTL time.Duration) *LockService {
	if defaultTTL <= 0 {
		defaultTTL = serviceDefaultTTL
	}
	return &LockService{
		cluster:        cluster,
		ttl:            defaultTTL,
		retryEvery:     10 * time.Millisecond,
		attemptTimeout: 250 * time.Millisecond,
	}
}

func (s *LockService) ttlOrDefault(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return s.ttl
	}
	return ttl
}

// leaderCandidates 返回按任期从高到低排序的、自认领导者的可达节点。
func (s *LockService) leaderCandidates() []*Node {
	var cands []*Node
	for i := 0; i < s.cluster.Size(); i++ {
		if s.cluster.Network().IsDown(i) {
			continue
		}
		node := s.cluster.Node(i)
		if node != nil && node.Role() == Leader {
			cands = append(cands, node)
		}
	}
	// 简单插入排序：任期大者优先（分区恢复后多数派新主任期更高）。
	for i := 1; i < len(cands); i++ {
		for j := i; j > 0 && cands[j].Term() > cands[j-1].Term(); j-- {
			cands[j], cands[j-1] = cands[j-1], cands[j]
		}
	}
	return cands
}

// submitToLeader 反复尝试把操作交给当前有效领导者。
// 只有状态机给出的业务拒绝（ErrHeldByOther / ErrLeaseExpired /
// ErrFenceTokenStale / ErrWrongOwner）会直接返回给调用方；
// ErrNotLeader 或"凑不齐多数派导致的单次超时"只会触发换节点重试。
func (s *LockService) submitToLeader(ctx context.Context, op Op) (any, error) {
	for {
		for _, node := range s.leaderCandidates() {
			attemptCtx, cancel := context.WithTimeout(ctx, s.attemptTimeout)
			val, err := node.Submit(attemptCtx, op)
			cancel()
			if err == nil {
				return val, nil
			}
			if isBusinessReject(err) {
				// 业务拒绝时一并返回状态机产出的负载（其中携带可解释 reason）。
				return val, err
			}
			// 非主 / 停止 / 单次超时：尝试下一个候选。
			if errors.Is(err, ErrNotLeader) || errors.Is(err, ErrStopped) ||
				errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			// 其他错误：若整体 ctx 已结束则返回，否则继续找。
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(s.retryEvery):
		}
	}
}

func isBusinessReject(err error) bool {
	return errors.Is(err, ErrHeldByOther) ||
		errors.Is(err, ErrLeaseExpired) ||
		errors.Is(err, ErrFenceTokenStale) ||
		errors.Is(err, ErrWrongOwner) ||
		errors.Is(err, ErrEmptyResource) ||
		errors.Is(err, ErrEmptyOwner)
}

// Acquire 获取资源锁；相同 requestID 重复提交返回同一结论，绝不重复发令牌。
// 同一 owner 在租约内重入只延长租约；其他 owner 持有效租约则得到 ErrHeldByOther。
func (s *LockService) Acquire(ctx context.Context, requestID, resource, owner string, ttl time.Duration) (LockResult, error) {
	if requestID == "" {
		return LockResult{}, ErrEmptyRequestID
	}
	if resource == "" {
		return LockResult{}, ErrEmptyResource
	}
	if owner == "" {
		return LockResult{}, ErrEmptyOwner
	}
	val, err := s.submitToLeader(ctx, Op{
		Kind:      OpAcquire,
		RequestID: requestID,
		Resource:  resource,
		Owner:     owner,
		TTL:       s.ttlOrDefault(ttl),
	})
	if err != nil {
		return LockResult{}, err
	}
	return val.(LockResult), nil
}

// Renew 在租约内续约，携带既有栅栏令牌；过期则得到 ErrLeaseExpired。
func (s *LockService) Renew(ctx context.Context, requestID, resource, owner string, token int64, ttl time.Duration) (LockResult, error) {
	if requestID == "" {
		return LockResult{}, ErrEmptyRequestID
	}
	val, err := s.submitToLeader(ctx, Op{
		Kind:      OpRenew,
		RequestID: requestID,
		Resource:  resource,
		Owner:     owner,
		Token:     token,
		TTL:       s.ttlOrDefault(ttl),
	})
	if err != nil {
		return LockResult{}, err
	}
	return val.(LockResult), nil
}

// Release 主动释放锁；重复提交同一 requestID 为幂等成功。
func (s *LockService) Release(ctx context.Context, requestID, resource, owner string, token int64) error {
	if requestID == "" {
		return ErrEmptyRequestID
	}
	_, err := s.submitToLeader(ctx, Op{
		Kind:      OpRelease,
		RequestID: requestID,
		Resource:  resource,
		Owner:     owner,
		Token:     token,
	})
	return err
}

// ValidateAndSubmit 携带栅栏令牌提交受保护操作。
// 租约过期 -> ErrLeaseExpired；令牌落后/超前 -> ErrFenceTokenStale；
// owner 不符 -> ErrWrongOwner；全部通过才返回 Accepted=true。
// 相同 requestID 重放得到首次的判定，不重复计数。
func (s *LockService) ValidateAndSubmit(ctx context.Context, requestID, resource, owner string, token int64) (ValidateResult, error) {
	if requestID == "" {
		return ValidateResult{}, ErrEmptyRequestID
	}
	val, err := s.submitToLeader(ctx, Op{
		Kind:      OpValidate,
		RequestID: requestID,
		Resource:  resource,
		Owner:     owner,
		Token:     token,
	})
	// 即便被拒绝，状态机也返回了带 reason 的 ValidateResult，透传给调用方。
	res, _ := val.(ValidateResult)
	if err != nil {
		return res, err
	}
	return res, nil
}

// Lookup 返回资源当前有效锁状态（读领导者已提交状态）。
func (s *LockService) Lookup(ctx context.Context, resource string) (LockState, error) {
	for {
		for _, node := range s.leaderCandidates() {
			st, found, err := node.ReadState(resource)
			if err == nil {
				if !found {
					return LockState{}, nil
				}
				return st, nil
			}
			if !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrStopped) {
				if ctx.Err() != nil {
					return LockState{}, ctx.Err()
				}
			}
		}
		select {
		case <-ctx.Done():
			return LockState{}, ctx.Err()
		case <-time.After(s.retryEvery):
		}
	}
}

// ---------------------------------------------------------------------------
// 归属历史查询
//
// 历史是已提交日志的确定性投影：同一历史位置在任意节点、任意时刻、
// 反复重启后查询，结果完全一致。因此历史查询可由任意存活节点服务
// （领导者候选优先，保证位置较新；位置超出某节点已投影范围时会换节点重试）。
// ---------------------------------------------------------------------------

// queryAnyNode 依次在候选节点上执行只读历史查询，直到某个节点成功。
func (s *LockService) queryAnyNode(ctx context.Context, q func(*Node) error) error {
	for {
		tried := map[int]bool{}
		// 领导者候选优先（历史最新），随后退到任意存活节点。
		for _, node := range append(s.leaderCandidates(), s.aliveNodes()...) {
			if node == nil || tried[node.cfg.ID] {
				continue
			}
			tried[node.cfg.ID] = true
			err := q(node)
			if err == nil {
				return nil
			}
			if errors.Is(err, ErrPositionUnknown) || errors.Is(err, ErrNotLeader) ||
				errors.Is(err, ErrStopped) {
				continue // 该节点还没投影到目标位置：换节点
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.retryEvery):
		}
	}
}

func (s *LockService) aliveNodes() []*Node {
	var out []*Node
	for i := 0; i < s.cluster.Size(); i++ {
		if s.cluster.Network().IsDown(i) {
			continue
		}
		if n := s.cluster.Node(i); n != nil {
			out = append(out, n)
		}
	}
	return out
}

// OwnershipHistory 返回资源从首次加锁至今的完整归属变化：
// 每段租约的持有者、令牌、开始位置、续约延长轨迹与结束原因。
// 历史为追加式：已关闭的段永不被改写或重排。
func (s *LockService) OwnershipHistory(ctx context.Context, resource string) ([]OwnershipSpan, error) {
	var spans []OwnershipSpan
	err := s.queryAnyNode(ctx, func(n *Node) error {
		v, err := n.OwnershipHistory(resource)
		if err != nil {
			return err
		}
		spans = v
		return nil
	})
	return spans, err
}

// HistoryEvents 返回资源的全部审计事件（每次提交的结论与原因）。
func (s *LockService) HistoryEvents(ctx context.Context, resource string) ([]HistoryEvent, error) {
	var events []HistoryEvent
	err := s.queryAnyNode(ctx, func(n *Node) error {
		v, err := n.HistoryEvents(resource)
		if err != nil {
			return err
		}
		events = v
		return nil
	})
	return events, err
}

// StateAt 重建资源在历史位置 seq（日志下标）的持有者、令牌与到期情况。
func (s *LockService) StateAt(ctx context.Context, resource string, seq int64) (HistoricalState, error) {
	var st HistoricalState
	err := s.queryAnyNode(ctx, func(n *Node) error {
		v, err := n.StateAt(resource, seq)
		if err != nil {
			return err
		}
		st = v
		return nil
	})
	return st, err
}

// ValidateAt 判定"携带 token 的提交放在历史位置 seq 是否通过"，
// 结论与当时状态机给出的判定一致。
func (s *LockService) ValidateAt(ctx context.Context, resource string, seq int64, owner string, token int64) (ValidateResult, error) {
	var res ValidateResult
	err := s.queryAnyNode(ctx, func(n *Node) error {
		v, err := n.ValidateAt(resource, seq, owner, token)
		if err != nil {
			return err
		}
		res = v
		return nil
	})
	return res, err
}

// ExportHistory 导出全量归属历史。runID 作为运行编号写入导出内容：
// 同一套操作场景独立跑两遍，除 RunID 外导出内容必须完全一致。
func (s *LockService) ExportHistory(ctx context.Context, runID string) (HistoryExport, error) {
	var exp HistoryExport
	err := s.queryAnyNode(ctx, func(n *Node) error {
		v, err := n.ExportHistory()
		if err != nil {
			return err
		}
		exp = v
		return nil
	})
	exp.RunID = runID
	return exp, err
}
