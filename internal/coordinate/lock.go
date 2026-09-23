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
