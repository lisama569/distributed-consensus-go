package coordinate

import "time"

// lockFSM 是锁服务的确定性状态机，只由已提交日志驱动。
// 任何"锁是否有效""令牌是否最新"的结论都只在这里产生，
// 从而保证所有节点对同一日志前缀得到完全一致的结论。
type lockFSM struct {
	locks    map[string]LockState
	maxToken map[string]int64
	dedup    map[string]DedupEntry
}

// NewFSM 创建空的锁状态机。
func NewFSM() FSM {
	return &lockFSM{
		locks:    map[string]LockState{},
		maxToken: map[string]int64{},
		dedup:    map[string]DedupEntry{},
	}
}

// NewFSMFromSnapshot 从快照重建状态机（节点重放历史日志后使用）。
func NewFSMFromSnapshot(snap FSMSnapshot) FSM {
	f := &lockFSM{
		locks:    map[string]LockState{},
		maxToken: map[string]int64{},
		dedup:    map[string]DedupEntry{},
	}
	for k, v := range snap.Locks {
		f.locks[k] = v
	}
	for k, v := range snap.MaxToken {
		f.maxToken[k] = v
	}
	for k, v := range snap.Dedup {
		f.dedup[k] = v
	}
	return f
}

// Apply 按顺序应用一条已提交日志。
// 幂等语义：相同 RequestID 永远返回首次提交时的结论（成功或拒绝），
// 不会造成重复加锁、重复释放或令牌跳变。
func (f *lockFSM) Apply(op Op, now time.Time) (any, error) {
	if op.RequestID != "" {
		if prev, ok := f.dedup[op.RequestID]; ok {
			return prev.Result, prev.Code.Error()
		}
	}

	var (
		result any
		err    error
	)
	switch op.Kind {
	case OpNoop:
		// 领导者上任空日志：除占用日志下标外没有任何状态影响。
		if op.RequestID != "" {
			f.dedup[op.RequestID] = DedupEntry{Kind: OpNoop, Term: op.Term}
		}
		return struct{}{}, nil
	case OpAcquire:
		result, err = f.applyAcquire(op, now)
	case OpRenew:
		result, err = f.applyRenew(op, now)
	case OpRelease:
		result, err = f.applyRelease(op, now)
	case OpValidate:
		result, err = f.applyValidate(op, now)
	}

	if op.RequestID != "" {
		f.dedup[op.RequestID] = DedupEntry{
			Kind:   op.Kind,
			Result: result,
			Code:   CodeOf(err),
			Term:   op.Term,
		}
	}
	return result, err
}

// applyAcquire 的规则：
//   - 资源无锁或旧租约已过期：发放新的、严格递增的栅栏令牌；
//   - 同一 owner 在有效租约内重入：仅延长到期时间，令牌不变；
//   - 其他 owner 仍持有效租约：以 ErrHeldByOther 明确拒绝。
func (f *lockFSM) applyAcquire(op Op, now time.Time) (LockResult, error) {
	if op.Resource == "" {
		return LockResult{}, ErrEmptyResource
	}
	if op.Owner == "" {
		return LockResult{}, ErrEmptyOwner
	}
	ttl := op.TTL
	if ttl <= 0 {
		ttl = defaultTTL
	}

	cur, exists := f.locks[op.Resource]
	if exists && cur.LeaseUntil.After(now) && cur.Owner != op.Owner {
		return LockResult{}, ErrHeldByOther
	}

	if exists && cur.LeaseUntil.After(now) {
		// 同一持有者重入：延长租约，绝不发放新令牌。
		cur.LeaseUntil = now.Add(ttl)
		f.locks[op.Resource] = cur
		return lockResultOf(cur, now), nil
	}

	// 旧租约过期（或从未加锁）：新的一代，令牌严格 +1。
	// maxToken 从 0 起，因此全新资源天然得到令牌 1；
	// 释放后重新加锁也只增不减，令牌不回退。
	token := f.maxToken[op.Resource] + 1
	f.maxToken[op.Resource] = token
	state := LockState{
		Resource:   op.Resource,
		Owner:      op.Owner,
		Token:      token,
		LeaseUntil: now.Add(ttl),
		Term:       op.Term,
	}
	f.locks[op.Resource] = state
	return lockResultOf(state, now), nil
}

// applyRenew 的规则：
//   - owner 与当前持有者不符：ErrWrongOwner；
//   - 租约已过期：ErrLeaseExpired（原持有者已失去保护，必须重新抢锁拿新令牌）；
//   - 令牌落后于当前代：ErrFenceTokenStale（锁虽有效，但已被更新持有者接管）；
//   - 其余：延长到期时间，令牌保持不变。
func (f *lockFSM) applyRenew(op Op, now time.Time) (LockResult, error) {
	cur, ok := f.locks[op.Resource]
	if !ok {
		return LockResult{}, ErrWrongOwner
	}
	if cur.Owner != op.Owner {
		return LockResult{}, ErrWrongOwner
	}
	if !cur.LeaseUntil.After(now) {
		return LockResult{}, ErrLeaseExpired
	}
	if op.Token != 0 && op.Token < cur.Token {
		return LockResult{}, ErrFenceTokenStale
	}
	ttl := op.TTL
	if ttl <= 0 {
		ttl = defaultTTL
	}
	cur.LeaseUntil = now.Add(ttl)
	f.locks[op.Resource] = cur
	return lockResultOf(cur, now), nil
}

// applyRelease 的规则：
//   - 不存在视为成功（释放幂等）；
//   - owner 不符：ErrWrongOwner；
//   - 携带的令牌落后：ErrFenceTokenStale，拒绝释放别人的一代。
func (f *lockFSM) applyRelease(op Op, now time.Time) (struct{}, error) {
	cur, ok := f.locks[op.Resource]
	if !ok {
		return struct{}{}, nil
	}
	if cur.Owner != op.Owner {
		return struct{}{}, ErrWrongOwner
	}
	if op.Token != 0 && op.Token < cur.Token {
		return struct{}{}, ErrFenceTokenStale
	}
	delete(f.locks, op.Resource)
	// maxToken 保留：即使释放后重新加锁，令牌也只增不减。
	return struct{}{}, nil
}

// applyValidate 实现栅栏（fencing token）校验：
// 受保护操作携带令牌提交，只有"当前有效持有者且令牌 == 最新代"才接受。
func (f *lockFSM) applyValidate(op Op, now time.Time) (ValidateResult, error) {
	cur, ok := f.locks[op.Resource]
	res := ValidateResult{Resource: op.Resource, Token: op.Token}
	if !ok || !cur.LeaseUntil.After(now) {
		res.Accepted = false
		res.Reason = "lease_expired"
		return res, ErrLeaseExpired
	}
	// 令牌是第一道栅栏：落后或超前都说明提交方不属于当前代。
	if op.Token != cur.Token {
		res.Accepted = false
		res.Reason = "fence_token_stale"
		return res, ErrFenceTokenStale
	}
	// 令牌正确但 owner 不符：属于持错令牌/冒充。
	if cur.Owner != op.Owner {
		res.Accepted = false
		res.Reason = "wrong_owner"
		return res, ErrWrongOwner
	}
	res.Accepted = true
	res.Reason = "ok"
	return res, nil
}

func lockResultOf(s LockState, now time.Time) LockResult {
	_ = now
	return LockResult{
		Resource:   s.Resource,
		Owner:      s.Owner,
		Token:      s.Token,
		LeaseUntil: s.LeaseUntil,
		Term:       s.Term,
	}
}

// Snapshot 返回深拷贝快照，读操作绝不触碰可变状态。
func (f *lockFSM) Lookup(resource string) (LockState, bool) {
	s, ok := f.locks[resource]
	if !ok {
		return LockState{}, false
	}
	if !s.LeaseUntil.After(time.Now()) {
		return LockState{}, false
	}
	return s, true
}

func (f *lockFSM) Snapshot() FSMSnapshot {
	snap := FSMSnapshot{
		Locks:    make(map[string]LockState, len(f.locks)),
		MaxToken: make(map[string]int64, len(f.maxToken)),
		Dedup:    make(map[string]DedupEntry, len(f.dedup)),
	}
	for k, v := range f.locks {
		snap.Locks[k] = v
	}
	for k, v := range f.maxToken {
		snap.MaxToken[k] = v
	}
	for k, v := range f.dedup {
		snap.Dedup[k] = v
	}
	return snap
}

// defaultTTL 是未显式指定时使用的租约时长。
const defaultTTL = 150 * time.Millisecond

// serviceDefaultTTL 是 LockService 未指定 TTL 时的默认租约。
const serviceDefaultTTL = 200 * time.Millisecond
