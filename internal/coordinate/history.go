package coordinate

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// 归属历史（audit / traceability）层
//
// 本文件实现"已提交日志的确定性投影"：历史只由日志内容（含领导者盖章的
// 时间 Op.At 与日志下标 Op.Seq）推导，因此
//   - 同一历史位置，在任意节点、任意时刻、反复重启后查询，结果完全一致；
//   - 历史一旦产生即为追加式，绝不改写或重排。
// 位置（position）一律使用日志下标 Seq：全序、可比较、重启后仍可定位。
// ---------------------------------------------------------------------------

// HistKind 标识一条审计事件对应的操作类型。
type HistKind int

const (
	HistAcquire HistKind = iota
	HistRenew
	HistRelease
	HistValidate
	HistReplay // 相同 RequestID 的重放：不改变命运，仅记录"又来过一次"
)

func (k HistKind) String() string {
	switch k {
	case HistAcquire:
		return "acquire"
	case HistRenew:
		return "renew"
	case HistRelease:
		return "release"
	case HistValidate:
		return "validate"
	case HistReplay:
		return "replay"
	default:
		return "unknown"
	}
}

// EndReason 标识一段租约归属如何结束。
type EndReason int

const (
	EndNone      EndReason = iota // 仍在进行中
	EndReleased                   // 持有者主动释放
	EndExpired                    // 租约到期（被后续操作观察到）
	EndTakenOver                  // 被新的持有者接管（旧租约已失效）
)

func (r EndReason) String() string {
	switch r {
	case EndReleased:
		return "released"
	case EndExpired:
		return "expired"
	case EndTakenOver:
		return "taken_over"
	default:
		return "ongoing"
	}
}

// Renewal 记录一次"租约被延长到何处"（续约或同持有者重入加锁）。
type Renewal struct {
	Seq        int64     `json:"seq"`         // 发生位置（日志下标）
	At         time.Time `json:"at"`          // 领导者盖章时间
	LeaseUntil time.Time `json:"lease_until"` // 延长后的到期时刻
}

// OwnershipSpan 是某资源一段连续归属：从加锁成功到释放/过期/被接管。
// 段一旦关闭（EndSeq != 0）就永远不再改变；进行中的段只会追加 Renewal。
type OwnershipSpan struct {
	Resource   string    `json:"resource"`
	Owner      string    `json:"owner"`
	Token      int64     `json:"token"`       // 本代的栅栏令牌
	StartSeq   int64     `json:"start_seq"`   // 归属开始位置
	StartedAt  time.Time `json:"started_at"`  // 开始时刻（领导者盖章）
	LeaseUntil time.Time `json:"lease_until"` // 初始到期时刻（续约见 Renewals）
	Renewals   []Renewal `json:"renewals,omitempty"`
	EndSeq     int64     `json:"end_seq,omitempty"` // 结束位置；0 表示进行中
	EndedAt    time.Time `json:"ended_at,omitempty"`
	EndReason  EndReason `json:"end_reason"`
}

// HistoryEvent 是一条审计事件：某次提交在某个位置得到了什么结论。
type HistoryEvent struct {
	Seq       int64     `json:"seq"`
	At        time.Time `json:"at"` // 领导者盖章时间（随日志复制，跨节点一致）
	Resource  string    `json:"resource"`
	Kind      HistKind  `json:"kind"`
	RequestID string    `json:"request_id,omitempty"`
	Owner     string    `json:"owner,omitempty"`
	Token     int64     `json:"token,omitempty"` // acquire 为发放令牌；其余为携带令牌
	Accepted  bool      `json:"accepted"`
	Reason    string    `json:"reason"` // ok / held_by_other / lease_expired / fence_token_stale / wrong_owner / ...
	Term      int64     `json:"term"`
}

// HistoricalState 是某资源在某个历史位置重建出的锁状态。
type HistoricalState struct {
	Found      bool      `json:"found"` // 该位置是否存在有效归属段
	Resource   string    `json:"resource"`
	Owner      string    `json:"owner,omitempty"`
	Token      int64     `json:"token,omitempty"`
	LeaseUntil time.Time `json:"lease_until,omitempty"`
	Expired    bool      `json:"expired"`   // 在该位置租约是否已过期
	SinceSeq   int64     `json:"since_seq"` // 该代归属开始位置
}

// HistoryExport 是一份可导出的全量历史（用于跨节点/跨运行比对）。
type HistoryExport struct {
	RunID  string          `json:"run_id"` // 运行编号：区分不同运行，其余内容必须一致
	Events []HistoryEvent  `json:"events"`
	Spans  []OwnershipSpan `json:"spans"` // 全部资源的归属段，按 (resource, start_seq) 排序
}

// projLock 是历史投影内部的锁状态（与在线 FSM 无关，只由日志推导）。
type projLock struct {
	owner      string
	token      int64
	leaseUntil time.Time
}

// Historian 是确定性历史投影器：按日志顺序逐条 Apply，产出归属段与审计事件。
// 只在节点事件循环 goroutine 内被读写，无需加锁。
type Historian struct {
	events  []HistoryEvent             // 全局有序（按 Seq 非降）
	byRes   map[string][]int           // resource -> events 下标（升序）
	spans   map[string][]OwnershipSpan // resource -> 有序归属段
	open    map[string]int             // resource -> 进行中段在 spans 中的下标；-1 表示无
	locks   map[string]projLock        // 投影锁状态
	maxTok  map[string]int64           // 投影最大令牌
	dedup   map[string]int             // requestID -> 首次事件下标
	lastSeq int64                      // 已见的最大日志下标
}

// NewHistorian 创建空的历史投影器。
func NewHistorian() *Historian {
	return &Historian{
		byRes:  map[string][]int{},
		spans:  map[string][]OwnershipSpan{},
		open:   map[string]int{},
		locks:  map[string]projLock{},
		maxTok: map[string]int64{},
		dedup:  map[string]int{},
	}
}

// Apply 把一条已提交日志投影进历史。op.Seq 为日志下标；
// op.At 为领导者盖章时间（为零时使用 fallbackNow，兼容未盖章的旧日志）。
//
// 投影规则与 lockFSM 的判定规则一一对应，但时间只取自日志（op.At），
// 因此它是"日志的纯函数"：同一份日志在任意节点、任意次重放后
// 投影出完全一致的历史。
func (h *Historian) Apply(op Op, fallbackNow time.Time) {
	if op.Kind == OpNoop {
		return // 空日志不产生归属历史
	}
	at := op.At
	if at.IsZero() {
		at = fallbackNow
	}
	if op.Seq > h.lastSeq {
		h.lastSeq = op.Seq
	}

	// 幂等重放：相同 RequestID 不改变命运，只补一条 replay 事件，
	// 结论与首次完全一致（与状态机去重表语义对齐）。
	if op.RequestID != "" {
		if firstIdx, ok := h.dedup[op.RequestID]; ok {
			first := h.events[firstIdx]
			h.record(HistoryEvent{
				Seq: op.Seq, At: at, Resource: op.Resource, Kind: HistReplay,
				RequestID: op.RequestID, Owner: op.Owner, Token: first.Token,
				Accepted: first.Accepted, Reason: first.Reason, Term: op.Term,
			})
			return
		}
	}

	ev := HistoryEvent{
		Seq: op.Seq, At: at, Resource: op.Resource,
		RequestID: op.RequestID, Owner: op.Owner, Term: op.Term,
	}
	switch op.Kind {
	case OpAcquire:
		ev.Kind = HistAcquire
		h.projAcquire(op, at, &ev)
	case OpRenew:
		ev.Kind = HistRenew
		ev.Token = op.Token
		h.projRenew(op, at, &ev)
	case OpRelease:
		ev.Kind = HistRelease
		ev.Token = op.Token
		h.projRelease(op, at, &ev)
	case OpValidate:
		ev.Kind = HistValidate
		ev.Token = op.Token
		h.projValidate(op, at, &ev)
	default:
		return
	}
	idx := h.record(ev)
	if op.RequestID != "" {
		h.dedup[op.RequestID] = idx
	}
}

// record 追加一条事件（追加式，绝不改写或重排），返回其下标。
func (h *Historian) record(ev HistoryEvent) int {
	h.events = append(h.events, ev)
	idx := len(h.events) - 1
	h.byRes[ev.Resource] = append(h.byRes[ev.Resource], idx)
	return idx
}

// closeSpan 关闭 resource 当前进行中的归属段（幂等：无进行中段时不做事）。
func (h *Historian) closeSpan(resource string, endSeq int64, endedAt time.Time, reason EndReason) {
	i, ok := h.open[resource]
	if !ok || i < 0 {
		return
	}
	sp := &h.spans[resource][i]
	sp.EndSeq = endSeq
	sp.EndedAt = endedAt
	sp.EndReason = reason
	h.open[resource] = -1
}

// openSpan 开启新一代归属段。若存在未关闭的旧段，先按规则关闭：
// 新持有者与旧持有者不同 -> 被接管（taken_over）；相同 -> 旧租约已到期（expired）。
func (h *Historian) openSpan(op Op, at time.Time, leaseUntil time.Time, token int64) {
	if i, ok := h.open[op.Resource]; ok && i >= 0 {
		reason := EndExpired
		if h.spans[op.Resource][i].Owner != op.Owner {
			reason = EndTakenOver
		}
		h.closeSpan(op.Resource, op.Seq, at, reason)
	}
	h.spans[op.Resource] = append(h.spans[op.Resource], OwnershipSpan{
		Resource:   op.Resource,
		Owner:      op.Owner,
		Token:      token,
		StartSeq:   op.Seq,
		StartedAt:  at,
		LeaseUntil: leaseUntil,
	})
	h.open[op.Resource] = len(h.spans[op.Resource]) - 1
}

// extendSpan 记录一次租约延长（续约或同持有者重入加锁）。
// 只追加 Renewal 记录，不改写段的初始 LeaseUntil——已产生的历史不可改写。
func (h *Historian) extendSpan(resource string, seq int64, at, leaseUntil time.Time) {
	i, ok := h.open[resource]
	if !ok || i < 0 {
		return
	}
	sp := &h.spans[resource][i]
	sp.Renewals = append(sp.Renewals, Renewal{Seq: seq, At: at, LeaseUntil: leaseUntil})
}

func ttlOrDefault(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return defaultTTL
	}
	return ttl
}

// projAcquire 投影加锁：规则同 lockFSM.applyAcquire，时间取自日志。
func (h *Historian) projAcquire(op Op, at time.Time, ev *HistoryEvent) {
	if op.Resource == "" {
		ev.Accepted, ev.Reason = false, "empty_resource"
		return
	}
	if op.Owner == "" {
		ev.Accepted, ev.Reason = false, "empty_owner"
		return
	}
	ttl := ttlOrDefault(op.TTL)
	cur, exists := h.locks[op.Resource]
	if exists && cur.leaseUntil.After(at) && cur.owner != op.Owner {
		ev.Accepted, ev.Reason = false, "held_by_other"
		return
	}
	if exists && cur.leaseUntil.After(at) {
		// 同一持有者重入：延长租约，令牌不变。
		cur.leaseUntil = at.Add(ttl)
		h.locks[op.Resource] = cur
		h.extendSpan(op.Resource, op.Seq, at, cur.leaseUntil)
		ev.Accepted, ev.Reason, ev.Token = true, "ok", cur.token
		return
	}
	// 新的一代：令牌严格 +1。
	token := h.maxTok[op.Resource] + 1
	h.maxTok[op.Resource] = token
	leaseUntil := at.Add(ttl)
	h.locks[op.Resource] = projLock{owner: op.Owner, token: token, leaseUntil: leaseUntil}
	h.openSpan(op, at, leaseUntil, token)
	ev.Accepted, ev.Reason, ev.Token = true, "ok", token
}

// projRenew 投影续约：规则同 lockFSM.applyRenew。
func (h *Historian) projRenew(op Op, at time.Time, ev *HistoryEvent) {
	cur, ok := h.locks[op.Resource]
	if !ok || cur.owner != op.Owner {
		ev.Accepted, ev.Reason = false, "wrong_owner"
		return
	}
	if !cur.leaseUntil.After(at) {
		// 系统在这一位置观察到租约到期：归属段以 expired 关闭。
		// 注意只关闭段、不删除投影锁条目——与在线 FSM 一致，
		// 后续过期续约/校验仍应判定为 lease_expired 而非 wrong_owner。
		h.closeSpan(op.Resource, op.Seq, at, EndExpired)
		ev.Accepted, ev.Reason = false, "lease_expired"
		return
	}
	if op.Token != 0 && op.Token < cur.token {
		ev.Accepted, ev.Reason = false, "fence_token_stale"
		return
	}
	cur.leaseUntil = at.Add(ttlOrDefault(op.TTL))
	h.locks[op.Resource] = cur
	h.extendSpan(op.Resource, op.Seq, at, cur.leaseUntil)
	ev.Accepted, ev.Reason, ev.Token = true, "ok", cur.token
}

// projRelease 投影释放：规则同 lockFSM.applyRelease。
func (h *Historian) projRelease(op Op, at time.Time, ev *HistoryEvent) {
	cur, ok := h.locks[op.Resource]
	if !ok {
		// 释放幂等：锁不存在视为成功（无归属段可关闭）。
		ev.Accepted, ev.Reason = true, "ok"
		return
	}
	if cur.owner != op.Owner {
		ev.Accepted, ev.Reason = false, "wrong_owner"
		return
	}
	if op.Token != 0 && op.Token < cur.token {
		ev.Accepted, ev.Reason = false, "fence_token_stale"
		return
	}
	delete(h.locks, op.Resource)
	h.closeSpan(op.Resource, op.Seq, at, EndReleased)
	ev.Accepted, ev.Reason, ev.Token = true, "ok", cur.token
}

// projValidate 投影栅栏校验：规则同 lockFSM.applyValidate。
func (h *Historian) projValidate(op Op, at time.Time, ev *HistoryEvent) {
	cur, ok := h.locks[op.Resource]
	if !ok || !cur.leaseUntil.After(at) {
		if ok {
			// 在这一位置观察到租约到期：归属段以 expired 关闭（条目保留）。
			h.closeSpan(op.Resource, op.Seq, at, EndExpired)
		}
		ev.Accepted, ev.Reason = false, "lease_expired"
		return
	}
	if op.Token != cur.token {
		ev.Accepted, ev.Reason = false, "fence_token_stale"
		return
	}
	if cur.owner != op.Owner {
		ev.Accepted, ev.Reason = false, "wrong_owner"
		return
	}
	ev.Accepted, ev.Reason = true, "ok"
}

// LastSeq 返回投影已覆盖的最大日志下标。
func (h *Historian) LastSeq() int64 { return h.lastSeq }

// Events 返回某资源的全部审计事件（按 Seq 升序，返回拷贝）。
func (h *Historian) Events(resource string) []HistoryEvent {
	idxs := h.byRes[resource]
	out := make([]HistoryEvent, len(idxs))
	for i, idx := range idxs {
		out[i] = h.events[idx]
	}
	return out
}

// Spans 返回某资源的全部归属段（按 StartSeq 升序，返回深拷贝）。
func (h *Historian) Spans(resource string) []OwnershipSpan {
	src := h.spans[resource]
	out := make([]OwnershipSpan, len(src))
	for i, sp := range src {
		out[i] = copySpan(sp)
	}
	return out
}

func copySpan(sp OwnershipSpan) OwnershipSpan {
	if sp.Renewals != nil {
		rs := make([]Renewal, len(sp.Renewals))
		copy(rs, sp.Renewals)
		sp.Renewals = rs
	}
	return sp
}

// timeAt 返回位置 seq 对应的（领导者盖章）时间：
// 取不超过 seq 的最近一条事件的时间；之前没有任何事件时返回零时间。
func (h *Historian) timeAt(seq int64) time.Time {
	i := sort.Search(len(h.events), func(i int) bool { return h.events[i].Seq > seq }) - 1
	if i < 0 {
		return time.Time{}
	}
	return h.events[i].At
}

// StateAt 重建 resource 在位置 seq 的锁状态。
// seq 超出已投影范围时返回 ErrPositionUnknown。
func (h *Historian) StateAt(resource string, seq int64) (HistoricalState, error) {
	if seq > h.lastSeq {
		return HistoricalState{}, fmt.Errorf("%w: seq %d > last known %d",
			ErrPositionUnknown, seq, h.lastSeq)
	}
	st := HistoricalState{Resource: resource}
	spans := h.spans[resource]
	// 最后一个 StartSeq <= seq 的段。
	i := sort.Search(len(spans), func(i int) bool { return spans[i].StartSeq > seq }) - 1
	if i < 0 {
		return st, nil // 该位置之前从未加锁
	}
	sp := spans[i]
	if sp.EndSeq != 0 && sp.EndSeq <= seq {
		return st, nil // 段已结束（释放/过期/被接管），该位置无持有者
	}
	// 该位置的租约到期时刻：不超过 seq 的最后一次续约，否则初始值。
	leaseUntil := sp.LeaseUntil
	rs := sp.Renewals
	j := sort.Search(len(rs), func(j int) bool { return rs[j].Seq > seq }) - 1
	if j >= 0 {
		leaseUntil = rs[j].LeaseUntil
	}
	st.Found = true
	st.Owner = sp.Owner
	st.Token = sp.Token
	st.LeaseUntil = leaseUntil
	st.SinceSeq = sp.StartSeq
	st.Expired = !leaseUntil.After(h.timeAt(seq))
	return st, nil
}

// ValidateAt 判定"携带 token 的提交放在位置 seq 是否通过"，
// 结论与当时状态机给出的判定一致（同一套栅栏规则、同一份投影时间）。
func (h *Historian) ValidateAt(resource string, seq int64, owner string, token int64) (ValidateResult, error) {
	st, err := h.StateAt(resource, seq)
	if err != nil {
		return ValidateResult{}, err
	}
	res := ValidateResult{Resource: resource, Token: token}
	switch {
	case !st.Found || st.Expired:
		res.Accepted, res.Reason = false, "lease_expired"
		return res, ErrLeaseExpired
	case token != st.Token:
		res.Accepted, res.Reason = false, "fence_token_stale"
		return res, ErrFenceTokenStale
	case owner != st.Owner:
		res.Accepted, res.Reason = false, "wrong_owner"
		return res, ErrWrongOwner
	default:
		res.Accepted, res.Reason = true, "ok"
		return res, nil
	}
}

// Export 导出全量历史（深拷贝），RunID 由调用方填写。
func (h *Historian) Export() HistoryExport {
	exp := HistoryExport{
		Events: make([]HistoryEvent, len(h.events)),
	}
	copy(exp.Events, h.events)
	resources := make([]string, 0, len(h.spans))
	for r := range h.spans {
		resources = append(resources, r)
	}
	sort.Strings(resources)
	for _, r := range resources {
		for _, sp := range h.spans[r] {
			exp.Spans = append(exp.Spans, copySpan(sp))
		}
	}
	return exp
}

// Canonical 返回导出的确定性文本形式：内容相同则字符串相同，
// 与节点、查询时机、重启次数无关；用于跨节点/跨运行一致性断言。
// 时间一律用 UnixNano 表示，避免时区/单调时钟造成的表示差异。
func (e HistoryExport) Canonical() string {
	var b strings.Builder
	fmt.Fprintf(&b, "run %s\n", e.RunID)
	for _, ev := range e.Events {
		fmt.Fprintf(&b, "event seq=%d at=%d res=%s kind=%s req=%s owner=%s token=%d accepted=%v reason=%s term=%d\n",
			ev.Seq, ev.At.UnixNano(), ev.Resource, ev.Kind, ev.RequestID, ev.Owner,
			ev.Token, ev.Accepted, ev.Reason, ev.Term)
	}
	for _, sp := range e.Spans {
		fmt.Fprintf(&b, "span res=%s owner=%s token=%d startSeq=%d startedAt=%d leaseUntil=%d endSeq=%d endedAt=%d end=%s\n",
			sp.Resource, sp.Owner, sp.Token, sp.StartSeq, sp.StartedAt.UnixNano(),
			sp.LeaseUntil.UnixNano(), sp.EndSeq, sp.EndedAt.UnixNano(), sp.EndReason)
		for _, r := range sp.Renewals {
			fmt.Fprintf(&b, "renewal res=%s spanStart=%d seq=%d at=%d leaseUntil=%d\n",
				sp.Resource, sp.StartSeq, r.Seq, r.At.UnixNano(), r.LeaseUntil.UnixNano())
		}
	}
	return b.String()
}
