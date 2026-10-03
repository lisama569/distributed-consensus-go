package coordinate

import (
	"sort"
	"time"
)

// ---------------------------------------------------------------------------
// 归属历史与历史时点重建
//
// 本文件实现"可追溯"层：FSM 在应用每条已提交日志时，把改变锁状态的事实
// 追加到只增不改的 History 中。历史完全由日志派生（操作内容 + 领导者提议
// 时盖的时间戳 Op.Time），因此：
//   - 所有节点对同一日志前缀得到逐字节一致的历史；
//   - 重启后重放日志，历史被一模一样地重建；
//   - 把同一段日志在全新节点上重放，导出内容与原节点完全一致。
//
// 历史位置用日志下标（事件顺序）或时间点表示；
// 两者都随日志持久化，重启之后仍能定位、仍能比较先后。
// ---------------------------------------------------------------------------

// EventKind 是归属历史事件的类型。只有"成功改变锁状态"的操作才产生事件；
// 被拒绝的或幂等重放的操作不占用历史（但占用日志下标，见 History.times）。
type EventKind int

const (
	// EvAcquire 新一代开始：发放了新的栅栏令牌。
	EvAcquire EventKind = iota
	// EvReenter 同一持有者在有效租约内重入：延长租约，令牌不变。
	EvReenter
	// EvRenew 持有者续约：延长租约，令牌不变。
	EvRenew
	// EvRelease 持有者主动释放。
	EvRelease
)

func (k EventKind) String() string {
	switch k {
	case EvAcquire:
		return "acquire"
	case EvReenter:
		return "reenter"
	case EvRenew:
		return "renew"
	case EvRelease:
		return "release"
	default:
		return "unknown"
	}
}

// EndReason 是一段租约归属的结束原因。段一旦关闭，结束原因即冻结，不再改写。
type EndReason int

const (
	// EndNone 段仍开放（是当前最新的一代），结束原因尚未由日志确定。
	EndNone EndReason = iota
	// EndReleased 持有者主动释放。
	EndReleased
	// EndExpired 租约自然到期后，同一持有者重新获取（资源未易主）。
	EndExpired
	// EndTakenOver 租约失效后被新的持有者接管（资源易主）。
	EndTakenOver
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
		return "open"
	}
}

// HistoryEvent 是一条只增不改的归属历史事件。
// Index 是产生该事件的日志下标（全局事件顺序，单调递增、可比较）；
// Time 是该日志条目由领导者提议时盖的时间戳（UTC，不含单调时钟读数）。
type HistoryEvent struct {
	Index      int64     `json:"index"`
	Time       time.Time `json:"time"`
	Kind       EventKind `json:"kind"`
	Owner      string    `json:"owner"`
	Token      int64     `json:"token"`
	LeaseUntil time.Time `json:"lease_until"` // 本事件生效后的到期时间
}

// LeaseSegment 是一段连续的租约归属：同一持有者、同一令牌的一代。
// 除最后一段（可能仍开放）外，所有段都已关闭且内容冻结。
type LeaseSegment struct {
	Seq        int64     `json:"seq"` // 每资源从 1 开始严格递增
	Resource   string    `json:"resource"`
	Owner      string    `json:"owner"`
	Token      int64     `json:"token"`
	StartIndex int64     `json:"start_index"` // 新一代开始的日志下标
	StartTime  time.Time `json:"start_time"`
	LastIndex  int64     `json:"last_index"` // 最近一次续约/重入的日志下标
	LeaseUntil time.Time `json:"lease_until"`
	Renewals   int       `json:"renewals"` // 续约 + 重入次数
	Closed     bool      `json:"closed"`
	EndIndex   int64     `json:"end_index"` // 确认结束的日志下标（释放或新一代开始）
	EndTime    time.Time `json:"end_time"`  // 事实结束时刻（释放时间或旧租约到期时刻）
	EndReason  EndReason `json:"end_reason"`
}

// ResourceHistory 是单个资源的完整归属历史（深拷贝，可安全读取）。
type ResourceHistory struct {
	Resource string         `json:"resource"`
	Events   []HistoryEvent `json:"events"`   // 按 Index 严格递增
	Segments []LeaseSegment `json:"segments"` // 按 Seq 严格递增
}

// HistoryPosition 指定一个历史位置：优先按日志下标（事件顺序），
// 否则按时间点。两者都随日志持久化，重启后仍能定位与比较。
type HistoryPosition struct {
	Index int64     `json:"index,omitempty"` // >0 时按日志下标定位
	Time  time.Time `json:"time,omitempty"`  // Index 为 0 时按时间点定位
}

// LockStateAt 是某资源在历史位置重建出的锁状态。
type LockStateAt struct {
	Resource   string    `json:"resource"`
	Position   int64     `json:"position"` // 解析后的日志下标
	Time       time.Time `json:"time"`     // 判定到期所用的该位置时间
	Held       bool      `json:"held"`
	Owner      string    `json:"owner,omitempty"`
	Token      int64     `json:"token,omitempty"`
	LeaseUntil time.Time `json:"lease_until,omitempty"`
	Expired    bool      `json:"expired"` // 在该位置租约是否已过期
}

// ExportedHistory 是全量历史导出：内容确定，可序列化、可跨节点/跨重放比较。
// Source 是导出节点标识（运行区分信息），不参与内容一致性比较。
type ExportedHistory struct {
	Source    string                     `json:"source"`
	LastIndex int64                      `json:"last_index"`
	Times     []ExportedTime             `json:"times"` // 每条已应用日志的时间（按下标升序）
	Resources map[string]ResourceHistory `json:"resources"`
}

// ExportedTime 记录某日志下标对应的时间戳，供按时间点定位历史位置。
type ExportedTime struct {
	Index int64     `json:"index"`
	Time  time.Time `json:"time"`
}

// History 是由已提交日志派生的只增历史。所有写方法只被 FSM.Apply
// 在节点事件循环中调用（单线程），读方法返回深拷贝，可并发安全使用。
type History struct {
	times []time.Time // times[i] 为日志下标 i 的时间；times[0] 不用
	res   map[string]*resourceHistory
}

type resourceHistory struct {
	events   []HistoryEvent
	segments []LeaseSegment
}

// newHistory 创建空历史。
func newHistory() *History {
	return &History{
		times: []time.Time{{}}, // 0 号哨兵
		res:   map[string]*resourceHistory{},
	}
}

func (h *History) resource(name string) *resourceHistory {
	rh, ok := h.res[name]
	if !ok {
		rh = &resourceHistory{}
		h.res[name] = rh
	}
	return rh
}

// -- 写方法（仅 FSM.Apply 在节点事件循环中调用）--

// noteEntry 记录一条已应用日志的时间（无论是否改变锁状态），
// 使任意日志下标都能作为历史位置参与"那一刻是否到期"的判定。
func (h *History) noteEntry(index int64, t time.Time) {
	if index <= 0 {
		return
	}
	for int64(len(h.times)) <= index {
		// 正常每步只追加一格；防御性补齐空洞（下标本应连续）。
		h.times = append(h.times, time.Time{})
	}
	h.times[index] = t.UTC()
}

// recordAcquire 记录加锁：reentrant 为同持有者重入（延长开放段），
// 否则为新一代（关闭上一开放段并开启新段）。
func (h *History) recordAcquire(index int64, t time.Time, op Op, st LockState, reentrant bool) {
	rh := h.resource(op.Resource)
	kind := EvAcquire
	if reentrant {
		kind = EvReenter
	}
	rh.events = append(rh.events, HistoryEvent{
		Index: index, Time: t.UTC(), Kind: kind,
		Owner: op.Owner, Token: st.Token, LeaseUntil: st.LeaseUntil.UTC(),
	})

	if reentrant {
		seg := &rh.segments[len(rh.segments)-1]
		seg.LastIndex = index
		seg.LeaseUntil = st.LeaseUntil.UTC()
		seg.Renewals++
		return
	}

	// 新一代：上一段若仍开放，到此由日志确认结束，原因即刻冻结。
	if len(rh.segments) > 0 {
		prev := &rh.segments[len(rh.segments)-1]
		if !prev.Closed {
			prev.Closed = true
			prev.EndIndex = index
			// 旧租约到期时刻即事实结束点（新持有者只可能在旧租约
			// 失效后接手，这是状态机语义保证的）。
			prev.EndTime = prev.LeaseUntil
			if prev.Owner != op.Owner {
				prev.EndReason = EndTakenOver
			} else {
				prev.EndReason = EndExpired
			}
		}
	}
	rh.segments = append(rh.segments, LeaseSegment{
		Seq:        int64(len(rh.segments)) + 1,
		Resource:   op.Resource,
		Owner:      op.Owner,
		Token:      st.Token,
		StartIndex: index,
		StartTime:  t.UTC(),
		LastIndex:  index,
		LeaseUntil: st.LeaseUntil.UTC(),
	})
}

// recordRenew 记录续约成功：延长当前开放段。
func (h *History) recordRenew(index int64, t time.Time, op Op, st LockState) {
	rh := h.resource(op.Resource)
	rh.events = append(rh.events, HistoryEvent{
		Index: index, Time: t.UTC(), Kind: EvRenew,
		Owner: op.Owner, Token: st.Token, LeaseUntil: st.LeaseUntil.UTC(),
	})
	seg := &rh.segments[len(rh.segments)-1]
	seg.LastIndex = index
	seg.LeaseUntil = st.LeaseUntil.UTC()
	seg.Renewals++
}

// recordRelease 记录主动释放成功（锁确实存在并被删除）：关闭当前开放段。
func (h *History) recordRelease(index int64, t time.Time, op Op) {
	rh := h.resource(op.Resource)
	rh.events = append(rh.events, HistoryEvent{
		Index: index, Time: t.UTC(), Kind: EvRelease, Owner: op.Owner,
	})
	if len(rh.segments) == 0 {
		return // 防御：正常路径下释放前必有开放段
	}
	seg := &rh.segments[len(rh.segments)-1]
	if seg.Closed {
		return
	}
	seg.Closed = true
	seg.EndIndex = index
	seg.EndTime = t.UTC()
	seg.EndReason = EndReleased
}

// -- 读方法（返回深拷贝 / 重建结论）--

// LastIndex 返回已记录的最大日志下标。
func (h *History) LastIndex() int64 {
	return int64(len(h.times) - 1)
}

// Resource 返回某资源的完整归属历史（深拷贝）。
func (h *History) Resource(resource string) ResourceHistory {
	out := ResourceHistory{Resource: resource}
	rh, ok := h.res[resource]
	if !ok {
		return out
	}
	out.Events = append([]HistoryEvent(nil), rh.events...)
	out.Segments = append([]LeaseSegment(nil), rh.segments...)
	return out
}

// resolve 把历史位置解析为（日志下标, 该位置的判定时间）。
func (h *History) resolve(pos HistoryPosition) (int64, time.Time, error) {
	if pos.Index > 0 {
		if pos.Index >= int64(len(h.times)) {
			return 0, time.Time{}, ErrHistoryBeyond
		}
		return pos.Index, h.times[pos.Index], nil
	}
	if !pos.Time.IsZero() {
		t := pos.Time.UTC()
		// 最大的 i 使 times[i] <= t；times 随下标单调不减
		// （同一领导者提议时间单调；本机模拟中换主后时钟亦单调）。
		i := sort.Search(len(h.times), func(i int) bool {
			return h.times[i].After(t)
		}) - 1
		if i < 0 {
			i = 0
		}
		return int64(i), t, nil
	}
	return 0, time.Time{}, ErrHistoryPosition
}

// StateAt 重建某资源在历史位置的锁状态。
// 同一位置无论在哪个节点、重启前后查询，结果完全一致。
func (h *History) StateAt(resource string, pos HistoryPosition) (LockStateAt, error) {
	idx, t, err := h.resolve(pos)
	if err != nil {
		return LockStateAt{}, err
	}
	st := LockStateAt{Resource: resource, Position: idx, Time: t}
	rh, ok := h.res[resource]
	if !ok {
		return st, nil
	}
	// 最后一个 Index <= idx 的事件即该位置的归属事实。
	j := sort.Search(len(rh.events), func(i int) bool {
		return rh.events[i].Index > idx
	}) - 1
	if j < 0 {
		return st, nil
	}
	ev := rh.events[j]
	if ev.Kind == EvRelease {
		return st, nil
	}
	st.Held = true
	st.Owner = ev.Owner
	st.Token = ev.Token
	st.LeaseUntil = ev.LeaseUntil
	st.Expired = !ev.LeaseUntil.After(t)
	return st, nil
}

// ValidateAt 判定"携带令牌的提交放在历史位置"是否通过。
// 判定规则与状态机 applyValidate 逐条对应，结论与当时一致。
func (h *History) ValidateAt(resource, owner string, token int64, pos HistoryPosition) (ValidateResult, error) {
	st, err := h.StateAt(resource, pos)
	if err != nil {
		return ValidateResult{}, err
	}
	res := ValidateResult{Resource: resource, Token: token}
	switch {
	case !st.Held || st.Expired:
		res.Reason = "lease_expired"
		return res, ErrLeaseExpired
	case token != st.Token:
		res.Reason = "fence_token_stale"
		return res, ErrFenceTokenStale
	case st.Owner != owner:
		res.Reason = "wrong_owner"
		return res, ErrWrongOwner
	}
	res.Accepted = true
	res.Reason = "ok"
	return res, nil
}

// Export 导出全量历史（深拷贝）。source 为导出方标识（运行区分信息）。
func (h *History) Export(source string) ExportedHistory {
	out := ExportedHistory{
		Source:    source,
		LastIndex: h.LastIndex(),
		Times:     make([]ExportedTime, 0, len(h.times)-1),
		Resources: make(map[string]ResourceHistory, len(h.res)),
	}
	for i := 1; i < len(h.times); i++ {
		out.Times = append(out.Times, ExportedTime{Index: int64(i), Time: h.times[i]})
	}
	for name := range h.res {
		out.Resources[name] = h.Resource(name)
	}
	return out
}
