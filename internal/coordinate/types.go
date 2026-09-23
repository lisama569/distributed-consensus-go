package coordinate

import "time"

// Role 是 Raft 节点在某一时刻的角色。
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "follower"
	}
}

// LockResult 是加锁/续约类操作复制到多数派后返回的结论。
type LockResult struct {
	Resource   string
	Owner      string
	Token      int64 // 栅栏令牌：同一资源严格单调递增
	LeaseUntil time.Time
	Term       int64 // 做出该结论的领导者任期
}

// FSM 是锁状态机接口：只通过已提交日志推进，绝不接受旁路写入。
type FSM interface {
	// Apply 按顺序应用一条已提交日志，返回操作结论。
	Apply(op Op, now time.Time) (any, error)
	// Snapshot 返回状态快照（用于新领导者/重启后的读状态）。
	Snapshot() FSMSnapshot
	// Lookup 返回某资源当前锁状态（不修改状态）。
	Lookup(resource string) (LockState, bool)
}

// LockState 是单个资源的锁状态。
type LockState struct {
	Resource   string
	Owner      string
	Token      int64
	LeaseUntil time.Time
	Term       int64
}

// FSMSnapshot 是状态机只读快照。
type FSMSnapshot struct {
	Locks map[string]LockState
	// 每个资源已见过的最大令牌，保证重启后令牌不回退。
	MaxToken map[string]int64
	// 去重表：requestID -> 已提交结论，保证重复提交幂等。
	Dedup map[string]DedupEntry
}

// DedupEntry 记录某个请求 ID 的首次提交结论。
type DedupEntry struct {
	Kind   OpKind
	Result any
	Code   RejectCode // 0 表示首次执行成功；否则为可区分的拒绝原因码
	Term   int64
}

// RejectCode 是拒绝原因的稳定编码，重放请求时原样返回。
type RejectCode int

const (
	RejectOK RejectCode = iota
	RejectHeldByOther
	RejectLeaseExpired
	RejectFenceTokenStale
	RejectWrongOwner
	RejectEmptyResource
	RejectEmptyOwner
	RejectEmptyRequestID
)

// Error 返回该拒绝码对应的哨兵错误，供 errors.Is 判定。
func (c RejectCode) Error() error {
	switch c {
	case RejectHeldByOther:
		return ErrHeldByOther
	case RejectLeaseExpired:
		return ErrLeaseExpired
	case RejectFenceTokenStale:
		return ErrFenceTokenStale
	case RejectWrongOwner:
		return ErrWrongOwner
	case RejectEmptyResource:
		return ErrEmptyResource
	case RejectEmptyOwner:
		return ErrEmptyOwner
	case RejectEmptyRequestID:
		return ErrEmptyRequestID
	default:
		return nil
	}
}

// CodeOf 把哨兵拒绝错误映射为稳定编码。
func CodeOf(err error) RejectCode {
	switch err {
	case ErrHeldByOther:
		return RejectHeldByOther
	case ErrLeaseExpired:
		return RejectLeaseExpired
	case ErrFenceTokenStale:
		return RejectFenceTokenStale
	case ErrWrongOwner:
		return RejectWrongOwner
	case ErrEmptyResource:
		return RejectEmptyResource
	case ErrEmptyOwner:
		return RejectEmptyOwner
	case ErrEmptyRequestID:
		return RejectEmptyRequestID
	default:
		return 0
	}
}

// OpKind 标识日志条目类型。
type OpKind int

const (
	OpAcquire OpKind = iota
	OpRenew
	OpRelease
	OpValidate // 携带栅栏令牌的受保护操作提交
	OpNoop     // 领导者上任时的空日志，用于确认提交位置（只读也靠它）
)

func (k OpKind) String() string {
	switch k {
	case OpAcquire:
		return "acquire"
	case OpRenew:
		return "renew"
	case OpRelease:
		return "release"
	case OpValidate:
		return "validate"
	default:
		return "unknown"
	}
}

// Op 是写入 Raft 日志的锁操作。
type Op struct {
	Kind      OpKind        `json:"kind"`
	RequestID string        `json:"request_id"` // 幂等键：相同请求 ID 必须得到相同结论
	Resource  string        `json:"resource"`
	Owner     string        `json:"owner"`
	Token     int64         `json:"token"` // renew/release/validate 时携带
	TTL       time.Duration `json:"ttl_nanos"`
	Term      int64         `json:"term"` // 提议该操作时的领导者任期
}

// ValidateResult 是受保护操作（栅栏校验）的结论。
type ValidateResult struct {
	Resource string
	Token    int64
	Accepted bool
	Reason   string // 拒绝时可区分的原因
}

// Clock 抽象时间源，便于测试中控制租约与选举计时。
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
}
