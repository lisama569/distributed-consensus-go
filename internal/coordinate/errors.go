package coordinate

import "errors"

// 可区分的拒绝原因：调用方可以用 errors.Is 判断，而不会拿到静默失败。
var (
	// ErrNotLeader 当前节点不是领导者，或此刻无法提供线性一致服务。
	ErrNotLeader = errors.New("coordinate: not leader")
	// ErrTermStale 请求携带的任期早于当前节点已知任期。
	ErrTermStale = errors.New("coordinate: stale term")
	// ErrLeaseExpired 租约已过期（超过 TTL 未续约）。
	ErrLeaseExpired = errors.New("coordinate: lease expired")
	// ErrFenceTokenStale 栅栏令牌已过期：资源已被更新的令牌持有者接管。
	ErrFenceTokenStale = errors.New("coordinate: fence token is stale")
	// ErrHeldByOther 资源正被其他持有者以有效租约占用。
	ErrHeldByOther = errors.New("coordinate: resource held by another owner")
	// ErrWrongOwner 释放/续约操作的持有者与当前持有者不匹配。
	ErrWrongOwner = errors.New("coordinate: caller is not the current owner")
	// ErrEmptyResource 资源名为空。
	ErrEmptyResource = errors.New("coordinate: empty resource name")
	// ErrEmptyOwner 持有者标识为空。
	ErrEmptyOwner = errors.New("coordinate: empty owner id")
	// ErrEmptyRequestID 请求去重 ID 为空。
	ErrEmptyRequestID = errors.New("coordinate: empty request id")
	// ErrStopped 节点已停止。
	ErrStopped = errors.New("coordinate: node stopped")
	// ErrUnreachable 对端不可达（宕机或被分区）。
	ErrUnreachable = errors.New("coordinate: peer unreachable")
	// ErrTimeout RPC 超时。
	ErrTimeout = errors.New("coordinate: rpc timeout")
	// ErrHistoryBeyond 查询的历史位置超出该节点已应用的日志范围。
	ErrHistoryBeyond = errors.New("coordinate: history position beyond applied log")
	// ErrHistoryPosition 历史位置参数无效（既未给日志下标也未给时间点）。
	ErrHistoryPosition = errors.New("coordinate: invalid history position")
)
