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
	// ErrPositionUnknown 查询的历史位置超出该节点已投影的日志范围
	// （位置太新，节点尚未复制到）；换个节点或稍后重试即可。
	ErrPositionUnknown = errors.New("coordinate: history position not yet known")
	// ErrUnreachable 对端不可达（宕机或被分区）。
	ErrUnreachable = errors.New("coordinate: peer unreachable")
	// ErrTimeout RPC 超时。
	ErrTimeout = errors.New("coordinate: rpc timeout")
)
