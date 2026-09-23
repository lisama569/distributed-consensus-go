package coordinate

import "context"

// Message 是节点间交换的 Raft RPC。
type Message struct {
	Type MsgType

	From int
	To   int
	Term int64

	// RequestVote
	LastLogIndex int64
	LastLogTerm  int64

	// AppendEntries
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64

	// 响应
	Granted bool
	Success bool
	// 匹配到的日志下标（AppendEntries 成功时回填，便于推进 commitIndex）。
	MatchIndex int64
}

// MsgType 区分请求与响应。
type MsgType int

const (
	MsgVote MsgType = iota
	MsgVoteResp
	MsgAppend
	MsgAppendResp
)

// Transport 是节点视角的网络抽象。
type Transport interface {
	// Send 向对端发送一条消息并同步取回响应；分区/超时时返回错误。
	Send(ctx context.Context, to int, msg Message) (Message, error)
}

// simTransport 是挂在 SimNetwork 上的本机传输端。
type simTransport struct {
	id  int
	net *SimNetwork
}

// newSimTransport 创建属于节点 id 的传输端。
func newSimTransport(id int, net *SimNetwork) *simTransport {
	return &simTransport{id: id, net: net}
}

func (t *simTransport) Send(ctx context.Context, to int, msg Message) (Message, error) {
	msg.From = t.id
	msg.To = to
	return t.net.Send(ctx, t.id, to, msg)
}
