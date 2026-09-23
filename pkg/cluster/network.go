package cluster

// Message is a single RPC-like message exchanged between simulated nodes.
type Message struct {
	From    string
	To      string
	Kind    string
	Payload []byte
}

// Handler processes one inbound message and optionally returns a reply.
type Handler func(msg Message) *Message

// Network simulates a fully-connected in-process network with partitions.
type Network interface {
	Register(id string, h Handler)
	Send(msg Message) *Message // blocking; returns nil when dropped
	IsConnected(a, b string) bool
	DropNode(id string) // remove handler until re-registration
}
