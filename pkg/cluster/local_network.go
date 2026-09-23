package cluster

import "sync"

// LocalNetwork is an in-process simulated network.
//
// Delivery semantics: Send is synchronous and ordered between a given
// ordered pair of peers (the sender goroutine blocks until the receiver's
// handler returns), which is stronger than real networks but makes the
// simulated outcomes deterministic. Partitions are bidirectional: when A
// is cut from B, neither Send(A->B) nor Send(B->A) is delivered.
type LocalNetwork struct {
	mu sync.Mutex
	// members: id -> handler. A missing entry means the node is down
	// (process stopped / crashed) and all messages to it are dropped.
	members map[string]Handler
	// partition, when non-nil, lists the ids isolated together.
	partition map[string]bool
}

// NewLocalNetwork creates an empty simulated network.
func NewLocalNetwork() *LocalNetwork {
	return &LocalNetwork{members: map[string]Handler{}}
}

// Register installs (or reinstalls after a restart) a node's handler.
func (n *LocalNetwork) Register(id string, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.members[id] = h
}

// Send delivers msg synchronously when the destination is alive and not
// partitioned from the source. It returns the reply, or nil if the
// message was dropped (dead peer or active partition between the two).
func (n *LocalNetwork) Send(msg Message) *Message {
	n.mu.Lock()
	h, alive := n.members[msg.To]
	srcAlive := n.members[msg.From] != nil
	connected := alive && srcAlive && n.reachableLocked(msg.From, msg.To)
	n.mu.Unlock()
	if !connected {
		return nil
	}
	return h(msg)
}

// IsConnected reports whether the two live peers can currently exchange
// messages. A registered node is always connected to itself.
func (n *LocalNetwork) IsConnected(a, b string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.members[a] == nil || n.members[b] == nil {
		return false
	}
	return n.reachableLocked(a, b)
}

// reachableLocked decides reachability using the current partition.
// Caller must hold n.mu.
func (n *LocalNetwork) reachableLocked(a, b string) bool {
	if a == b {
		return true
	}
	if n.partition == nil {
		return true
	}
	return n.partition[a] == n.partition[b]
}

// Partition isolates the given members from every node not in the set
// (and vice versa). Calling Partition again replaces the previous split.
// Pass an empty/nil slice to remove the partition (see also Heal).
func (n *LocalNetwork) Partition(members map[string]bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partition = members
}

// Heal removes all partitions.
func (n *LocalNetwork) Heal() {
	n.Partition(nil)
}

// DropNode simulates a hard failure: the node's handler is removed so it
// can neither send nor receive anything until it re-registers.
func (n *LocalNetwork) DropNode(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.members, id)
}
