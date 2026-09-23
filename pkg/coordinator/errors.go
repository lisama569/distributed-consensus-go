package coordinator

// Machine-readable rejection reasons. These are part of the protocol
// contract: callers may branch on them.
const (
	ReasonNotLeader      = "not_leader"
	ReasonUnknownCmd     = "unknown_command"
	ReasonResourceBusy   = "resource_busy"
	ReasonNotHolder      = "not_holder"
	ReasonLeaseExpired   = "lease_expired"
	ReasonTokenStale     = "token_stale"
	ReasonTokenUnknown   = "token_unknown"
	ReasonEmptyRequestID = "empty_request_id"
)

// RejectError is returned by client-side helpers when the state machine
// explicitly rejected a request.
type RejectError struct {
	Reason string
}

func (e *RejectError) Error() string { return "coordinator: request rejected: " + e.Reason }
