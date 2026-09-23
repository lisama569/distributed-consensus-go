package coordinator

// Command kinds.
const (
	CmdAcquire = "acquire"
	CmdRenew   = "renew"
	CmdRelease = "release"
	CmdCommit  = "commit" // validate fence token
)

// Command is a state-machine operation. RequestID provides idempotency:
// a duplicate submission replays the recorded response verbatim.
type Command struct {
	Kind      string `json:"kind"`
	RequestID string `json:"request_id"`
	Resource  string `json:"resource"`
	Holder    string `json:"holder"`
	Token     int64  `json:"token"`
	LeaseMS   int64  `json:"lease_ms"`
	NowMS     int64  `json:"now_ms"`
}

// Response is the deterministic result of applying a Command.
type Response struct {
	OK           bool   `json:"ok"`
	Token        int64  `json:"token,omitempty"`
	LeaseUntilMS int64  `json:"lease_until_ms,omitempty"`
	Reason       string `json:"reason,omitempty"`
	Replayed     bool   `json:"replayed,omitempty"`
}
