package consensus

// Role of a Raft node.
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

// LogEntry is one replicated state-machine command.
type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}

// VoteArgs is a RequestVote RPC request.
type VoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

// VoteReply is a RequestVote RPC response.
type VoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendArgs is an AppendEntries RPC request.
type AppendArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendReply is an AppendEntries RPC response.
type AppendReply struct {
	Term    int
	Success bool
	// ConflictHint lets the follower quickly tell the leader where the
	// log divergence starts (Raft dissertation §5.3 optimization).
	ConflictIndex int
}

// Apply is emitted when a committed entry is handed to the state machine.
type Apply struct {
	Entry LogEntry
}

// Persister durably stores Raft's hard state.
type Persister interface {
	SaveState(currentTerm int, votedFor string) error
	LoadState() (term int, votedFor string, err error)
	SaveCommitIndex(index int) error
	LoadCommitIndex() (int, error)
	AppendLog(entries []LogEntry) error
	TruncateSuffix(fromIndex int) error
	LoadLog() ([]LogEntry, error)
}
