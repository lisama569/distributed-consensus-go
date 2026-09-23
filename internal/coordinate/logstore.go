package coordinate

import "sync"

// LogEntry 是一条 Raft 日志条目。
type LogEntry struct {
	Index int64 `json:"index"`
	Term  int64 `json:"term"`
	Op    Op    `json:"op"`
}

// LogStore 保存节点的 Raft 日志。
type LogStore interface {
	FirstIndex() int64
	LastIndex() int64
	TermAt(index int64) (int64, bool)
	Get(index int64) (LogEntry, bool)
	// EntriesFrom 返回 index >= from 的全部条目（不含哨兵）。
	EntriesFrom(from int64) []LogEntry
	// Append 在末尾追加条目（条目必须连续）。
	Append(entries []LogEntry)
	// TruncateFrom 删除所有 index >= from 的条目，用于解决日志冲突。
	TruncateFrom(from int64)
}

// memLogStore 是进程内日志存储，entries[0] 为 index=0,term=0 的哨兵。
type memLogStore struct {
	mu      sync.Mutex
	entries []LogEntry
}

func newMemLogStore() *memLogStore {
	return &memLogStore{entries: []LogEntry{{Index: 0, Term: 0}}}
}

func (s *memLogStore) FirstIndex() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[0].Index
}

func (s *memLogStore) LastIndex() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[len(s.entries)-1].Index
}

func (s *memLogStore) TermAt(index int64) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < 0 || index >= int64(len(s.entries)) {
		return 0, false
	}
	return s.entries[index].Term, true
}

func (s *memLogStore) Get(index int64) (LogEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < 0 || index >= int64(len(s.entries)) {
		return LogEntry{}, false
	}
	return s.entries[index], true
}

func (s *memLogStore) EntriesFrom(from int64) []LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if from < 0 {
		from = 0
	}
	if from >= int64(len(s.entries)) {
		return nil
	}
	out := make([]LogEntry, len(s.entries)-int(from))
	copy(out, s.entries[from:])
	return out
}

func (s *memLogStore) Append(entries []LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entries...)
}

func (s *memLogStore) TruncateFrom(from int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if from <= 0 {
		// 永远保留哨兵。
		from = 1
	}
	if from < int64(len(s.entries)) {
		s.entries = s.entries[:from]
	}
}

// PersistentState 是必须在重启后保留的 Raft 状态。
type PersistentState struct {
	CurrentTerm int64 `json:"current_term"`
	VotedFor    int   `json:"voted_for"` // -1 表示本任期未投票
}

// StateStore 持久化 currentTerm / votedFor。
type StateStore interface {
	Load() (PersistentState, error)
	Save(state PersistentState) error
}

// memStateStore 是非持久化实现（进程重启即丢）。
type memStateStore struct {
	mu    sync.Mutex
	state PersistentState
}

func newMemStateStore() *memStateStore {
	return &memStateStore{state: PersistentState{CurrentTerm: 0, VotedFor: -1}}
}

func (s *memStateStore) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, nil
}

func (s *memStateStore) Save(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	return nil
}
