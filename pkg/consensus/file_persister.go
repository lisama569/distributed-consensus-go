package consensus

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrNotFound indicates no durable state exists yet.
var ErrNotFound = errors.New("consensus: persisted state not found")

// stateFile is the single on-disk record. A simulation-grade Raft store
// keeps everything in one JSON document and rewrites it atomically; the
// rewrite is small in these tests and gives the restart tests exactly-one
// durable-copy semantics without a partial-record window.
type stateFile struct {
	CurrentTerm int        `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	CommitIndex int        `json:"commit_index"`
	Log         []LogEntry `json:"log"`
}

// FilePersister stores Raft hard state in a single directory.
type FilePersister struct {
	mu  sync.Mutex
	dir string
}

// NewFilePersister prepares (but does not yet write to) a directory.
func NewFilePersister(dir string) *FilePersister { return &FilePersister{dir: dir} }

func (p *FilePersister) path() string { return filepath.Join(p.dir, "state.json") }

func (p *FilePersister) readLocked() (stateFile, error) {
	var s stateFile
	b, err := os.ReadFile(p.path())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return s, ErrNotFound
		}
		return s, err
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("decode %s: %w", p.path(), err)
	}
	return s, nil
}

func (p *FilePersister) writeLocked(s stateFile) error {
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := p.path() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p.path())
}

// SaveState persists term and vote, preserving log and commit index.
func (p *FilePersister) SaveState(currentTerm int, votedFor string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, err := p.readLocked()
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	s.CurrentTerm = currentTerm
	s.VotedFor = votedFor
	return p.writeLocked(s)
}

// LoadState returns the persisted term and vote.
func (p *FilePersister) LoadState() (term int, votedFor string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, err := p.readLocked()
	if err != nil {
		return 0, "", err
	}
	return s.CurrentTerm, s.VotedFor, nil
}

// LoadCommitIndex returns the persisted commit index.
func (p *FilePersister) LoadCommitIndex() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, err := p.readLocked()
	if err != nil {
		return 0, err
	}
	return s.CommitIndex, nil
}

// SaveCommitIndex persists the commit index.
func (p *FilePersister) SaveCommitIndex(index int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, _ := p.readLocked()
	s.CommitIndex = index
	return p.writeLocked(s)
}

// AppendLog appends entries to the durable log.
func (p *FilePersister) AppendLog(entries []LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, _ := p.readLocked()
	s.Log = append(s.Log, entries...)
	return p.writeLocked(s)
}

// TruncateSuffix removes every entry with index >= fromIndex.
func (p *FilePersister) TruncateSuffix(fromIndex int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, _ := p.readLocked()
	kept := s.Log[:0]
	for _, e := range s.Log {
		if e.Index < fromIndex {
			kept = append(kept, e)
		}
	}
	s.Log = kept
	if s.CommitIndex > len(s.Log) {
		s.CommitIndex = len(s.Log)
	}
	return p.writeLocked(s)
}

// LoadLog returns the persisted log.
func (p *FilePersister) LoadLog() ([]LogEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, err := p.readLocked()
	if err != nil {
		return nil, err
	}
	return s.Log, nil
}
