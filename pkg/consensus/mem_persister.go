package consensus

import "sync"

// MemPersister keeps hard state in memory. Data is lost when the node
// process stops; use FilePersister for restart-durability tests.
type MemPersister struct {
	mu          sync.Mutex
	term        int
	votedFor    string
	commitIndex int
	log         []LogEntry
	written     bool
}

// NewMemPersister returns an empty in-memory persister.
func NewMemPersister() *MemPersister { return &MemPersister{} }

func (p *MemPersister) SaveState(currentTerm int, votedFor string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.term, p.votedFor, p.written = currentTerm, votedFor, true
	return nil
}

func (p *MemPersister) LoadState() (int, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.written {
		return 0, "", ErrNotFound
	}
	return p.term, p.votedFor, nil
}

func (p *MemPersister) SaveCommitIndex(index int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.commitIndex = index
	return nil
}

func (p *MemPersister) LoadCommitIndex() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.written {
		return 0, ErrNotFound
	}
	return p.commitIndex, nil
}

func (p *MemPersister) AppendLog(entries []LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log = append(p.log, entries...)
	return nil
}

func (p *MemPersister) TruncateSuffix(fromIndex int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	kept := p.log[:0]
	for _, e := range p.log {
		if e.Index < fromIndex {
			kept = append(kept, e)
		}
	}
	p.log = kept
	if p.commitIndex > len(p.log) {
		p.commitIndex = len(p.log)
	}
	return nil
}

func (p *MemPersister) LoadLog() ([]LogEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := append([]LogEntry(nil), p.log...)
	return out, nil
}
