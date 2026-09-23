package coordinate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// fileData 是单个节点落盘的全部持久内容：
// currentTerm/votedFor 加上整条 Raft 日志。FSM 不单独持久化，
// 重启时重放日志即可确定性重建锁状态、令牌代数与去重表。
type fileData struct {
	CurrentTerm int64      `json:"current_term"`
	VotedFor    int        `json:"voted_for"`
	Entries     []LogEntry `json:"entries"` // 不含哨兵
}

// FileStore 用 JSON 文件 + 原子 rename 模拟节点磁盘。
// 同一个节点的 LogStore 与 StateStore 由它一并实现，保证一起更新。
type FileStore struct {
	mu   sync.Mutex
	path string
	data fileData
}

// OpenFileStore 打开（不存在则创建）节点 id 的持久文件。
func OpenFileStore(dir string, id int) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	fs := &FileStore{
		path: filepath.Join(dir, fmt.Sprintf("node-%d.json", id)),
		data: fileData{VotedFor: -1, Entries: []LogEntry{}},
	}
	raw, err := os.ReadFile(fs.path)
	if err != nil {
		if os.IsNotExist(err) {
			return fs, fs.flushLocked()
		}
		return nil, err
	}
	if err := json.Unmarshal(raw, &fs.data); err != nil {
		return nil, err
	}
	return fs, nil
}

func (f *FileStore) flushLocked() error {
	// 不做缩进美化：每次提交都全量重写，紧凑 JSON 显著降低写放大，
	// 避免领导者事件循环被磁盘 IO 长时间阻塞（仍保证原子 rename）。
	raw, err := json.Marshal(f.data)
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}

// -- StateStore --

func (f *FileStore) Load() (PersistentState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return PersistentState{CurrentTerm: f.data.CurrentTerm, VotedFor: f.data.VotedFor}, nil
}

func (f *FileStore) Save(s PersistentState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data.CurrentTerm = s.CurrentTerm
	f.data.VotedFor = s.VotedFor
	return f.flushLocked()
}

// -- LogStore --
// 注意：日志下标 = 切片下标，entries[0]（哨兵）不落盘。

func (f *FileStore) FirstIndex() int64 {
	return 0 // 哨兵
}

func (f *FileStore) LastIndex() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.data.Entries))
}

func (f *FileStore) TermAt(index int64) (int64, bool) {
	e, ok := f.Get(index)
	return e.Term, ok
}

// Get 支持下标 0 的哨兵（term=0），与 memLogStore 完全一致。
func (f *FileStore) Get(index int64) (LogEntry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index == 0 {
		return LogEntry{Index: 0, Term: 0}, true
	}
	if index < 0 || index > int64(len(f.data.Entries)) {
		return LogEntry{}, false
	}
	return f.data.Entries[index-1], true
}

func (f *FileStore) EntriesFrom(from int64) []LogEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	if from <= 0 {
		out := make([]LogEntry, len(f.data.Entries))
		copy(out, f.data.Entries)
		return out
	}
	if from > int64(len(f.data.Entries)) {
		return nil
	}
	out := make([]LogEntry, int64(len(f.data.Entries))-from+1)
	copy(out, f.data.Entries[from-1:])
	return out
}

func (f *FileStore) Append(entries []LogEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data.Entries = append(f.data.Entries, entries...)
	_ = f.flushLocked()
}

func (f *FileStore) TruncateFrom(from int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if from <= 1 {
		f.data.Entries = f.data.Entries[:0]
	} else if from <= int64(len(f.data.Entries)) {
		f.data.Entries = f.data.Entries[:from-1]
	}
	_ = f.flushLocked()
}
