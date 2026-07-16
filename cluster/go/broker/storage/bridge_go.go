package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type CommitLog struct {
	mu           sync.RWMutex
	basePath     string
	segments     map[int64]*segment
	lastOffset   int64
	batchBuffer  []batchEntry
	batchMu      sync.Mutex
	batchSize    int
	batchTimeout time.Duration
	batchTicker  *time.Ticker
	flushCh      chan struct{}
}

type batchEntry struct {
	offset    int64
	timestamp int64
	data      []byte
}

type segment struct {
	file   *os.File
	offset int64
}

type SparseIndex struct {
	mu      sync.RWMutex
	entries map[int64]*IndexEntry
}

type IndexEntry struct {
	Offset    int64
	Size      int32
	Timestamp int64
}

func OpenCommitLog(path string, fileSize int64) (*CommitLog, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	files, err := filepath.Glob(filepath.Join(path, "segment_*.log"))
	if err != nil {
		return nil, err
	}

	var lastOffset int64
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			continue
		}
		segOffset := info.Size()
		if segOffset > lastOffset {
			lastOffset = segOffset
		}
	}

	cl := &CommitLog{
		basePath:     path,
		segments:     make(map[int64]*segment),
		lastOffset:   lastOffset,
		batchBuffer:  make([]batchEntry, 0, 100),
		batchSize:    100,
		batchTimeout: 10 * time.Millisecond,
		flushCh:      make(chan struct{}, 1),
	}

	go cl.batchFlushLoop()

	return cl, nil
}

func (cl *CommitLog) SetBatchConfig(size int, timeout time.Duration) {
	cl.batchMu.Lock()
	defer cl.batchMu.Unlock()
	cl.batchSize = size
	cl.batchTimeout = timeout
}

func (cl *CommitLog) batchFlushLoop() {
	ticker := time.NewTicker(cl.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cl.batchFlush()
		case <-cl.flushCh:
			cl.batchFlush()
		}
	}
}

func (cl *CommitLog) batchFlush() {
	cl.batchMu.Lock()
	if len(cl.batchBuffer) == 0 {
		cl.batchMu.Unlock()
		return
	}
	batch := make([]batchEntry, len(cl.batchBuffer))
	copy(batch, cl.batchBuffer)
	cl.batchBuffer = cl.batchBuffer[:0]
	cl.batchMu.Unlock()

	if len(batch) == 1 {
		cl.Append(batch[0].data)
		return
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()

	for _, entry := range batch {
		offset := cl.lastOffset
		timestamp := time.Now().UnixNano()

		header := make([]byte, 20)
		for i := 0; i < 8; i++ {
			header[i] = byte((offset >> (56 - i*8)) & 0xFF)
		}
		for i := 0; i < 8; i++ {
			header[8+i] = byte((timestamp >> (56 - i*8)) & 0xFF)
		}
		for i := 0; i < 4; i++ {
			header[16+i] = byte((len(entry.data) >> (24 - i*8)) & 0xFF)
		}

		fullData := append(header, entry.data...)

		segFile := filepath.Join(cl.basePath, fmt.Sprintf("segment_%d.log", offset/1024/1024))
		segKey := offset / 1024 / 1024

		seg, ok := cl.segments[segKey]
		if !ok {
			f, err := os.OpenFile(segFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				continue
			}
			seg = &segment{file: f, offset: segKey}
			cl.segments[segKey] = seg
		}

		if _, err := seg.file.Write(fullData); err != nil {
			continue
		}

		cl.lastOffset += int64(len(fullData))
	}
}

func (cl *CommitLog) Close() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	for _, seg := range cl.segments {
		if seg.file != nil {
			seg.file.Sync()
			seg.file.Close()
		}
	}
}

func (cl *CommitLog) Append(data []byte) (int64, error) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	offset := cl.lastOffset
	timestamp := time.Now().UnixNano()

	header := make([]byte, 20)
	for i := 0; i < 8; i++ {
		header[i] = byte((offset >> (56 - i*8)) & 0xFF)
	}
	for i := 0; i < 8; i++ {
		header[8+i] = byte((timestamp >> (56 - i*8)) & 0xFF)
	}
	for i := 0; i < 4; i++ {
		header[16+i] = byte((len(data) >> (24 - i*8)) & 0xFF)
	}

	fullData := append(header, data...)

	segFile := filepath.Join(cl.basePath, fmt.Sprintf("segment_%d.log", offset/1024/1024))
	segKey := offset / 1024 / 1024

	seg, ok := cl.segments[segKey]
	if !ok {
		f, err := os.OpenFile(segFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return 0, err
		}
		seg = &segment{file: f, offset: segKey}
		cl.segments[segKey] = seg
	}

	if _, err := seg.file.Write(fullData); err != nil {
		return 0, err
	}

	cl.lastOffset += int64(len(fullData))
	return offset, nil
}

func (cl *CommitLog) AppendAsync(data []byte) {
	cl.batchMu.Lock()
	cl.batchBuffer = append(cl.batchBuffer, batchEntry{data: data})
	shouldFlush := len(cl.batchBuffer) >= cl.batchSize
	cl.batchMu.Unlock()

	if shouldFlush {
		select {
		case cl.flushCh <- struct{}{}:
		default:
		}
	}
}

const headerSize = 20

func (cl *CommitLog) Read(offset int64, size int32) ([]byte, error) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	segFile := filepath.Join(cl.basePath, fmt.Sprintf("segment_%d.log", offset/1024/1024))
	f, err := os.Open(segFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	seekOffset := offset%int64(1024*1024) + headerSize
	_, err = f.Seek(seekOffset, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)
	_, err = f.Read(buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (cl *CommitLog) GetOffset() int64 {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.lastOffset
}

func (cl *CommitLog) Sync() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	for _, seg := range cl.segments {
		if err := seg.file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func CleanupOldSegments(segmentDir string, retentionMs int64, retentionBytes int64) (int, error) {
	files, err := filepath.Glob(filepath.Join(segmentDir, "segment_*.log"))
	if err != nil {
		return 0, err
	}

	count := 0
	now := time.Now().Unix()
	retentionSec := retentionMs / 1000

	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			continue
		}

		if retentionMs > 0 && (now-info.ModTime().Unix()) > retentionSec {
			os.Remove(file)
			count++
			continue
		}

		if retentionBytes > 0 && info.Size() > int64(retentionBytes) {
			os.Remove(file)
			count++
			continue
		}
	}

	return count, nil
}

func OpenSparseIndex(path string) (*SparseIndex, error) {
	return &SparseIndex{
		entries: make(map[int64]*IndexEntry),
	}, nil
}

func (idx *SparseIndex) Close() {
}

func (idx *SparseIndex) Put(offset int64, size int32, timestamp int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.entries[offset] = &IndexEntry{Offset: offset, Size: size, Timestamp: timestamp}
	return nil
}

func (idx *SparseIndex) Get(offset int64) (*IndexEntry, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if entry, ok := idx.entries[offset]; ok {
		return entry, nil
	}
	return nil, fmt.Errorf("not found")
}

func (idx *SparseIndex) Lookup(timestamp int64) (*IndexEntry, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var best *IndexEntry
	for _, entry := range idx.entries {
		if entry.Timestamp <= timestamp {
			if best == nil || entry.Timestamp > best.Timestamp {
				best = entry
			}
		}
	}
	if best != nil {
		return best, nil
	}
	return nil, fmt.Errorf("not found")
}

func (idx *SparseIndex) GetLastOffset() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var maxOffset int64
	for offset := range idx.entries {
		if offset > maxOffset {
			maxOffset = offset
		}
	}
	return maxOffset
}

func (idx *SparseIndex) GetEntriesAfter(offset int64) ([]*IndexEntry, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	result := make([]*IndexEntry, 0)
	for _, entry := range idx.entries {
		if entry.Offset > offset {
			result = append(result, entry)
		}
	}
	return result, nil
}

func (idx *SparseIndex) PutBatch(entries []*IndexEntry) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for _, entry := range entries {
		idx.entries[entry.Offset] = entry
	}
	return nil
}