package index

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/iamhimansu/csvquery/pkg/csvquery/storage"
	"github.com/iamhimansu/csvquery/pkg/csvquery/types"
	"github.com/pierrec/lz4/v4"
)

var (
	bufWriterPool = sync.Pool{
		New: func() interface{} {
			return bufio.NewWriterSize(nil, 256*1024)
		},
	}
	bufReaderPool = sync.Pool{
		New: func() interface{} {
			return bufio.NewReaderSize(nil, 64*1024)
		},
	}
)

type Sorter struct {
	Name           string
	outputPath     string
	tempDir        string
	chunkSize      int
	chunkFiles     []string
	totalRecords   int64
	bytesWritten   int64
	mergedRecords  int64
	state          int32
	memBuffer      []types.IndexRecord
	chunkDistincts []int64
	bloom          *BloomFilter
}

func NewSorter(name, outputPath, tempDir string, memoryLimit int, bloom *BloomFilter) *Sorter {
	chunkSize := memoryLimit / 100
	if chunkSize < 1000 {
		chunkSize = 1000
	}

	return &Sorter{
		Name:       name,
		outputPath: outputPath,
		tempDir:    tempDir,
		chunkSize:  chunkSize,
		memBuffer:  make([]types.IndexRecord, 0, chunkSize),
		bloom:      bloom,
	}
}

func (s *Sorter) Add(record types.IndexRecord) error {
	s.memBuffer = append(s.memBuffer, record)
	atomic.AddInt64(&s.totalRecords, 1)
	if len(s.memBuffer) >= s.chunkSize {
		return s.flushChunk()
	}
	return nil
}

func (s *Sorter) flushChunk() error {
	if len(s.memBuffer) == 0 {
		return nil
	}

	slices.SortFunc(s.memBuffer, func(a, b types.IndexRecord) int {
		cmp := bytes.Compare(a.Key[:], b.Key[:])
		if cmp != 0 {
			return cmp
		}
		if a.Offset < b.Offset {
			return -1
		}
		if a.Offset > b.Offset {
			return 1
		}
		return 0
	})

	chunkPath := filepath.Join(s.tempDir, fmt.Sprintf("chunk_%d.tmp", len(s.chunkFiles)))
	file, err := os.Create(chunkPath)
	if err != nil {
		return fmt.Errorf("failed to create chunk file: %w", err)
	}

	lzWriter := lz4.NewWriter(file)
	bufferedWriter := bufWriterPool.Get().(*bufio.Writer)
	bufferedWriter.Reset(lzWriter)
	defer func() {
		bufferedWriter.Reset(nil)
		bufWriterPool.Put(bufferedWriter)
	}()

	var distinctCount int64 = 0
	var lastKey [64]byte
	for i, rec := range s.memBuffer {
		if i == 0 || rec.Key != lastKey {
			distinctCount++
			lastKey = rec.Key
		}
	}

	if err := storage.WriteBatchRecords(bufferedWriter, s.memBuffer); err != nil {
		bufferedWriter.Flush()
		lzWriter.Close()
		file.Close()
		return err
	}
	atomic.AddInt64(&s.bytesWritten, int64(len(s.memBuffer))*types.RecordSize)

	if err := bufferedWriter.Flush(); err != nil {
		lzWriter.Close()
		file.Close()
		return err
	}

	if err := lzWriter.Close(); err != nil {
		file.Close()
		return err
	}
	file.Close()

	s.chunkFiles = append(s.chunkFiles, chunkPath)
	s.chunkDistincts = append(s.chunkDistincts, distinctCount)
	s.memBuffer = s.memBuffer[:0]

	return nil
}

func (s *Sorter) Finalize() (int64, error) {
	if err := s.flushChunk(); err != nil {
		return 0, err
	}
	atomic.StoreInt32(&s.state, int32(StateMerging))

	if len(s.chunkFiles) == 0 {
		f, err := os.Create(s.outputPath)
		if err != nil {
			return 0, err
		}
		f.Close()
		atomic.StoreInt32(&s.state, int32(StateDone))
		return 0, nil
	}

	count, err := s.kWayMerge()
	if err == nil {
		atomic.StoreInt32(&s.state, int32(StateDone))
	}
	return count, err
}

type mergeItem struct {
	record types.IndexRecord
	source int
}

type manualHeap []mergeItem

func (h manualHeap) Len() int           { return len(h) }
func (h manualHeap) Less(i, j int) bool { return h[i].Less(h[j]) }
func (h manualHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *manualHeap) Push(x mergeItem) {
	*h = append(*h, x)
	h.up(len(*h) - 1)
}
func (h *manualHeap) Pop() mergeItem {
	old := *h
	n := len(old)
	x := old[0]
	old[0] = old[n-1]
	*h = old[0 : n-1]
	h.down(0, n-1)
	return x
}
func (h *manualHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !(*h)[j].Less((*h)[i]) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}
func (h *manualHeap) down(i0, n int) {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && (*h)[j2].Less((*h)[j1]) {
			j = j2
		}
		if !(*h)[j].Less((*h)[i]) {
			break
		}
		h.Swap(j, i)
		i = j
	}
}
func (m mergeItem) Less(other mergeItem) bool {
	cmp := bytes.Compare(m.record.Key[:], other.record.Key[:])
	if cmp != 0 {
		return cmp < 0
	}
	return m.record.Offset < other.record.Offset
}

func (s *Sorter) kWayMerge() (int64, error) {
	k := len(s.chunkFiles)
	readers := make([]*bufio.Reader, k)
	files := make([]*os.File, k)

	for i, path := range s.chunkFiles {
		f, err := os.Open(path)
		if err != nil {
			return 0, fmt.Errorf("failed to open chunk %d: %w", i, err)
		}
		files[i] = f
		lzReader := lz4.NewReader(f)
		bufReader := bufReaderPool.Get().(*bufio.Reader)
		bufReader.Reset(lzReader)
		readers[i] = bufReader
	}

	defer func() {
		for _, r := range readers {
			if r != nil {
				r.Reset(nil)
				bufReaderPool.Put(r)
			}
		}
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()

	outFile, err := os.Create(s.outputPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	writer, err := NewBlockWriter(outFile)
	if err != nil {
		return 0, err
	}

	h := make(manualHeap, 0, k)
	for i := 0; i < k; i++ {
		rec, err := storage.ReadRecord(readers[i])
		if err == nil {
			h = append(h, mergeItem{record: rec, source: i})
		}
	}
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}

	var distinctCount int64 = 0
	var lastKey [64]byte
	var firstRecord = true

	for len(h) > 0 {
		item := h.Pop()
		rec := item.record

		if firstRecord || rec.Key != lastKey {
			distinctCount++
			if s.bloom != nil {
				keyStr := string(bytes.TrimRight(rec.Key[:], "\x00"))
				s.bloom.Add(keyStr)
			}
			lastKey = rec.Key
			firstRecord = false
		}

		if err := writer.WriteRecord(rec); err != nil {
			return 0, err
		}
		atomic.AddInt64(&s.mergedRecords, 1)

		nextRec, err := storage.ReadRecord(readers[item.source])
		if err == nil {
			h.Push(mergeItem{record: nextRec, source: item.source})
		}
	}

	if err := writer.Close(); err != nil {
		return 0, err
	}

	return distinctCount, nil
}

func (s *Sorter) Cleanup() {
	for _, path := range s.chunkFiles {
		os.Remove(path)
	}
	s.chunkFiles = nil
}

const (
	StateCollecting = iota
	StateMerging
	StateDone
)

type SorterStats struct {
	TotalRecords  int64
	MergedRecords int64
	BytesWritten  int64
	ChunkCount    int
	State         int
}

func (s *Sorter) GetStats() SorterStats {
	state := int(atomic.LoadInt32(&s.state))
	chunkCount := 0
	if state != StateDone {
		chunkCount = len(s.chunkFiles)
	}
	return SorterStats{
		TotalRecords:  atomic.LoadInt64(&s.totalRecords),
		MergedRecords: atomic.LoadInt64(&s.mergedRecords),
		BytesWritten:  atomic.LoadInt64(&s.bytesWritten),
		ChunkCount:    chunkCount,
		State:         state,
	}
}
