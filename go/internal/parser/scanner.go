package parser

import (
	"bytes"
	"fmt"
	"math/bits"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iamhimansu/csvquery/go/internal/parser/simd"
	"github.com/iamhimansu/csvquery/go/internal/storage"
)

// Scanner reads CSV files efficiently using Mmap and Parallelism
type Scanner struct {
	filePath    string
	separator   byte
	headers     []string
	headerMap   map[string]int
	data        []byte // mmapped data
	fileSize    int64
	workers     int
	startTime   time.Time
	rowsScanned int64
	scanBytes   int64
}

// NewScanner creates a new Mmap-based CSV scanner
func NewScanner(filePath, separator string) (*Scanner, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	stats, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := stats.Size()

	data, err := storage.MmapFile(file)
	if err != nil {
		return nil, err
	}

	s := &Scanner{
		filePath:  filePath,
		separator: separator[0],
		data:      data,
		fileSize:  size,
		workers:   runtime.NumCPU(),
		startTime: time.Now(),
	}

	if err := s.readHeaders(); err != nil {
		s.Close()
		return nil, err
	}

	return s, nil
}

func (s *Scanner) readHeaders() error {
	idx := bytes.IndexByte(s.data, '\n')
	if idx == -1 {
		return fmt.Errorf("empty or invalid csv")
	}

	line := s.data[:idx]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}

	if len(line) >= 3 && line[0] == 0xEF && line[1] == 0xBB && line[2] == 0xBF {
		line = line[3:]
	}

	parts := bytes.Split(line, []byte{s.separator})
	s.headers = make([]string, len(parts))
	s.headerMap = make(map[string]int)

	for i, part := range parts {
		name := string(bytes.TrimSpace(part))
		if len(name) >= 2 && name[0] == '"' && name[len(name)-1] == '"' {
			name = name[1 : len(name)-1]
		}
		s.headers[i] = name
		s.headerMap[strings.ToLower(name)] = i
	}

	return nil
}

func (s *Scanner) GetColumnIndex(name string) (int, bool) {
	idx, ok := s.headerMap[strings.ToLower(strings.TrimSpace(name))]
	return idx, ok
}

func (s *Scanner) GetHeaders() []string {
	return s.headers
}

func (s *Scanner) ValidateColumns(columns []string) error {
	for _, col := range columns {
		normalized := strings.ToLower(strings.TrimSpace(col))
		if _, ok := s.headerMap[normalized]; !ok {
			return fmt.Errorf("column not found: %s", col)
		}
	}
	return nil
}

func (s *Scanner) SetWorkers(n int) {
	if n > 0 {
		s.workers = n
	}
}

func (s *Scanner) Scan(indexDefs [][]int, handler func(workerID int, keys [][]byte, offset, line int64)) error {
	startIdx := bytes.IndexByte(s.data, '\n') + 1
	if startIdx <= 0 || startIdx >= len(s.data) {
		return nil
	}

	dataSize := len(s.data)
	chunkSize := (dataSize - startIdx) / s.workers

	boundaries := make([]int, s.workers+1)
	boundaries[0] = startIdx
	boundaries[s.workers] = dataSize

	for i := 1; i < s.workers; i++ {
		hint := startIdx + (i * chunkSize)
		if hint < dataSize {
			boundaries[i] = findSafeRecordBoundary(s.data, hint)
		} else {
			boundaries[i] = dataSize
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < s.workers; i++ {
		start := boundaries[i]
		end := boundaries[i+1]
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(chunkStart, chunkEnd int, workerID int) {
			defer wg.Done()
			s.processChunk(chunkStart, chunkEnd, workerID, indexDefs, handler)
		}(start, end, i)
	}

	wg.Wait()
	s.scanBytes = int64(dataSize)
	return nil
}

func findSafeRecordBoundary(data []byte, hint int) int {
	pos := hint
	if pos >= len(data) {
		return len(data)
	}

	nextNL := bytes.IndexByte(data[pos:], '\n')
	if nextNL == -1 {
		return len(data)
	}
	pos += nextNL
	currentNL := pos

	for {
		if currentNL+1 >= len(data) {
			return len(data)
		}
		nextNL := bytes.IndexByte(data[currentNL+1:], '\n')
		if nextNL == -1 {
			return currentNL + 1
		}
		nextPos := currentNL + 1 + nextNL
		quotes := 0
		for i := currentNL + 1; i < nextPos; i++ {
			if data[i] == '"' {
				quotes++
			}
		}
		if quotes%2 == 0 {
			return currentNL + 1
		}
		currentNL = nextPos
	}
}

func (s *Scanner) processChunk(start, end int, workerID int, indexDefs [][]int, handler func(workerID int, keys [][]byte, offset, line int64)) {
	if start >= len(s.data) {
		return
	}
	if end > len(s.data) {
		end = len(s.data)
	}
	if start >= end {
		return
	}

	chunkData := s.data[start:end]
	chunkLen := len(chunkData)
	if chunkLen == 0 {
		return
	}

	sep := s.separator
	keys := make([][]byte, len(indexDefs))
	maxCol := -1
	for _, indices := range indexDefs {
		for _, idx := range indices {
			if idx > maxCol {
				maxCol = idx
			}
		}
	}

	currentRowValues := make([][]byte, maxCol+1)
	scratchBuf := make([]byte, 0, 1024)

	bitmapLen := (chunkLen + 63) / 64
	quotesBitmap := make([]uint64, bitmapLen)
	sepsBitmap := make([]uint64, bitmapLen)
	newlinesBitmap := make([]uint64, bitmapLen)

	if sep == ',' {
		simd.Scan(chunkData, quotesBitmap, sepsBitmap, newlinesBitmap)
	} else {
		simd.ScanWithSeparator(chunkData, sep, quotesBitmap, sepsBitmap, newlinesBitmap)
	}

	var localRowsScanned int64
	var localScanBytes int64
	lineStart := 0
	inQuote := false

	for wordIdx := 0; wordIdx < bitmapLen; wordIdx++ {
		quoteMask := quotesBitmap[wordIdx]
		newlineMask := newlinesBitmap[wordIdx]

		if quoteMask == 0 && newlineMask == 0 && !inQuote {
			continue
		}

		combined := quoteMask | newlineMask
		for combined != 0 {
			tz := bits.TrailingZeros64(combined)
			bitMask := uint64(1) << tz
			combined &^= bitMask

			bytePos := wordIdx*64 + tz
			if bytePos >= chunkLen {
				break
			}

			isQuote := (quoteMask & bitMask) != 0
			isNewline := (newlineMask & bitMask) != 0

			if isQuote {
				inQuote = !inQuote
				continue
			}

			if isNewline && !inQuote {
				lineEnd := bytePos
				lineBytes := chunkData[lineStart:lineEnd]
				if len(lineBytes) > 0 && lineBytes[len(lineBytes)-1] == '\r' {
					lineBytes = lineBytes[:len(lineBytes)-1]
				}

				if len(lineBytes) > 0 {
					for k := range currentRowValues {
						currentRowValues[k] = nil
					}
					s.parseLineSimd(lineBytes, sep, int64(start+lineStart), workerID, indexDefs, handler, keys, currentRowValues, &scratchBuf, lineStart, quotesBitmap, sepsBitmap)
					localRowsScanned++
				}
				localScanBytes += int64(lineEnd - lineStart + 1)
				lineStart = bytePos + 1
			}
		}

		if wordIdx%1024 == 0 {
			atomic.AddInt64(&s.scanBytes, localScanBytes)
			atomic.AddInt64(&s.rowsScanned, localRowsScanned)
			localScanBytes = 0
			localRowsScanned = 0
		}
	}

	if lineStart < chunkLen && !inQuote {
		lineBytes := chunkData[lineStart:]
		if len(lineBytes) > 0 && lineBytes[len(lineBytes)-1] == '\r' {
			lineBytes = lineBytes[:len(lineBytes)-1]
		}
		if len(lineBytes) > 0 {
			for k := range currentRowValues {
				currentRowValues[k] = nil
			}
			s.parseLineSimd(lineBytes, sep, int64(start+lineStart), workerID, indexDefs, handler, keys, currentRowValues, &scratchBuf, lineStart, quotesBitmap, sepsBitmap)
			localRowsScanned++
		}
		localScanBytes += int64(chunkLen - lineStart)
	}

	atomic.AddInt64(&s.scanBytes, localScanBytes)
	atomic.AddInt64(&s.rowsScanned, localRowsScanned)
}

func (s *Scanner) parseLineSimd(
	line []byte,
	sep byte,
	offset int64,
	workerID int,
	indexDefs [][]int,
	handler func(workerID int, keys [][]byte, offset, line int64),
	keys [][]byte,
	currentRowValues [][]byte,
	scratchBuf *[]byte,
	lineStartInChunk int,
	quotesBitmap, sepsBitmap []uint64,
) {
	maxCol := len(currentRowValues) - 1
	lineLen := len(line)

	if lineLen == 0 {
		return
	}

	colIdx := 0
	fieldStart := 0
	inQuote := false

	for i := 0; i < lineLen && colIdx <= maxCol; i++ {
		bitmapPos := lineStartInChunk + i
		wordIdx := bitmapPos / 64
		bitPos := uint(bitmapPos % 64)

		if wordIdx >= len(quotesBitmap) {
			break
		}

		isQuote := (quotesBitmap[wordIdx] & (1 << bitPos)) != 0
		isSep := (sepsBitmap[wordIdx] & (1 << bitPos)) != 0

		if isQuote {
			inQuote = !inQuote
			continue
		}

		if isSep && !inQuote {
			valBytes := line[fieldStart:i]
			if len(valBytes) >= 2 && valBytes[0] == '"' && valBytes[len(valBytes)-1] == '"' {
				valBytes = valBytes[1 : len(valBytes)-1]
			}
			currentRowValues[colIdx] = valBytes
			colIdx++
			fieldStart = i + 1
		}
	}

	if colIdx <= maxCol && fieldStart <= lineLen {
		valBytes := line[fieldStart:]
		if len(valBytes) >= 2 && valBytes[0] == '"' && valBytes[len(valBytes)-1] == '"' {
			valBytes = valBytes[1 : len(valBytes)-1]
		}
		currentRowValues[colIdx] = valBytes
	}

	*scratchBuf = (*scratchBuf)[:0]
	for i, indices := range indexDefs {
		if len(indices) == 1 {
			idx := indices[0]
			if idx < len(currentRowValues) && currentRowValues[idx] != nil {
				keys[i] = currentRowValues[idx]
			} else {
				keys[i] = []byte{}
			}
		} else {
			startLen := len(*scratchBuf)
			*scratchBuf = append(*scratchBuf, '[')
			for j, idx := range indices {
				if j > 0 {
					*scratchBuf = append(*scratchBuf, ',')
				}
				*scratchBuf = append(*scratchBuf, '"')
				if idx < len(currentRowValues) && currentRowValues[idx] != nil {
					*scratchBuf = append(*scratchBuf, currentRowValues[idx]...)
				}
				*scratchBuf = append(*scratchBuf, '"')
			}
			*scratchBuf = append(*scratchBuf, ']')
			endLen := len(*scratchBuf)
			keys[i] = (*scratchBuf)[startLen:endLen]
		}
	}

	handler(workerID, keys, offset, 0)

	for k := 0; k < len(currentRowValues); k++ {
		currentRowValues[k] = nil
	}
}

func (s *Scanner) GetStats() (rowsScanned int64, bytesRead int64, elapsed time.Duration) {
	return atomic.LoadInt64(&s.rowsScanned), atomic.LoadInt64(&s.scanBytes), time.Since(s.startTime)
}

func (s *Scanner) Close() error {
	return storage.MunmapFile(s.data)
}
