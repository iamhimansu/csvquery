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

	"github.com/csvquery/csvquery/pkg/csvquery/parser/simd"
	"github.com/csvquery/csvquery/pkg/csvquery/storage"
)

// SIMDParser reads CSV files efficiently using Mmap and SIMD parallelism
type SIMDParser struct {
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

// NewSIMDParser creates a new Mmap-based CSV scanner
func NewSIMDParser(filePath, separator string) (*SIMDParser, error) {
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

	p := &SIMDParser{
		filePath:  filePath,
		separator: separator[0],
		data:      data,
		fileSize:  size,
		workers:   runtime.NumCPU(),
		startTime: time.Now(),
	}

	if err := p.readHeaders(); err != nil {
		p.Close()
		return nil, err
	}

	return p, nil
}

func (p *SIMDParser) readHeaders() error {
	idx := bytes.IndexByte(p.data, '\n')
	if idx == -1 {
		return fmt.Errorf("empty or invalid csv")
	}

	line := p.data[:idx]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}

	if len(line) >= 3 && line[0] == 0xEF && line[1] == 0xBB && line[2] == 0xBF {
		line = line[3:]
	}

	parts := bytes.Split(line, []byte{p.separator})
	p.headers = make([]string, len(parts))
	p.headerMap = make(map[string]int)

	for i, part := range parts {
		name := string(bytes.TrimSpace(part))
		if len(name) >= 2 && name[0] == '"' && name[len(name)-1] == '"' {
			name = name[1 : len(name)-1]
		}
		p.headers[i] = name
		p.headerMap[strings.ToLower(name)] = i
	}

	return nil
}

func (p *SIMDParser) GetColumnIndex(name string) (int, bool) {
	idx, ok := p.headerMap[strings.ToLower(strings.TrimSpace(name))]
	return idx, ok
}

func (p *SIMDParser) GetHeaders() []string {
	return p.headers
}

func (p *SIMDParser) ValidateColumns(columns []string) error {
	for _, col := range columns {
		normalized := strings.ToLower(strings.TrimSpace(col))
		if _, ok := p.headerMap[normalized]; !ok {
			return fmt.Errorf("column not found: %s", col)
		}
	}
	return nil
}

func (p *SIMDParser) SetWorkers(n int) {
	if n > 0 {
		p.workers = n
	}
}

func (p *SIMDParser) Scan(indexDefs [][]int, handler func(workerID int, keys [][]byte, offset, line int64)) error {
	startIdx := bytes.IndexByte(p.data, '\n') + 1
	if startIdx <= 0 || startIdx >= len(p.data) {
		return nil
	}

	dataSize := len(p.data)
	chunkSize := (dataSize - startIdx) / p.workers

	boundaries := make([]int, p.workers+1)
	boundaries[0] = startIdx
	boundaries[p.workers] = dataSize

	for i := 1; i < p.workers; i++ {
		hint := startIdx + (i * chunkSize)
		if hint < dataSize {
			boundaries[i] = findSafeRecordBoundary(p.data, hint)
		} else {
			boundaries[i] = dataSize
		}
	}

	// Pre-pass: Count lines per chunk to establish global line numbers
	chunkLines := make([]int64, p.workers)
	var wgCount sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		start := boundaries[i]
		end := boundaries[i+1]
		if start >= end {
			continue
		}
		wgCount.Add(1)
		go func(idx, s, e int) {
			defer wgCount.Done()
			chunkLines[idx] = p.countChunkLines(s, e)
		}(i, start, end)
	}
	wgCount.Wait()

	// Calculate start lines
	startLines := make([]int64, p.workers)
	currentLine := int64(2) // 1-based index, skipping header (row 1)
	for i := 0; i < p.workers; i++ {
		startLines[i] = currentLine
		currentLine += chunkLines[i]
	}

	var wg sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		start := boundaries[i]
		end := boundaries[i+1]
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(chunkStart, chunkEnd int, workerID int, startLine int64) {
			defer wg.Done()
			p.processChunk(chunkStart, chunkEnd, workerID, startLine, indexDefs, handler)
		}(start, end, i, startLines[i])
	}

	wg.Wait()
	p.scanBytes = int64(dataSize)
	return nil
}

func (p *SIMDParser) processChunk(start, end int, workerID int, startLine int64, indexDefs [][]int, handler func(workerID int, keys [][]byte, offset, line int64)) {
	if start >= len(p.data) {
		return
	}
	if end > len(p.data) {
		end = len(p.data)
	}
	if start >= end {
		return
	}

	chunkData := p.data[start:end]
	chunkLen := len(chunkData)
	if chunkLen == 0 {
		return
	}

	sep := p.separator
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
	currentLine := startLine

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
					p.parseLineSimd(lineBytes, sep, int64(start+lineStart), workerID, indexDefs, handler, keys, currentRowValues, &scratchBuf, lineStart, quotesBitmap, sepsBitmap, currentLine)
					localRowsScanned++
					currentLine++
				}
				localScanBytes += int64(lineEnd - lineStart + 1)
				lineStart = bytePos + 1
			}
		}

		if wordIdx%1024 == 0 {
			atomic.AddInt64(&p.scanBytes, localScanBytes)
			atomic.AddInt64(&p.rowsScanned, localRowsScanned)
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
			p.parseLineSimd(lineBytes, sep, int64(start+lineStart), workerID, indexDefs, handler, keys, currentRowValues, &scratchBuf, lineStart, quotesBitmap, sepsBitmap, currentLine)
			localRowsScanned++
			currentLine++
		}
		localScanBytes += int64(chunkLen - lineStart)
	}

	atomic.AddInt64(&p.scanBytes, localScanBytes)
	atomic.AddInt64(&p.rowsScanned, localRowsScanned)
}

func (p *SIMDParser) parseLineSimd(
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
	lineNum int64,
) {
	maxCol := len(currentRowValues) - 1
	lineLen := len(line)

	if lineLen == 0 {
		return
	}

	colIdx := 0
	fieldStart := 0
	inQuote := false

	// Optimized scanning using pre-calculated bitmaps if possible,
	// but here we are inside a line, so we iterate manually or use the bitmaps.
	// Since we passed bitmaps, let's use them.
	// But calculating offset into bitmap is tricky here because lineStartInChunk varies.
	// Loop over chars is safer/ simpler for line level parsing unless we use the bitmaps directly.

	// Original code used this loop:
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

	handler(workerID, keys, offset, lineNum)

	for k := 0; k < len(currentRowValues); k++ {
		currentRowValues[k] = nil
	}
}

func (p *SIMDParser) countChunkLines(start, end int) int64 {
	if start >= len(p.data) || start >= end {
		return 0
	}
	chunkData := p.data[start:end]
	chunkLen := len(chunkData)
	if chunkLen == 0 {
		return 0
	}

	bitmapLen := (chunkLen + 63) / 64
	quotesBitmap := make([]uint64, bitmapLen)
	sepsBitmap := make([]uint64, bitmapLen)
	newlinesBitmap := make([]uint64, bitmapLen)

	if p.separator == ',' {
		simd.Scan(chunkData, quotesBitmap, sepsBitmap, newlinesBitmap)
	} else {
		simd.ScanWithSeparator(chunkData, p.separator, quotesBitmap, sepsBitmap, newlinesBitmap)
	}

	var count int64
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

			isQuote := (quoteMask & bitMask) != 0
			isNewline := (newlineMask & bitMask) != 0

			if isQuote {
				inQuote = !inQuote
			} else if isNewline && !inQuote {
				count++
			}
		}
	}

	if chunkLen > 0 && chunkData[chunkLen-1] != '\n' && !inQuote {
		count++
	}

	return count
}

func (p *SIMDParser) GetStats() (rowsScanned int64, bytesRead int64) {
	return atomic.LoadInt64(&p.rowsScanned), atomic.LoadInt64(&p.scanBytes)
}

func (p *SIMDParser) Close() error {
	return storage.MunmapFile(p.data)
}
