package query

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/iamhimansu/csvquery/go/internal/index"
	"github.com/iamhimansu/csvquery/go/internal/storage"
	"github.com/iamhimansu/csvquery/go/internal/types"
)

type QueryEngine struct {
	config          types.QueryConfig
	VirtualDefaults []string
	Writer          io.Writer
	Updates         *UpdateManager
}

func NewQueryEngine(config types.QueryConfig) *QueryEngine {
	qe := &QueryEngine{
		config: config,
		Writer: os.Stdout,
	}
	if config.CsvPath != "" {
		if um, err := LoadUpdates(config.CsvPath); err == nil {
			qe.Updates = um
		}
	}
	return qe
}

func (q *QueryEngine) Run(where *Condition) error {
	if q.config.CsvPath == "" {
		return fmt.Errorf("csv path required")
	}
	totalStart := time.Now()

	if where == nil && q.config.GroupBy == "" && !q.config.CountOnly {
		return fmt.Errorf("no WHERE conditions or GROUP BY specified")
	}

	if q.config.CountOnly && where == nil && q.config.GroupBy == "" {
		return q.runCountAll()
	}

	if q.Updates != nil && len(q.Updates.Overrides) > 0 {
		return q.runFullScan(where)
	}

	indexPath, searchKey, hasSearchKey, plan, err := q.findBestIndex(where)
	if err != nil {
		return q.runFullScan(where)
	}

	if where != nil {
		if covered, ok := plan["covered_columns"].([]string); ok && len(covered) > 0 {
			allCovered := true
			conds := where.ExtractIndexConditions()
			for k := range conds {
				isCovered := false
				for _, c := range covered {
					if strings.EqualFold(c, k) {
						isCovered = true
						break
					}
				}
				if !isCovered {
					allCovered = false
					break
				}
			}
			if allCovered {
				where = nil
			}
		}
	}

	if q.config.Explain {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(plan)
	}

	execStart := time.Now()
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return fmt.Errorf("failed to open index: %w", err)
	}
	defer indexFile.Close()

	br, err := index.NewBlockReader(indexFile)
	if err != nil {
		return fmt.Errorf("failed to init block reader: %w", err)
	}

	if hasSearchKey {
		bloomPath := indexPath + ".bloom"
		if _, err := os.Stat(bloomPath); err == nil {
			bloom, bloomCleanup, err := index.LoadBloomFilterMmap(bloomPath)
			if err == nil {
				if bloomCleanup != nil {
					defer bloomCleanup()
				}
				if !bloom.MightContain(searchKey) {
					if q.config.CountOnly {
						fmt.Fprintln(q.Writer, "0")
					}
					return nil
				}
			}
		}
	}

	startBlockIdx := 0
	endBlockIdx := len(br.Footer.Blocks) - 1
	if hasSearchKey {
		startBlockIdx = q.findStartBlock(br.Footer, searchKey)
		if startBlockIdx == -1 {
			if q.config.CountOnly {
				fmt.Fprintln(q.Writer, "0")
			}
			return nil
		}
	}

	if q.config.GroupBy != "" {
		indexName, _ := plan["index"].(string)
		return q.runAggregation(br, searchKey, hasSearchKey, startBlockIdx, endBlockIdx, indexName, where)
	}
	return q.runStandardOutput(br, searchKey, hasSearchKey, startBlockIdx, endBlockIdx, where)
}

func (q *QueryEngine) runCountAll() error {
	if count, ok := q.tryCountFromIndex(); ok {
		fmt.Fprintln(q.Writer, count)
		return nil
	}
	return q.runCountAllViaCsv()
}

func (q *QueryEngine) tryCountFromIndex() (int64, bool) {
	if q.config.IndexDir == "" {
		return 0, false
	}
	csvBase := filepath.Base(q.config.CsvPath)
	csvBase = strings.TrimSuffix(csvBase, filepath.Ext(csvBase))
	pattern := filepath.Join(q.config.IndexDir, csvBase+"_*.cidx")
	matches, _ := filepath.Glob(pattern)
	if len(matches) == 0 {
		return 0, false
	}
	f, err := os.Open(matches[0])
	if err != nil {
		return 0, false
	}
	defer f.Close()
	br, err := index.NewBlockReader(f)
	if err != nil {
		return 0, false
	}
	var total int64
	for _, block := range br.Footer.Blocks {
		if block.RecordCount == 0 {
			return 0, false
		}
		total += block.RecordCount
	}
	return total, true
}

func (q *QueryEngine) runCountAllViaCsv() error {
	f, err := os.Open(q.config.CsvPath)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := storage.MmapFile(f)
	if err != nil {
		return err
	}
	defer storage.MunmapFile(data)
	if len(data) == 0 {
		fmt.Fprintln(q.Writer, 0)
		return nil
	}
	workers := runtime.NumCPU()
	if workers > 16 {
		workers = 16
	}
	chunkSize := len(data) / workers
	var totalCount int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < workers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == workers-1 {
			end = len(data)
		}
		wg.Add(1)
		go func(chunk []byte) {
			defer wg.Done()
			c := int64(bytes.Count(chunk, []byte{'\n'}))
			mu.Lock()
			totalCount += c
			mu.Unlock()
		}(data[start:end])
	}
	wg.Wait()
	if len(data) > 0 && data[len(data)-1] != '\n' {
		totalCount++
	}
	if totalCount > 0 {
		totalCount--
	}
	fmt.Fprintln(q.Writer, totalCount)
	return nil
}

func (q *QueryEngine) findStartBlock(sparse index.SparseIndex, key string) int {
	left, right := 0, len(sparse.Blocks)-1
	result := -1
	for left <= right {
		mid := (left + right) / 2
		if sparse.Blocks[mid].StartKey <= key {
			result = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	if result == -1 {
		return -1
	}
	targetKey := sparse.Blocks[result].StartKey
	if targetKey == key {
		for result > 0 && sparse.Blocks[result-1].StartKey == key {
			result--
		}
	}
	return result
}

func compareRecordKey(key *[64]byte, searchKey []byte) int {
	keyLen := 64
	for keyLen > 0 && key[keyLen-1] == 0 {
		keyLen--
	}
	return bytes.Compare(key[:keyLen], searchKey)
}

func (q *QueryEngine) runStandardOutput(br *index.BlockReader, searchKey string, hasSearchKey bool, startBlockIdx, endBlockIdx int, where *Condition) error {
	headers, virtualDefaults, err := q.getHeaderMap()
	if err != nil {
		return err
	}
	q.VirtualDefaults = virtualDefaults
	var csvF *os.File
	var csvData []byte
	ensureCsvLoaded := func() error {
		if csvData != nil {
			return nil
		}
		var err error
		csvF, err = os.Open(q.config.CsvPath)
		if err != nil {
			return err
		}
		csvData, err = storage.MmapFile(csvF)
		return err
	}
	defer func() {
		if csvData != nil {
			storage.MunmapFile(csvData)
		}
		if csvF != nil {
			csvF.Close()
		}
	}()
	maxCol := -1
	for _, idx := range headers {
		if idx > maxCol {
			maxCol = idx
		}
	}
	count := int64(0)
	skipped := 0
	limitReached := false
	writer := bufio.NewWriter(q.Writer)
	defer writer.Flush()
	searchKeyBytes := []byte(searchKey)
	rowMap := make(map[string]string, len(headers))
	colsBuf := make([]string, 0, maxCol+1)
	for i := startBlockIdx; i <= endBlockIdx; i++ {
		if limitReached {
			break
		}
		blockMeta := br.Footer.Blocks[i]
		if hasSearchKey && blockMeta.StartKey > searchKey {
			break
		}
		records, err := br.ReadBlock(blockMeta)
		if err != nil {
			return err
		}
		for index := range records {
			rec := &records[index]
			if hasSearchKey {
				cmp := compareRecordKey(&rec.Key, searchKeyBytes)
				if cmp < 0 {
					continue
				}
				if cmp > 0 {
					limitReached = true
					break
				}
			}
			if where != nil || !q.config.CountOnly {
				if err := ensureCsvLoaded(); err != nil {
					return err
				}
				rowEnd := bytes.IndexByte(csvData[rec.Offset:], '\n')
				if rowEnd == -1 {
					rowEnd = len(csvData) - int(rec.Offset)
				}
				row := csvData[rec.Offset : int(rec.Offset)+rowEnd]
				row = bytes.TrimSuffix(row, []byte{'\r'})
				if where != nil {
					cols := extractCols(row, ',', maxCol, colsBuf)
					if len(q.VirtualDefaults) > 0 {
						cols = append(cols, q.VirtualDefaults...)
					}
					clear(rowMap)
					for k, v := range headers {
						if v < len(cols) {
							rowMap[strings.ToLower(k)] = cols[v]
						} else {
							rowMap[strings.ToLower(k)] = ""
						}
					}
					if !where.Evaluate(rowMap) {
						continue
					}
					colsBuf = cols
				}
			}
			if skipped < q.config.Offset {
				skipped++
				continue
			}
			count++
			if !q.config.CountOnly {
				fmt.Fprintf(writer, "%d,%d\n", rec.Offset, rec.Line)
			}
			if q.config.Limit > 0 && count >= int64(q.config.Limit) {
				limitReached = true
				break
			}
		}
	}
	if q.config.CountOnly {
		fmt.Fprintln(writer, count)
	}
	return nil
}

func (q *QueryEngine) runAggregation(br *index.BlockReader, searchKey string, hasSearchKey bool, startBlockIdx, endBlockIdx int, indexName string, where *Condition) error {
	headers, virtualDefaults, err := q.getHeaderMap()
	if err != nil {
		return err
	}
	q.VirtualDefaults = virtualDefaults
	isGroupingByIndex := strings.EqualFold(indexName, q.config.GroupBy)
	canUseMetadata := q.config.AggFunc == "count" || q.config.AggFunc == ""
	var csvF *os.File
	var csvData []byte
	ensureCsvLoaded := func() error {
		if csvData != nil {
			return nil
		}
		var err error
		csvF, err = os.Open(q.config.CsvPath)
		if err != nil {
			return err
		}
		csvData, err = storage.MmapFile(csvF)
		return err
	}
	defer func() {
		if csvData != nil {
			storage.MunmapFile(csvData)
		}
		if csvF != nil {
			csvF.Close()
		}
	}()
	groupKey := strings.ToLower(q.config.GroupBy)
	groupC, ok := headers[groupKey]
	if !ok {
		return fmt.Errorf("column '%s' not found", q.config.GroupBy)
	}
	aggC := 0
	if q.config.AggCol != "" && q.config.AggCol != "*" {
		var ok bool
		aggC, ok = headers[strings.ToLower(q.config.AggCol)]
		if !ok {
			return fmt.Errorf("aggregation column '%s' not found", q.config.AggCol)
		}
	}
	isCountOnly := q.config.AggFunc == "count"
	maxCol := groupC
	if aggC > maxCol {
		maxCol = aggC
	}
	if where != nil {
		for _, idx := range headers {
			if idx > maxCol {
				maxCol = idx
			}
		}
	}
	results := make(map[string]float64)
	counts := make(map[string]int64)
	limitReached := false
	searchKeyBytes := []byte(searchKey)
	rowMap := make(map[string]string, len(headers))
	colsBuf := make([]string, 0, maxCol+1)
	for i := startBlockIdx; i <= endBlockIdx; i++ {
		if limitReached {
			break
		}
		blockMeta := br.Footer.Blocks[i]
		if hasSearchKey && blockMeta.StartKey > searchKey {
			break
		}
		if isGroupingByIndex && blockMeta.IsDistinct && canUseMetadata {
			groupKey := blockMeta.StartKey
			if q.config.AggFunc == "count" {
				results[groupKey] += float64(blockMeta.RecordCount)
			} else {
				results[groupKey] = 1
			}
			continue
		}
		records, err := br.ReadBlock(blockMeta)
		if err != nil {
			return err
		}
		if csvData == nil {
			if err := ensureCsvLoaded(); err != nil {
				return err
			}
		}
		for index := range records {
			rec := &records[index]
			if hasSearchKey {
				cmp := compareRecordKey(&rec.Key, searchKeyBytes)
				if cmp < 0 {
					continue
				}
				if cmp > 0 {
					limitReached = true
					break
				}
			}
			rowEnd := bytes.IndexByte(csvData[rec.Offset:], '\n')
			if rowEnd == -1 {
				rowEnd = len(csvData) - int(rec.Offset)
			}
			row := csvData[rec.Offset : int(rec.Offset)+rowEnd]
			row = bytes.TrimSuffix(row, []byte{'\r'})
			cols := extractCols(row, ',', maxCol, colsBuf)
			if len(q.VirtualDefaults) > 0 {
				cols = append(cols, q.VirtualDefaults...)
			}
			var groupVal string
			if groupC < len(cols) {
				groupVal = cols[groupC]
			}
			if where != nil {
				clear(rowMap)
				for k, v := range headers {
					if v < len(cols) {
						rowMap[strings.ToLower(k)] = cols[v]
					} else {
						rowMap[strings.ToLower(k)] = ""
					}
				}
				if !where.Evaluate(rowMap) {
					continue
				}
			}
			var val float64
			if !isCountOnly && aggC < len(cols) {
				val, _ = strconv.ParseFloat(cols[aggC], 64)
			}
			switch q.config.AggFunc {
			case "count":
				results[groupVal]++
			case "sum":
				results[groupVal] += val
			case "min":
				if curr, ok := results[groupVal]; !ok || val < curr {
					results[groupVal] = val
				}
			case "max":
				if curr, ok := results[groupVal]; !ok || val > curr {
					results[groupVal] = val
				}
			case "avg":
				results[groupVal] += val
				counts[groupVal]++
			case "":
				results[groupVal] = 1
			}
			colsBuf = cols
		}
	}
	return json.NewEncoder(q.Writer).Encode(results)
}

func extractCols(line []byte, sep byte, maxCol int, buf []string) []string {
	cols := buf[:0]
	start := 0
	inQuote := false
	for i := 0; i < len(line); i++ {
		if line[i] == '"' {
			inQuote = !inQuote
		}
		if line[i] == sep && !inQuote {
			val := string(line[start:i])
			if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
				val = val[1 : len(val)-1]
			}
			cols = append(cols, val)
			start = i + 1
			if len(cols) > maxCol {
				return cols
			}
		}
	}
	val := string(line[start:])
	if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
		val = val[1 : len(val)-1]
	}
	cols = append(cols, val)
	return cols
}

func (q *QueryEngine) getHeaderMap() (map[string]int, []string, error) {
	f, err := os.Open(q.config.CsvPath)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	br := bufio.NewReader(f)
	r, _, err := br.ReadRune()
	if err != nil {
		return nil, nil, err
	}
	if r != '\uFEFF' {
		br.UnreadRune()
	}
	csvReader := csv.NewReader(br)
	header, err := csvReader.Read()
	if err != nil {
		return nil, nil, err
	}
	m := make(map[string]int)
	for i, h := range header {
		clean := strings.TrimSpace(h)
		m[strings.ToLower(clean)] = i
	}
	s, err := LoadSchema(q.config.CsvPath)
	if err == nil {
		var keys []string
		for k := range s.VirtualColumns {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var virtualDefaults []string
		startIdx := len(header)
		for _, k := range keys {
			if _, exists := m[k]; !exists {
				m[k] = startIdx
				startIdx++
				virtualDefaults = append(virtualDefaults, s.VirtualColumns[k])
			}
		}
		return m, virtualDefaults, nil
	}
	return m, nil, nil
}

func (q *QueryEngine) findBestIndex(where *Condition) (string, string, bool, map[string]interface{}, error) {
	plan := make(map[string]interface{})
	csvName := strings.TrimSuffix(filepath.Base(q.config.CsvPath), filepath.Ext(q.config.CsvPath))
	if where != nil {
		conds := where.ExtractIndexConditions()
		if len(conds) > 0 {
			var cols []string
			for col := range conds {
				cols = append(cols, col)
			}
			sort.Strings(cols)
			for i := len(cols); i >= 1; i-- {
				currentCols := cols[:i]
				indexName := strings.Join(currentCols, "_")
				var searchKey string
				if i == 1 {
					searchKey = conds[currentCols[0]]
				} else {
					var b strings.Builder
					b.WriteByte('[')
					for k, col := range currentCols {
						if k > 0 {
							b.WriteByte(',')
						}
						b.WriteByte('"')
						b.WriteString(conds[col])
						b.WriteByte('"')
					}
					b.WriteByte(']')
					searchKey = b.String()
				}
				indexPath := filepath.Join(q.config.IndexDir, csvName+"_"+indexName+".cidx")
				if _, err := os.Stat(indexPath); err == nil {
					plan["strategy"] = "Index Scan"
					plan["index"] = indexName
					plan["covered_columns"] = currentCols
					return indexPath, searchKey, true, plan, nil
				}
			}
		}
	}
	if q.config.GroupBy != "" {
		groupName := strings.ReplaceAll(q.config.GroupBy, ",", "_")
		indexPath := filepath.Join(q.config.IndexDir, csvName+"_"+groupName+".cidx")
		if _, err := os.Stat(indexPath); err == nil {
			plan["strategy"] = "GroupBy Index Scan"
			plan["index"] = groupName
			return indexPath, "", false, plan, nil
		}
	}
	return "", "", false, nil, fmt.Errorf("no index found")
}

func (q *QueryEngine) runFullScan(where *Condition) error {
	f, err := os.Open(q.config.CsvPath)
	if err != nil {
		return err
	}
	defer f.Close()
	headers, virtualDefaults, err := q.getHeaderMap()
	if err != nil {
		return err
	}
	q.VirtualDefaults = virtualDefaults
	headerMap := make(map[string]int)
	for k, v := range headers {
		headerMap[k] = v
	}
	reader := bufio.NewReader(f)
	lineNum := int64(1)
	currentOffset := int64(0)
	headerLine, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	currentOffset += int64(len(headerLine))
	writer := bufio.NewWriter(q.Writer)
	defer writer.Flush()
	count := int64(0)
	skipped := 0
	rowMap := make(map[string]string, len(headers))
	colsBuf := make([]string, 0, len(headers))
	maxCol := 0
	for _, v := range headers {
		if v > maxCol {
			maxCol = v
		}
	}
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				if len(line) == 0 {
					break
				}
			} else {
				return err
			}
		}
		rowOffset := currentOffset
		currentOffset += int64(len(line))
		lineNum++
		trimmed := bytes.TrimSpace(line)
		cols := extractCols(trimmed, ',', maxCol, colsBuf)
		if len(q.VirtualDefaults) > 0 {
			cols = append(cols, q.VirtualDefaults...)
		}
		if q.Updates != nil {
			rowId := fmt.Sprintf("%d", lineNum)
			if override, exists := q.Updates.Overrides[rowId]; exists {
				for col, val := range override {
					if idx, ok := headerMap[col]; ok && idx < len(cols) {
						cols[idx] = val
					}
				}
			}
		}
		if where != nil {
			clear(rowMap)
			for k, v := range headers {
				if v < len(cols) {
					rowMap[strings.ToLower(k)] = cols[v]
				}
			}
			if !where.Evaluate(rowMap) {
				continue
			}
		}
		if skipped < q.config.Offset {
			skipped++
			continue
		}
		count++
		if !q.config.CountOnly {
			fmt.Fprintf(writer, "%d,%d\n", rowOffset, lineNum)
		}
		if q.config.Limit > 0 && count >= int64(q.config.Limit) {
			break
		}
		colsBuf = cols
	}
	if q.config.CountOnly {
		fmt.Fprintln(writer, count)
	}
	return nil
}
