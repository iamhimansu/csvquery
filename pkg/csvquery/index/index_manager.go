package index

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/csvquery/csvquery/pkg/csvquery/parser"
	"github.com/csvquery/csvquery/pkg/csvquery/types"
)

type IndexerConfig struct {
	InputFile   string
	OutputDir   string
	Columns     string // JSON array of columns to index
	Separator   string
	Workers     int
	MemoryMB    int
	BloomFPRate float64
	Verbose     bool
}

type IndexManager struct {
	config      IndexerConfig
	colDefs     [][]string
	scanner     parser.Parser
	tempDir     string
	meta        types.IndexMeta
	metaMutex   sync.Mutex
	sorters     []*Sorter
	sorterMutex sync.RWMutex
	stopReport  chan struct{}
}

func NewIndexManager(config IndexerConfig) *IndexManager {
	return &IndexManager{
		config: config,
		meta: types.IndexMeta{
			Indexes: make(map[string]types.IndexStats),
		},
		stopReport: make(chan struct{}),
	}
}

func (idx *IndexManager) Run() error {
	if err := idx.parseColumns(); err != nil {
		return err
	}

	if err := os.MkdirAll(idx.config.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	idx.tempDir = filepath.Join(idx.config.OutputDir, ".csvquery_temp")
	if err := os.MkdirAll(idx.tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}

	var err error
	idx.scanner, err = parser.NewSIMDParser(idx.config.InputFile, idx.config.Separator)
	if err != nil {
		return err
	}
	if idx.config.Workers > 0 {
		idx.scanner.SetWorkers(idx.config.Workers)
	}
	defer idx.scanner.Close()

	for _, cols := range idx.colDefs {
		if err := idx.scanner.ValidateColumns(cols); err != nil {
			return err
		}
	}

	numIndexes := len(idx.colDefs)
	channels := make([]chan []types.IndexRecord, numIndexes)
	errors := make(chan error, numIndexes)
	results := make(chan string, numIndexes)

	var wg sync.WaitGroup
	// Skipping console reporting in library mode
	// idx.startReporting()
	// defer idx.stopReporting()

	for i, cols := range idx.colDefs {
		channels[i] = make(chan []types.IndexRecord, 100)
		wg.Add(1)
		go func(indexIdx int, columns []string, ch <-chan []types.IndexRecord) {
			defer wg.Done()
			colName := strings.ToLower(strings.Join(columns, "_"))
			err := idx.runSorterNode(colName, ch)
			if err != nil {
				errors <- fmt.Errorf("%s: %v", colName, err)
			} else {
				results <- colName
			}
		}(i, cols, channels[i])
	}

	colIndices := make([][]int, len(idx.colDefs))
	for i, cols := range idx.colDefs {
		colIndices[i] = make([]int, len(cols))
		for j, col := range cols {
			colIndices[i][j], _ = idx.scanner.GetColumnIndex(col)
		}
	}

	numWorkers := idx.config.Workers
	if numWorkers == 0 {
		numWorkers = runtime.NumCPU()
	}
	workerBuffers := make([][][]types.IndexRecord, numWorkers)
	const batchSize = 1000

	for w := 0; w < numWorkers; w++ {
		workerBuffers[w] = make([][]types.IndexRecord, numIndexes)
		for i := 0; i < numIndexes; i++ {
			workerBuffers[w][i] = make([]types.IndexRecord, 0, batchSize)
		}
	}

	err = idx.scanner.Scan(colIndices, func(workerID int, keys [][]byte, offset, line int64) {
		if workerID >= len(workerBuffers) {
			return
		}
		buffers := workerBuffers[workerID]
		for i, key := range keys {
			var keyBytes [64]byte
			copy(keyBytes[:], key)
			rec := types.IndexRecord{
				Key:    keyBytes,
				Offset: offset,
				Line:   line,
			}
			buffers[i] = append(buffers[i], rec)
			if len(buffers[i]) >= batchSize {
				batchToSend := buffers[i]
				channels[i] <- batchToSend
				buffers[i] = make([]types.IndexRecord, 0, batchSize)
			}
		}
	})

	for w := 0; w < numWorkers; w++ {
		for i := 0; i < numIndexes; i++ {
			if len(workerBuffers[w][i]) > 0 {
				channels[i] <- workerBuffers[w][i]
			}
		}
	}
	for _, ch := range channels {
		close(ch)
	}

	if err != nil {
		return fmt.Errorf("scanning failed: %w", err)
	}

	wg.Wait()
	close(results)
	close(errors)

	// Collect errors
	var errs []string
	for {
		select {
		case _, ok := <-results:
			if !ok {
				results = nil
			}
		case err, ok := <-errors:
			if !ok {
				errors = nil
			} else {
				errs = append(errs, err.Error())
			}
		}
		if results == nil && errors == nil {
			break
		}
	}

	rows, _ := idx.scanner.GetStats()
	idx.meta.TotalRows = rows

	if csvMeta, err := idx.calculateFingerprint(); err == nil {
		idx.meta.CsvSize = csvMeta.size
		idx.meta.CsvMtime = csvMeta.mtime
		idx.meta.CsvHash = csvMeta.hash
	}

	idx.Cleanup()
	if err := idx.saveMeta(); err != nil {
		errs = append(errs, fmt.Sprintf("failed to save metadata: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("indexing failed with errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (idx *IndexManager) runSorterNode(name string, ch <-chan []types.IndexRecord) error {
	csvName := strings.TrimSuffix(filepath.Base(idx.config.InputFile), filepath.Ext(idx.config.InputFile))
	indexPath := filepath.Join(idx.config.OutputDir, csvName+"_"+name+".cidx")
	bloomPath := indexPath + ".bloom"

	tempSortDir := filepath.Join(idx.tempDir, fmt.Sprintf("sort_%s", name))
	if err := os.MkdirAll(tempSortDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp sort dir: %w", err)
	}

	totalMemBytes := idx.config.MemoryMB * 1024 * 1024
	numIndexes := len(idx.colDefs)
	memoryPerIndex := totalMemBytes / numIndexes
	if memoryPerIndex < 10*1024*1024 {
		memoryPerIndex = 10 * 1024 * 1024
	}

	var bloom *BloomFilter
	if idx.config.BloomFPRate > 0 {
		bloom = NewBloomFilter(10_000_000, idx.config.BloomFPRate)
	}

	sorter := NewSorter(name, indexPath, tempSortDir, memoryPerIndex, bloom)
	idx.sorterMutex.Lock()
	idx.sorters = append(idx.sorters, sorter)
	idx.sorterMutex.Unlock()
	defer sorter.Cleanup()

	for batch := range ch {
		for _, rec := range batch {
			if err := sorter.Add(rec); err != nil {
				return err
			}
		}
	}

	distinctCount, err := sorter.Finalize()
	if err != nil {
		return err
	}

	stat, _ := os.Stat(indexPath)
	fileSize := stat.Size()
	idx.metaMutex.Lock()
	idx.meta.Indexes[name] = types.IndexStats{
		DistinctCount: distinctCount,
		FileSize:      fileSize,
	}
	idx.metaMutex.Unlock()

	if bloom != nil {
		if err := os.WriteFile(bloomPath, bloom.Serialize(), 0644); err != nil {
			return fmt.Errorf("bloom filter failed for %s: %w", name, err)
		}
	}
	return nil
}

func (idx *IndexManager) parseColumns() error {
	var raw interface{}
	if err := json.Unmarshal([]byte(idx.config.Columns), &raw); err != nil {
		return fmt.Errorf("failed to parse columns JSON: %w", err)
	}
	switch v := raw.(type) {
	case []interface{}:
		for _, item := range v {
			switch col := item.(type) {
			case string:
				idx.colDefs = append(idx.colDefs, []string{col})
			case []interface{}:
				var cols []string
				for _, c := range col {
					if s, ok := c.(string); ok {
						cols = append(cols, s)
					}
				}
				if len(cols) > 0 {
					idx.colDefs = append(idx.colDefs, cols)
				}
			}
		}
	default:
		return fmt.Errorf("columns must be a JSON array")
	}
	if len(idx.colDefs) == 0 {
		return fmt.Errorf("no valid column definitions found")
	}
	return nil
}

func (idx *IndexManager) saveMeta() error {
	idx.meta.CapturedAt = time.Now()
	data, err := json.MarshalIndent(idx.meta, "", "  ")
	if err != nil {
		return err
	}
	csvName := strings.TrimSuffix(filepath.Base(idx.config.InputFile), filepath.Ext(idx.config.InputFile))
	metaPath := filepath.Join(idx.config.OutputDir, csvName+"_meta.json")
	return os.WriteFile(metaPath, data, 0644)
}

type csvDNA struct {
	size  int64
	mtime int64
	hash  string
}

func (idx *IndexManager) calculateFingerprint() (csvDNA, error) {
	file, err := os.Open(idx.config.InputFile)
	if err != nil {
		return csvDNA{}, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return csvDNA{}, err
	}
	size := stat.Size()
	mtime := stat.ModTime().Unix()
	sampleSize := int64(512 * 1024)
	hasher := sha1.New()
	buf := make([]byte, sampleSize)
	n, _ := file.ReadAt(buf, 0)
	hasher.Write(buf[:n])
	if size > sampleSize*3 {
		n, _ = file.ReadAt(buf, (size/2)-(sampleSize/2))
		hasher.Write(buf[:n])
	}
	if size > sampleSize {
		start := size - sampleSize
		if start < 0 {
			start = 0
		}
		n, _ = file.ReadAt(buf, start)
		hasher.Write(buf[:n])
	}
	return csvDNA{
		size:  size,
		mtime: mtime,
		hash:  hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func (idx *IndexManager) Cleanup() {
	if idx.tempDir != "" {
		os.RemoveAll(idx.tempDir)
	}
}
