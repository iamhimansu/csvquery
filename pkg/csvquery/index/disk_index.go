package index

import (
	"bytes"
	"fmt"
	"os"

	"github.com/csvquery/csvquery/pkg/csvquery/types"
)

// DiskIndex implements Index using on-disk compressed blocks
type DiskIndex struct {
	path         string
	file         *os.File
	reader       *BlockReader
	bloom        *BloomFilter
	bloomCleanup func()
}

// OpenDiskIndex opens an existing index file
func OpenDiskIndex(path string) (*DiskIndex, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	br, err := NewBlockReader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to init block reader: %w", err)
	}

	idx := &DiskIndex{
		path:   path,
		file:   file,
		reader: br,
	}

	// Try loading bloom filter
	bloomPath := path + ".bloom"
	if _, err := os.Stat(bloomPath); err == nil {
		bloom, cleanup, err := LoadBloomFilterMmap(bloomPath)
		if err == nil {
			idx.bloom = bloom
			idx.bloomCleanup = cleanup
		}
	}

	return idx, nil
}

func (idx *DiskIndex) Search(key string) (Iterator, error) {
	if idx.bloom != nil {
		if !idx.bloom.MightContain(key) {
			return &emptyIterator{}, nil
		}
	}

	startBlockIdx := idx.findStartBlock(key)
	if startBlockIdx == -1 {
		return &emptyIterator{}, nil
	}

	return &diskIterator{
		idx:          idx,
		searchKey:    []byte(key),
		currentBlock: startBlockIdx,
		records:      nil,
		recordIndex:  0,
		totalBlocks:  len(idx.reader.Footer.Blocks),
	}, nil
}

func (idx *DiskIndex) Scan() (Iterator, error) {
	return &diskIterator{
		idx:          idx,
		scanMode:     true,
		currentBlock: 0,
		records:      nil,
		recordIndex:  0,
		totalBlocks:  len(idx.reader.Footer.Blocks),
	}, nil
}

func (idx *DiskIndex) Close() error {
	if idx.bloomCleanup != nil {
		idx.bloomCleanup()
	}
	return idx.file.Close()
}

func (idx *DiskIndex) ApproximateCount() int64 {
	var total int64
	for _, block := range idx.reader.Footer.Blocks {
		total += block.RecordCount
	}
	return total
}

func (idx *DiskIndex) findStartBlock(key string) int {
	blocks := idx.reader.Footer.Blocks
	left, right := 0, len(blocks)-1
	result := -1
	for left <= right {
		mid := (left + right) / 2
		if blocks[mid].StartKey <= key {
			result = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	if result == -1 {
		return -1
	}
	// Check previous blocks for duplicate start keys (rare but possible)
	targetKey := blocks[result].StartKey
	if targetKey == key {
		for result > 0 && blocks[result-1].StartKey == key {
			result--
		}
	}
	return result
}

// diskIterator iterates over results matching a key
type diskIterator struct {
	idx           *DiskIndex
	searchKey     []byte
	scanMode      bool
	currentBlock  int
	records       []types.IndexRecord
	recordIndex   int
	totalBlocks   int
	currentRecord types.IndexRecord
	err           error
	done          bool
}

func (it *diskIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}

	for {
		if it.recordIndex >= len(it.records) {
			if it.currentBlock >= it.totalBlocks {
				it.done = true
				return false
			}

			// Check if we should even read the next block
			blockMeta := it.idx.reader.Footer.Blocks[it.currentBlock]
			if !it.scanMode && blockMeta.StartKey > string(it.searchKey) {
				it.done = true
				return false
			}

			recs, err := it.idx.reader.ReadBlock(blockMeta)
			if err != nil {
				it.err = err
				return false
			}
			it.records = recs
			it.recordIndex = 0
			it.currentBlock++
		}

		if it.recordIndex < len(it.records) {
			rec := it.records[it.recordIndex]
			it.recordIndex++

			if it.scanMode {
				it.currentRecord = rec
				return true
			}

			cmp := compareRecordKey(&rec.Key, it.searchKey)

			if cmp < 0 {
				continue // Should not happen often given binary search, but safety
			}
			if cmp == 0 {
				it.currentRecord = rec
				return true
			}
			if cmp > 0 {
				it.done = true
				return false
			}
		}
	}
}

func (it *diskIterator) Record() types.IndexRecord {
	return it.currentRecord
}

func (it *diskIterator) Close() {
	it.records = nil
}

func (it *diskIterator) Error() error {
	return it.err
}

type emptyIterator struct{}

func (e *emptyIterator) Next() bool                { return false }
func (e *emptyIterator) Record() types.IndexRecord { return types.IndexRecord{} }
func (e *emptyIterator) Close()                    {}
func (e *emptyIterator) Error() error              { return nil }

func compareRecordKey(key *[64]byte, searchKey []byte) int {
	keyLen := 64
	for keyLen > 0 && key[keyLen-1] == 0 {
		keyLen--
	}
	return bytes.Compare(key[:keyLen], searchKey)
}
