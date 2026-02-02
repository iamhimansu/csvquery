package parser

import "io"

// Parser defines the interface for parsing CSV files
type Parser interface {
	io.Closer
	GetHeaders() []string
	GetColumnIndex(name string) (int, bool)
	ValidateColumns(columns []string) error

	// Scan iterates over the CSV file using SIMD acceleration where possible.
	// indexDefs specifies which columns to extract as keys for indexing or filtering.
	// handler is called for each record found.
	Scan(indexDefs [][]int, handler func(workerID int, keys [][]byte, offset, line int64)) error

	// SetWorkers sets the number of concurrent workers
	SetWorkers(n int)

	// GetStats returns scanning statistics
	GetStats() (rowsScanned int64, bytesRead int64)
}
