package index

import "github.com/csvquery/csvquery/pkg/csvquery/types"

// Index defines the interface for an index lookup
type Index interface {
	// Search returns an iterator over records matching the key
	Search(key string) (Iterator, error)

	// Scan returns an iterator over all records in the index
	Scan() (Iterator, error)

	// Close releases resources
	Close() error

	// ApproximateCount returns an estimate of records for verification
	ApproximateCount() int64
}

// Iterator allows iterating over index results
type Iterator interface {
	Next() bool
	Record() types.IndexRecord
	Close()
	Error() error
}
