package types

const (
	// RecordSize is the size of an index record in bytes
	// Key(64) + Offset(8) + Line(8) = 80 bytes
	RecordSize = 64 + 8 + 8

	// KeySize is the size of the key in the index record
	KeySize = 64

	// MaxBatchSize is the maximum number of rows to process in a batch
	MaxBatchSize = 1000
)
