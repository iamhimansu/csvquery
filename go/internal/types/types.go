package types

import "time"

/**
 * Core types for CsvQuery Go Engine
 */

type QueryRequest struct {
	Action  string                 `json:"action"`
	CSV     string                 `json:"csv"`
	Where   map[string]interface{} `json:"where"`
	Limit   int                    `json:"limit"`
	Offset  int                    `json:"offset"`
	GroupBy string                 `json:"groupBy"`
	AggCol  string                 `json:"aggCol"`
	AggFunc string                 `json:"aggFunc"`
}

type QueryResult struct {
	Status string      `json:"status"`
	Count  int         `json:"count,omitempty"`
	Rows   []RowOffset `json:"rows,omitempty"`
	Groups interface{} `json:"groups,omitempty"`
	Error  string      `json:"error,omitempty"`
	Stats  QueryStats  `json:"stats,omitempty"`
}

type RowOffset struct {
	Offset int64 `json:"offset"`
	Line   int64 `json:"line"`
}

type QueryStats struct {
	ExecutionTime string `json:"execution_time"`
	FetchingTime  string `json:"fetching_time"`
	TotalTime     string `json:"total_time"`
}

// Indexing Types

const RecordSize = 64 + 8 + 8 // Key(64) + Offset(8) + Line(8) = 80 bytes

type IndexRecord struct {
	Key    [64]byte `json:"key"`
	Offset int64    `json:"offset"`
	Line   int64    `json:"line"`
}

type IndexMeta struct {
	CapturedAt time.Time             `json:"capturedAt"`
	TotalRows  int64                 `json:"totalRows"`
	CsvSize    int64                 `json:"csvSize"`
	CsvMtime   int64                 `json:"csvMtime"`
	CsvHash    string                `json:"csvHash"`
	Indexes    map[string]IndexStats `json:"indexes"`
}

type IndexStats struct {
	DistinctCount int64 `json:"distinctCount"`
	FileSize      int64 `json:"fileSize"`
}
