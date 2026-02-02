package types

import "time"

// FilterOp represents a comparison operator
type FilterOp string

const (
	OpEq        FilterOp = "="
	OpNeq       FilterOp = "!="
	OpGt        FilterOp = ">"
	OpLt        FilterOp = "<"
	OpGte       FilterOp = ">="
	OpLte       FilterOp = "<="
	OpLike      FilterOp = "LIKE"
	OpIsNull    FilterOp = "IS NULL"
	OpIsNotNull FilterOp = "IS NOT NULL"
	OpIn        FilterOp = "IN"
)

// Condition represents a node in the filter tree
type Condition struct {
	Operator       FilterOp    `json:"operator"`
	Column         string      `json:"column,omitempty"`
	Value          interface{} `json:"value,omitempty"`
	Children       []Condition `json:"children,omitempty"`
	ResolvedTarget string      `json:"-"` // Internal use for optimization
}

// QueryRequest represents an incoming query
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

// QueryConfig holds configuration for the query engine
type QueryConfig struct {
	CsvPath   string
	IndexDir  string
	GroupBy   string
	AggCol    string
	AggFunc   string
	CountOnly bool
	Limit     int
	Offset    int
	Explain   bool
}

// QueryResult represents the response to a query
type QueryResult struct {
	Status string      `json:"status"`
	Count  int         `json:"count,omitempty"`
	Rows   []RowOffset `json:"rows,omitempty"`
	Groups interface{} `json:"groups,omitempty"`
	Error  string      `json:"error,omitempty"`
	Stats  QueryStats  `json:"stats,omitempty"`
}

// RowOffset defines a precise location of a row in the CSV file
type RowOffset struct {
	Offset int64 `json:"offset"`
	Line   int64 `json:"line"`
}

// QueryStats holds performance metrics for a query
type QueryStats struct {
	ExecutionTime string `json:"execution_time"`
	FetchingTime  string `json:"fetching_time"`
	TotalTime     string `json:"total_time"`
}

// IndexRecord represents a single entry in an index file
type IndexRecord struct {
	Key    [64]byte `json:"key"`
	Offset int64    `json:"offset"`
	Line   int64    `json:"line"`
}

// IndexMeta stores metadata about a specific CSV's indexes
type IndexMeta struct {
	CapturedAt time.Time             `json:"capturedAt"`
	TotalRows  int64                 `json:"totalRows"`
	CsvSize    int64                 `json:"csvSize"`
	CsvMtime   int64                 `json:"csvMtime"`
	CsvHash    string                `json:"csvHash"`
	Indexes    map[string]IndexStats `json:"indexes"`
}

// IndexStats provides summary statistics for a specific column index
type IndexStats struct {
	DistinctCount int64 `json:"distinctCount"`
	FileSize      int64 `json:"fileSize"`
}
