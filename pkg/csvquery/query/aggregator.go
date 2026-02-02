package query

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"

	"github.com/iamhimansu/csvquery/pkg/csvquery/types"
)

type Aggregator struct {
	config types.QueryConfig
}

func NewAggregator(config types.QueryConfig) *Aggregator {
	return &Aggregator{config: config}
}

// Run computes aggregations on the filtered result set
func (a *Aggregator) Run(rows []map[string]string, writer io.Writer) error {
	results := make(map[string]float64)
	counts := make(map[string]int64)

	groupKey := strings.ToLower(a.config.GroupBy)
	aggCol := strings.ToLower(a.config.AggCol)
	isCountOnly := a.config.AggFunc == "count"

	for _, row := range rows {
		groupVal := row[groupKey]

		var val float64
		if !isCountOnly && aggCol != "" {
			if v, ok := row[aggCol]; ok {
				val, _ = strconv.ParseFloat(v, 64)
			}
		}

		switch a.config.AggFunc {
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
	}

	if a.config.AggFunc == "avg" {
		for k, v := range results {
			if c := counts[k]; c > 0 {
				results[k] = v / float64(c)
			}
		}
	}

	return json.NewEncoder(writer).Encode(results)
}

// StreamAggregator is a stateful aggregator for streaming processing
type StreamAggregator struct {
	config  types.QueryConfig
	results map[string]float64
	counts  map[string]int64
}

func NewStreamAggregator(config types.QueryConfig) *StreamAggregator {
	return &StreamAggregator{
		config:  config,
		results: make(map[string]float64),
		counts:  make(map[string]int64),
	}
}

func (sa *StreamAggregator) Add(groupVal string, val float64) {
	switch sa.config.AggFunc {
	case "count":
		sa.results[groupVal]++
	case "sum":
		sa.results[groupVal] += val
	case "min":
		if curr, ok := sa.results[groupVal]; !ok || val < curr {
			sa.results[groupVal] = val
		}
	case "max":
		if curr, ok := sa.results[groupVal]; !ok || val > curr {
			sa.results[groupVal] = val
		}
	case "avg":
		sa.results[groupVal] += val
		sa.counts[groupVal]++
	case "":
		sa.results[groupVal] = 1
	}
}

func (sa *StreamAggregator) Finalize(writer io.Writer) error {
	if sa.config.AggFunc == "avg" {
		for k, v := range sa.results {
			if c := sa.counts[k]; c > 0 {
				sa.results[k] = v / float64(c)
			}
		}
	}
	return json.NewEncoder(writer).Encode(sa.results)
}
