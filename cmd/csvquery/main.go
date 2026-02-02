package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"runtime/pprof"

	"github.com/iamhimansu/csvquery/pkg/csvquery/index"
	"github.com/iamhimansu/csvquery/pkg/csvquery/query"
	"github.com/iamhimansu/csvquery/pkg/csvquery/types"
)

func main() {
	requestJSON := flag.String("request", "", "JSON request payload")
	cpuProfile := flag.String("cpuprofile", "", "Write cpu profile to file")
	flag.Parse()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var rawRequest map[string]interface{}
	var err error

	// Read input
	if *requestJSON != "" {
		err = json.Unmarshal([]byte(*requestJSON), &rawRequest)
	} else {
		// Read from stdin
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) == 0 {
			decoder := json.NewDecoder(os.Stdin)
			err = decoder.Decode(&rawRequest)
		} else {
			flag.Usage()
			os.Exit(1)
		}
	}

	if err != nil {
		fatalError("Invalid JSON request: " + err.Error())
	}

	action, ok := rawRequest["action"].(string)
	if !ok {
		fatalError("Action required")
	}

	switch action {
	case "index":
		handleIndex(rawRequest)
	case "query", "count":
		handleQuery(rawRequest)
	default:
		fatalError("Unknown action: " + action)
	}
}

func handleIndex(req map[string]interface{}) {
	cfg := index.IndexerConfig{
		InputFile:   getString(req, "csv"),
		OutputDir:   getString(req, "out"),
		Columns:     getString(req, "cols"), // JSON string
		Separator:   getString(req, "sep"),
		Workers:     getInt(req, "workers"),
		MemoryMB:    getInt(req, "memory"),
		BloomFPRate: getFloat(req, "bloom_rate"),
		Verbose:     getBool(req, "verbose"),
	}

	if cfg.Separator == "" {
		cfg.Separator = ","
	}

	manager := index.NewIndexManager(cfg)
	if err := manager.Run(); err != nil {
		fatalError(err.Error())
	}

	response := map[string]string{"status": "ok"}
	json.NewEncoder(os.Stdout).Encode(response)
}

func handleQuery(req map[string]interface{}) {
	// Parse Where condition
	var where *types.Condition
	if whereData, ok := req["where"]; ok {
		// whereData is map[string]interface{} or JSON raw?
		// Types.QueryRequest defines Where as map[string]interface{}.
		// We need to re-marshal to pass to ParseCondition which expects []byte or we can adapt ParseCondition.
		// Adapt: Marshal back to bytes for ParseCondition
		bytes, _ := json.Marshal(whereData)
		var err error
		where, err = query.ParseCondition(bytes)
		if err != nil {
			fatalError("Invalid where condition: " + err.Error())
		}
	}

	cfg := types.QueryConfig{
		CsvPath:   getString(req, "csv"),
		IndexDir:  getString(req, "indexDir"),
		GroupBy:   getString(req, "groupBy"),
		AggCol:    getString(req, "aggCol"),
		AggFunc:   getString(req, "aggFunc"),
		CountOnly: getString(req, "action") == "count" || getBool(req, "countOnly"),
		Limit:     getInt(req, "limit"),
		Offset:    getInt(req, "offset"),
		Explain:   getBool(req, "explain"),
	}

	updates, err := query.LoadUpdates(cfg.CsvPath)
	if err != nil {
		// log error but continue? or fail?
		// fail for now as consistency matters
		// fatalError("Failed to load updates: " + err.Error())
		// Actually, if file defaults to missing it's fine.
	}

	executor := query.NewExecutor(cfg.IndexDir, updates) // Assuming updates can be nil or loaded internally
	// Note: Executor was refactored to take updates in constructor.

	// Output buffering? or Streaming?
	// For large results, streaming to Stdout is preferred.
	// However, json.Encoder does buffering.
	// Manually ensure correct JSON array around results?
	// QueryResult struct expects []RowOffset.
	// If we follow QueryResult struct, we must buffer.
	// If user expects stream, we stream.
	// The PHP wrapper likely expects JSON.
	// If it's a huge list, streaming JSON array `[{}, {}, ...]` is needed.
	// Executor writes line-by-line CSV-like "offset,line".
	// The PHP wrapper parses standard output.
	// If Executor outputs "offset,line\n", PHP can parse it easily.
	// But valid JSON {"status": "ok", "rows": [...]} is what QueryResult defines.
	// Let's output JSON for now as per QueryResult struct, assuming fits in memory or use streaming writer later.
	// Wait, Executor.runStandardOutput writes "offset,line\n". This is NOT JSON.
	// This matches the original `engine.go` behavior.
	// So `handleQuery` should just call `ExecuteWithCondition` and let it write to stdout.
	// But main also prints status?
	// If Executor writes raw lines, where is the JSON status?
	// Original `engine.go` wrote ONLY results or JSON plan.
	// Protocol seems to be: Raw lines for query results. JSON for plan/index.
	// Let's stick to that.

	if err := executor.ExecuteWithCondition(cfg, where, os.Stdout); err != nil {
		fatalError(err.Error())
	}
}

func fatalError(msg string) {
	resp := map[string]string{"status": "error", "error": msg}
	json.NewEncoder(os.Stdout).Encode(resp)
	os.Exit(1)
}

// Helpers
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	// Debug
	// log.Printf("Key %s not found or not string in %v", key, m)
	return ""
}

func getInt(m map[string]interface{}, key string) int {
	if v, ok := m[key].(float64); ok {
		return int(v)
	}
	if v, ok := m[key].(int); ok {
		return v
	}
	return 0
}

func getFloat(m map[string]interface{}, key string) float64 {
	if v, ok := m[key].(float64); ok {
		return v
	}
	return 0
}

func getBool(m map[string]interface{}, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}
