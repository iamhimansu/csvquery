package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/iamhimansu/csvquery/go/internal/index"
	"github.com/iamhimansu/csvquery/go/internal/query"
	"github.com/iamhimansu/csvquery/go/internal/server"
	"github.com/iamhimansu/csvquery/go/internal/storage"
	"github.com/iamhimansu/csvquery/go/internal/types"
)

const (
	Version   = "1.2.0-modular"
	BuildDate = "2026-02-01"
)

var (
	shutdownChan = make(chan os.Signal, 1)
	cleanupFuncs []func()
)

func main() {
	setupSignalHandler()

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "index":
		runIndex(os.Args[2:])
	case "query":
		runQuery(os.Args[2:])
	case "daemon":
		runDaemon(os.Args[2:])
	case "write":
		runWrite(os.Args[2:])
	case "version":
		fmt.Printf("CsvQuery v%s (%s)\n", Version, BuildDate)
	case "help":
		printUsage()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func setupSignalHandler() {
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)
	go handleShutdown()
}

func handleShutdown() {
	<-shutdownChan
	fmt.Fprintln(os.Stderr, "\n⚠️  Received shutdown signal, cleaning up...")
	for i := len(cleanupFuncs) - 1; i >= 0; i-- {
		cleanupFuncs[i]()
	}
	fmt.Fprintln(os.Stderr, "✅ Cleanup complete")
	os.Exit(130)
}

func printUsage() {
	fmt.Println(`CsvQuery - High Performance CSV Indexer & Query Engine

Usage:
    csvquery <command> [arguments]

Commands:
    index    Create indexes from CSV
    query    Query CSV (using indexes if available)
    daemon   Start Unix Domain Socket server
    write    Append data to CSV
    version  Show version
    help     Show this help

Use "csvquery <command> --help" for command-specific options.`)
}

func runIndex(args []string) {
	fs := flag.NewFlagSet("index", flag.ExitOnError)
	input := fs.String("input", "", "Input CSV file path")
	output := fs.String("output", "", "Output directory for indexes")
	columns := fs.String("columns", "[]", "JSON array of columns to index")
	separator := fs.String("separator", ",", "CSV separator")
	workers := fs.Int("workers", runtime.NumCPU(), "Number of parallel workers")
	memoryMB := fs.Int("memory", 500, "Memory limit in MB per worker")
	bloomFP := fs.Float64("bloom", 0.01, "Bloom filter false positive rate")
	verbose := fs.Bool("verbose", false, "Enable verbose output")
	fs.Parse(args)

	if *input == "" {
		fmt.Fprintln(os.Stderr, "Error: --input is required")
		fs.PrintDefaults()
		os.Exit(1)
	}
	if *output == "" {
		*output = filepath.Dir(*input)
	}

	idx := index.NewIndexer(index.IndexerConfig{
		InputFile:   *input,
		OutputDir:   *output,
		Columns:     *columns,
		Separator:   *separator,
		Workers:     *workers,
		MemoryMB:    *memoryMB,
		BloomFPRate: *bloomFP,
		Verbose:     *verbose,
	})
	if err := idx.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runQuery(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	csvPath := fs.String("csv", "", "Path to CSV file")
	indexDir := fs.String("index-dir", "", "Directory containing index files")
	whereJSON := fs.String("where", "{}", "JSON object of conditions")
	limit := fs.Int("limit", 0, "Maximum results (0 = no limit)")
	offset := fs.Int("offset", 0, "Skip first N results")
	countOnly := fs.Bool("count", false, "Only output count")
	explain := fs.Bool("explain", false, "Explain query plan")
	groupBy := fs.String("group-by", "", "Column to group by")
	aggCol := fs.String("agg-col", "", "Column to aggregate")
	aggFunc := fs.String("agg-func", "", "Aggregation function")
	fs.Parse(args)

	if *indexDir == "" && *csvPath != "" {
		*indexDir = filepath.Dir(*csvPath)
	}
	if *indexDir == "" {
		fmt.Fprintln(os.Stderr, "Error: --index-dir or --csv is required")
		fs.PrintDefaults()
		os.Exit(1)
	}

	cond, err := query.ParseCondition([]byte(*whereJSON))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing --where JSON: %v\n", err)
		os.Exit(1)
	}

	engine := query.NewQueryEngine(types.QueryConfig{
		CsvPath:   *csvPath,
		IndexDir:  *indexDir,
		Limit:     *limit,
		Offset:    *offset,
		CountOnly: *countOnly,
		Explain:   *explain,
		GroupBy:   *groupBy,
		AggCol:    *aggCol,
		AggFunc:   *aggFunc,
	})
	if err := engine.Run(cond); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	}
}

func runDaemon(args []string) {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)
	socket := fs.String("socket", "/tmp/csvquery.sock", "Socket path")
	csvPath := fs.String("csv", "", "Path to CSV")
	indexDir := fs.String("index-dir", "", "Index directory")
	workers := fs.Int("workers", 50, "Max concurrency")
	fs.Parse(args)

	if err := server.RunDaemon(*socket, *csvPath, *indexDir, *workers); err != nil {
		fmt.Fprintf(os.Stderr, "Daemon Error: %v\n", err)
		os.Exit(1)
	}
}

func runWrite(args []string) {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	csvPath := fs.String("csv", "", "Path to CSV file")
	headersJSON := fs.String("headers", "[]", "JSON array of headers")
	dataJSON := fs.String("data", "[]", "JSON array of rows")
	separator := fs.String("separator", ",", "CSV separator")
	fs.Parse(args)

	if *csvPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --csv is required")
		os.Exit(1)
	}

	var headers []string
	json.Unmarshal([]byte(*headersJSON), &headers)
	var data [][]string
	json.Unmarshal([]byte(*dataJSON), &data)

	w := storage.NewCsvWriter(storage.WriterConfig{
		CsvPath:   *csvPath,
		Separator: *separator,
	})
	if err := w.Write(headers, data); err != nil {
		fmt.Fprintf(os.Stderr, "Write Error: %v\n", err)
		os.Exit(1)
	}
}
