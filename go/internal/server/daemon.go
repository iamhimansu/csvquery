package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/iamhimansu/csvquery/go/internal/query"
	"github.com/iamhimansu/csvquery/go/internal/storage"
	"github.com/iamhimansu/csvquery/go/internal/types"
)

type DaemonConfig struct {
	SocketPath     string
	CsvPath        string
	IndexDir       string
	MaxConcurrency int
	IdleTimeout    time.Duration
}

type UDSDaemon struct {
	config   DaemonConfig
	listener net.Listener
	sem      chan struct{}
	shutdown chan struct{}
	wg       sync.WaitGroup

	csvData   []byte
	headers   []string
	headerMap map[string]int
	separator byte

	mu sync.RWMutex
}

func NewUDSDaemon(cfg DaemonConfig) *UDSDaemon {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 50
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 30 * time.Second
	}
	if cfg.SocketPath == "" {
		cfg.SocketPath = "/tmp/csvquery.sock"
	}

	return &UDSDaemon{
		config:   cfg,
		sem:      make(chan struct{}, cfg.MaxConcurrency),
		shutdown: make(chan struct{}),
	}
}

func (d *UDSDaemon) Start() error {
	if _, err := os.Stat(d.config.SocketPath); err == nil {
		if err := os.Remove(d.config.SocketPath); err != nil {
			return fmt.Errorf("failed to remove stale socket: %w", err)
		}
	}
	if d.config.CsvPath != "" {
		if err := d.loadCSV(); err != nil {
			return fmt.Errorf("failed to load CSV: %w", err)
		}
	}
	listener, err := net.Listen("unix", d.config.SocketPath)
	if err != nil {
		return fmt.Errorf("failed to bind socket %s: %w", d.config.SocketPath, err)
	}
	d.listener = listener
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigChan
		d.Shutdown()
	}()
	fmt.Printf("CsvQuery Daemon started on %s\n", d.config.SocketPath)
	for {
		select {
		case <-d.shutdown:
			return nil
		default:
		}
		if ul, ok := listener.(*net.UnixListener); ok {
			ul.SetDeadline(time.Now().Add(1 * time.Second))
		}
		conn, err := listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			select {
			case <-d.shutdown:
				return nil
			default:
				fmt.Fprintf(os.Stderr, "Accept error: %v\n", err)
				continue
			}
		}
		d.wg.Add(1)
		go d.handleConnection(conn)
	}
}

func (d *UDSDaemon) Shutdown() {
	close(d.shutdown)
	if d.listener != nil {
		d.listener.Close()
	}
	d.wg.Wait()
	os.Remove(d.config.SocketPath)
	fmt.Println("Daemon shutdown complete")
}

func (d *UDSDaemon) loadCSV() error {
	f, err := os.Open(d.config.CsvPath)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := storage.MmapFile(f)
	if err != nil {
		return err
	}
	d.separator = ','
	nlIdx := bytes.IndexByte(data, '\n')
	if nlIdx == -1 {
		return fmt.Errorf("no newline found in CSV")
	}
	headerLine := string(data[:nlIdx])
	headerLine = strings.TrimSuffix(headerLine, "\r")
	d.headers = strings.Split(headerLine, string(d.separator))
	d.headerMap = make(map[string]int, len(d.headers))
	for i, h := range d.headers {
		d.headerMap[strings.ToLower(strings.TrimSpace(h))] = i
	}
	d.csvData = data
	return nil
}

func (d *UDSDaemon) countRows() int {
	if d.csvData == nil {
		return 0
	}
	count := 0
	for _, b := range d.csvData {
		if b == '\n' {
			count++
		}
	}
	return count - 1
}

func (d *UDSDaemon) handleConnection(conn net.Conn) {
	defer d.wg.Done()
	defer conn.Close()
	select {
	case d.sem <- struct{}{}:
		defer func() { <-d.sem }()
	case <-d.shutdown:
		return
	}
	reader := bufio.NewReader(conn)
	for {
		select {
		case <-d.shutdown:
			return
		default:
		}
		conn.SetReadDeadline(time.Now().Add(d.config.IdleTimeout))
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		response := d.processRequest(line)
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		conn.Write(response)
		conn.Write([]byte("\n"))
	}
}

type DaemonRequest struct {
	Action  string            `json:"action"`
	Csv     string            `json:"csv,omitempty"`
	Where   map[string]string `json:"where,omitempty"`
	Column  string            `json:"column,omitempty"`
	AggFunc string            `json:"aggFunc,omitempty"`
	Limit   int               `json:"limit,omitempty"`
	Offset  int               `json:"offset,omitempty"`
	GroupBy string            `json:"groupBy,omitempty"`
	Verbose bool              `json:"verbose,omitempty"`
}

func (d *UDSDaemon) processRequest(data []byte) []byte {
	var req DaemonRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return d.errorResponse("invalid JSON: " + err.Error())
	}
	switch req.Action {
	case "ping":
		return d.successResponse(map[string]interface{}{"pong": true})
	case "count":
		return d.handleCount(req)
	case "select":
		return d.handleSelect(req)
	case "groupby":
		return d.handleGroupBy(req)
	case "status":
		return d.handleStatus()
	default:
		return d.errorResponse("unknown action: " + req.Action)
	}
}

func (d *UDSDaemon) handleCount(req DaemonRequest) []byte {
	csvPath := req.Csv
	if csvPath == "" {
		csvPath = d.config.CsvPath
	}
	whereJSON, _ := json.Marshal(req.Where)
	cond, _ := query.ParseCondition(whereJSON)
	cfg := types.QueryConfig{
		CsvPath:   csvPath,
		IndexDir:  d.config.IndexDir,
		CountOnly: true,
		Verbose:   req.Verbose,
	}
	var outBuf bytes.Buffer
	engine := query.NewQueryEngine(cfg)
	engine.Writer = &outBuf
	if err := engine.Run(cond); err != nil {
		return d.errorResponse(err.Error())
	}
	countStr := strings.TrimSpace(outBuf.String())
	var count int
	fmt.Sscanf(countStr, "%d", &count)
	return d.successResponse(map[string]interface{}{"count": count})
}

func (d *UDSDaemon) handleSelect(req DaemonRequest) []byte {
	csvPath := req.Csv
	if csvPath == "" {
		csvPath = d.config.CsvPath
	}
	whereJSON, _ := json.Marshal(req.Where)
	cond, _ := query.ParseCondition(whereJSON)
	cfg := types.QueryConfig{
		CsvPath:  csvPath,
		IndexDir: d.config.IndexDir,
		Limit:    req.Limit,
		Offset:   req.Offset,
		Verbose:  req.Verbose,
	}
	var outBuf bytes.Buffer
	engine := query.NewQueryEngine(cfg)
	engine.Writer = &outBuf
	if err := engine.Run(cond); err != nil {
		return d.errorResponse(err.Error())
	}
	result := strings.TrimSpace(outBuf.String())
	lines := strings.Split(result, "\n")
	offsets := make([]map[string]interface{}, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) >= 2 {
			var offset, lineNum int
			fmt.Sscanf(parts[0], "%d", &offset)
			fmt.Sscanf(parts[1], "%d", &lineNum)
			offsets = append(offsets, map[string]interface{}{"offset": offset, "line": lineNum})
		}
	}
	return d.successResponse(map[string]interface{}{"rows": offsets})
}

func (d *UDSDaemon) handleGroupBy(req DaemonRequest) []byte {
	csvPath := req.Csv
	if csvPath == "" {
		csvPath = d.config.CsvPath
	}
	whereJSON, _ := json.Marshal(req.Where)
	cond, _ := query.ParseCondition(whereJSON)
	groupCol := req.GroupBy
	if groupCol == "" {
		groupCol = req.Column
	}
	aggFunc := req.AggFunc
	if aggFunc == "" {
		aggFunc = "count"
	}
	cfg := types.QueryConfig{
		CsvPath:  csvPath,
		IndexDir: d.config.IndexDir,
		GroupBy:  groupCol,
		AggFunc:  aggFunc,
		Verbose:  req.Verbose,
	}
	var outBuf bytes.Buffer
	engine := query.NewQueryEngine(cfg)
	engine.Writer = &outBuf
	if err := engine.Run(cond); err != nil {
		return d.errorResponse(err.Error())
	}
	var groups map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(outBuf.String())), &groups); err != nil {
		return d.errorResponse("failed to parse groupby result: " + err.Error())
	}
	return d.successResponse(map[string]interface{}{"groups": groups})
}

func (d *UDSDaemon) handleStatus() []byte {
	return d.successResponse(map[string]interface{}{
		"status":     "running",
		"csv":        d.config.CsvPath,
		"indexDir":   d.config.IndexDir,
		"rows":       d.countRows(),
		"columns":    len(d.headers),
		"socketPath": d.config.SocketPath,
	})
}

func (d *UDSDaemon) errorResponse(msg string) []byte {
	b, _ := json.Marshal(map[string]interface{}{"error": msg})
	return b
}

func (d *UDSDaemon) successResponse(data map[string]interface{}) []byte {
	data["error"] = nil
	b, _ := json.Marshal(data)
	return b
}

func RunDaemon(socketPath, csvPath, indexDir string, maxConcurrency int) error {
	cfg := DaemonConfig{
		SocketPath:     socketPath,
		CsvPath:        csvPath,
		IndexDir:       indexDir,
		MaxConcurrency: maxConcurrency,
	}
	if csvPath != "" {
		if _, err := os.Stat(csvPath); os.IsNotExist(err) {
			return fmt.Errorf("CSV file not found: %s", csvPath)
		}
	}
	daemon := NewUDSDaemon(cfg)
	return daemon.Start()
}
