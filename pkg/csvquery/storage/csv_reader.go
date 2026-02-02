package storage

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

// CSVReader defines the interface for reading CSV files
type CSVReader interface {
	io.Closer
	Read() ([]string, error)
	ReadAll() ([][]string, error)
	GetHeaders() ([]string, error)
}

// SimpleCSVReader is a standard implementation using encoding/csv
type SimpleCSVReader struct {
	file      *os.File
	reader    *csv.Reader
	headers   []string
	separator rune
}

// NewSimpleCSVReader creates a new reader for the given path
func NewSimpleCSVReader(path string, separator rune) (*SimpleCSVReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	reader := csv.NewReader(file)
	reader.Comma = separator
	reader.LazyQuotes = true

	return &SimpleCSVReader{
		file:      file,
		reader:    reader,
		separator: separator,
	}, nil
}

// Read reads a single record
func (r *SimpleCSVReader) Read() ([]string, error) {
	return r.reader.Read()
}

// ReadAll reads all remaining records
func (r *SimpleCSVReader) ReadAll() ([][]string, error) {
	return r.reader.ReadAll()
}

// GetHeaders reads the first line as headers or returns cached headers
func (r *SimpleCSVReader) GetHeaders() ([]string, error) {
	if r.headers != nil {
		return r.headers, nil
	}

	// Store current position
	currentPos, err := r.file.Seek(0, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}

	// Seek to start
	if _, err := r.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	// Read first line
	reader := csv.NewReader(r.file)
	reader.Comma = r.separator
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}
	r.headers = headers

	// Restore position
	if _, err := r.file.Seek(currentPos, 0); err != nil {
		return nil, fmt.Errorf("failed to restore position: %w", err)
	}

	return r.headers, nil
}

func (r *SimpleCSVReader) Close() error {
	return r.file.Close()
}
