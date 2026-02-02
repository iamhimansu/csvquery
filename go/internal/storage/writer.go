package storage

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
)

// WriterConfig holds configuration for the writer
type WriterConfig struct {
	CsvPath   string
	Separator string
}

// CsvWriter handles writing to CSV files
type CsvWriter struct {
	config WriterConfig
}

// NewCsvWriter creates a new writer instance
func NewCsvWriter(config WriterConfig) *CsvWriter {
	if config.Separator == "" {
		config.Separator = ","
	}
	return &CsvWriter{config: config}
}

// Write appends rows to the CSV file.
func (w *CsvWriter) Write(headers []string, rows [][]string) error {
	dir := filepath.Dir(w.config.CsvPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	file, err := os.OpenFile(w.config.CsvPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	if err := lockFile(file); err != nil {
		return fmt.Errorf("failed to lock file: %v", err)
	}
	defer unlockFile(file)

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	csvW := csv.NewWriter(file)
	csvW.Comma = rune(w.config.Separator[0])

	if stat.Size() == 0 {
		if len(headers) == 0 {
			return fmt.Errorf("cannot create new file without headers")
		}
		if err := csvW.Write(headers); err != nil {
			return err
		}
	} else {
		if len(headers) > 0 {
			if _, err := file.Seek(0, 0); err != nil {
				return fmt.Errorf("failed to seek: %v", err)
			}

			reader := csv.NewReader(file)
			reader.Comma = rune(w.config.Separator[0])
			existingHeaders, err := reader.Read()
			if err != nil {
				return fmt.Errorf("failed to read existing headers: %v", err)
			}

			if !reflect.DeepEqual(existingHeaders, headers) {
				return fmt.Errorf("header mismatch. File: %v, New: %v", existingHeaders, headers)
			}
		}
	}

	if err := csvW.WriteAll(rows); err != nil {
		return err
	}

	csvW.Flush()
	return csvW.Error()
}
