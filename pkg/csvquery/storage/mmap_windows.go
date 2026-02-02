//go:build windows
// +build windows

package storage

import (
	"io"
	"os"
)

// MmapFile memory maps a file (Fallback to ReadAll on Windows for now)
func MmapFile(f *os.File) ([]byte, error) {
	return io.ReadAll(f)
}

// MunmapFile unmaps the memory (No-op for ReadAll)
func MunmapFile(data []byte) error {
	return nil
}
