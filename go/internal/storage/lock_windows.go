//go:build windows

package storage

import (
	"os"
)

// lockFile acquires an exclusive lock on the file
func lockFile(file *os.File) error {
	// TODO: Implement actual Windows locking if needed
	return nil
}

// unlockFile releases the lock
func unlockFile(file *os.File) error {
	return nil
}
