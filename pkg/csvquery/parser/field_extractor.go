package parser

import (
	"bytes"
)

// findSafeRecordBoundary scans forward from hint to find the next valid record start.
// It handles quoted newlines correctly.
func findSafeRecordBoundary(data []byte, hint int) int {
	pos := hint
	if pos >= len(data) {
		return len(data)
	}

	nextNL := bytes.IndexByte(data[pos:], '\n')
	if nextNL == -1 {
		return len(data)
	}
	pos += nextNL
	currentNL := pos

	for {
		if currentNL+1 >= len(data) {
			return len(data)
		}
		nextNL := bytes.IndexByte(data[currentNL+1:], '\n')
		if nextNL == -1 {
			return currentNL + 1
		}
		nextPos := currentNL + 1 + nextNL
		quotes := 0
		for i := currentNL + 1; i < nextPos; i++ {
			if data[i] == '"' {
				quotes++
			}
		}
		if quotes%2 == 0 {
			return currentNL + 1
		}
		currentNL = nextPos
	}
}
