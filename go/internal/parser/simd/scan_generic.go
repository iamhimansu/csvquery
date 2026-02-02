//go:build !amd64

package simd

// HasAVX2 returns false on non-AMD64 platforms.
func HasAVX2() bool {
	return false
}

func Scan(input []byte, quotes, commas, newlines []uint64) {
	for i, b := range input {
		wordIdx := i / 64
		bitPos := uint(i % 64)
		if b == '"' {
			quotes[wordIdx] |= 1 << bitPos
		} else if b == ',' {
			commas[wordIdx] |= 1 << bitPos
		} else if b == '\n' {
			newlines[wordIdx] |= 1 << bitPos
		}
	}
}

func ScanWithSeparator(input []byte, sep byte, quotes, seps, newlines []uint64) {
	for i, b := range input {
		wordIdx := i / 64
		bitPos := uint(i % 64)
		if b == '"' {
			quotes[wordIdx] |= 1 << bitPos
		} else if b == sep {
			seps[wordIdx] |= 1 << bitPos
		} else if b == '\n' {
			newlines[wordIdx] |= 1 << bitPos
		}
	}
}
