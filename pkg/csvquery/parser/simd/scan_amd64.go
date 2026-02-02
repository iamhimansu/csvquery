//go:build amd64

package simd

import (
	"unsafe"
)

// scanAVX2 scans data using AVX2 instructions.
//
//go:noescape
func scanAVX2(data unsafe.Pointer, len int, quotes, commas, newlines unsafe.Pointer) int

// scanSSE42 scans data using SSE4.2 instructions.
//
//go:noescape
func scanSSE42(data unsafe.Pointer, len int, quotes, commas, newlines unsafe.Pointer) int

// checkAVX2 checks if the CPU supports AVX2.
//
//go:noescape
func checkAVX2() bool

var useAVX2 bool

func init() {
	useAVX2 = checkAVX2()
}

func HasAVX2() bool {
	return useAVX2
}

func Scan(input []byte, quotes, commas, newlines []uint64) {
	if len(input) == 0 {
		return
	}

	pInput := unsafe.Pointer(&input[0])
	pQuotes := unsafe.Pointer(&quotes[0])
	pCommas := unsafe.Pointer(&commas[0])
	pNewlines := unsafe.Pointer(&newlines[0])
	size := len(input)

	processed := 0
	if useAVX2 {
		processed = scanAVX2(pInput, size, pQuotes, pCommas, pNewlines)
	} else {
		processed = scanSSE42(pInput, size, pQuotes, pCommas, pNewlines)
	}

	for i := processed; i < size; i++ {
		b := input[i]
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
	if len(input) == 0 {
		return
	}

	pInput := unsafe.Pointer(&input[0])
	pQuotes := unsafe.Pointer(&quotes[0])
	pNewlines := unsafe.Pointer(&newlines[0])
	size := len(input)

	processed := 0
	if useAVX2 {
		tempCommas := make([]uint64, len(seps))
		pTempCommas := unsafe.Pointer(&tempCommas[0])
		processed = scanAVX2(pInput, size, pQuotes, pTempCommas, pNewlines)
	} else {
		tempCommas := make([]uint64, len(seps))
		pTempCommas := unsafe.Pointer(&tempCommas[0])
		processed = scanSSE42(pInput, size, pQuotes, pTempCommas, pNewlines)
	}

	for i := processed; i < size; i++ {
		b := input[i]
		wordIdx := i / 64
		bitPos := uint(i % 64)
		if b == '"' {
			quotes[wordIdx] |= 1 << bitPos
		} else if b == '\n' {
			newlines[wordIdx] |= 1 << bitPos
		}
	}

	for i := 0; i < size; i++ {
		if input[i] == sep {
			wordIdx := i / 64
			bitPos := uint(i % 64)
			seps[wordIdx] |= 1 << bitPos
		}
	}
}
