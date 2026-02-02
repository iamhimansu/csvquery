package index

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/iamhimansu/csvquery/go/internal/storage"
)

type BloomFilter struct {
	bits      []byte
	size      int
	hashCount int
	count     int
}

func NewBloomFilter(n int, fpRate float64) *BloomFilter {
	if n < 1 {
		n = 1
	}
	if fpRate <= 0 {
		fpRate = 0.01
	}

	m := int(-float64(n) * ln(fpRate) / 0.4804)
	if m < 1024 {
		m = 1024
	}
	m = ((m + 7) / 8) * 8

	k := int(float64(m) / float64(n) * 0.693)
	if k < 1 {
		k = 1
	}
	if k > 10 {
		k = 10
	}

	return &BloomFilter{
		bits:      make([]byte, m/8),
		size:      m,
		hashCount: k,
		count:     0,
	}
}

func ln(x float64) float64 {
	if x == 0.01 {
		return -4.605
	}
	if x == 0.001 {
		return -6.907
	}
	result := 0.0
	for x > 1 {
		x /= 2.718
		result += 1
	}
	return result + (x - 1)
}

func (bf *BloomFilter) Add(key string) {
	keyBytes := []byte(key)
	h1 := crc32.ChecksumIEEE(keyBytes)

	var buf [256]byte
	reversed := appendReversed(buf[:0], keyBytes)
	reversed = append(reversed, "salt"...)
	h2 := crc32.ChecksumIEEE(reversed)

	for i := 0; i < bf.hashCount; i++ {
		combined := int(h1) + i*int(h2)
		if combined < 0 {
			combined = -combined
		}
		pos := combined % bf.size
		byteIdx := pos / 8
		bitIdx := pos % 8
		bf.bits[byteIdx] |= (1 << bitIdx)
	}
	bf.count++
}

func (bf *BloomFilter) MightContain(key string) bool {
	keyBytes := []byte(key)
	h1 := crc32.ChecksumIEEE(keyBytes)

	var buf [256]byte
	reversed := appendReversed(buf[:0], keyBytes)
	reversed = append(reversed, "salt"...)
	h2 := crc32.ChecksumIEEE(reversed)

	for i := 0; i < bf.hashCount; i++ {
		combined := int(h1) + i*int(h2)
		if combined < 0 {
			combined = -combined
		}
		pos := combined % bf.size
		byteIdx := pos / 8
		bitIdx := pos % 8
		if (bf.bits[byteIdx] & (1 << bitIdx)) == 0 {
			return false
		}
	}
	return true
}

func appendReversed(dst []byte, s []byte) []byte {
	start := len(dst)
	dst = append(dst, s...)
	for i, j := start, len(dst)-1; i < j; i, j = i+1, j-1 {
		dst[i], dst[j] = dst[j], dst[i]
	}
	return dst
}

func (bf *BloomFilter) Serialize() []byte {
	header := make([]byte, 24)
	binary.BigEndian.PutUint64(header[0:8], uint64(bf.size))
	binary.BigEndian.PutUint64(header[8:16], uint64(bf.hashCount))
	binary.BigEndian.PutUint64(header[16:24], uint64(bf.count))
	return append(header, bf.bits...)
}

func DeserializeBloom(data []byte) *BloomFilter {
	if len(data) < 24 {
		return nil
	}
	size := int(binary.BigEndian.Uint64(data[0:8]))
	hashCount := int(binary.BigEndian.Uint64(data[8:16]))
	count := int(binary.BigEndian.Uint64(data[16:24]))
	return &BloomFilter{
		bits:      data[24:],
		size:      size,
		hashCount: hashCount,
		count:     count,
	}
}

func LoadBloomFilter(path string) (*BloomFilter, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	bloom := DeserializeBloom(data)
	if bloom == nil {
		return nil, fmt.Errorf("invalid bloom filter data")
	}
	return bloom, nil
}

func LoadBloomFilterMmap(path string) (*BloomFilter, func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	data, err := storage.MmapFile(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	f.Close()
	bloom := DeserializeBloom(data)
	if bloom == nil {
		storage.MunmapFile(data)
		return nil, nil, fmt.Errorf("invalid bloom filter data")
	}
	cleanup := func() {
		storage.MunmapFile(data)
	}
	return bloom, cleanup, nil
}
