package storage

import (
	"encoding/binary"
	"io"

	"github.com/iamhimansu/csvquery/pkg/csvquery/types"
)

// ReadRecord reads a single IndexRecord into the provided pointer
func ReadRecord(reader io.Reader) (types.IndexRecord, error) {
	var buf [types.RecordSize]byte
	if _, err := io.ReadFull(reader, buf[:]); err != nil {
		return types.IndexRecord{}, err
	}

	return types.IndexRecord{
		Key:    *(*[64]byte)(buf[0:64]),
		Offset: int64(binary.BigEndian.Uint64(buf[64:72])),
		Line:   int64(binary.BigEndian.Uint64(buf[72:80])),
	}, nil
}

// ReadBatchRecords reads count records into a slice
func ReadBatchRecords(r io.Reader, count int) ([]types.IndexRecord, error) {
	totalBytes := count * types.RecordSize
	buf := make([]byte, totalBytes)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	recs := make([]types.IndexRecord, count)
	for i := 0; i < count; i++ {
		offset := i * types.RecordSize
		recs[i] = types.IndexRecord{
			Key:    *(*[64]byte)(buf[offset : offset+64]),
			Offset: int64(binary.BigEndian.Uint64(buf[offset+64 : offset+72])),
			Line:   int64(binary.BigEndian.Uint64(buf[offset+72 : offset+80])),
		}
	}
	return recs, nil
}

// WriteRecord writes a single IndexRecord to a writer
func WriteRecord(w io.Writer, rec types.IndexRecord) error {
	var buf [types.RecordSize]byte
	copy(buf[0:64], rec.Key[:])
	binary.BigEndian.PutUint64(buf[64:72], uint64(rec.Offset))
	binary.BigEndian.PutUint64(buf[72:80], uint64(rec.Line))
	_, err := w.Write(buf[:])
	return err
}

// WriteBatchRecords writes a slice of records in a single write call
func WriteBatchRecords(w io.Writer, recs []types.IndexRecord) error {
	if len(recs) == 0 {
		return nil
	}
	totalSize := len(recs) * types.RecordSize
	buf := make([]byte, totalSize)
	for i, rec := range recs {
		offset := i * types.RecordSize
		copy(buf[offset:offset+64], rec.Key[:])
		binary.BigEndian.PutUint64(buf[offset+64:offset+72], uint64(rec.Offset))
		binary.BigEndian.PutUint64(buf[offset+72:offset+80], uint64(rec.Line))
	}
	_, err := w.Write(buf)
	return err
}
