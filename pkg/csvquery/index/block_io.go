package index

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/csvquery/csvquery/pkg/csvquery/storage"
	"github.com/csvquery/csvquery/pkg/csvquery/types"
	"github.com/pierrec/lz4/v4"
)

const (
	MagicCIDX       = "CIDX"
	BlockTargetSize = 64 * 1024
)

type BlockMeta struct {
	StartKey    string `json:"startKey"`
	Offset      int64  `json:"offset"`
	Length      int64  `json:"length"`
	RecordCount int64  `json:"recordCount"`
	IsDistinct  bool   `json:"isDistinct"`
}

type SparseIndex struct {
	Blocks []BlockMeta `json:"blocks"`
}

type BlockWriter struct {
	w           io.Writer
	buffer      []types.IndexRecord
	currentSize int
	sparseIndex SparseIndex
	offset      int64
	lw          *lz4.Writer
	rawBuf      bytes.Buffer
	compBuf     bytes.Buffer
}

func NewBlockWriter(w io.Writer) (*BlockWriter, error) {
	n, err := w.Write([]byte(MagicCIDX))
	if err != nil {
		return nil, err
	}
	lw := lz4.NewWriter(io.Discard)
	_ = lw.Apply(lz4.BlockSizeOption(lz4.Block64Kb))

	return &BlockWriter{
		w:      w,
		buffer: make([]types.IndexRecord, 0, 1000),
		offset: int64(n),
		lw:     lw,
	}, nil
}

func (bw *BlockWriter) WriteRecord(rec types.IndexRecord) error {
	bw.buffer = append(bw.buffer, rec)
	bw.currentSize += len(rec.Key) + 16
	if bw.currentSize >= BlockTargetSize {
		return bw.FlushBlock()
	}
	return nil
}

func (bw *BlockWriter) FlushBlock() error {
	if len(bw.buffer) == 0 {
		return nil
	}

	bw.rawBuf.Reset()
	if err := storage.WriteBatchRecords(&bw.rawBuf, bw.buffer); err != nil {
		return err
	}

	bw.compBuf.Reset()
	bw.lw.Reset(&bw.compBuf)
	if _, err := bw.lw.Write(bw.rawBuf.Bytes()); err != nil {
		return err
	}
	if err := bw.lw.Close(); err != nil {
		return err
	}
	compressedBytes := bw.compBuf.Bytes()

	keyStr := string(bytes.TrimRight(bw.buffer[0].Key[:], "\x00"))
	isDistinct := true
	if len(bw.buffer) > 1 {
		firstKey := bw.buffer[0].Key
		for i := 1; i < len(bw.buffer); i++ {
			if firstKey != bw.buffer[i].Key {
				isDistinct = false
				break
			}
		}
	}

	meta := BlockMeta{
		StartKey:    keyStr,
		Offset:      bw.offset,
		Length:      int64(len(compressedBytes)),
		RecordCount: int64(len(bw.buffer)),
		IsDistinct:  isDistinct,
	}
	bw.sparseIndex.Blocks = append(bw.sparseIndex.Blocks, meta)

	n, err := bw.w.Write(compressedBytes)
	if err != nil {
		return err
	}
	bw.offset += int64(n)

	bw.buffer = bw.buffer[:0]
	bw.currentSize = 0
	return nil
}

func (bw *BlockWriter) Close() error {
	if err := bw.FlushBlock(); err != nil {
		return err
	}

	footerBytes, err := json.Marshal(bw.sparseIndex)
	if err != nil {
		return err
	}

	n, err := bw.w.Write(footerBytes)
	if err != nil {
		return err
	}

	if err := binary.Write(bw.w, binary.BigEndian, int64(n)); err != nil {
		return err
	}

	return nil
}

type BlockReader struct {
	r       io.ReadSeeker
	Footer  SparseIndex
	compBuf []byte
	recBuf  []types.IndexRecord
}

func NewBlockReader(r io.ReadSeeker) (*BlockReader, error) {
	if _, err := r.Seek(-8, io.SeekEnd); err != nil {
		return nil, err
	}

	var footerLen int64
	if err := binary.Read(r, binary.BigEndian, &footerLen); err != nil {
		return nil, err
	}

	if _, err := r.Seek(-(8 + footerLen), io.SeekEnd); err != nil {
		return nil, err
	}

	footerBytes := make([]byte, footerLen)
	if _, err := io.ReadFull(r, footerBytes); err != nil {
		return nil, err
	}

	var footer SparseIndex
	if err := json.Unmarshal(footerBytes, &footer); err != nil {
		return nil, err
	}

	return &BlockReader{
		r:      r,
		Footer: footer,
	}, nil
}

func (br *BlockReader) ReadBlock(meta BlockMeta) ([]types.IndexRecord, error) {
	if _, err := br.r.Seek(meta.Offset, io.SeekStart); err != nil {
		return nil, err
	}

	needed := int(meta.Length)
	if cap(br.compBuf) < needed {
		br.compBuf = make([]byte, needed)
	}
	br.compBuf = br.compBuf[:needed]

	if _, err := io.ReadFull(br.r, br.compBuf); err != nil {
		return nil, err
	}

	lr := lz4.NewReader(bytes.NewReader(br.compBuf))
	br.recBuf = br.recBuf[:0]
	for {
		rec, err := storage.ReadRecord(lr)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		br.recBuf = append(br.recBuf, rec)
	}

	return br.recBuf, nil
}
