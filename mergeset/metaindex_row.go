package mergeset

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"io"
	"sort"
)

type metaindexRow struct {
	firstItem         []byte
	blockHeadersCount uint32
	indexBlockOffset  uint64
	indexBlockSize    uint32
}

func (mr *metaindexRow) Unmarshal(src []byte) ([]byte, error) {

	tail, fi, err := encoding.UnmarshalBytes(src)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	mr.firstItem = append(mr.firstItem[:0], fi...)
	src = tail

	if len(src) < 4 {
		fmt.Println(err)
		return nil, err
	}
	mr.blockHeadersCount = encoding.UnmarshalUint32(src)
	src = src[4:]

	if len(src) < 8 {
		return nil, err
	}
	mr.indexBlockOffset = encoding.UnmarshalUint64(src)
	src = src[8:]

	if len(src) < 4 {
		fmt.Println(err)
		return nil, err
	}
	mr.indexBlockSize = encoding.UnmarshalUint32(src)
	src = src[4:]

	return src, nil
}

func unmarshalMetaindexRows(dst []metaindexRow, r io.Reader) ([]metaindexRow, error) {
	// It is ok to read all the metaindex in memory,
	// since it is quite small.
	compressedData, err := io.ReadAll(r)
	if err != nil {
		return dst, fmt.Errorf("cannot read metaindex data: %w", err)
	}
	data, err := encoding.DecompressZSTD(nil, compressedData)
	if err != nil {
		return dst, fmt.Errorf("cannot decompress metaindex data: %w", err)
	}

	dstLen := len(dst)
	for len(data) > 0 {
		if len(dst) < cap(dst) {
			dst = dst[:len(dst)+1]
		} else {
			dst = append(dst, metaindexRow{})
		}
		mr := &dst[len(dst)-1]
		tail, err := mr.Unmarshal(data)
		if err != nil {
			return dst, fmt.Errorf("cannot unmarshal metaindexRow #%d from metaindex data: %w", len(dst)-dstLen, err)
		}
		data = tail
	}
	if dstLen == len(dst) {
		return dst, fmt.Errorf("expecting non-zero metaindex rows; got zero")
	}

	// Make sure metaindexRows are sorted by firstItem.
	tmp := dst[dstLen:]
	ok := sort.SliceIsSorted(tmp, func(i, j int) bool {
		return string(tmp[i].firstItem) < string(tmp[j].firstItem)
	})
	if !ok {
		return dst, fmt.Errorf("metaindex %d rows aren't sorted by firstItem", len(tmp))
	}

	return dst, nil
}

func (mr *metaindexRow) Marshal(dst []byte) []byte {
	dst = encoding.MarshalBytes(dst, mr.firstItem)
	dst = encoding.MarshalUint32(dst, mr.blockHeadersCount)
	dst = encoding.MarshalUint64(dst, mr.indexBlockOffset)
	dst = encoding.MarshalUint32(dst, mr.indexBlockSize)
	return dst
}

func (mr *metaindexRow) Reset() {
	mr.firstItem = mr.firstItem[:0]
	mr.blockHeadersCount = 0
	mr.indexBlockOffset = 0
	mr.indexBlockSize = 0
}