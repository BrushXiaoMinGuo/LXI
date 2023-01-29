package mergeset

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"sort"
)

type blockHeader struct {
	commonPrefix []byte

	firstItem []byte

	noCopy bool

	marshalType marshalType

	itemsCount uint32

	itemsBlockOffset uint64

	lensBlockOffset uint64

	itemsBlockSize uint32

	lensBlockSize uint32
}

type marshalType uint8

func unmarshalBlockHeadersNoCopy(dst []blockHeader, src []byte, blockHeadersCount int) ([]blockHeader, error) {
	if blockHeadersCount <= 0 {
		logger.Panicf("BUG: blockHeadersCount must be greater than 0; got %d", blockHeadersCount)
	}
	dstLen := len(dst)
	if n := dstLen + blockHeadersCount - cap(dst); n > 0 {
		dst = append(dst[:cap(dst)], make([]blockHeader, n)...)
	}
	dst = dst[:dstLen+blockHeadersCount]
	for i := 0; i < blockHeadersCount; i++ {
		tail, err := dst[dstLen+i].UnmarshalNoCopy(src)
		if err != nil {
			return dst, fmt.Errorf("cannot unmarshal block header #%d out of %d: %w", i, blockHeadersCount, err)
		}
		src = tail
	}
	if len(src) > 0 {
		return dst, fmt.Errorf("unexpected non-zero tail left after unmarshaling %d block headers; len(tail)=%d", blockHeadersCount, len(src))
	}
	newBHS := dst[dstLen:]

	// Verify that block headers are sorted by firstItem.
	if !sort.SliceIsSorted(newBHS, func(i, j int) bool { return string(newBHS[i].firstItem) < string(newBHS[j].firstItem) }) {
		return dst, fmt.Errorf("block headers must be sorted by firstItem; unmarshaled unsorted block headers: %#v", newBHS)
	}

	return dst, nil
}


func (bh *blockHeader) UnmarshalNoCopy(src []byte) ([]byte, error) {
	bh.noCopy = true
	// Unmarshal commonPrefix
	tail, cp, err := encoding.UnmarshalBytes(src)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal commonPrefix: %w", err)
	}
	bh.commonPrefix = cp[:len(cp):len(cp)]
	src = tail

	// Unmarshal firstItem
	tail, fi, err := encoding.UnmarshalBytes(src)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal firstItem: %w", err)
	}
	bh.firstItem = fi[:len(fi):len(fi)]
	src = tail

	// Unmarshal marshalType
	if len(src) == 0 {
		return src, fmt.Errorf("cannot unmarshal marshalType from zero bytes")
	}
	bh.marshalType = marshalType(src[0])
	src = src[1:]


	// Unmarshal itemsCount
	if len(src) < 4 {
		return src, fmt.Errorf("cannot unmarshal itemsCount from %d bytes; need at least %d bytes", len(src), 4)
	}
	bh.itemsCount = encoding.UnmarshalUint32(src)
	src = src[4:]

	// Unmarshal itemsBlockOffset
	if len(src) < 8 {
		return src, fmt.Errorf("cannot unmarshal itemsBlockOffset from %d bytes; neet at least %d bytes", len(src), 8)
	}
	bh.itemsBlockOffset = encoding.UnmarshalUint64(src)
	src = src[8:]

	// Unmarshal lensBlockOffset
	if len(src) < 8 {
		return src, fmt.Errorf("cannot unmarshal lensBlockOffset from %d bytes; need at least %d bytes", len(src), 8)
	}
	bh.lensBlockOffset = encoding.UnmarshalUint64(src)
	src = src[8:]

	// Unmarshal itemsBlockSize
	if len(src) < 4 {
		return src, fmt.Errorf("cannot unmarshal itemsBlockSize from %d bytes; need at least %d bytes", len(src), 4)
	}
	bh.itemsBlockSize = encoding.UnmarshalUint32(src)
	src = src[4:]

	// Unmarshal lensBlockSize
	if len(src) < 4 {
		return src, fmt.Errorf("cannot unmarshal lensBlockSize from %d bytes; need at least %d bytes", len(src), 4)
	}
	bh.lensBlockSize = encoding.UnmarshalUint32(src)
	src = src[4:]

	if bh.itemsCount <= 0 {
		return src, fmt.Errorf("itemsCount must be bigger than 0; got %d", bh.itemsCount)
	}


	return src, nil
}

func (bh *blockHeader) Reset() {
	if bh.noCopy {
		bh.commonPrefix = nil
		bh.firstItem = nil
	} else {
		bh.commonPrefix = bh.commonPrefix[:0]
		bh.firstItem = bh.firstItem[:0]
	}
	bh.marshalType = marshalTypePlain
	bh.itemsCount = 0
	bh.itemsBlockOffset = 0
	bh.lensBlockOffset = 0
	bh.itemsBlockSize = 0
	bh.lensBlockSize = 0
}

func (bh *blockHeader) Marshal(dst []byte) []byte {
	dst = encoding.MarshalBytes(dst, bh.commonPrefix)
	dst = encoding.MarshalBytes(dst, bh.firstItem)
	dst = append(dst, byte(bh.marshalType))
	dst = encoding.MarshalUint32(dst, bh.itemsCount)
	dst = encoding.MarshalUint64(dst, bh.itemsBlockOffset)
	dst = encoding.MarshalUint64(dst, bh.lensBlockOffset)
	dst = encoding.MarshalUint32(dst, bh.itemsBlockSize)
	dst = encoding.MarshalUint32(dst, bh.lensBlockSize)
	return dst
}