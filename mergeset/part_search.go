package mergeset

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"io"
	"sort"
)

type PartSearch struct {
	Item []byte
	p    *part
	mrs  []metaindexRow

	bhs []blockHeader

	indexBuf           []byte
	compressedIndexBuf []byte

	sb          storageBlock
	ib          *inMemoryBlock
	ibItemIndex int
}

func (ps *PartSearch) Init(p *part) {
	ps.p = p
}

func (ps *PartSearch) Seek(k []byte) {
	if string(k) > string(ps.p.ph.lastItem) {
		return
	}

	if string(k) <= string(ps.p.ph.firstItem) {
		err := ps.nextBlock()
		fmt.Println(err)
		return
	}

	ps.mrs = ps.p.mrs

	n := sort.Search(len(ps.mrs), func(i int) bool {
		return string(k) <= string(ps.mrs[i].firstItem)
	})

	if n > 0 {
		n--
	}

	ps.mrs = ps.mrs[n:]
	if err := ps.nextBHS(); err != nil {
		return
	}

	n = sort.Search(len(ps.bhs), func(i int) bool {
		return string(k) < string(ps.bhs[i].firstItem)
	})

	if n > 0 {
		n--
	}
	ps.bhs = ps.bhs[n:]

	if err := ps.nextBlock(); err != nil {
		return
	}

	items := ps.ib.items
	data := ps.ib.data
	cpLen := commonPrefixLen(ps.ib.commonPrefix, k)

	if cpLen > 0 {
		keySuffix := k[cpLen:]
		ps.ibItemIndex = sort.Search(len(items), func(i int) bool {
			it := items[i]
			it.Start += uint32(cpLen)
			return string(keySuffix) <= it.String(data)

		})
	} else {
		ps.ibItemIndex = binarySearchKey(data, items, k)
	}

	if ps.ibItemIndex < len(items) {
		return
	}

	if err := ps.nextBlock(); err != nil {
		return
	}
}

func binarySearchKey(data []byte, items []Item, key []byte) int {
	if len(items) == 0 {
		return 0
	}
	if string(key) <= items[0].String(data) {
		// Fast path - the item is the first.
		return 0
	}
	items = items[1:]
	offset := uint(1)

	// This has been copy-pasted from https://golang.org/src/sort/search.go
	n := uint(len(items))
	i, j := uint(0), n
	for i < j {
		h := uint(i+j) >> 1
		if h >= 0 && h < uint(len(items)) && string(key) > items[h].String(data) {
			i = h + 1
		} else {
			j = h
		}
	}
	return int(i + offset)
}
func (ps *PartSearch) nextBHS() error {
	if len(ps.mrs) <= 0 {
		return io.EOF
	}

	mr := &ps.mrs[0]
	ps.mrs = ps.mrs[1:]
	idxb, err := ps.readIndexBlock(mr)
	if err != nil {
		return err
	}
	ps.bhs = idxb.bhs
	return nil

}

func (ps *PartSearch) nextBlock() error {

	if len(ps.bhs) == 0 {
		if err := ps.nextBHS(); err != nil {
			return err
		}
	}

	bh := &ps.bhs[0]
	ps.bhs = ps.bhs[1:]

	ib, err := ps.getInmemoryBlock(bh)
	if err != nil {
		return err
	}
	ps.ib = ib
	ps.ibItemIndex = 0
	return nil
}

func (ps *PartSearch) getInmemoryBlock(bh *blockHeader) (*inMemoryBlock, error) {
	ib, err := ps.readInmemoryBlock(bh)
	if err != nil {
		return nil, err
	}
	return ib, nil
}

func (ps *PartSearch) readInmemoryBlock(bh *blockHeader) (*inMemoryBlock, error) {
	ps.sb.Reset()
	ps.sb.itemsData = bytesutil.ResizeNoCopyMayOverallocate(ps.sb.itemsData, int(bh.itemsBlockSize))
	ps.p.itemsFile.MustReadAt(ps.sb.itemsData, int64(bh.itemsBlockOffset))

	ps.sb.lensData = bytesutil.ResizeNoCopyMayOverallocate(ps.sb.lensData, int(bh.lensBlockSize))
	ps.p.lensFile.MustReadAt(ps.sb.lensData, int64(bh.lensBlockOffset))

	ib := getInMemoryBlock()
	if err := ib.UnmarshalData(&ps.sb, bh.firstItem, bh.commonPrefix, bh.itemsCount, bh.marshalType); err != nil {
		return nil, fmt.Errorf("cannot unmarshal storage block with %d items: %w", bh.itemsCount, err)
	}

	return ib, nil

}

func (ps *PartSearch) NextItem() bool {
	items := ps.ib.items

	if ps.ibItemIndex < len(items) {
		ps.Item = items[ps.ibItemIndex].Bytes(ps.ib.data)
		ps.ibItemIndex++
		return true
	}

	if err := ps.nextBlock(); err != nil {
		return false
	}
	ps.Item = ps.ib.items[0].Bytes(ps.ib.data)
	ps.ibItemIndex++
	return true
}

func (ps *PartSearch) readIndexBlock(mr *metaindexRow) (*indexBlock, error) {
	ps.compressedIndexBuf = bytesutil.ResizeNoCopyMayOverallocate(ps.compressedIndexBuf, int(mr.indexBlockSize))
	ps.p.indexFile.MustReadAt(ps.compressedIndexBuf, int64(mr.indexBlockOffset))

	var err error
	ps.indexBuf, err = encoding.DecompressZSTD(ps.indexBuf[:0], ps.compressedIndexBuf)
	if err != nil {
		return nil, fmt.Errorf("cannot decompress index block: %w", err)
	}
	idxb := &indexBlock{
		buf: append([]byte{}, ps.indexBuf...),
	}
	idxb.bhs, err = unmarshalBlockHeadersNoCopy(idxb.bhs[:0], idxb.buf, int(mr.blockHeadersCount))
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal block headers from index block (offset=%d, size=%d): %w", mr.indexBlockOffset, mr.indexBlockSize, err)
	}
	return idxb, nil
}

type partSearchHeap []*PartSearch

func (psh *partSearchHeap) Len() int {
	return len(*psh)
}

func (psh *partSearchHeap) Less(i, j int) bool {
	x := *psh
	return string(x[i].Item) < string(x[j].Item)
}

func (psh *partSearchHeap) Swap(i, j int) {
	x := *psh
	x[i], x[j] = x[j], x[i]
}

func (psh *partSearchHeap) Push(x interface{}) {
	*psh = append(*psh, x.(*PartSearch))
}

func (psh *partSearchHeap) Pop() interface{} {
	a := *psh
	v := a[len(a)-1]
	*psh = a[:len(a)-1]
	return v
}
