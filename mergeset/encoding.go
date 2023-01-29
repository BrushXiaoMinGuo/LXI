package mergeset

import (
	"bytes"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"reflect"
	"sort"
	"sync"
	"unsafe"
)

const (
	IbPoolSize           = 128
	MaxInMemoryBlockSize = 16
)

const (
	marshalTypePlain = marshalType(0)
	marshalTypeZSTD  = marshalType(1)
)

type inMemoryBlock struct {
	commonPrefix []byte
	data         []byte
	items        []Item
}

type Item struct {
	Start uint32
	End   uint32
}

type storageBlock struct {
	itemsData []byte
	lensData  []byte
}

func (sb *storageBlock) Reset() {
	sb.itemsData = sb.itemsData[:0]
	sb.lensData = sb.lensData[:0]
}

func (it Item) String(data []byte) string {
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sh.Data += uintptr(it.Start)
	sh.Len = int(it.End - it.Start)
	return *(*string)(unsafe.Pointer(sh))
}

func (ib *inMemoryBlock) Add(x []byte) bool {
	data := ib.data
	if len(data)+len(x) > MaxInMemoryBlockSize {
		return false
	}

	if len(data) == 0 {
		data = make([]byte, 0, MaxInMemoryBlockSize)
		ib.items = make([]Item, 0, 512)
	}

	dataLen := len(data)
	data = append(data, x...)
	ib.items = append(ib.items, Item{
		Start: uint32(dataLen),
		End:   uint32(len(data)),
	})
	ib.data = data
	return true

}

func (ib *inMemoryBlock) Reset() {
	ib.commonPrefix = ib.commonPrefix[:0]
	ib.data = ib.data[:0]
	ib.items = ib.items[:0]
}

func (it Item) Bytes(data []byte) []byte {
	n := int(it.End - it.Start)
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sh.Cap = n
	sh.Len = n
	sh.Data += uintptr(it.Start)
	return data
}

func (ib *inMemoryBlock) CopyFrom(src *inMemoryBlock) {
	ib.commonPrefix = append(ib.commonPrefix[:0], src.commonPrefix...)
	ib.data = append(ib.data[:0], src.data...)
	ib.items = append(ib.items[:0], src.items...)
}

func (ib *inMemoryBlock) MarshalUnsortedData(sb *storageBlock, firstItemDst, commonPrefixDst []byte, compressLevel int) ([]byte, []byte, uint32, marshalType) {
	ib.SortItems()
	return ib.marshalData(sb, firstItemDst, commonPrefixDst, compressLevel)
}

func (ib *inMemoryBlock) SortItems() {
	if !ib.isSorted() {
		ib.updateCommonPrefixUnsorted()
		sort.Sort(ib)
	} else {
		ib.updateCommonPrefixSorted()
	}
}

func (ib *inMemoryBlock) updateCommonPrefixUnsorted() {
	ib.commonPrefix = ib.commonPrefix[:0]
	items := ib.items
	if len(items) == 0 {
		return
	}
	data := ib.data
	cp := items[0].Bytes(data)
	for _, it := range items[1:] {
		item := it.Bytes(data)
		if bytes.HasPrefix(item, cp) {
			continue
		}
		cpLen := commonPrefixLen(cp, item)
		if cpLen == 0 {
			return
		}
		cp = cp[:cpLen]
	}
	ib.commonPrefix = append(ib.commonPrefix[:0], cp...)
}

func (ib *inMemoryBlock) MarshalSortedData(sb *storageBlock, firstItemDst, commonPrefixDst []byte, compressLevel int) ([]byte, []byte, uint32, marshalType) {
	ib.isSorted()
	ib.updateCommonPrefixSorted()
	return ib.marshalData(sb,firstItemDst,commonPrefixDst,compressLevel)
}

func (ib *inMemoryBlock) marshalData(sb *storageBlock, firstItemDst, commonPrefixDst []byte, compressLevel int)  ([]byte, []byte, uint32, marshalType){

	data := ib.data
	firstItem := ib.items[0].Bytes(data)
	firstItemDst = append(firstItemDst, firstItem...)
	commonPrefixDst = append(commonPrefixDst, ib.commonPrefix...)


	if len(data)-len(ib.commonPrefix)*len(ib.items) < 64 || len(ib.items) < 2 {
		// Use plain encoding form small block, since it is cheaper.
		ib.marshalDataPlain(sb)
		return firstItemDst, commonPrefixDst, uint32(len(ib.items)), marshalTypePlain
	}

	bbItems := bbPool.Get()
	bItems := bbItems.B[:0]

	bbLens := bbPool.Get()
	bLens := bbLens.B[:0]

	// Marshal items data.
	xs := encoding.GetUint64s(len(ib.items) - 1)
	defer encoding.PutUint64s(xs)

	cpLen := len(ib.commonPrefix)
	prevItem := firstItem[cpLen:]
	prevPrefixLen := uint64(0)
	for i, it := range ib.items[1:] {
		it.Start += uint32(cpLen)
		item := it.Bytes(data)
		prefixLen := uint64(commonPrefixLen(prevItem, item))
		bItems = append(bItems, item[prefixLen:]...)
		xLen := prefixLen ^ prevPrefixLen
		prevItem = item
		prevPrefixLen = prefixLen

		xs.A[i] = xLen
	}
	bLens = encoding.MarshalVarUint64s(bLens, xs.A)
	sb.itemsData = encoding.CompressZSTDLevel(sb.itemsData[:0], bItems, compressLevel)

	bbItems.B = bItems
	bbPool.Put(bbItems)

	// Marshal lens data.
	prevItemLen := uint64(len(firstItem) - cpLen)
	for i, it := range ib.items[1:] {
		itemLen := uint64(int(it.End-it.Start) - cpLen)
		xLen := itemLen ^ prevItemLen
		prevItemLen = itemLen

		xs.A[i] = xLen
	}
	bLens = encoding.MarshalVarUint64s(bLens, xs.A)
	sb.lensData = encoding.CompressZSTDLevel(sb.lensData[:0], bLens, compressLevel)

	bbLens.B = bLens
	bbPool.Put(bbLens)

	if float64(len(sb.itemsData)) > 0.9*float64(len(data)-len(ib.commonPrefix)*len(ib.items)) {
		// Bad compression rate. It is cheaper to use plain encoding.
		ib.marshalDataPlain(sb)
		return firstItemDst, commonPrefixDst, uint32(len(ib.items)), marshalTypePlain
	}

	// Good compression rate.
	return firstItemDst, commonPrefixDst, uint32(len(ib.items)), marshalTypeZSTD

}

func (ib *inMemoryBlock) marshalDataPlain(sb *storageBlock) {
	data := ib.data

	// Marshal items data.
	// There is no need in marshaling the first item, since it is returned
	// to the caller in marshalData.
	cpLen := len(ib.commonPrefix)
	b := sb.itemsData[:0]
	for _, it := range ib.items[1:] {
		it.Start += uint32(cpLen)
		b = append(b, it.String(data)...)
	}
	sb.itemsData = b

	// Marshal length data.
	b = sb.lensData[:0]
	for _, it := range ib.items[1:] {
		b = encoding.MarshalUint64(b, uint64(int(it.End-it.Start)-cpLen))
	}
	sb.lensData = b
}

func (ib *inMemoryBlock) updateCommonPrefixSorted() {
	ib.commonPrefix = ib.commonPrefix[:0]
	items := ib.items
	if len(items) == 0 {
		return
	}
	data := ib.data
	cp := items[0].Bytes(data)
	if len(items) > 1{
		cpLen := commonPrefixLen(cp, items[len(items)-1].Bytes(data))
		cp = cp[:cpLen]
	}
	ib.commonPrefix = append(ib.commonPrefix[:0], cp...)
}

var bbPool bytesutil.ByteBufferPool

func (ib *inMemoryBlock) UnmarshalData(sb *storageBlock, firstItem, commonPrefix []byte, itemsCount uint32, mt marshalType) error {

	ib.commonPrefix = append(ib.commonPrefix, commonPrefix...)

	switch mt {
	case marshalTypePlain:
		if err := ib.unmarshalDataPlain(sb, firstItem, itemsCount); err != nil {
			return fmt.Errorf("cannot unmarshal plain data: %w", err)
		}
		if !ib.isSorted() {
			return nil
		}
		return nil
	case marshalTypeZSTD:
		// it is handled below.
	default:
		return fmt.Errorf("unknown marshalType=%d", mt)
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	var err error
	// Unmarshal lens data.
	bb.B, err = encoding.DecompressZSTD(bb.B[:0], sb.lensData)
	if err != nil {
		return fmt.Errorf("cannot decompress lensData: %w", err)
	}

	lb := getLensBuffer(int(2 * itemsCount))
	defer putLensBuffer(lb)

	prefixLens := lb.lens[:itemsCount]
	lens := lb.lens[itemsCount:]

	is := encoding.GetUint64s(int(itemsCount) - 1)
	defer encoding.PutUint64s(is)

	tail, err := encoding.UnmarshalVarUint64s(is.A, bb.B)
	if err != nil {
		return fmt.Errorf("cannot unmarshal prefixLens from lensData: %w", err)
	}
	for i, xLen := range is.A {
		prefixLens[i+1] = xLen ^ prefixLens[i]
	}

	// Unmarshal lens
	tail, err = encoding.UnmarshalVarUint64s(is.A, tail)
	if err != nil {
		return fmt.Errorf("cannot unmarshal lens from lensData: %w", err)
	}
	if len(tail) > 0 {
		return fmt.Errorf("unexpected tail left unmarshaling %d lens; tail size=%d; contents=%X", itemsCount, len(tail), tail)
	}
	lens[0] = uint64(len(firstItem) - len(commonPrefix))
	dataLen := len(commonPrefix) * int(itemsCount)
	dataLen += int(lens[0])
	for i, xLen := range is.A {
		itemLen := xLen ^ lens[i]
		lens[i+1] = itemLen
		dataLen += int(itemLen)
	}
	bb.B, err = encoding.DecompressZSTD(bb.B[:0], sb.itemsData)
	if err != nil {
		return fmt.Errorf("cannot decompress lensData: %w", err)
	}
	// Resize ib.data to dataLen instead of maxInmemoryBlockSize,
	// since the data isn't going to be resized after unmarshaling.
	// This may save memory for caching the unmarshaled block.
	data := bytesutil.ResizeNoCopyNoOverallocate(ib.data, dataLen)
	if n := int(itemsCount) - cap(ib.items); n > 0 {
		ib.items = append(ib.items[:cap(ib.items)], make([]Item, n)...)
	}
	ib.items = ib.items[:itemsCount]
	data = append(data[:0], firstItem...)
	items := ib.items
	items[0] = Item{
		Start: 0,
		End:   uint32(len(data)),
	}
	prevItem := data[len(commonPrefix):]
	b := bb.B
	for i := 1; i < int(itemsCount); i++ {
		itemLen := lens[i]
		prefixLen := prefixLens[i]
		if prefixLen > itemLen {
			return fmt.Errorf("prefixLen=%d exceeds itemLen=%d", prefixLen, itemLen)
		}
		suffixLen := itemLen - prefixLen
		if uint64(len(b)) < suffixLen {
			return fmt.Errorf("not enough data for decoding item from itemsData; want %d bytes; remained %d bytes", suffixLen, len(b))
		}
		if prefixLen > uint64(len(prevItem)) {
			return fmt.Errorf("prefixLen cannot exceed %d; got %d", len(prevItem), prefixLen)
		}
		dataStart := len(data)
		data = append(data, commonPrefix...)
		data = append(data, prevItem[:prefixLen]...)
		data = append(data, b[:suffixLen]...)
		items[i] = Item{
			Start: uint32(dataStart),
			End:   uint32(len(data)),
		}
		b = b[suffixLen:]
		prevItem = data[len(data)-int(itemLen):]
	}
	if len(data) != dataLen {
		return fmt.Errorf("unexpected data len; got %d; want %d", len(data), dataLen)
	}
	ib.data = data
	if !ib.isSorted() {
		return nil
	}
	return nil
}

func (ib *inMemoryBlock) isSorted() bool {
	// Use sort.IsSorted instead of sort.SliceIsSorted in order to eliminate memory allocation.
	return sort.IsSorted(ib)
}

func (ib *inMemoryBlock) Len() int { return len(ib.items) }

func (ib *inMemoryBlock) Less(i, j int) bool {
	items := ib.items
	a := items[i]
	b := items[j]
	cpLen := uint32(len(ib.commonPrefix))
	a.Start += cpLen
	b.Start += cpLen
	data := ib.data
	return a.String(data) < b.String(data)
}

func (ib *inMemoryBlock) Swap(i, j int) {
	items := ib.items
	items[i], items[j] = items[j], items[i]
}

func (ib *inMemoryBlock) unmarshalDataPlain(sb *storageBlock, firstItem []byte, itemsCount uint32) error {
	commonPrefix := ib.commonPrefix

	// Unmarshal lens data.
	lb := getLensBuffer(int(itemsCount))
	defer putLensBuffer(lb)

	lb.lens[0] = uint64(len(firstItem) - len(commonPrefix))
	b := sb.lensData

	for i := 1; i < int(itemsCount); i++ {
		iLen := encoding.UnmarshalUint64(b)
		b = b[8:]
		lb.lens[i] = iLen
	}

	data := ib.data
	items := ib.items

	dataLen := len(firstItem) + len(sb.itemsData) + len(commonPrefix)*(int(itemsCount)-1)
	data = bytesutil.ResizeNoCopyNoOverallocate(data, dataLen)

	data = append(data[:0], firstItem...)
	items = append(items[:0], Item{
		Start: 0,
		End:   uint32(len(data)),
	})
	b = sb.itemsData

	for i := 1; i < int(itemsCount); i++ {
		itemsLen := lb.lens[i]
		dataStart := len(data)
		data = append(data, commonPrefix...)
		data = append(data, b[:itemsLen]...)
		items = append(items, Item{
			Start: uint32(dataStart),
			End:   uint32(len(data)),
		})
		b = b[itemsLen:]
	}

	ib.items = items
	ib.data = data
	return nil
}

func commonPrefixLen(a, b []byte) int {
	i := 0
	if len(a) > len(b) {
		for i < len(b) && a[i] == b[i] {
			i++
		}
	} else {
		for i < len(a) && a[i] == b[i] {
			i++
		}
	}
	return i
}

type lensBuffer struct {
	lens []uint64
}

var lensBufferPool sync.Pool

func getLensBuffer(n int) *lensBuffer {
	v := lensBufferPool.Get()
	if v == nil {
		v = &lensBuffer{}
	}
	lb := v.(*lensBuffer)
	if nn := n - cap(lb.lens); nn > 0 {
		lb.lens = append(lb.lens[:cap(lb.lens)], make([]uint64, nn)...)
	}
	lb.lens = lb.lens[:n]
	return lb
}

func putLensBuffer(lb *lensBuffer) {
	lensBufferPool.Put(lb)
}

func getInMemoryBlock() *inMemoryBlock {
	select {
	case ib := <-ibPoolChan:
		return ib
	default:
		return &inMemoryBlock{}
	}
}

func pubInMemoryBlock(ib *inMemoryBlock) {

	ib.Reset()
	select {
	case ibPoolChan <- ib:
	default:
	}
}

var ibPoolChan = make(chan *inMemoryBlock, IbPoolSize)
