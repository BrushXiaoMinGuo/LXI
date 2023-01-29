package mergeset

import (
	"container/heap"
	"sync"
)

func mergeBlockStreams(ph *partHeader, bsw *blockStreamWriter, bsrs []*blockStreamReader,
	prepareBlock PrepareBlockCallback, stopCh <-chan struct{}, itemsMerged *uint64) {
	bsm := bsmPool.Get().(*blockStreamMerger)
	bsm.Init(bsrs, prepareBlock)

	bsm.Merge(bsw, ph, stopCh, itemsMerged)
	bsmPool.Put(bsm)
	bsm.reset()
}

type PrepareBlockCallback func(data []byte, items []Item) ([]byte, []Item)

var bsmPool = &sync.Pool{
	New: func() interface{} {
		return &blockStreamMerger{}
	},
}

type blockStreamMerger struct {
	bsrHeap bsrHeap
	ib      inMemoryBlock
	phFirstItemCaught bool
	prepareBlock PrepareBlockCallback
}

func (bsm *blockStreamMerger) Init(bsrs []*blockStreamReader, prepareBlock PrepareBlockCallback) {
	for _, bsr := range bsrs {
		bsm.bsrHeap = append(bsm.bsrHeap, bsr)
	}

	heap.Init(&bsm.bsrHeap)

}

func (bsm *blockStreamMerger) reset() {
	bsm.prepareBlock = nil

	for i := range bsm.bsrHeap {
		bsm.bsrHeap[i] = nil
	}
	bsm.bsrHeap = bsm.bsrHeap[:0]
	bsm.ib.Reset()

	bsm.phFirstItemCaught = false
}

func (bsm *blockStreamMerger) Merge(bsw *blockStreamWriter, ph *partHeader, stopCh <-chan struct{}, itemsMerged *uint64) error {
again:
	if len(bsm.bsrHeap) == 0 {
		bsm.flushIB(bsw, ph, itemsMerged)
		return nil
	}

	bsr := bsm.bsrHeap[0]

	var nextItem string
	hasNextItem := false
	if len(bsm.bsrHeap) > 1 {
		bsr := bsm.bsrHeap.getNextReader()
		nextItem = bsr.CurrItem()
		hasNextItem = true
	}

	items := bsr.Block.items
	data := bsr.Block.data
	for bsr.currItemIdx < len(bsr.Block.items) {
		item := items[bsr.currItemIdx].Bytes(data)
		if hasNextItem && string(item) > nextItem {
			break
		}
		if !bsm.ib.Add(item) {
			bsm.flushIB(bsw, ph, itemsMerged)
			continue
		}
		bsr.currItemIdx++
	}

	if bsr.currItemIdx == len(bsr.Block.items) {
		//if bsr.Next() {
		//	heap.Fix(&bsm.bsrHeap, 0)
		//	goto again
		//}
		heap.Pop(&bsm.bsrHeap)
		goto again
	}

	heap.Fix(&bsm.bsrHeap,0)
	goto again
}

func (bsm *blockStreamMerger) flushIB(bsw *blockStreamWriter, ph *partHeader, itemsMerged *uint64) {
	items := bsm.ib.items
	data := bsm.ib.data

	if len(items) == 0 {
		return
	}
	ph.itemsCount += uint64(len(items))
	if !bsm.phFirstItemCaught {
		ph.firstItem = append(ph.firstItem[:0], items[0].String(data)...)
		bsm.phFirstItemCaught = true
	}
	ph.lastItem = append(ph.lastItem[:0], items[len(items)-1].String(data)...)

	bsw.WriteBlock(&bsm.ib)
	bsm.ib.Reset()
	ph.blocksCount ++
}



type bsrHeap []*blockStreamReader

func (bh bsrHeap) getNextReader() *blockStreamReader {
	if len(bh) < 2 {
		return nil
	}
	if len(bh) < 3 {
		return bh[1]
	}
	a := bh[1]
	b := bh[2]
	if a.CurrItem() <= b.CurrItem() {
		return a
	}
	return b
}

func (bh *bsrHeap) Len() int {
	return len(*bh)
}

func (bh *bsrHeap) Swap(i, j int) {
	x := *bh
	x[i], x[j] = x[j], x[i]
}

func (bh *bsrHeap) Less(i, j int) bool {
	x := *bh
	return x[i].CurrItem() < x[j].CurrItem()
}

func (bh *bsrHeap) Pop() interface{} {
	a := *bh
	v := a[len(a)-1]
	*bh = a[:len(a)-1]
	return v
}

func (bh *bsrHeap) Push(x interface{}) {
	v := x.(*blockStreamReader)
	*bh = append(*bh, v)
}
