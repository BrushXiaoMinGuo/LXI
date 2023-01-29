package mergeset

import "container/heap"

type TableSearch struct {
	tb           *Table
	Item         []byte
	pws          []*partWrapper
	psPool       []PartSearch
	psHeap       partSearchHeap
	nextItemNoop bool
}

func (ts *TableSearch) Init(tb *Table) {

	ts.tb = tb
	ts.pws = ts.tb.getParts(ts.pws[:0])

	if cap(ts.psPool) < len(ts.pws) {
		ts.psPool = append(ts.psPool[:cap(ts.psPool)], make([]PartSearch, len(ts.pws)-cap(ts.psPool))...)
	}
	ts.psPool = ts.psPool[:len(ts.pws)]
	for i, pw := range ts.pws {
		ts.psPool[i].Init(pw.p)
	}

}

func (ts *TableSearch) Seek(k []byte) {
	ts.psHeap = ts.psHeap[:]

	ts.psHeap = ts.psHeap[:0]
	for i, _ := range ts.psPool {
		ps := &ts.psPool[i]
		ps.Seek(k)

		if !ps.NextItem() {
			continue
		}
		ts.psHeap = append(ts.psHeap, ps)
	}

	if len(ts.psHeap) == 0 {
		return
	}
	heap.Init(&ts.psHeap)
	ts.Item = ts.psHeap[0].Item
	ts.nextItemNoop = true

}
