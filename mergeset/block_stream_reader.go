package mergeset

import (
	"io"
	"sync"
)

type blockStreamReader struct {
	Block           inMemoryBlock
	isInMemoryBlock bool
	currItemIdx     int
	// The last error.
	err error
}

func (bsr *blockStreamReader) InitFromBlockStreamReader(ib *inMemoryBlock) {
	bsr.reset()
	bsr.Block.CopyFrom(ib)
	bsr.Block.SortItems()
	bsr.isInMemoryBlock = true
}

func (bsr *blockStreamReader) CurrItem() string {
	return bsr.Block.items[bsr.currItemIdx].String(bsr.Block.data)

}

func (bsr *blockStreamReader) reset() {
	bsr.Block.Reset()
	bsr.isInMemoryBlock = false
	bsr.currItemIdx = 0
	//bsr.path = ""
	//bsr.ph.Reset()
	//bsr.mrs = nil
	//bsr.mrIdx = 0
	//bsr.bhs = bsr.bhs[:0]
	//bsr.bhIdx = 0
	//
	//bsr.indexReader = nil
	//bsr.itemsReader = nil
	//bsr.lensReader = nil
	//
	//bsr.bh = nil
	//bsr.sb.Reset()
	//
	//bsr.itemsRead = 0
	//bsr.blocksRead = 0
	//bsr.firstItemChecked = false
	//
	//bsr.packedBuf = bsr.packedBuf[:0]
	//bsr.unpackedBuf = bsr.unpackedBuf[:0]

	bsr.err = nil
}


func (bsr *blockStreamReader) Next() bool {
	if bsr.isInMemoryBlock {
		bsr.err = io.EOF
		return true
	}
	return true
}

func getBlockStreamReader() *blockStreamReader {
	v := bsrPool.Get()
	if v == nil {
		return &blockStreamReader{}
	}
	return v.(*blockStreamReader)
}


func putBlockStreamReader(bsr *blockStreamReader) {
	bsrPool.Put(bsr)
}

var bsrPool sync.Pool
