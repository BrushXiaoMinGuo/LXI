package mergeset

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

type inMemoryPart struct {
	ph            partHeader
	bh            blockHeader
	mr            metaindexRow
	metaindexData bytesutil.ByteBuffer
	indexData     bytesutil.ByteBuffer
	itemsData     bytesutil.ByteBuffer
	lensData      bytesutil.ByteBuffer
}

func (mp *inMemoryPart) Init(ib *inMemoryBlock) {
	mp.Reset()

	sb := &storageBlock{}
	sb.itemsData = mp.itemsData.B[:0]
	sb.lensData = mp.lensData.B[:0]

	compressLevel := -5
	mp.bh.firstItem, mp.bh.commonPrefix, mp.bh.itemsCount, mp.bh.marshalType = ib.MarshalUnsortedData(sb, mp.bh.firstItem[:0], mp.bh.commonPrefix[:0], compressLevel)


	mp.ph.itemsCount = uint64(len(ib.items))
	mp.ph.blocksCount = 1
	mp.ph.firstItem = append(mp.ph.firstItem[:0], ib.items[0].String(ib.data)...)
	mp.ph.lastItem = append(mp.ph.lastItem[:0], ib.items[len(ib.items)-1].String(ib.data)...)

	mp.itemsData.B = sb.itemsData
	mp.bh.itemsBlockOffset = 0
	mp.bh.itemsBlockSize = uint32(len(mp.itemsData.B))

	mp.lensData.B = sb.lensData
	mp.bh.lensBlockOffset = 0
	mp.bh.lensBlockSize = uint32(len(mp.lensData.B))

	bb := inmemoryPartBytePool.Get()
	bb.B = mp.bh.Marshal(bb.B[:0])
	mp.indexData.B = encoding.CompressZSTDLevel(mp.indexData.B[:0], bb.B, 0)

	mp.mr.firstItem = append(mp.mr.firstItem[:0], mp.bh.firstItem...)
	mp.mr.blockHeadersCount = 1
	mp.mr.indexBlockOffset = 0
	mp.mr.indexBlockSize = uint32(len(mp.indexData.B))
	bb.B = mp.mr.Marshal(bb.B[:0])
	mp.metaindexData.B = encoding.CompressZSTDLevel(mp.metaindexData.B[:0], bb.B, 0)
	inmemoryPartBytePool.Put(bb)
}

var inmemoryPartBytePool bytesutil.ByteBufferPool

func (mp *inMemoryPart) NewPart() *part {
	return nil
}

func (mp *inMemoryPart) Reset() {
	mp.ph.Reset()
	mp.bh.Reset()
	mp.mr.Reset()

	mp.metaindexData.Reset()
	mp.indexData.Reset()
	mp.itemsData.Reset()
	mp.lensData.Reset()
}