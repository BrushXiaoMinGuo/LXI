package mergeset

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TableShards         = 2
	MaxBlocksPerShard   = 16
	DefaultPartsToMerge = 15
)

type Table struct {
	path     string
	mergeIdx uint64

	parts     []*partWrapper
	partsLock sync.RWMutex

	rawItems     rawItemShards
	prepareBlock PrepareBlockCallback
	itemsMerged  uint64
}

func OpenTable(path string) *Table {

	path = filepath.Clean(path)
	if err := fs.MkdirAllIfNotExist(path); err != nil {
		return nil
	}

	pws, err := openParts(path)
	if err != nil {
		return nil
	}

	t := &Table{
		path:     path,
		parts:    pws,
		mergeIdx: uint64(time.Now().UnixNano()),
	}
	t.rawItems.init()
	return t
}

func openParts(path string) ([]*partWrapper, error) {

	var pws []*partWrapper
	d, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	fis, err := d.Readdir(-1)
	if err != nil {
		return nil, err
	}

	for _, fi := range fis {
		if !fs.IsDirOrSymlink(fi) {
			continue
		}
		if fi.Name() == "tmp" || fi.Name() == "txn" {
			continue
		}

		p, err := openFilePart(path + "/" + fi.Name())
		if err != nil {
			return nil, err
		}

		pw := partWrapper{
			p:        p,
			refCount: 1,
		}
		pws = append(pws, &pw)

	}

	return pws, nil
}

func (tb *Table) AddItems(items [][]byte) {
	tb.rawItems.addItems(tb, items)
}

func (tb *Table) getParts(dst []*partWrapper) []*partWrapper {

	tb.partsLock.Lock()
	for _, pw := range tb.parts {
		pw.inxRef()
	}
	dst = append(dst, tb.parts...)
	tb.partsLock.Unlock()
	return dst
}

func (tb *Table) mergeRawItemBlocks(ibs []*inMemoryBlock, final bool) {
	if len(ibs) == 0 {
		return
	}


	var wg sync.WaitGroup
	pws := make([]*partWrapper, 0, (len(ibs)+DefaultPartsToMerge-1)/DefaultPartsToMerge)

	for i := 0; i < len(ibs); i++ {
		n := DefaultPartsToMerge
		if n > len(ibs) {
			n = len(ibs)
		}

		wg.Add(1)
		go func(ibsPart []*inMemoryBlock) {
			defer wg.Done()
			pw := tb.mergeInMemoryBlocks(ibsPart)
			if pw == nil {
				return
			}
			pws = append(pws, pw)

		}(ibs[:n])
		ibs = ibs[n:]
	}

	wg.Wait()

	if len(pws) > 0 {
		tb.mergeParts()
	}

}

func (tb *Table) mergeInMemoryBlocks(ibs []*inMemoryBlock) *partWrapper {

	bsrs := make([]*blockStreamReader, 0, len(ibs))
	for _, ib := range ibs {
		if len(ib.items) == 0 {
			continue
		}
		bsr := getBlockStreamReader()
		bsr.InitFromBlockStreamReader(ib)
		pubInMemoryBlock(ib)
		fmt.Println(bsr.Block.items)
		bsrs = append(bsrs, bsr)
	}

	if len(bsrs) == 0 {
		return nil
	}

	if len(bsrs) == 1 {
		mp := &inMemoryPart{}
		mp.Init(&bsrs[0].Block)
		p := mp.NewPart()

		return &partWrapper{
			p:        p,
			mp:       mp,
			refCount: 1,
		}
	}

	bsw := getBlockStreamWriter()
	mpDst := &inMemoryPart{}
	bsw.InitFromInMemoryPart(mpDst)


	mergeBlockStreams(&mpDst.ph, bsw, bsrs, tb.prepareBlock, nil, &tb.itemsMerged)

	putBlockStreamWriter(bsw)
	for _, bsr := range bsrs {
		putBlockStreamReader(bsr)
	}

	p := mpDst.NewPart()

	return &partWrapper{
		p:        p,
		mp:       mpDst,
		refCount: 1,
	}
}

func (tb *Table) mergeParts() error{

	mergeIdx := tb.nextMergeIdx()
	tmpPartPath := fmt.Sprintf("%s/tmp/%016X", tb.path, mergeIdx)
	bsw := getBlockStreamWriter()
	compressLevel := -5
	if err := bsw.InitFromFilePart(tmpPartPath, false, compressLevel); err != nil {
		return fmt.Errorf("cannot create destination part %q: %w", tmpPartPath, err)
	}


	return nil
}

func (tb *Table) nextMergeIdx() uint64 {
	return atomic.AddUint64(&tb.mergeIdx, 1)
}

type partWrapper struct {
	p        *part
	mp       *inMemoryPart
	refCount uint64
}

func (pw *partWrapper) inxRef() {
	atomic.AddUint64(&pw.refCount, 1)
}

type rawItemShards struct {
	shardIndex uint32
	shards     []rawItemShard
}

func (riss *rawItemShards) init() {
	riss.shards = make([]rawItemShard, TableShards)
}

func (riss *rawItemShards) addItems(tb *Table, items [][]byte) {
	n := atomic.AddUint32(&riss.shardIndex, 1)
	shards := riss.shards
	index := n % uint32(len(shards))
	shard := &shards[index]
	shard.addItems(tb, items)

}

type rawItemShard struct {
	rawItemShardNopad
}

func (ris *rawItemShard) addItems(tb *Table, items [][]byte) {

	ris.mu.Lock()
	defer ris.mu.Unlock()

	var blocksToFlush []*inMemoryBlock

	ibs := ris.ibs
	if len(ibs) == 0 {
		ib := getInMemoryBlock()
		ibs = append(ibs, ib)
		ris.ibs = ibs
	}
	ib := ibs[len(ibs)-1]

	for _, item := range items {
		if !ib.Add(item) {
			ib = getInMemoryBlock()
			ib.Add(item)
			ibs = append(ibs, ib)
			ris.ibs = ibs
		}
	}
	fmt.Println(len(ib.data))

	if len(ibs) >= MaxBlocksPerShard {
		blocksToFlush = append(blocksToFlush, ibs...)
	}

	//blocksToFlush = append(blocksToFlush, ibs...)
	tb.mergeRawItemBlocks(blocksToFlush, false)
}

type rawItemShardNopad struct {
	lastFlashTime uint64
	mu            sync.RWMutex
	ibs           []*inMemoryBlock
}
