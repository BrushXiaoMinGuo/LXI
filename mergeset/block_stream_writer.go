package mergeset

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"path/filepath"
	"sync"
)

type blockStreamWriter struct {
	sb storageBlock
	bh blockHeader
	mr metaindexRow
	path          string


	compressLevel int

	metaindexWriter filestream.WriteCloser
	indexWriter     filestream.WriteCloser
	itemsWriter     filestream.WriteCloser
	lensWriter      filestream.WriteCloser

	mrFirstItemCaught bool


	unpackedIndexBlockBuf []byte
	packedIndexBlockBuf   []byte

	unpackedMetaindexBuf []byte
	packedMetaindexBuf   []byte

	itemsBlockOffset uint64
	lensBlockOffset  uint64
	indexBlockOffset uint64
}

func (bsw *blockStreamWriter) InitFromInMemoryPart(mp *inMemoryPart) {
	bsw.compressLevel = -5
	bsw.metaindexWriter = &mp.metaindexData
	bsw.indexWriter = &mp.indexData
	bsw.itemsWriter = &mp.itemsData
	bsw.lensWriter = &mp.lensData
}

const maxIndexBlockSize = 64 * 1024

func (bsw *blockStreamWriter) WriteBlock(ib *inMemoryBlock) {
	bsw.bh.firstItem, bsw.bh.commonPrefix, bsw.bh.itemsCount, bsw.bh.marshalType = ib.MarshalSortedData(&bsw.sb, bsw.bh.firstItem[:0], bsw.bh.commonPrefix[:0], bsw.compressLevel)

	if !bsw.mrFirstItemCaught {
		bsw.mr.firstItem = append(bsw.mr.firstItem[:0], bsw.bh.firstItem...)
		bsw.mrFirstItemCaught = true
	}

	// Write itemsData
	fs.MustWriteData(bsw.itemsWriter, bsw.sb.itemsData)
	bsw.bh.itemsBlockSize = uint32(len(bsw.sb.itemsData))
	bsw.bh.itemsBlockOffset = bsw.itemsBlockOffset
	bsw.itemsBlockOffset += uint64(bsw.bh.itemsBlockSize)

	// Write lensData
	fs.MustWriteData(bsw.lensWriter, bsw.sb.lensData)
	bsw.bh.lensBlockSize = uint32(len(bsw.sb.lensData))
	bsw.bh.lensBlockOffset = bsw.lensBlockOffset
	bsw.lensBlockOffset += uint64(bsw.bh.lensBlockSize)

	// Write blockHeader
	bsw.unpackedIndexBlockBuf = bsw.bh.Marshal(bsw.unpackedIndexBlockBuf)
	bsw.bh.Reset()
	bsw.mr.blockHeadersCount++
	if len(bsw.unpackedIndexBlockBuf) >= maxIndexBlockSize {
		bsw.flushIndexData()
	}
}

func (bsw *blockStreamWriter) InitFromFilePart(path string, nocache bool, compressLevel int) error {
	path = filepath.Clean(path)

	// Create the directory
	if err := fs.MkdirAllFailIfExist(path); err != nil {
		return fmt.Errorf("cannot create directory %q: %w", path, err)
	}

	// Create part files in the directory.

	// Always cache metaindex file in OS page cache, since it is immediately
	// read after the merge.
	metaindexPath := path + "/metaindex.bin"
	metaindexFile, err := filestream.Create(metaindexPath, false)
	if err != nil {
		fs.MustRemoveDirAtomic(path)
		return fmt.Errorf("cannot create metaindex file: %w", err)
	}

	indexPath := path + "/index.bin"
	indexFile, err := filestream.Create(indexPath, nocache)
	if err != nil {
		metaindexFile.MustClose()
		fs.MustRemoveDirAtomic(path)
		return fmt.Errorf("cannot create index file: %w", err)
	}

	itemsPath := path + "/items.bin"
	itemsFile, err := filestream.Create(itemsPath, nocache)
	if err != nil {
		metaindexFile.MustClose()
		indexFile.MustClose()
		fs.MustRemoveDirAtomic(path)
		return fmt.Errorf("cannot create items file: %w", err)
	}

	lensPath := path + "/lens.bin"
	lensFile, err := filestream.Create(lensPath, nocache)
	if err != nil {
		metaindexFile.MustClose()
		indexFile.MustClose()
		itemsFile.MustClose()
		fs.MustRemoveDirAtomic(path)
		return fmt.Errorf("cannot create lens file: %w", err)
	}

	bsw.reset()
	bsw.compressLevel = compressLevel
	bsw.path = path

	bsw.metaindexWriter = metaindexFile
	bsw.indexWriter = indexFile
	bsw.itemsWriter = itemsFile
	bsw.lensWriter = lensFile

	return nil

}

func (bsw *blockStreamWriter) reset() {
	bsw.compressLevel = 0
	bsw.path = ""

	bsw.metaindexWriter = nil
	bsw.indexWriter = nil
	bsw.itemsWriter = nil
	bsw.lensWriter = nil

	bsw.sb.Reset()
	bsw.bh.Reset()
	bsw.mr.Reset()

	bsw.unpackedIndexBlockBuf = bsw.unpackedIndexBlockBuf[:0]
	bsw.packedIndexBlockBuf = bsw.packedIndexBlockBuf[:0]

	bsw.unpackedMetaindexBuf = bsw.unpackedMetaindexBuf[:0]
	bsw.packedMetaindexBuf = bsw.packedMetaindexBuf[:0]

	bsw.itemsBlockOffset = 0
	bsw.lensBlockOffset = 0
	bsw.indexBlockOffset = 0

	bsw.mrFirstItemCaught = false
}

func (bsw *blockStreamWriter) flushIndexData() {
	if len(bsw.unpackedIndexBlockBuf) == 0 {
		// Nothing to flush.
		return
	}

	// Write indexBlock.
	bsw.packedIndexBlockBuf = encoding.CompressZSTDLevel(bsw.packedIndexBlockBuf[:0], bsw.unpackedIndexBlockBuf, bsw.compressLevel)
	fs.MustWriteData(bsw.indexWriter, bsw.packedIndexBlockBuf)
	bsw.mr.indexBlockSize = uint32(len(bsw.packedIndexBlockBuf))
	bsw.mr.indexBlockOffset = bsw.indexBlockOffset
	bsw.indexBlockOffset += uint64(bsw.mr.indexBlockSize)
	bsw.unpackedIndexBlockBuf = bsw.unpackedIndexBlockBuf[:0]

	// Write metaindexRow.
	bsw.unpackedMetaindexBuf = bsw.mr.Marshal(bsw.unpackedMetaindexBuf)
	bsw.mr.Reset()

	// Notify that the next call to WriteBlock must catch the first item.
	bsw.mrFirstItemCaught = false
}

func getBlockStreamWriter() *blockStreamWriter {
	v := bswPool.Get()
	if v == nil {
		return &blockStreamWriter{}
	}
	return v.(*blockStreamWriter)
}

func putBlockStreamWriter(bsw *blockStreamWriter) {
	bswPool.Put(bsw)
}

var bswPool sync.Pool
