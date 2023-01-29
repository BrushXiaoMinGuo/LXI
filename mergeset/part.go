package mergeset

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

type part struct {
	ph partHeader

	path string

	size uint64

	mrs []metaindexRow

	indexFile fs.MustReadAtCloser
	itemsFile fs.MustReadAtCloser
	lensFile  fs.MustReadAtCloser
}

func openFilePart(path string) (*part, error) {

	var ph partHeader

	if err := ph.ParseFromPath(path); err != nil {
		return nil, err
	}

	metaIndexFile, err := filestream.Open(path+"/metaindex.bin", true)
	if err != nil {
		return nil, err
	}
	metaIndexSize := fs.MustFileSize(path + "/metaindex.bin")

	indexFile := fs.MustOpenReaderAt(path + "/index.bin")
	indexSize := fs.MustFileSize(path + "/index.bin")

	itemsFile := fs.MustOpenReaderAt(path + "/items.bin")
	itemsSize := fs.MustFileSize(path + "/items.bin")

	lenFile := fs.MustOpenReaderAt(path + "/lens.bin")
	lenSize := fs.MustFileSize(path + "/lens.bin")

	size := metaIndexSize + indexSize + itemsSize + lenSize
	return newPart(&ph, path, size, metaIndexFile, indexFile, itemsFile, lenFile)

}

func newPart(ph *partHeader, path string, size uint64, metaindexReader filestream.ReadCloser, indexFile, itemsFile, lensFile fs.MustReadAtCloser) (*part, error) {
	mrs, err := unmarshalMetaindexRows(nil, metaindexReader)
	if err != nil {
		return nil, err
	}
	metaindexReader.MustClose()

	var p part
	p.path = path
	p.size = size
	p.mrs = mrs
	p.itemsFile = itemsFile
	p.indexFile = indexFile
	p.lensFile = lensFile
	p.ph.CopyFrom(ph)
	return &p, nil
}

type indexBlock struct {
	bhs []blockHeader

	buf []byte
}
