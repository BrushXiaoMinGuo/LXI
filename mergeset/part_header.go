package mergeset

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type partHeader struct {
	itemsCount  uint64
	blocksCount uint64
	firstItem   []byte
	lastItem    []byte
}

func (ph *partHeader) CopyFrom(src *partHeader) {
	ph.itemsCount = src.itemsCount
	ph.blocksCount = src.blocksCount
	ph.firstItem = append(ph.firstItem, src.firstItem...)
	ph.lastItem = append(ph.lastItem, src.lastItem...)

}

func (ph *partHeader) Reset() {
	ph.itemsCount = 0
	ph.blocksCount = 0
	ph.firstItem = ph.firstItem[:0]
	ph.lastItem = ph.lastItem[:0]
}


func (ph *partHeader) ParseFromPath(path string) error {

	n := strings.LastIndexByte(path, '/')
	fn := path[n+1:]
	a := strings.Split(fn, "_")

	itemsCount, err := strconv.ParseUint(a[0], 10, 64)
	if err != nil {
		return err
	}
	ph.itemsCount = itemsCount

	blocksCount, err := strconv.ParseUint(a[1], 10, 64)
	if err != nil {
		return err
	}
	ph.blocksCount = blocksCount

	metadata, err := os.ReadFile(path + "/metadata.json")
	if err != nil {
		return err
	}

	var phj partHeaderJson
	if err = json.Unmarshal(metadata, &phj); err != nil {
		fmt.Println(err)
		return err
	}

	ph.firstItem = append(ph.firstItem, phj.FirstItem...)
	ph.lastItem = append(ph.lastItem, phj.LastItem...)

	return nil
}

type partHeaderJson struct {
	ItemsCount  uint64
	BlocksCount uint64
	FirstItem   hexString
	LastItem    hexString
}

type hexString []byte

func (hs hexString) MarshalJSON() ([]byte, error) {
	h := hex.EncodeToString(hs)
	b := make([]byte, 0, len(h)+2)
	b = append(b, '"')
	b = append(b, h...)
	b = append(b, '"')
	return b, nil
}

func (hs *hexString) UnmarshalJSON(data []byte) error {
	if len(data) < 2 {
		return fmt.Errorf("too small data string: got %q; must be at least 2 bytes", data)
	}
	if data[0] != '"' || data[len(data)-1] != '"' {
		return fmt.Errorf("missing heading and/or tailing quotes in the data string %q", data)
	}
	data = data[1 : len(data)-1]
	b, err := hex.DecodeString(string(data))
	if err != nil {
		return fmt.Errorf("cannot hex-decode %q: %w", data, err)
	}
	*hs = b
	return nil
}
