package mergeset

import (
	"fmt"
	"testing"
)

func TestTableSearch(t *testing.T) {

	path := "TestTableSearchSerial"
	tb := OpenTable(path)
	var ts TableSearch
	ts.Init(tb)
	ts.Seek([]byte("1234"))
	fmt.Println(string(ts.Item))

}
