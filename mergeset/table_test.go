package mergeset

import (
	"strconv"
	"testing"
)

func TestOpenTable(t *testing.T) {
	path := "TestTableSearchSerial3"
	OpenTable(path)

}

func TestAddItems(t *testing.T) {
	path := "TestTableSearchSerial3"
	table := OpenTable(path)
	//fmt.Println(table.parts[0].p)
	for i := 0; i < 1e5; i++ {
		table.AddItems([][]byte{
			[]byte("key"+strconv.Itoa(i)),
		})
	}

}