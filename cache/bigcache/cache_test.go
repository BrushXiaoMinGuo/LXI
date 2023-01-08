package bigcache

import (
	"context"
	"testing"
	"time"
)

func TestNewBigCache(t *testing.T) {
	c := NewBigCache(context.Background())
	c.Set([]byte("key"), []byte("value"))
	time.Sleep(time.Second*60)
	c.Get([]byte("key"))
}
