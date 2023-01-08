package bigcache

import (
	"context"
	xxhash "github.com/cespare/xxhash/v2"
	"time"
)

const (
	Shards      = 512
	ShardMask   = 511
	LifeWindow  = 30
	CleanWindow = 5
)

type BigCache struct {
	shards []*CacheShard
}

func NewBigCache(ctx context.Context) *BigCache {
	c := BigCache{}
	c.shards = make([]*CacheShard, Shards)
	for i := 0; i < Shards; i++ {
		c.shards[i] = c.shards[i].InitShard()
	}

	go func() {
		for {
			ticker := time.NewTicker(time.Second * CleanWindow)
			select {
			case t := <-ticker.C:
				c.ClearUp(t)
			case <-ctx.Done():
				return
			}
		}
	}()

	return &c
}

func (c *BigCache) Set(k, v []byte) {
	hashIndex := xxhash.Sum64(k)
	shardIndex := hashIndex & ShardMask
	c.shards[shardIndex].Set(k, v, hashIndex)
}

func (c *BigCache) Get(k []byte) {
	hashIndex := xxhash.Sum64(k)
	shardIndex := hashIndex & ShardMask
	c.shards[shardIndex].Get(k, hashIndex)

}

func (c *BigCache) ClearUp(t time.Time) {

	for i := 0; i < Shards; i++ {
		c.shards[i].CleanUp(t)
	}
}
