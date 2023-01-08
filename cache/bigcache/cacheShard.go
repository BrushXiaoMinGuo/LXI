package bigcache

import (
	"fmt"
	"sync"
	"time"
)

const EntryCounts = 1024

type CacheShard struct {
	mu          sync.RWMutex
	data        *BytesQueue
	indexHash   map[uint64]int
	entryBuffer []byte
}

func (s *CacheShard) InitShard() *CacheShard {

	return &CacheShard{
		data:        NewBytesQueue(),
		indexHash:   make(map[uint64]int, EntryCounts),
		entryBuffer: make([]byte, ShardSize),
	}
}

func (s *CacheShard) Set(k, v []byte, hashIndex uint64) {
	timeStamp := uint64(time.Now().Unix())

	s.mu.Lock()
	defer s.mu.Unlock()

	w := warpEntry(k, v, timeStamp, hashIndex, &s.entryBuffer)
	index := s.data.Push(w)
	s.indexHash[hashIndex] = index
}

func (s *CacheShard) Get(k []byte, hashIndex uint64) {

	index := s.indexHash[hashIndex]
	fmt.Println(index)
	s.getWarpedEntry(index)

}

func (s *CacheShard) getWarpedEntry(index int) {
	entry, _, err := s.data.peek(index)
	if err != nil {
		return
	}
	k, v, timeStamp, hashIndex := readEntry(entry)
	fmt.Println(string(k))
	fmt.Println(string(v))
	fmt.Println(timeStamp)
	fmt.Println(hashIndex)
}

func (s *CacheShard) CleanUp(t time.Time) {

	for {
		if oldestEntry, _, err := s.data.Peek(); err != nil {
			break
		} else if !s.onEvict(oldestEntry, t, s.removeEvictedEntry) {
			break
		}
	}
}

func (s *CacheShard) onEvict(entry []byte, t time.Time, f func()) bool {
	timeStamp := readEntryTimestamp(entry)
	if uint64(t.Unix())-timeStamp > LifeWindow {
		f()
		return true
	}
	return false
}

func (s *CacheShard) removeEvictedEntry() {
	entry, _ := s.data.Pop()
	hash := readEntryHash(entry)
	delete(s.indexHash, hash)
	fmt.Println("clean up ", hash)
}
