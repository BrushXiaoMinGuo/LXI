package bigcache

import (
	"encoding/binary"
)

const (
	ShardSize     = 64 * 1024
	LeftMargin    = 1
	MaxHeaderSize = 6
)

type BytesQueue struct {
	entries     []byte
	headBuffer  []byte
	head        int
	tail        int
	count       int
	rightMargin int
}

var (
	emptyError = &QueueError{message: "queue is empty"}
)

func NewBytesQueue() *BytesQueue {
	return &BytesQueue{
		entries:     make([]byte, ShardSize),
		headBuffer:  make([]byte, MaxHeaderSize),
		head:        LeftMargin,
		tail:        LeftMargin,
		rightMargin: LeftMargin,
	}
}

func (q *BytesQueue) Push(data []byte) int {
	needSize := getNeedSize(len(data))
	return q.push(data, needSize)

}

func (q *BytesQueue) push(data []byte, needSize int) int {
	headLength := binary.PutUvarint(q.headBuffer, uint64(needSize))
	index := q.tail
	q.copy(q.headBuffer, headLength)
	q.copy(data, needSize-headLength)
	if q.tail > q.head {
		q.rightMargin = q.tail
	}
	q.count += 1
	return index
}

func (q *BytesQueue) Peek() ([]byte, int, error) {
	return q.peek(q.head)
}

func (q *BytesQueue) Pop() ([]byte, error) {
	data, blockSize, err := q.peek(q.head)
	if err != nil {
		return nil, err
	}
	q.head += blockSize
	q.count -= 1
	return data, nil
}

func (q *BytesQueue) peek(index int) ([]byte, int, error) {
	if err := q.peekCheck(); err != nil {
		return nil, 0, err
	}
	blockSize, n := binary.Uvarint(q.entries[index:])
	return q.entries[index+n : index+int(blockSize)], int(blockSize), nil

}

func (q *BytesQueue) peekCheck() error {
	if q.count == 0 {
		return emptyError
	}
	return nil
}

func (q *BytesQueue) copy(data []byte, length int) {
	q.tail += copy(q.entries[q.tail:], data[:length])
}

func getNeedSize(length int) int {
	var header int
	switch {
	case length < 127: // 1<<7-1
		header = 1
	case length < 16382: // 1<<14-2
		header = 2
	case length < 2097149: // 1<<21 -3
		header = 3
	case length < 268435452: // 1<<28 -4
		header = 4
	default:
		header = 5
	}
	return header + length
}

type QueueError struct {
	message string
}

func (e *QueueError) Error() string {
	return e.message
}
