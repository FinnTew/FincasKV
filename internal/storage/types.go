package storage

import (
	"os"
	"sync"
	"sync/atomic"
)

type KVItem struct {
	Key   []byte
	Value []byte
}

type Record struct {
	Timestamp int64
	Checksum  uint64
	Flags     uint32
	KVItem
}

type Entry struct {
	FileID    int
	Offset    int64
	Size      uint32
	Timestamp int64
}

type DataFile struct {
	ID     int
	Path   string
	File   *os.File
	Offset atomic.Int64
	Closed atomic.Bool
	mu     sync.Mutex
}
