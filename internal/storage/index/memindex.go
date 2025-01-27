package index

import (
	"fmt"
	"github.com/FinnTew/FincasKV/internal/storage"
	"hash/fnv"
	"log"
	"math/rand"
	"sync"
)

type MemIndexShard[K comparable, V any] struct {
	shardCount int
	shards     []storage.MemIndex[K, V]
	sync.RWMutex
}

func NewMemIndexShard[K comparable, V any](
	memIndexType storage.MemIndexType,
	shardCount int,
	btreeDegree int,
	btreeLessFunc func(a, b K) bool,
	skipListRandSource rand.Source,
	skipListLessFunc func(a, b K) int,
) *MemIndexShard[K, V] {
	index := &MemIndexShard[K, V]{
		shardCount: shardCount,
		shards:     make([]storage.MemIndex[K, V], shardCount),
	}

	for i := 0; i < shardCount; i++ {
		switch memIndexType {
		case storage.BTree:
			if btreeDegree <= 0 {
				log.Fatal("BTree degree must be greater than 0")
			}
			if btreeLessFunc == nil {
				log.Fatal("BTree less func cannot be nil")
			}
			index.shards[i] = NewBTreeIndex[K, V](btreeDegree, btreeLessFunc)
		case storage.SkipList:
			if skipListLessFunc == nil {
				log.Fatal("SkipList less func cannot be nil")
			}
			if skipListRandSource == nil {
				index.shards[i] = NewSkipListIndex[K, V](skipListLessFunc)
			} else {
				index.shards[i] = NewSkipListIndex[K, V](skipListLessFunc, WithRandSource(skipListRandSource))
			}
		default:
			log.Fatal("Unsupported memIndex type")
		}
	}

	return index
}

func (s *MemIndexShard[K, V]) getShard(key K) storage.MemIndex[K, V] {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%v", key)))
	return s.shards[h.Sum32()%uint32(s.shardCount)]
}

func (s *MemIndexShard[K, V]) Put(key K, value V) error {
	s.Lock()
	defer s.Unlock()
	shard := s.getShard(key)
	return shard.Put(key, value)
}

func (s *MemIndexShard[K, V]) Get(key K) (V, error) {
	s.RLock()
	defer s.RUnlock()
	shard := s.getShard(key)
	return shard.Get(key)
}

func (s *MemIndexShard[K, V]) Del(key K) error {
	s.Lock()
	defer s.Unlock()
	shard := s.getShard(key)
	return shard.Del(key)
}

func (s *MemIndexShard[K, V]) Foreach(f func(key K, value V) bool) error {
	s.RLock()
	defer s.RUnlock()
	for _, shard := range s.shards {
		stop := false
		err := shard.Foreach(func(key K, value V) bool {
			if stop {
				return false
			}
			if !f(key, value) {
				stop = true
				return false
			}
			return true
		})
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}

func (s *MemIndexShard[K, V]) Clear() error {
	s.Lock()
	defer s.Unlock()

	var wg sync.WaitGroup
	errChan := make(chan error, len(s.shards))
	defer close(errChan)

	for _, shard := range s.shards {
		wg.Add(1)
		go func(s storage.MemIndex[K, V]) {
			defer wg.Done()
			if err := s.Clear(); err != nil {
				errChan <- err
			}
		}(shard)
	}
	wg.Wait()

	select {
	case err := <-errChan:
		return fmt.Errorf("could not clear index: %w", err)
	default:
		return nil
	}
}
