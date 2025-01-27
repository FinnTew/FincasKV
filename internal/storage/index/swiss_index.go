package index

import (
	"fmt"
	"github.com/dolthub/swiss"
	"sync"
)

type SwissIndex[K comparable, V any] struct {
	swissTable *swiss.Map[K, V]
	mu         sync.RWMutex
}

func NewSwissIndex[K comparable, V any](size uint32) *SwissIndex[K, V] {
	return &SwissIndex[K, V]{
		swissTable: swiss.NewMap[K, V](size),
	}
}

func (s *SwissIndex[K, V]) Put(key K, value V) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.swissTable.Put(key, value)
	return nil
}

func (s *SwissIndex[K, V]) Get(key K) (V, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.swissTable.Get(key)
	if !ok {
		var zero V
		return zero, fmt.Errorf("no value found for key %v", key)
	}
	return value, nil
}

func (s *SwissIndex[K, V]) Del(key K) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ok := s.swissTable.Delete(key)
	if !ok {
		return fmt.Errorf("delete failed")
	}
	return nil
}

func (s *SwissIndex[K, V]) Foreach(f func(key K, value V) bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.swissTable.Iter(func(key K, value V) bool {
		if stop := f(key, value); stop {
			return false
		} else {
			return true
		}
	})

	return nil
}

func (s *SwissIndex[K, V]) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.swissTable.Clear()
	return nil
}
