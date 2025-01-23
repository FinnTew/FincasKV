package storage

import (
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
)

type LRUCache[K comparable, V any] struct {
	*lru.Cache[K, V]
}

func NewLRUCache[K comparable, V any](size int) *LRUCache[K, V] {
	cache, _ := lru.New[K, V](size)
	return &LRUCache[K, V]{
		Cache: cache,
	}
}

func (c *LRUCache[K, V]) Insert(key K, value V) error {
	if c.Add(key, value) {
		return nil
	}
	return fmt.Errorf("cannot insert value [%v] into LRU cache", value)
}

func (c *LRUCache[K, V]) Get(key K) (V, error) {
	value, err := c.Get(key)
	var zero V
	if err != nil {
		return zero, err
	}
	return value, nil
}

func (c *LRUCache[K, V]) Remove(key K) error {
	if err := c.Remove(key); err != nil {
		return fmt.Errorf("cannot remove value [%v] into LRU cache: %v", key, err)
	}
	return nil
}

func (c *LRUCache[K, V]) Contains(key K) bool {
	return c.Contains(key)
}
