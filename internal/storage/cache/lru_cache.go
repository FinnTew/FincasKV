package cache

import (
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"log"
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
	evicted := c.Add(key, value)
	if evicted {
		log.Printf("LRUCache: evicted when insert {key=%v value=%v}", key, value)
	}
	return nil
}

func (c *LRUCache[K, V]) Find(key K) (V, error) {
	value, exist := c.Get(key)
	var zero V
	if !exist {
		return zero, fmt.Errorf("cannot find value [%v] into LRU cache", key)
	}
	return value, nil
}

func (c *LRUCache[K, V]) Delete(key K) error {
	if present := c.Remove(key); !present {
		return fmt.Errorf("cannot find value [%v] into LRU cache", key)
	}
	return nil
}

func (c *LRUCache[K, V]) Exist(key K) bool {
	return c.Contains(key)
}
