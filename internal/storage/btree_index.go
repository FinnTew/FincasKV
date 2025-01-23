package storage

import (
	"fmt"
	"github.com/google/btree"
	"sync"
)

type Item[K comparable, V any] struct {
	key   K
	value V
	less  func(a, b K) bool
}

func (i *Item[K, V]) Less(than btree.Item) bool {
	other := than.(*Item[K, V])
	return i.less(i.key, other.key)
}

type BTreeIndex[K comparable, V any] struct {
	tree       *btree.BTree
	lock       sync.RWMutex
	comparator func(a, b K) bool
}

func NewBTreeIndex[K comparable, V any](degree int, less func(a, b K) bool) *BTreeIndex[K, V] {
	return &BTreeIndex[K, V]{
		tree:       btree.New(degree),
		comparator: less,
	}
}

func (b *BTreeIndex[K, V]) Put(key K, value V) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	item := &Item[K, V]{
		key:   key,
		value: value,
		less:  b.comparator,
	}
	b.tree.ReplaceOrInsert(item)
	return nil
}

func (b *BTreeIndex[K, V]) Get(key K) (V, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	item := &Item[K, V]{
		key:  key,
		less: b.comparator,
	}
	if found := b.tree.Get(item); found != nil {
		return found.(*Item[K, V]).value, nil
	}

	var zero V
	return zero, fmt.Errorf("key not found: %v", key)
}

func (b *BTreeIndex[K, V]) Del(key K) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	item := &Item[K, V]{
		key:  key,
		less: b.comparator,
	}
	if deleted := b.tree.Delete(item); deleted == nil {
		return fmt.Errorf("key not found: %v", key)
	}
	return nil
}

func (b *BTreeIndex[K, V]) Foreach(f func(key K, value V) bool) error {
	b.lock.RLock()
	defer b.lock.RUnlock()

	b.tree.Ascend(func(i btree.Item) bool {
		item := i.(*Item[K, V])
		return f(item.key, item.value)
	})
	return nil
}

func (b *BTreeIndex[K, V]) Clear() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.tree.Clear(false)
	return nil
}
