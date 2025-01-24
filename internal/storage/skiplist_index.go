package storage

import (
	"math/rand"
	"sync"
	"time"
)

const (
	maxLevel    = 32
	probability = 0.25
)

type CompareFunc[K any] func(a, b K) int

type node[K any, V any] struct {
	key   K
	value V
	next  []*node[K, V]
}

type SkipListIndex[K any, V any] struct {
	head    *node[K, V]
	level   int
	size    int
	lock    sync.RWMutex
	rand    *rand.Rand
	compare CompareFunc[K]
}

type SkipListOption func(interface{})

func WithRandSource(source rand.Source) SkipListOption {
	return func(sl interface{}) {
		if s, ok := sl.(*rand.Rand); ok {
			*s = *rand.New(source)
		}
	}
}

func NewSkipListIndex[K any, V any](compare CompareFunc[K], opts ...SkipListOption) *SkipListIndex[K, V] {
	if compare == nil {
		panic("compare function cannot be nil")
	}

	sl := &SkipListIndex[K, V]{
		head:    &node[K, V]{next: make([]*node[K, V], maxLevel)},
		level:   1,
		compare: compare,
		rand:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	for _, opt := range opts {
		opt(sl.rand)
	}

	return sl
}

func (sl *SkipListIndex[K, V]) randomLevel() int {
	level := 1
	for level < maxLevel && sl.rand.Float64() < probability {
		level++
	}
	return level
}

func (sl *SkipListIndex[K, V]) Put(key K, value V) error {
	sl.lock.Lock()
	defer sl.lock.Unlock()

	update := make([]*node[K, V], maxLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && sl.compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}

	current = current.next[0]

	if current != nil && sl.compare(current.key, key) == 0 {
		current.value = value
		return nil
	}

	level := sl.randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.head
		}
		sl.level = level
	}

	newNode := &node[K, V]{
		key:   key,
		value: value,
		next:  make([]*node[K, V], level),
	}

	for i := 0; i < level; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	sl.size++
	return nil
}

func (sl *SkipListIndex[K, V]) Get(key K) (V, error) {
	sl.lock.RLock()
	defer sl.lock.RUnlock()

	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && sl.compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
	}

	current = current.next[0]
	if current != nil && sl.compare(current.key, key) == 0 {
		return current.value, nil
	}

	var zero V
	return zero, ErrKeyNotFound
}

func (sl *SkipListIndex[K, V]) Del(key K) error {
	sl.lock.Lock()
	defer sl.lock.Unlock()

	update := make([]*node[K, V], maxLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && sl.compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}

	current = current.next[0]
	if current == nil || sl.compare(current.key, key) != 0 {
		return ErrKeyNotFound
	}

	for i := 0; i < sl.level; i++ {
		if update[i].next[i] != current {
			break
		}
		update[i].next[i] = current.next[i]
	}

	for sl.level > 1 && sl.head.next[sl.level-1] == nil {
		sl.level--
	}

	sl.size--
	return nil
}

func (sl *SkipListIndex[K, V]) Foreach(f func(key K, value V) bool) error {
	sl.lock.RLock()
	defer sl.lock.RUnlock()

	current := sl.head.next[0]
	for current != nil {
		if !f(current.key, current.value) {
			break
		}
		current = current.next[0]
	}
	return nil
}

func (sl *SkipListIndex[K, V]) Clear() error {
	sl.lock.Lock()
	defer sl.lock.Unlock()

	sl.head = &node[K, V]{next: make([]*node[K, V], maxLevel)}
	sl.level = 1
	sl.size = 0
	return nil
}

func (sl *SkipListIndex[K, V]) Size() int {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	return sl.size
}
