package storage

type Storage[KeyType comparable, ValueType any] interface {
	Open(opts ...Options) (Storage[KeyType, ValueType], error)
	Put(key KeyType, value ValueType) error
	Get(key KeyType) (ValueType, error)
	Del(key ValueType) error
	ListKeys() ([]KeyType, error)
	Fold(f func(key KeyType, value ValueType) bool) error
	Merge() error
	Sync() error
	Close() error
}

type MemIndex[KeyType comparable, ValueType any] interface {
	Put(key KeyType, value ValueType) error
	Get(key KeyType) (ValueType, error)
	Del(key KeyType) error
	Foreach(f func(key KeyType, value ValueType) bool) error
	Clear() error
}

type MemCache[KeyType comparable, ValueType any] interface {
	Insert(key KeyType, value ValueType) error
	Get(key KeyType) (ValueType, error)
	Remove(key KeyType) error
	Contains(key KeyType) bool
}
