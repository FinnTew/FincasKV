package storage

var (
	FilePrefix = "data-"
	FileSuffix = ".flog"
	// HeaderSize 记录头部大小: timestamp(8) + flags(4) + keyLen(4) + valueLen(4) = 20 bytes
	HeaderSize = 20
	// MaxKeySize 键最大长度 32MB
	MaxKeySize = 32 << 20
	// MaxValueSize 值最大长度 32MB
	MaxValueSize = 32 << 20
)

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
	Find(key KeyType) (ValueType, error)
	Delete(key KeyType) error
	Exist(key KeyType) bool
}
