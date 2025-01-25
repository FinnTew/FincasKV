package storage

import (
	"github.com/FinnTew/FincasKV/util"
	"log"
	"math/rand"
	"time"
)

type MemIndexType string

const (
	BTree    MemIndexType = "btree"
	SkipList MemIndexType = "skiplist"
)

type MemCacheType string

const (
	LRU MemCacheType = "lru"
)

type Options struct {
	// 内存索引相关
	MemIndexDS         MemIndexType // 内存索引数据结构
	BTreeDegree        int          // B树的度
	SkipListRandSource rand.Source  // 跳表的随机源

	// 内存缓存相关
	OpenMemCache bool         // 是否开启内存缓存
	MemCacheDS   MemCacheType // 内存缓存数据结构
	MemCacheSize int          // 内存缓存大小

	// 文件管理器相关
	MaxFileSize  int64         // 每个文件的最大大小
	MaxOpenFiles int           // 最大打开文件数
	SyncInterval time.Duration // 同步间隔
}

type Option func(opt *Options)

func DefaultOptions() *Options {
	source, err := util.NewSecureRandSource()
	if err != nil {
		log.Panic("Get secure rand source failed: ", err)
	}
	return &Options{
		MemIndexDS:         SkipList,
		BTreeDegree:        8,
		SkipListRandSource: rand.New(source),
		OpenMemCache:       true,
		MemCacheDS:         LRU,
		MemCacheSize:       1 << 10,
		MaxFileSize:        1 << 30,
		MaxOpenFiles:       10,
		SyncInterval:       5 * time.Second,
	}
}

func WithMemIndexDS(memIndexDS MemIndexType) Option {
	return func(opt *Options) {
		opt.MemIndexDS = memIndexDS
	}
}

func WithBTreeDegree(bTreeDegree int) Option {
	return func(opt *Options) {
		opt.BTreeDegree = bTreeDegree
	}
}

func WithSkipListRandSource(skipListRandSource rand.Source) Option {
	return func(opt *Options) {
		opt.SkipListRandSource = skipListRandSource
	}
}

func WithOpenMemCache(openMemCache bool) Option {
	return func(opt *Options) {
		opt.OpenMemCache = openMemCache
	}
}

func WithMemCacheDS(memCacheDS MemCacheType) Option {
	return func(opt *Options) {
		opt.MemCacheDS = memCacheDS
	}
}

func WithMemCacheSize(memCacheSize int) Option {
	return func(opt *Options) {
		opt.MemCacheSize = memCacheSize
	}
}

func WithMaxFileSize(maxFileSize int64) Option {
	return func(opt *Options) {
		opt.MaxFileSize = maxFileSize
	}
}

func WithMaxOpenFiles(maxOpenFiles int) Option {
	return func(opt *Options) {
		opt.MaxOpenFiles = maxOpenFiles
	}
}

func WithSyncInterval(interval time.Duration) Option {
	return func(opt *Options) {
		opt.SyncInterval = interval
	}
}
