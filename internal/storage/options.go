package storage

import (
	"github.com/FinnTew/FincasKV/util"
	"log"
	"math/rand"
	"time"
)

type BTreeLessFunc[K comparable] func(a, b K) bool
type SkipListLessFunc[K comparable] func(a, b K) int

type MemIndexType string

const (
	BTree      MemIndexType = "btree"
	SkipList   MemIndexType = "skiplist"
	SwissTable MemIndexType = "swisstable"
)

type MemCacheType string

const (
	LRU MemCacheType = "lru"
)

type Options struct {
	// 基本配置
	DataDir string

	// 内存索引相关
	MemIndexDS         MemIndexType             // 内存索引数据结构
	MemIndexShardCount int                      // 内存索引分片数量
	BTreeDegree        int                      // B树的度
	BTreeComparator    BTreeLessFunc[string]    // B树的比较器
	SkipListRandSource rand.Source              // 跳表的随机源
	SkipListComparator SkipListLessFunc[string] // 跳表的比较器
	SwissTableSize     uint32                   // SwissTable 的大小

	// 内存缓存相关
	OpenMemCache bool         // 是否开启内存缓存
	MemCacheDS   MemCacheType // 内存缓存数据结构
	MemCacheSize int          // 内存缓存大小

	// 文件管理器相关
	MaxFileSize  int64         // 每个文件的最大大小
	MaxOpenFiles int           // 最大打开文件数
	SyncInterval time.Duration // 同步间隔

	// Merge 相关
	AutoMerge     bool
	MergeInterval time.Duration
	MinMergeRatio float64
}

type Option func(opt *Options)

func DefaultOptions() *Options {
	source, err := util.NewSecureRandSource()
	if err != nil {
		log.Panic("Get secure rand source failed: ", err)
	}
	return &Options{
		DataDir:            "/tmp/fincas",
		MemIndexDS:         SwissTable,
		MemIndexShardCount: 1 << 8,
		BTreeDegree:        8,
		BTreeComparator: func(a, b string) bool {
			return a < b
		},
		SkipListRandSource: rand.New(source),
		SkipListComparator: func(a, b string) int {
			if a < b {
				return -1
			} else if a > b {
				return 1
			} else {
				return 0
			}
		},
		SwissTableSize: 1 << 10,
		OpenMemCache:   true,
		MemCacheDS:     LRU,
		MemCacheSize:   1 << 10,
		MaxFileSize:    1 << 30,
		MaxOpenFiles:   10,
		SyncInterval:   5 * time.Second,
		AutoMerge:      true,
		MergeInterval:  time.Hour,
		MinMergeRatio:  0.3,
	}
}

func WithDataDir(dataDir string) Option {
	return func(opt *Options) {
		opt.DataDir = dataDir
	}
}

func WithMemIndexDS(memIndexDS MemIndexType) Option {
	return func(opt *Options) {
		opt.MemIndexDS = memIndexDS
	}
}

func WithMemIndexShardCount(memIndexShardCount int) Option {
	return func(opt *Options) {
		opt.MemIndexShardCount = memIndexShardCount
	}
}

func WithBTreeDegree(bTreeDegree int) Option {
	return func(opt *Options) {
		opt.BTreeDegree = bTreeDegree
	}
}

func WithBTreeComparator(bTreeComparator BTreeLessFunc[string]) Option {
	return func(opt *Options) {
		opt.BTreeComparator = bTreeComparator
	}
}

func WithSkipListRandSource(skipListRandSource rand.Source) Option {
	return func(opt *Options) {
		opt.SkipListRandSource = skipListRandSource
	}
}

func WithSkipListComparator(skipListComparator SkipListLessFunc[string]) Option {
	return func(opt *Options) {
		opt.SkipListComparator = skipListComparator
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

func WithAutoMerge(autoMerge bool) Option {
	return func(opt *Options) {
		opt.AutoMerge = autoMerge
	}
}

func WithMergeInterval(interval time.Duration) Option {
	return func(opt *Options) {
		opt.MergeInterval = interval
	}
}

func WithMinMergeRatio(minMergeRatio float64) Option {
	return func(opt *Options) {
		opt.MinMergeRatio = minMergeRatio
	}
}
