package storage

import (
	"github.com/FinnTew/FincasKV/util"
	"log"
	"math/rand"
)

type MemIndexType int

const (
	BTree MemIndexType = iota
	SkipList
)

type Options struct {
	// 内存索引相关
	MemIndexDS         MemIndexType // 内存索引数据结构
	BTreeDegree        int          // B树的度
	SkipListRandSource rand.Source  // 跳表的随机源

	// 内存缓存相关
	OpenMemCache bool // 是否开启内存缓存
	MemCacheSize int  // 内存缓存大小

	// TODO: add config options here
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
		// TODO: add more default options here
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

func WithMemCacheSize(memCacheSize int) Option {
	return func(opt *Options) {
		opt.MemCacheSize = memCacheSize
	}
}
