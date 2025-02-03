package database

import (
	"github.com/FinnTew/FincasKV/internal/database/redis"
	"github.com/FinnTew/FincasKV/internal/storage"
	"log"
)

type FincasDB struct {
	*redis.RString
	*redis.RHash
	*redis.RList
	*redis.RSet
	*redis.RZSet
}

func NewFincasDB() *FincasDB {
	var bcOpts []storage.Option
	conf := GetConf()

	if conf.Base.DataDir != "" {
		bcOpts = append(bcOpts, storage.WithDataDir(conf.Base.DataDir))
	}

	if conf.MemIndex.DataStructure != "" {
		var memDS storage.MemIndexType
		switch conf.MemIndex.DataStructure {
		case "btree":
			memDS = storage.BTree
			bcOpts = append(bcOpts, storage.WithBTreeDegree(max(conf.MemIndex.BTreeDegree, 8)))
		case "skiplist":
			memDS = storage.SkipList
		case "swisstable":
			memDS = storage.SwissTable
			bcOpts = append(bcOpts, storage.WithBTreeDegree(max(conf.MemIndex.SwissTableInitialSize, 1024)))
		default:
			log.Fatal("Unsupported MemIndex data structure: " + conf.MemIndex.DataStructure)
		}
		bcOpts = append(bcOpts, storage.WithMemIndexDS(memDS))
	}
	bcOpts = append(bcOpts, storage.WithMemIndexShardCount(max(conf.MemIndex.ShardCount, 128)))

	if conf.MemCache.Enable {
		bcOpts = append(bcOpts, storage.WithOpenMemCache(true))
		switch conf.MemCache.DataStructure {
		case "lru":
			bcOpts = append(bcOpts, storage.WithMemCacheDS(storage.LRU))
			bcOpts = append(bcOpts, storage.WithMemCacheSize(max(conf.MemCache.Size, 1024)))
		default:
			log.Fatal("Unsupported MemCache data structure: " + conf.MemCache.DataStructure)
		}
	} else {
		bcOpts = append(bcOpts, storage.WithOpenMemCache(false))
	}

	bcOpts = append(bcOpts, storage.WithMaxFileSize(max(storage.DefaultOptions().MaxFileSize, int64(conf.FileManager.MaxSize))))
	bcOpts = append(bcOpts, storage.WithMaxOpenFiles(max(storage.DefaultOptions().MaxOpenFiles, conf.FileManager.MaxOpened)))
	bcOpts = append(bcOpts, storage.WithSyncInterval(max(storage.DefaultOptions().SyncInterval, conf.FileManager.SyncInterval)))

	if conf.Merge.Auto {
		bcOpts = append(bcOpts, storage.WithAutoMerge(true))
		bcOpts = append(bcOpts, storage.WithMergeInterval(max(storage.DefaultOptions().MergeInterval, conf.Merge.Interval)))
		bcOpts = append(bcOpts, storage.WithMinMergeRatio(max(storage.DefaultOptions().MinMergeRatio, conf.Merge.MinRatio)))
	} else {
		bcOpts = append(bcOpts, storage.WithAutoMerge(false))
	}

	dw := redis.NewBDWrapper(nil, bcOpts...)
	return &FincasDB{
		RString: redis.NewRString(dw),
		RHash:   redis.NewRHash(dw),
		RList:   redis.NewRList(dw),
		RSet:    redis.NewRSet(dw),
		RZSet:   redis.NewRZSet(dw),
	}
}

func (db *FincasDB) Close() {
	db.RString.Release()
	db.RHash.Release()
	db.RList.Release()
	db.RSet.Release()
	db.RZSet.Release()
}
