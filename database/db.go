package database

import (
	"github.com/FinnTew/FincasKV/config"
	redis2 "github.com/FinnTew/FincasKV/database/redis"
	"github.com/FinnTew/FincasKV/storage"
	"log"
)

type FincasDB struct {
	*redis2.RString
	*redis2.RHash
	*redis2.RList
	*redis2.RSet
	*redis2.RZSet
}

func NewFincasDB(dataDir string) *FincasDB {
	var bcOpts []storage.Option
	conf := config.Get()

	if conf.Base.DataDir != "" && dataDir == "" {
		bcOpts = append(bcOpts, storage.WithDataDir(conf.Base.DataDir))
	} else if dataDir != "" {
		bcOpts = append(bcOpts, storage.WithDataDir(dataDir))
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

	dw := redis2.NewBDWrapper(nil, bcOpts...)
	return &FincasDB{
		RString: redis2.NewRString(dw),
		RHash:   redis2.NewRHash(dw),
		RList:   redis2.NewRList(dw),
		RSet:    redis2.NewRSet(dw),
		RZSet:   redis2.NewRZSet(dw),
	}
}

func (db *FincasDB) Close() {
	db.RString.Release()
	db.RHash.Release()
	db.RList.Release()
	db.RSet.Release()
	db.RZSet.Release()
}
