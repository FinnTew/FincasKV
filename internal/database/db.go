package database

import (
	"github.com/FinnTew/FincasKV/internal/database/redis"
	"github.com/FinnTew/FincasKV/internal/storage"
)

type FincasDB struct {
	*redis.RString
	*redis.RHash
	*redis.RList
	*redis.RSet
	*redis.RZSet
}

func NewFincasDB(bcOpts ...storage.Option) *FincasDB {
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
