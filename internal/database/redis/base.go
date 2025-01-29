package redis

import (
	"github.com/FinnTew/FincasKV/internal/database/base"
	"github.com/FinnTew/FincasKV/internal/storage"
	"sync"
	"time"
)

var (
	StringPrefix = "string:"
	HashPrefix   = "hash:"
	ListPrefix   = "list:"
	SetPrefix    = "set:"
	ZSetPrefix   = "zset:"
)

var bitcaskOpts = []storage.Option{
	storage.WithDataDir("./fincas"),
}

type DBWrapper struct {
	db     *base.BaseDB
	dbOnce sync.Once
}

func (d *DBWrapper) GetDB() *base.BaseDB {
	d.dbOnce.Do(func() {
		d.db, _ = base.NewDB(
			base.DefaultBaseDBOptions(),
			bitcaskOpts...,
		)
	})

	return d.db
}

type ZMember struct {
	Score  float64
	Member string
}

type RedisString interface {
	Set(key, value string) error
	Get(key string) (string, error)
	Incr(key string) (int64, error)
	IncrBy(key string, value int64) (int64, error)
	Decr(key string) (int64, error)
	DecrBy(key string, value int64) (int64, error)
	Append(key, value string) (int64, error)
	GetSet(key, value string) (string, error)
	SetNX(key, value string) (bool, error)
	MSet(pairs map[string]string) error
	MGet(keys ...string) (map[string]string, error)
	StrLen(key string) (int64, error)
	TTL(key string) (time.Duration, error)
}

type RedisList interface {
	LPush(key string, values ...string) (int64, error)
	RPush(key string, values ...string) (int64, error)
	LPop(key string) (string, error)
	RPop(key string) (string, error)
	LLen(key string) (int64, error)
	LRange(key string, start, stop int) ([]string, error)
	LTrim(key string, start, stop int) error
	BLPop(timeout time.Duration, keys ...string) (map[string]string, error)
	BRPop(timeout time.Duration, keys ...string) (map[string]string, error)
	LInsertBefore(key, pivot, value string) (int64, error)
	LInsertAfter(key, pivot, value string) (int64, error)
}

type RedisHash interface {
	HSet(key, field, value string) error
	HGet(key, field string) (string, error)
	HMSet(key string, fields map[string]string) error
	HMGet(key string, fields ...string) (map[string]string, error)
	HDel(key string, fields ...string) (int64, error)
	HExists(key, field string) (bool, error)
	HKeys(key string) ([]string, error)
	HVals(key string) ([]string, error)
	HGetAll(key string) (map[string]string, error)
	HLen(key string) (int64, error)
	HIncrBy(key, field string, incr int64) (int64, error)
	HIncrByFloat(key, field string, incr float64) (float64, error)
	HSetNX(key, field, value string) (bool, error)
	HStrLen(key, field string) (int64, error)
}

type RedisSet interface {
	SAdd(key string, members ...string) (int64, error)
	SRem(key string, members ...string) (int64, error)
	SIsMember(key, member string) (bool, error)
	SMembers(key string) ([]string, error)
	SCard(key string) (int64, error)
	SPop(key string) (string, error)
	SPopN(key string, count int) ([]string, error)
	SRandMember(key string, count int) ([]string, error)
	SDiff(keys ...string) ([]string, error)
	SUnion(keys ...string) ([]string, error)
	SInter(keys ...string) ([]string, error)
	SMove(source, destination, member string) (bool, error)
}

type RedisZSet interface {
	ZAdd(key string, members ...ZMember) (int64, error)
	ZRange(key string, start, stop int) ([]ZMember, error)
	ZRevRange(key string, start, stop int) ([]ZMember, error)
	ZRangeWithScores(key string, start, stop int) ([]ZMember, error)
	ZRevRangeWithScores(key string, start, stop int) ([]ZMember, error)
	ZRank(key, member string) (int64, error)
	ZRevRank(key, member string) (int64, error)
	ZRem(key string, members ...string) (int64, error)
	ZCard(key string) (int64, error)
	ZScore(key, member string) (float64, error)
	ZIncrBy(key, member string, increment float64) (float64, error)
	ZRangeByScore(key string, min, max float64) ([]ZMember, error)
	ZRangeByScoreWithScores(key string, min, max float64) ([]ZMember, error)
	ZCount(key string, min, max float64) (int64, error)
	ZRemRangeByRank(key string, start, stop int) (int64, error)
	ZRemRangeByScore(key string, min, max float64) (int64, error)
}
