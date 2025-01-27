package database

import "time"

type DB interface {
	Put(key string, value string) error
	Get(key string) (string, error)
	Del(key string) error
	PutWithTTL(key, value string, ttl time.Duration) error
	Begin() (Transaction, error)
	Close()
}

type Transaction interface {
	Put(key string, value string) error
	Get(key string) (string, error)
	Del(key string) error
	PutWithTTL(key, value string, ttl time.Duration) error
	Commit() error
	Rollback() error
}

type RedisString interface {
	// TODO: add methods definitions for redis string
}

type RedisList interface {
	// TODO: add methods definitions for redis list
}

type RedisHash interface {
	// TODO: add methods definitions for redis hash
}

type RedisSet interface {
	// TODO: add methods definitions for redis set
}

type RedisZSet interface {
	// TODO: add methods definitions for redis zset
}

type FincasDB interface {
	// TODO: add methods definitions for fincas db,
	//       supported redis data struct both Transaction and Without-Transaction
}
