package redis

import (
	"errors"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/storage/err_def"
	"strconv"
)

type RHash struct {
}

func (rh *RHash) HSet(key, field, value string) error {
	db := (&DBWrapper{}).GetDB()
	key = GetHashFieldKey(key, field)
	return db.Put(key, value)
}

func (rh *RHash) HGet(key, field string) (string, error) {
	db := (&DBWrapper{}).GetDB()
	key = GetHashFieldKey(key, field)
	return db.Get(key)
}

func (rh *RHash) HMSet(key string, fields map[string]string) error {
	db := (&DBWrapper{}).GetDB()
	// TODO: implement HMSet
	return nil
}

func (rh *RHash) HMGet(key string, fields ...string) (map[string]string, error) {
	db := (&DBWrapper{}).GetDB()
	// TODO: implement HMGet
	return nil, nil
}

func (rh *RHash) HDel(key string, fields ...string) (int64, error) {
	db := (&DBWrapper{}).GetDB()
	// TODO: implement HDel
	return 0, nil
}

func (rh *RHash) HExists(key, field string) (bool, error) {
	db := (&DBWrapper{}).GetDB()
	key = GetHashFieldKey(key, field)
	return db.Exists(key)
}

func (rh *RHash) HKeys(key string) ([]string, error) {
	db := (&DBWrapper{}).GetDB()
	// TODO: implement HKeys
	return nil, nil
}

func (rh *RHash) HVals(key string) ([]string, error) {
	db := (&DBWrapper{}).GetDB()
	// TODO: implement HVals
	return nil, nil
}

func (rh *RHash) HGetAll(key string) (map[string]string, error) {
	db := (&DBWrapper{}).GetDB()
	// TODO: implement HGetAll
	return nil, nil
}

func (rh *RHash) HLen(key string) (int64, error) {
	db := (&DBWrapper{}).GetDB()
	key = GetHashLenKey(key)
	val, err := db.Get(key)
	if err != nil && !errors.Is(err, err_def.ErrKeyNotFound) {
		return 0, err
	}

	if errors.Is(err, err_def.ErrKeyNotFound) {
		return 0, fmt.Errorf("hash %s not found", key)
	}

	length, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}

	return int64(length), nil
}

func (rh *RHash) HIncrBy(key, field string, incr int64) (int64, error) {
	return 0, nil
}

func (rh *RHash) HIncrByFloat(key, field string, incr float64) (float64, error) {
	return 0, nil
}

func (rh *RHash) HSetNX(key, field, value string) (bool, error) {
	return false, nil
}

func (rh *RHash) HStrLen(key, field string) (int64, error) {
	db := (&DBWrapper{}).GetDB()
	key = GetHashFieldKey(key, field)
	data, err := db.Get(key)
	if err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}
