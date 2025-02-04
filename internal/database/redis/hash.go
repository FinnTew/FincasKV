package redis

import (
	"errors"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/err_def"
	"strconv"
	"strings"
	"sync"
)

type RHash struct {
	dw *DBWrapper
}

var hashPool = sync.Pool{
	New: func() interface{} {
		return &RHash{}
	},
}

func NewRHash(dw *DBWrapper) *RHash {
	rh := hashPool.Get().(*RHash)
	rh.dw = dw
	return rh
}

func (rh *RHash) Release() {
	hashPool.Put(rh)
}

func (rh *RHash) batchSetFields(key string, fields map[string]string, nx bool) error {
	if len(key) == 0 {
		return err_def.ErrEmptyKey
	}
	if len(fields) == 0 {
		return nil
	}

	wb := rh.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	lenKey := GetHashLenKey(key)
	currentLen, err := rh.HLen(key)
	if err != nil && !errors.Is(err, err_def.ErrKeyNotFound) {
		return err
	}

	newFields := 0
	for field, value := range fields {
		hashKey := GetHashFieldKey(key, field)

		if nx {
			exists, err := rh.dw.GetDB().Exists(hashKey)
			if err != nil {
				return err
			}
			if exists {
				continue
			}
		}

		if err := wb.Put(hashKey, value); err != nil {
			return err
		}

		if !nx {
			exists, err := rh.dw.GetDB().Exists(hashKey)
			if err != nil {
				return err
			}
			if !exists {
				newFields++
			}
		} else {
			newFields++
		}
	}

	if newFields > 0 {
		if err := wb.Put(lenKey, strconv.FormatInt(currentLen+int64(newFields), 10)); err != nil {
			return err
		}
	}

	return wb.Commit()
}

func (rh *RHash) HSet(key, field, value string) error {
	return rh.HMSet(key, map[string]string{field: value})
}

func (rh *RHash) HGet(key, field string) (string, error) {
	if len(key) == 0 || len(field) == 0 {
		return "", err_def.ErrEmptyKey
	}

	return rh.dw.GetDB().Get(GetHashFieldKey(key, field))
}

func (rh *RHash) HMSet(key string, fields map[string]string) error {
	return rh.batchSetFields(key, fields, false)
}

func (rh *RHash) HMGet(key string, fields ...string) (map[string]string, error) {
	if len(key) == 0 || len(fields) == 0 {
		return nil, err_def.ErrEmptyKey
	}

	result := make(map[string]string, len(fields))
	var errs []string
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, field := range fields {
		if len(field) == 0 {
			errs = append(errs, "empty field name")
			continue
		}

		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			val, err := rh.dw.GetDB().Get(GetHashFieldKey(key, f))
			if err != nil {
				if !errors.Is(err, err_def.ErrKeyNotFound) {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("error getting field %s: %v", f, err))
					mu.Unlock()
				}
			}

			mu.Lock()
			result[f] = val
			mu.Unlock()
		}(field)
	}

	wg.Wait()

	if len(errs) > 0 {
		return result, fmt.Errorf("multiple errors occurred: %s", strings.Join(errs, "; "))
	}

	return result, nil
}

func (rh *RHash) HDel(key string, fields ...string) (int64, error) {
	if len(key) == 0 || len(fields) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	wb := rh.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	var deleted int64
	for _, field := range fields {
		hashKey := GetHashFieldKey(key, field)
		exists, err := rh.dw.GetDB().Exists(hashKey)
		if err != nil {
			return 0, err
		}
		if !exists {
			continue
		}

		if err := wb.Delete(hashKey); err != nil {
			return 0, err
		}
		deleted++
	}

	if deleted > 0 {
		currLen, err := rh.HLen(key)
		if err != nil && !errors.Is(err, err_def.ErrKeyNotFound) {
			return 0, err
		}
		if err := wb.Put(GetHashLenKey(key), strconv.FormatInt(currLen-deleted, 10)); err != nil {
			return 0, err
		}
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return deleted, nil
}

func (rh *RHash) HExists(key, field string) (bool, error) {
	if len(key) == 0 || len(field) == 0 {
		return false, err_def.ErrEmptyKey
	}

	ok, err := rh.dw.GetDB().Exists(GetHashFieldKey(key, field))
	if err != nil {
		if !strings.HasPrefix(err.Error(), "no value found") {
			return false, err
		}
		return false, nil
	}

	return ok, nil
}

func (rh *RHash) HKeys(key string) ([]string, error) {
	if len(key) == 0 {
		return nil, err_def.ErrEmptyKey
	}

	pattern := fmt.Sprintf("%s:%s:*", HashPrefix, key)
	keys, err := rh.dw.GetDB().Keys(pattern)
	if err != nil {
		return nil, err
	}

	prefix := fmt.Sprintf("%s:%s:", HashPrefix, key)
	lenKey := GetHashLenKey(key)
	result := make([]string, 0, len(keys))

	for _, k := range keys {
		if k == lenKey {
			continue
		}
		field := strings.TrimPrefix(k, prefix)
		result = append(result, field)
	}

	return result, nil
}

func (rh *RHash) HVals(key string) ([]string, error) {
	if len(key) == 0 {
		return nil, err_def.ErrEmptyKey
	}

	fields, err := rh.HKeys(key)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(fields))
	for _, field := range fields {
		val, err := rh.HGet(key, field)
		if err != nil {
			continue
		}
		result = append(result, val)
	}

	return result, nil
}

func (rh *RHash) HGetAll(key string) (map[string]string, error) {
	if len(key) == 0 {
		return nil, err_def.ErrEmptyKey
	}

	fields, err := rh.HKeys(key)
	if err != nil {
		return nil, err
	}

	return rh.HMGet(key, fields...)
}

func (rh *RHash) HLen(key string) (int64, error) {
	if len(key) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	val, err := rh.dw.GetDB().Get(GetHashLenKey(key))
	if err != nil {
		if errors.Is(err, err_def.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}

	return strconv.ParseInt(val, 10, 64)
}

func (rh *RHash) HIncrBy(key, field string, incr int64) (int64, error) {
	if len(key) == 0 || len(field) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	hashKey := GetHashFieldKey(key, field)
	wb := rh.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	val, err := rh.dw.GetDB().Get(hashKey)
	if err != nil {
		if !errors.Is(err, err_def.ErrKeyNotFound) {
			return 0, err
		}
		if err := wb.Put(hashKey, strconv.FormatInt(incr, 10)); err != nil {
			return 0, err
		}

		if err := wb.Put(GetHashLenKey(key), "1"); err != nil {
			return 0, err
		}

		if err := wb.Commit(); err != nil {
			return 0, err
		}
		return incr, nil
	}

	current, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err_def.ErrValueNotInteger
	}

	result := current + incr
	if err := wb.Put(hashKey, strconv.FormatInt(result, 10)); err != nil {
		return 0, err
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return result, nil
}

func (rh *RHash) HIncrByFloat(key, field string, incr float64) (float64, error) {
	if len(key) == 0 || len(field) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	hashKey := GetHashFieldKey(key, field)
	wb := rh.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	val, err := rh.dw.GetDB().Get(hashKey)
	if err != nil {
		if !errors.Is(err, err_def.ErrKeyNotFound) {
			return 0, err
		}
		if err := wb.Put(hashKey, strconv.FormatFloat(incr, 'f', -1, 64)); err != nil {
			return 0, err
		}

		if err := wb.Put(GetHashLenKey(key), "1"); err != nil {
			return 0, err
		}

		if err := wb.Commit(); err != nil {
			return 0, err
		}
		return incr, nil
	}

	current, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, err_def.ErrValueNotFloat
	}

	result := current + incr
	if err := wb.Put(hashKey, strconv.FormatFloat(result, 'f', -1, 64)); err != nil {
		return 0, err
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return result, nil
}

func (rh *RHash) HSetNX(key, field, value string) (bool, error) {
	if len(key) == 0 || len(field) == 0 {
		return false, err_def.ErrEmptyKey
	}

	err := rh.batchSetFields(key, map[string]string{field: value}, true)
	if err != nil {
		return false, err
	}

	exists, err := rh.HExists(key, field)
	if err != nil {
		return false, err
	}

	return !exists, nil
}

func (rh *RHash) HStrLen(key, field string) (int64, error) {
	if len(key) == 0 || len(field) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	val, err := rh.HGet(key, field)
	if err != nil {
		if errors.Is(err, err_def.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}

	return int64(len(val)), nil
}
