package redis

import (
	"errors"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/err_def"
	"strconv"
	"strings"
	"sync"
)

type RString struct {
	dw *DBWrapper
}

var stringPool = sync.Pool{
	New: func() interface{} {
		return &RString{
			dw: &DBWrapper{},
		}
	},
}

func NewRString(dw *DBWrapper) *RString {
	rs := stringPool.Get().(*RString)
	rs.dw = dw
	return rs
}

func (rs *RString) Release() {
	stringPool.Put(rs)
}

func (rs *RString) Set(key, value string) error {
	if len(key) == 0 {
		return err_def.ErrEmptyKey
	}
	return rs.dw.GetDB().Put(GetStringKey(key), value)
}

func (rs *RString) Get(key string) (string, error) {
	if len(key) == 0 {
		return "", err_def.ErrEmptyKey
	}
	return rs.dw.GetDB().Get(GetStringKey(key))
}

func (rs *RString) Del(keys ...string) error {
	if len(keys) == 0 {
		return err_def.ErrEmptyKey
	}

	wb := rs.dw.GetDB().NewWriteBatch(nil)
	for _, key := range keys {
		if err := wb.Delete(GetStringKey(key)); err != nil {
			return err
		}
	}

	return wb.Commit()
}

func (rs *RString) Incr(key string) (int64, error) {
	return rs.IncrBy(key, 1)
}

func (rs *RString) IncrBy(key string, value int64) (int64, error) {
	if len(key) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	strKey := GetStringKey(key)
	wb := rs.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	val, err := rs.dw.GetDB().Get(strKey)
	if err != nil {
		// Key不存在时设为初始值
		if err := wb.Put(strKey, strconv.FormatInt(value, 10)); err != nil {
			return 0, err
		}
		if err := wb.Commit(); err != nil {
			return 0, err
		}
		return value, nil
	}

	// 尝试转换为int64
	current, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err_def.ErrValueNotInteger
	}

	result := current + value
	if err := wb.Put(strKey, strconv.FormatInt(result, 10)); err != nil {
		return 0, err
	}
	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return result, nil
}

func (rs *RString) Decr(key string) (int64, error) {
	return rs.DecrBy(key, 1)
}

func (rs *RString) DecrBy(key string, value int64) (int64, error) {
	return rs.IncrBy(key, -value)
}

func (rs *RString) Append(key, value string) (int64, error) {
	if len(key) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	strKey := GetStringKey(key)
	wb := rs.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	val, err := rs.dw.GetDB().Get(strKey)
	if err != nil {
		if err := wb.Put(strKey, value); err != nil {
			return 0, err
		}
		if err := wb.Commit(); err != nil {
			return 0, err
		}
		return int64(len(value)), nil
	}

	newVal := val + value
	if err := wb.Put(strKey, newVal); err != nil {
		return 0, err
	}
	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return int64(len(newVal)), nil
}

func (rs *RString) GetSet(key, value string) (string, error) {
	if len(key) == 0 {
		return "", err_def.ErrEmptyKey
	}

	strKey := GetStringKey(key)
	wb := rs.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	oldVal, err := rs.dw.GetDB().Get(strKey)
	if err != nil && !errors.Is(err, err_def.ErrKeyNotFound) {
		return "", err
	}

	if err := wb.Put(strKey, value); err != nil {
		return "", err
	}
	if err := wb.Commit(); err != nil {
		return "", err
	}

	return oldVal, nil
}

func (rs *RString) SetNX(key, value string) (bool, error) {
	if len(key) == 0 {
		return false, err_def.ErrEmptyKey
	}

	strKey := GetStringKey(key)
	wb := rs.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	exists, err := rs.dw.GetDB().Exists(strKey)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}

	if err := wb.Put(strKey, value); err != nil {
		return false, err
	}
	if err := wb.Commit(); err != nil {
		return false, err
	}

	return true, nil
}

func (rs *RString) MSet(pairs map[string]string) error {
	if len(pairs) == 0 {
		return nil
	}

	wb := rs.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	for k, v := range pairs {
		if len(k) == 0 {
			return err_def.ErrEmptyKey
		}
		if err := wb.Put(GetStringKey(k), v); err != nil {
			return err
		}
	}

	return wb.Commit()
}

func (rs *RString) MGet(keys ...string) (map[string]string, error) {
	if len(keys) == 0 {
		return make(map[string]string), nil
	}

	result := make(map[string]string, len(keys))
	var errs []string

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, key := range keys {
		if len(key) == 0 {
			errs = append(errs, fmt.Sprintf("empty key found in position %d", len(result)))
			continue
		}

		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			val, err := rs.dw.GetDB().Get(GetStringKey(k))
			if err != nil {
				if !errors.Is(err, err_def.ErrKeyNotFound) {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("error getting key %s: %v", k, err))
					mu.Unlock()
				}
				return
			}

			mu.Lock()
			result[k] = val
			mu.Unlock()
		}(key)
	}

	wg.Wait()

	if len(errs) > 0 {
		return result, fmt.Errorf("multiple errors occurred: %s", strings.Join(errs, "; "))
	}

	return result, nil
}

func (rs *RString) StrLen(key string) (int64, error) {
	if len(key) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	val, err := rs.dw.GetDB().Get(GetStringKey(key))
	if err != nil {
		if errors.Is(err, err_def.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}

	return int64(len(val)), nil
}
