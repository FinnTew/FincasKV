package redis

import (
	"errors"
	"github.com/FinnTew/FincasKV/internal/database/base"
	"github.com/FinnTew/FincasKV/internal/err_def"
	"strconv"
	"sync"
	"time"
)

type RList struct {
	dw *DBWrapper
	mu sync.RWMutex // 用于阻塞操作
}

var listPool = sync.Pool{
	New: func() interface{} {
		return &RList{
			dw: &DBWrapper{},
		}
	},
}

func NewRList() *RList {
	return listPool.Get().(*RList)
}

func (rl *RList) Release() {
	listPool.Put(rl)
}

func (rl *RList) getListLen(key string) (int64, error) {
	lenStr, err := rl.dw.GetDB().Get(GetListLenKey(key))
	if err != nil {
		if errors.Is(err, err_def.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return strconv.ParseInt(lenStr, 10, 64)
}

func (rl *RList) setListLen(wb *base.WriteBatch, key string, length int64) error {
	return wb.Put(GetListLenKey(key), strconv.FormatInt(length, 64))
}

func (rl *RList) getListPointers(key string) (head, tail int64, err error) {
	headStr, err := rl.dw.GetDB().Get(GetListHeadKey(key))
	if err != nil && !errors.Is(err, err_def.ErrKeyNotFound) {
		return 0, 0, err
	}
	if errors.Is(err, err_def.ErrKeyNotFound) {
		headStr = "0"
	}

	tailStr, err := rl.dw.GetDB().Get(GetListTailKey(key))
	if err != nil && !errors.Is(err, err_def.ErrKeyNotFound) {
		return 0, 0, err
	}
	if errors.Is(err, err_def.ErrKeyNotFound) {
		tailStr = "0"
	}

	head, _ = strconv.ParseInt(headStr, 10, 64)
	tail, _ = strconv.ParseInt(tailStr, 10, 64)
	return head, tail, nil
}

func (rl *RList) LPush(key string, values ...string) (int64, error) {
	if len(values) == 0 {
		return 0, nil
	}

	wb := rl.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	length, err := rl.getListLen(key)
	if err != nil {
		return 0, err
	}

	head, tail, err := rl.getListPointers(key)
	if err != nil {
		return 0, err
	}

	for _, value := range values {
		head--
		if err := wb.Put(GetListItemKey(key, head), value); err != nil {
			return 0, err
		}
		length++
	}

	if err := wb.Put(GetListHeadKey(key), strconv.FormatInt(head, 10)); err != nil {
		return 0, err
	}
	if tail == 0 {
		if err := wb.Put(GetListTailKey(key), strconv.FormatInt(head+int64(len(values))-1, 10)); err != nil {
			return 0, err
		}
	}
	if err := rl.setListLen(wb, key, length); err != nil {
		return 0, err
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return length, nil
}

func (rl *RList) RPush(key string, values ...string) (int64, error) {
	if len(values) == 0 {
		return 0, nil
	}

	wb := rl.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	length, err := rl.getListLen(key)
	if err != nil {
		return 0, err
	}

	head, tail, err := rl.getListPointers(key)
	if err != nil {
		return 0, err
	}

	if length == 0 {
		head = 0
		tail = -1
	}

	for _, value := range values {
		tail++
		if err := wb.Put(GetListItemKey(key, tail), value); err != nil {
			return 0, err
		}
		length++
	}

	if err := wb.Put(GetListTailKey(key), strconv.FormatInt(tail, 10)); err != nil {
		return 0, err
	}
	if head == 0 && length == int64(len(values)) {
		if err := wb.Put(GetListHeadKey(key), "0"); err != nil {
			return 0, err
		}
	}
	if err := rl.setListLen(wb, key, length); err != nil {
		return 0, err
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return length, nil
}

func (rl *RList) LPop(key string) (string, error) {
	wb := rl.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	length, err := rl.getListLen(key)
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", err_def.ErrKeyNotFound
	}

	head, _, err := rl.getListPointers(key)
	if err != nil {
		return "", err
	}

	value, err := rl.dw.GetDB().Get(GetListItemKey(key, head))
	if err != nil {
		return "", err
	}

	if err := wb.Delete(GetListItemKey(key, head)); err != nil {
		return "", err
	}

	head++
	length--

	if length > 0 {
		if err := wb.Put(GetListHeadKey(key), strconv.FormatInt(head, 10)); err != nil {
			return "", err
		}
	} else {
		if err := wb.Delete(GetListHeadKey(key)); err != nil {
			return "", err
		}
		if err := wb.Delete(GetListTailKey(key)); err != nil {
			return "", err
		}
	}

	if err := rl.setListLen(wb, key, length); err != nil {
		return "", err
	}

	if err := wb.Commit(); err != nil {
		return "", err
	}

	return value, nil
}

func (rl *RList) RPop(key string) (string, error) {
	wb := rl.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	length, err := rl.getListLen(key)
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", err_def.ErrKeyNotFound
	}

	_, tail, err := rl.getListPointers(key)
	if err != nil {
		return "", err
	}

	value, err := rl.dw.GetDB().Get(GetListItemKey(key, tail))
	if err != nil {
		return "", err
	}

	if err := wb.Delete(GetListItemKey(key, tail)); err != nil {
		return "", err
	}

	tail--
	length--

	if length > 0 {
		if err := wb.Put(GetListTailKey(key), strconv.FormatInt(tail, 10)); err != nil {
			return "", err
		}
	} else {
		if err := wb.Delete(GetListHeadKey(key)); err != nil {
			return "", err
		}
		if err := wb.Delete(GetListTailKey(key)); err != nil {
			return "", err
		}
	}

	if err := rl.setListLen(wb, key, length); err != nil {
		return "", err
	}

	if err := wb.Commit(); err != nil {
		return "", err
	}

	return value, nil
}

func (rl *RList) LLen(key string) (int64, error) {
	return rl.getListLen(key)
}

func (rl *RList) LRange(key string, start, stop int) ([]string, error) {
	length, err := rl.getListLen(key)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return []string{}, nil
	}

	head, _, err := rl.getListPointers(key)
	if err != nil {
		return nil, err
	}

	if start < 0 {
		start = int(length) + start
	}
	if stop < 0 {
		stop = int(length) + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= int(length) {
		stop = int(length) - 1
	}
	if start > stop {
		return []string{}, nil
	}

	result := make([]string, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		value, err := rl.dw.GetDB().Get(GetListItemKey(key, head+int64(i)))
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}

	return result, nil
}

func (rl *RList) LTrim(key string, start, stop int) error {
	wb := rl.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	length, err := rl.getListLen(key)
	if err != nil {
		return err
	}
	if length == 0 {
		return nil
	}

	head, tail, err := rl.getListPointers(key)
	if err != nil {
		return err
	}

	if start < 0 {
		start = int(length) + start
	}
	if stop < 0 {
		stop = int(length) + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= int(length) {
		stop = int(length) - 1
	}
	if start > stop {
		for i := head; i <= tail; i++ {
			if err := wb.Delete(GetListItemKey(key, i)); err != nil {
				return err
			}
		}
		if err := wb.Delete(GetListHeadKey(key)); err != nil {
			return err
		}
		if err := wb.Delete(GetListTailKey(key)); err != nil {
			return err
		}
		if err := rl.setListLen(wb, key, 0); err != nil {
			return err
		}
		return wb.Commit()
	}

	for i := head; i < head+int64(start); i++ {
		if err := wb.Delete(GetListItemKey(key, i)); err != nil {
			return err
		}
	}
	for i := head + int64(stop) + 1; i <= tail; i++ {
		if err := wb.Delete(GetListItemKey(key, i)); err != nil {
			return err
		}
	}

	newHead := head + int64(start)
	newTail := head + int64(stop)
	newLength := int64(stop - start + 1)

	if err := wb.Put(GetListHeadKey(key), strconv.FormatInt(newHead, 10)); err != nil {
		return err
	}
	if err := wb.Put(GetListTailKey(key), strconv.FormatInt(newTail, 10)); err != nil {
		return err
	}
	if err := rl.setListLen(wb, key, newLength); err != nil {
		return err
	}

	return wb.Commit()
}

func (rl *RList) BLPop(timeout time.Duration, keys ...string) (map[string]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	deadline := time.Now().Add(timeout)
	result := make(map[string]string)

	for time.Now().Before(deadline) {
		rl.mu.Lock()
		for _, key := range keys {
			value, err := rl.LPop(key)
			if err == nil {
				result[key] = value
				rl.mu.Unlock()
				return result, nil
			}
			if !errors.Is(err, err_def.ErrKeyNotFound) {
				rl.mu.Unlock()
				return nil, err
			}
		}
		rl.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}

	return nil, nil
}

func (rl *RList) BRPop(timeout time.Duration, keys ...string) (map[string]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	deadline := time.Now().Add(timeout)
	result := make(map[string]string)

	for time.Now().Before(deadline) {
		rl.mu.Lock()
		for _, key := range keys {
			value, err := rl.RPop(key)
			if err == nil {
				result[key] = value
				rl.mu.Unlock()
				return result, nil
			}
			if !errors.Is(err, err_def.ErrKeyNotFound) {
				rl.mu.Unlock()
				return nil, err
			}
		}
		rl.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}

	return nil, nil
}

func (rl *RList) LInsertBefore(key, pivot, value string) (int64, error) {
	return rl.lInsert(key, pivot, value, true)
}

func (rl *RList) LInsertAfter(key, pivot, value string) (int64, error) {
	return rl.lInsert(key, pivot, value, false)
}

func (rl *RList) lInsert(key, pivot, value string, before bool) (int64, error) {
	wb := rl.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	length, err := rl.getListLen(key)
	if err != nil {
		return 0, err
	}
	if length == 0 {
		return -1, nil
	}

	head, tail, err := rl.getListPointers(key)
	if err != nil {
		return 0, err
	}

	var pivotIdx int64
	found := false
	for i := head; i <= tail; i++ {
		val, err := rl.dw.GetDB().Get(GetListItemKey(key, i))
		if err != nil {
			return 0, err
		}
		if val == pivot {
			pivotIdx = i
			found = true
			break
		}
	}

	if !found {
		return -1, nil
	}

	insertIdx := pivotIdx
	if !before {
		insertIdx++
	}

	for i := tail; i >= insertIdx; i-- {
		val, err := rl.dw.GetDB().Get(GetListItemKey(key, i))
		if err != nil {
			return 0, err
		}
		if err := wb.Put(GetListItemKey(key, i+1), val); err != nil {
			return 0, err
		}
	}

	if err := wb.Put(GetListItemKey(key, insertIdx), value); err != nil {
		return 0, err
	}

	tail++
	length++

	if err := wb.Put(GetListTailKey(key), strconv.FormatInt(tail, 10)); err != nil {
		return 0, err
	}
	if err := rl.setListLen(wb, key, length); err != nil {
		return 0, err
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return length, nil
}
