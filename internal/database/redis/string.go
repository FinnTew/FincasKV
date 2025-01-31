package redis

import (
	"errors"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/storage/err_def"
	"sort"
	"strconv"
	"sync"
)

type RString struct {
	keyLocks sync.Map // map[string]*sync.Mutex
}

func (rs *RString) Set(key, value string) error {
	db := (&DBWrapper{}).GetDB()
	key = GetStringKey(key)
	return db.Put(key, value)
}

func (rs *RString) Get(key string) (string, error) {
	db := (&DBWrapper{}).GetDB()
	key = GetStringKey(key)
	return db.Get(key)
}

func (rs *RString) Incr(key string) (int64, error) {
	db := (&DBWrapper{}).GetDB()
	mu, _ := rs.keyLocks.LoadOrStore(key, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	key = GetStringKey(key)
	data, err := db.Get(key)
	var curr int
	if errors.Is(err, err_def.ErrKeyNotFound) {
		curr = 0
	} else if err != nil {
		return 0, err
	} else {
		curr, err = strconv.Atoi(data)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer")
		}
	}

	curr++
	err = db.Put(key, strconv.Itoa(curr))
	if err != nil {
		return 0, err
	}

	return int64(curr), nil
}

func (rs *RString) IncrBy(key string, value int64) (int64, error) {
	db := (&DBWrapper{}).GetDB()
	mu, _ := rs.keyLocks.LoadOrStore(key, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	key = GetStringKey(key)
	data, err := db.Get(key)
	var curr int

	if errors.Is(err, err_def.ErrKeyNotFound) {
		curr = 0
	} else if err != nil {
		return 0, err
	} else {
		curr, err = strconv.Atoi(data)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer")
		}
	}

	curr += int(value)
	err = db.Put(key, strconv.Itoa(curr))
	if err != nil {
		return 0, err
	}

	return int64(curr), nil
}

func (rs *RString) Decr(key string) (int64, error) {
	db := (&DBWrapper{}).GetDB()
	mu, _ := rs.keyLocks.LoadOrStore(key, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	key = GetStringKey(key)
	data, err := db.Get(key)
	var curr int

	if errors.Is(err, err_def.ErrKeyNotFound) {
		curr = 0
	} else if err != nil {
		return 0, err
	} else {
		curr, err = strconv.Atoi(data)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer")
		}
	}

	curr--
	err = db.Put(key, strconv.Itoa(curr))
	if err != nil {
		return 0, err
	}

	return int64(curr), nil
}

func (rs *RString) DecrBy(key string, value int64) (int64, error) {
	db := (&DBWrapper{}).GetDB()
	mu, _ := rs.keyLocks.LoadOrStore(key, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	key = GetStringKey(key)
	data, err := db.Get(key)
	var curr int

	if errors.Is(err, err_def.ErrKeyNotFound) {
		curr = 0
	} else if err != nil {
		return 0, err
	} else {
		curr, err = strconv.Atoi(data)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer")
		}
	}

	curr -= int(value)
	err = db.Put(key, strconv.Itoa(curr))
	if err != nil {
		return 0, err
	}

	return int64(curr), nil
}

func (rs *RString) Append(key, value string) (int64, error) {
	db := (&DBWrapper{}).GetDB()
	mu, _ := rs.keyLocks.LoadOrStore(key, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	key = GetStringKey(key)
	data, err := db.Get(key)
	if err != nil && !errors.Is(err, err_def.ErrKeyNotFound) {
		return 0, err
	}

	if errors.Is(err, err_def.ErrKeyNotFound) {
		data = ""
	}

	newData := append([]byte(data), []byte(value)...)

	if err := db.Put(key, string(newData)); err != nil {
		return 0, err
	}

	return int64(len(newData)), nil
}

func (rs *RString) GetSet(key, value string) (string, error) {
	db := (&DBWrapper{}).GetDB()
	mu, _ := rs.keyLocks.LoadOrStore(key, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	key = GetStringKey(key)
	data, err := db.Get(key)
	if err != nil && !errors.Is(err, err_def.ErrKeyNotFound) {
		return "", err
	}

	if err := db.Put(key, value); err != nil {
		return "", err
	}

	if errors.Is(err, err_def.ErrKeyNotFound) {
		return "", nil
	}

	return data, nil
}

func (rs *RString) SetNX(key, value string) (bool, error) {
	db := (&DBWrapper{}).GetDB()
	mu, _ := rs.keyLocks.LoadOrStore(key, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	key = GetStringKey(key)
	_, err := db.Get(key)
	if err == nil {
		return false, nil
	}

	if !errors.Is(err, err_def.ErrKeyNotFound) {
		return false, err
	}

	if err := db.Put(key, value); err != nil {
		return false, err
	}

	return true, nil
}

func (rs *RString) MSet(pairs map[string]string) error {
	db := (&DBWrapper{}).GetDB()

	if len(pairs) == 0 {
		return nil
	}

	keys := make([]string, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	locks := make([]*sync.Mutex, len(keys))
	defer func() {
		for i := len(locks) - 1; i >= 0; i-- {
			if locks[i] != nil {
				locks[i].Unlock()
			}
		}
	}()

	for i, key := range keys {
		mu, _ := rs.keyLocks.LoadOrStore(key, &sync.Mutex{})
		mu.(*sync.Mutex).Lock()
		locks[i] = mu.(*sync.Mutex)
	}

	if len(pairs) <= 100 {
		for _, key := range keys {
			if err := db.Put(GetStringKey(key), pairs[key]); err != nil {
				return fmt.Errorf("failed to put key-value pair {%s, %s}: %v", key, pairs[key], err)
			}
		}
		return nil
	}

	batchSize := 50
	concurrency := (len(keys) + batchSize - 1) / batchSize
	if concurrency > 20 {
		batchSize = (len(keys) + 19) / 20
		concurrency = 20
	}

	errChan := make(chan error, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < len(keys); i += batchSize {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()

			end := start + batchSize
			if end > len(keys) {
				end = len(keys)
			}

			for j := start; j < end; j++ {
				key := keys[j]
				if err := db.Put(GetStringKey(key), pairs[key]); err != nil {
					errChan <- fmt.Errorf("failed to put key-value pair {%s, %s}: %v", key, pairs[key], err)
					return
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func (rs *RString) MGet(keys ...string) (map[string]string, error) {
	db := (&DBWrapper{}).GetDB()

	if len(keys) == 0 {
		return map[string]string{}, nil
	}

	result := make(map[string]string, len(keys))

	type getRes struct {
		key   string
		value string
		err   error
	}

	resultChan := make(chan getRes, len(keys))
	var wg sync.WaitGroup

	for _, key := range keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			value, err := db.Get(GetStringKey(k))
			resultChan <- getRes{k, value, err}
		}(key)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for r := range resultChan {
		if r.err != nil {
			return nil, fmt.Errorf("failed to get {%s}: %v", r.key, r.err)
		}
		result[r.key] = r.value
	}

	return result, nil
}

func (rs *RString) StrLen(key string) (int64, error) {
	db := (&DBWrapper{}).GetDB()
	value, err := db.Get(GetStringKey(key))
	if err != nil {
		return 0, err
	}
	return int64(len(value)), nil
}
