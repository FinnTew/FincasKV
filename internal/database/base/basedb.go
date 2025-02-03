package base

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/err_def"
	"github.com/FinnTew/FincasKV/internal/storage"
	"github.com/FinnTew/FincasKV/internal/storage/bitcask"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type BaseDB struct {
	bc *bitcask.Bitcask

	expireMap map[string]time.Time

	expireMu sync.RWMutex

	closeCh chan struct{}
	wg      sync.WaitGroup

	ttlPath string

	needFlush bool

	dbOpts *BaseDBOptions
}

func NewDB(dbOpts *BaseDBOptions, bcOpts ...storage.Option) (*BaseDB, error) {
	if dbOpts == nil {
		dbOpts = DefaultBaseDBOptions()
	}

	bc, err := bitcask.Open(bcOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to open bitcask: %w", err)
	}

	db := &BaseDB{
		bc:        bc,
		expireMap: make(map[string]time.Time),
		closeCh:   make(chan struct{}),
		dbOpts:    dbOpts,
	}

	dataDirField := bc.GetDataDir()
	db.ttlPath = filepath.Join(dataDirField, db.dbOpts.TTLMetadataFile)

	if err := db.loadTTLMetadata(); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			_ = bc.Close()
			return nil, fmt.Errorf("failed to load TTL metadata: %w", err)
		}
	}

	db.wg.Add(1)
	go db.expirationWorker(dbOpts.ExpireCheckInterval)

	return db, nil
}

func (db *BaseDB) Put(key string, value string) error {
	return db.bc.Put(key, []byte(value))
}

func (db *BaseDB) Get(key string) (string, error) {
	if db.isExpired(key) {
		_ = db.deleteExpiredKey(key)
		return "", err_def.ErrKeyNotFound
	}

	val, err := db.bc.Get(key)
	if err != nil {
		return "", err
	}
	return string(val), nil
}

func (db *BaseDB) Del(key string) error {
	return db.bc.Del(key)
}

func (db *BaseDB) Close() {
	close(db.closeCh)
	db.wg.Wait()

	_ = db.saveTTLMetadata()

	_ = db.bc.Close()
}

func (db *BaseDB) Exists(key string) (bool, error) {
	if db.isExpired(key) {
		_ = db.deleteExpiredKey(key)
		return false, nil
	}

	filter := db.bc.GetFilter()
	if filter != nil && !filter.Contains([]byte(key)) {
		return false, nil
	}

	if _, err := db.bc.GetMemIndex().Get(key); err != nil {
		if errors.Is(err, err_def.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (db *BaseDB) Expire(key string, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("invalid TTL: %v", ttl)
	}
	ex, err := db.Exists(key)
	if err != nil {
		return err
	}
	if !ex {
		return err_def.ErrKeyNotFound
	}
	expireAt := time.Now().Add(ttl)

	db.expireMu.Lock()
	db.expireMap[key] = expireAt
	db.needFlush = db.needFlush || db.dbOpts.FlushTTLOnChange
	db.expireMu.Unlock()

	if db.dbOpts.FlushTTLOnChange {
		_ = db.saveTTLMetadata()
	}

	return nil
}

func (db *BaseDB) Keys(pattern string) ([]string, error) {
	allKeys, err := db.bc.ListKeys()
	if err != nil {
		return nil, err
	}

	results := make([]string, 0, len(allKeys))
	now := time.Now()

	if !strings.ContainsAny(pattern, "*?[]") {
		for _, k := range allKeys {
			db.expireMu.RLock()
			expAt, ok := db.expireMap[k]
			db.expireMu.RUnlock()
			if ok && now.After(expAt) {
				continue
			}
			if k == pattern {
				results = append(results, k)
			}
		}
		return results, nil
	}

	prefix := ""
	if strings.HasSuffix(pattern, "*") {
		prefix = pattern[:len(pattern)-1]
	}

	for _, k := range allKeys {
		db.expireMu.RLock()
		expAt, ok := db.expireMap[k]
		db.expireMu.RUnlock()
		if ok && now.After(expAt) {
			continue
		}
		if prefix != "" {
			if strings.HasPrefix(k, prefix) {
				results = append(results, k)
			}
			continue
		}
		matched, _ := filepath.Match(pattern, k)
		if matched {
			results = append(results, k)
		}
	}
	return results, nil
}

func (db *BaseDB) Type(key string) (string, error) {
	if db.isExpired(key) {
		_ = db.deleteExpiredKey(key)
		return "none", nil
	}
	_, err := db.bc.Get(key)
	if err != nil {
		if errors.Is(err, err_def.ErrKeyNotFound) {
			return "none", nil
		}
		return "none", err
	}
	return "string", nil
}

func (db *BaseDB) Persist(key string) error {
	ex, err := db.Exists(key)
	if err != nil {
		return err
	}
	if !ex {
		return err_def.ErrKeyNotFound
	}
	db.expireMu.Lock()
	delete(db.expireMap, key)
	db.needFlush = db.needFlush || db.dbOpts.FlushTTLOnChange
	db.expireMu.Unlock()

	if db.dbOpts.FlushTTLOnChange {
		_ = db.saveTTLMetadata()
	}
	return nil
}

func (db *BaseDB) NewWriteBatch(opts *BatchOptions) *WriteBatch {
	if opts == nil {
		opts = DefaultBatchOptions()
	}

	wb := batchPool.Get().(*WriteBatch)
	wb.db = db
	wb.committed = false
	wb.opts = opts
	wb.operations = wb.operations[:0]

	return wb
}

func (db *BaseDB) expirationWorker(interval time.Duration) {
	defer db.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			db.evictExpiredKeys()

			db.expireMu.Lock()
			if db.needFlush {
				_ = db.saveTTLMetadata()
				db.needFlush = false
			}
			db.expireMu.Unlock()
		}
	}
}

func (db *BaseDB) evictExpiredKeys() {
	now := time.Now()
	db.expireMu.Lock()
	defer db.expireMu.Unlock()

	for k, expAt := range db.expireMap {
		if now.After(expAt) {
			_ = db.bc.Del(k)
			delete(db.expireMap, k)
			db.needFlush = true
		}
	}
}

func (db *BaseDB) isExpired(key string) bool {
	db.expireMu.RLock()
	expAt, ok := db.expireMap[key]
	db.expireMu.RUnlock()
	return ok && time.Now().After(expAt)
}

func (db *BaseDB) deleteExpiredKey(key string) error {
	err := db.bc.Del(key)
	db.expireMu.Lock()
	delete(db.expireMap, key)
	db.needFlush = db.needFlush || db.dbOpts.FlushTTLOnChange
	db.expireMu.Unlock()

	if db.dbOpts.FlushTTLOnChange {
		_ = db.saveTTLMetadata()
	}
	return err
}

func (db *BaseDB) loadTTLMetadata() error {
	f, err := os.Open(db.ttlPath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			continue
		}
		k := parts[0]
		expNano, parseErr := strconv.ParseInt(parts[1], 10, 64)
		if parseErr != nil {
			continue
		}
		db.expireMap[k] = time.Unix(0, expNano)
	}
	return scanner.Err()
}

func (db *BaseDB) saveTTLMetadata() error {
	tmpFile := db.ttlPath + ".tmp"
	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := bufio.NewWriter(f)

	db.expireMu.RLock()
	for k, expAt := range db.expireMap {
		line := fmt.Sprintf("%s %d\n", k, expAt.UnixNano())
		if _, werr := writer.WriteString(line); werr != nil {
			db.expireMu.RUnlock()
			return werr
		}
	}
	db.expireMu.RUnlock()

	if err := writer.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpFile, db.ttlPath)
}
