package bitcask

import (
	"encoding/binary"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/storage"
	"github.com/FinnTew/FincasKV/internal/storage/cache"
	"github.com/FinnTew/FincasKV/internal/storage/err_def"
	"github.com/FinnTew/FincasKV/internal/storage/file_manager"
	"github.com/FinnTew/FincasKV/internal/storage/index"
	"github.com/FinnTew/FincasKV/util"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Flag 标记位，用于标识记录的状态
const (
	FlagNormal uint32 = iota
	FlagDeleted
)

// Bitcask 实现
type Bitcask struct {
	cfg *storage.Options

	fm *file_manager.FileManager

	memIndex *index.MemIndexShard[string, storage.Entry]
	memCache storage.MemCache[string, []byte]

	filter *util.ShardedBloomFilter

	mergeRunning  atomic.Bool
	mergeTicker   *time.Ticker
	mergeStopChan chan struct{}

	closed bool
	mu     sync.RWMutex
}

// Open 打开或创建一个 Bitcask 实例
func Open(options ...storage.Option) (*Bitcask, error) {
	// 初始化默认配置
	cfg := storage.DefaultOptions()
	for _, opt := range options {
		opt(cfg)
	}

	// 创建文件管理器
	fm, err := file_manager.NewFileManager(
		cfg.DataDir,
		cfg.MaxFileSize,
		cfg.MaxOpenFiles,
		cfg.SyncInterval,
	)
	if err != nil {
		return nil, fmt.Errorf("create file manager failed: %w", err)
	}

	// 创建内存索引
	memIndex := index.NewMemIndexShard[string, storage.Entry](
		cfg.MemIndexDS,
		cfg.MemIndexShardCount,
		cfg.BTreeDegree,
		cfg.BTreeComparator,
		cfg.SkipListRandSource,
		cfg.SkipListComparator,
		cfg.SwissTableSize,
	)

	var memCache storage.MemCache[string, []byte]
	if cfg.OpenMemCache {
		switch cfg.MemCacheDS {
		case storage.LRU:
			memCache = cache.NewLRUCache[string, []byte](cfg.MemCacheSize)
		default:
			return nil, fmt.Errorf("unsopported memcache DS: %s", cfg.MemCacheDS)
		}
	}

	filter, err := util.NewShardedBloomFilter(util.BloomConfig{
		ExpectedElements:  1 << 10,
		FalsePositiveRate: 0.01,
		AutoScale:         true,
	})
	if err != nil {
		return nil, fmt.Errorf("create filter failed: %w", err)
	}

	db := &Bitcask{
		cfg:           cfg,
		fm:            fm,
		memIndex:      memIndex,
		memCache:      memCache,
		filter:        filter,
		mergeStopChan: make(chan struct{}),
	}

	// 加载数据文件，重建内存索引
	if err := db.loadDataFiles(); err != nil {
		return nil, fmt.Errorf("load data files failed: %w", err)
	}

	// 启动自动 Merge
	if cfg.AutoMerge {
		db.mergeTicker = time.NewTicker(cfg.MergeInterval)
		go db.autoMerge()
	}

	return db, nil
}

// loadDataFiles 从磁盘加载所有数据文件并重建内存索引
func (db *Bitcask) loadDataFiles() error {
	files, err := os.ReadDir(db.cfg.DataDir)
	if err != nil {
		return fmt.Errorf("read data directory failed: %w", err)
	}

	// 按文件ID排序处理
	for _, file := range files {
		var fileID int
		_, err := fmt.Sscanf(file.Name(), storage.FilePrefix+"%d"+storage.FileSuffix, &fileID)
		if err != nil {
			continue // 跳过不符合命名规则的文件
		}

		if err := db.loadDataFile(fileID); err != nil {
			return fmt.Errorf("load data file %d failed: %w", fileID, err)
		}
	}

	return nil
}

// loadDataFile 加载单个数据文件，重建索引
func (db *Bitcask) loadDataFile(fileID int) error {
	file, err := db.fm.GetFile(fileID)
	if err != nil {
		return err
	}

	var offset int64
	for {
		// 读取头部信息
		header := make([]byte, storage.HeaderSize)
		n, err := file.ReadAt(header, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read header failed: %w", err)
		}
		if n < storage.HeaderSize {
			break
		}

		// 解析头部
		keyLen := binary.BigEndian.Uint32(header[12:16])
		valueLen := binary.BigEndian.Uint32(header[16:20])
		recordSize := int64(storage.HeaderSize) + int64(keyLen) + int64(valueLen) + 8 // +8 for checksum

		// 读取完整记录
		record := make([]byte, recordSize)
		n, err = file.ReadAt(record, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read record failed: %w", err)
		}
		if n < int(recordSize) {
			break
		}

		// 解码记录
		r, err := file_manager.DecodeRecord(record)
		if err != nil {
			return fmt.Errorf("decode record failed: %w", err)
		}

		// 更新内存索引
		entry := storage.Entry{
			FileID:    fileID,
			Offset:    offset,
			Size:      uint32(recordSize),
			Timestamp: r.Timestamp,
		}

		// 如果是删除标记，则删除索引
		if r.Flags == FlagDeleted {
			db.memIndex.Del(string(r.Key))
		} else {
			if err := db.memIndex.Put(string(r.Key), entry); err != nil {
				return fmt.Errorf("update index failed: %w", err)
			}
			if err := db.filter.Add(r.Key); err != nil {
				return fmt.Errorf("update filter failed: %w", err)
			}
		}

		offset += recordSize
	}

	return nil
}

// Put 写入键值对
func (db *Bitcask) Put(key string, value []byte) error {
	if db.closed {
		return err_def.ErrDBClosed
	}
	if len(key) == 0 {
		return err_def.ErrEmptyKey
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 构造记录并直接使用FileManager的异步写入
	record := &storage.Record{
		Timestamp: time.Now().UnixNano(),
		Flags:     FlagNormal,
		KVItem: storage.KVItem{
			Key:   []byte(key),
			Value: value,
		},
	}

	// 直接使用FileManager的异步写入
	respCh := db.fm.WriteAsync(record)
	resp := <-respCh
	if resp.Err != nil {
		return fmt.Errorf("write record failed: %w", resp.Err)
	}

	// 更新内存索引
	if err := db.memIndex.Put(key, resp.Entry); err != nil {
		return fmt.Errorf("update index failed: %w", err)
	}

	// 更新内存缓存
	if db.memCache != nil {
		if err := db.memCache.Insert(key, value); err != nil {
			return fmt.Errorf("update cache failed: %w", err)
		}
	}

	// 更新布隆过滤器
	if db.filter != nil {
		_ = db.filter.Add(record.Key)
	}

	return nil
}

// Get 读取键值对
func (db *Bitcask) Get(key string) ([]byte, error) {
	if db.closed {
		return nil, err_def.ErrDBClosed
	}
	if len(key) == 0 {
		return nil, err_def.ErrEmptyKey
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	// 检查布隆过滤器
	if db.filter != nil {
		if !db.filter.Contains([]byte(key)) {
			return nil, err_def.ErrKeyNotFound
		}
	}

	// 检查内存缓存
	if db.memCache != nil {
		if value, err := db.memCache.Find(key); err == nil {
			return value, nil
		}
	}

	// 查找内存索引
	entry, err := db.memIndex.Get(key)
	if err != nil {
		return nil, err_def.ErrKeyNotFound
	}

	// 直接使用FileManager的读取
	record, err := db.fm.Read(entry)
	if err != nil {
		return nil, fmt.Errorf("read record failed: %w", err)
	}

	// 检查删除标记
	if record.Flags == FlagDeleted {
		return nil, err_def.ErrKeyNotFound
	}

	// 更新缓存
	if db.memCache != nil {
		_ = db.memCache.Insert(key, record.Value)
	}

	return record.Value, nil
}

// Del 删除键值对
func (db *Bitcask) Del(key string) error {
	if db.closed {
		return err_def.ErrDBClosed
	}
	if len(key) == 0 {
		return err_def.ErrEmptyKey
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 先检查键是否存在
	//if _, err := db.memIndex.Get(key); err != nil {
	//	return ErrKeyNotFound
	//}
	if db.filter != nil {
		if !db.filter.Contains([]byte(key)) {
			return err_def.ErrKeyNotFound
		}
	}

	// 写入删除标记记录
	record := &storage.Record{
		Timestamp: time.Now().UnixNano(),
		Flags:     FlagDeleted,
		KVItem: storage.KVItem{
			Key:   []byte(key),
			Value: nil,
		},
	}

	// 直接使用FileManager的异步写入
	respCh := db.fm.WriteAsync(record)
	resp := <-respCh
	if resp.Err != nil {
		return fmt.Errorf("write delete record failed: %w", resp.Err)
	}

	// 从内存索引删除
	if err := db.memIndex.Del(key); err != nil {
		return fmt.Errorf("remove from index failed: %w", err)
	}

	// 从缓存删除
	if db.memCache != nil {
		_ = db.memCache.Delete(key)
	}

	return nil
}

// ListKeys 列出所有键
func (db *Bitcask) ListKeys() ([]string, error) {
	if db.closed {
		return nil, err_def.ErrDBClosed
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	var keys []string
	err := db.memIndex.Foreach(func(key string, _ storage.Entry) bool {
		keys = append(keys, key)
		return true
	})

	if err != nil {
		return nil, fmt.Errorf("list keys failed: %w", err)
	}

	return keys, nil
}

// Fold 遍历所有键值对
func (db *Bitcask) Fold(f func(key string, value []byte) bool) error {
	if db.closed {
		return err_def.ErrDBClosed
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.memIndex.Foreach(func(key string, entry storage.Entry) bool {
		record, err := db.fm.Read(entry)
		if err != nil {
			return false
		}
		if record.Flags == FlagDeleted {
			return true
		}
		return f(key, record.Value)
	})
}

// Merge 合并数据文件，删除无效记录
func (db *Bitcask) Merge() error {
	if db.closed {
		return err_def.ErrDBClosed
	}

	if !db.mergeRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("merge is already running")
	}
	defer db.mergeRunning.Store(false)

	db.mu.Lock()
	defer db.mu.Unlock()

	// 创建合并目录
	mergeDir := filepath.Join(filepath.Dir(db.cfg.DataDir), "merge")
	if err := os.MkdirAll(mergeDir, 0755); err != nil {
		return fmt.Errorf("create merge directory failed: %w", err)
	}

	// 创建新的文件管理器
	mergeFM, err := file_manager.NewFileManager(
		mergeDir,
		db.cfg.MaxFileSize,
		db.cfg.MaxOpenFiles,
		db.cfg.SyncInterval,
	)
	if err != nil {
		return fmt.Errorf("create merge file manager failed: %w", err)
	}

	// 遍历所有有效的键值对
	err = db.memIndex.Foreach(func(key string, entry storage.Entry) bool {
		record, err := db.fm.Read(entry)
		if err != nil {
			return false
		}
		if record.Flags == FlagDeleted {
			return true
		}

		// 写入新文件
		respCh := mergeFM.WriteAsync(record)
		resp := <-respCh
		if resp.Err != nil {
			err = resp.Err
			return false
		}
		return true
	})

	if err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	// 关闭合并文件管理器
	if err := mergeFM.Close(); err != nil {
		return fmt.Errorf("close merge file manager failed: %w", err)
	}

	// 替换原始文件
	if err := db.fm.Close(); err != nil {
		return fmt.Errorf("close original file manager failed: %w", err)
	}

	if err := os.RemoveAll(db.cfg.DataDir); err != nil {
		return fmt.Errorf("remove original directory failed: %w", err)
	}

	if err := os.Rename(mergeDir, db.cfg.DataDir); err != nil {
		return fmt.Errorf("rename merge directory failed: %w", err)
	}

	// 重新打开文件管理器
	fm, err := file_manager.NewFileManager(
		db.cfg.DataDir,
		db.cfg.MaxFileSize,
		db.cfg.MaxOpenFiles,
		db.cfg.SyncInterval,
	)
	if err != nil {
		return fmt.Errorf("reopen file manager failed: %w", err)
	}

	db.fm = fm
	return nil
}

func (db *Bitcask) autoMerge() {
	for {
		select {
		case <-db.mergeTicker.C:
			if ratio, err := db.EstimateInvalidRatio(); err == nil {
				if ratio >= db.cfg.MinMergeRatio {
					_ = db.Merge()
				}
			}
		case <-db.mergeStopChan:
			return
		}
	}
}

func (db *Bitcask) EstimateInvalidRatio() (float64, error) {
	if db.closed {
		return 0, err_def.ErrDBClosed
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	var totalSize, validSize int64

	files, err := os.ReadDir(db.cfg.DataDir)
	if err != nil {
		return 0, fmt.Errorf("read directory failed: %w", err)
	}

	for _, f := range files {
		info, err := f.Info()
		if err != nil {
			continue
		}
		totalSize += info.Size()
	}

	if totalSize == 0 {
		return 0, nil
	}

	err = db.memIndex.Foreach(func(_ string, entry storage.Entry) bool {
		validSize += int64(entry.Size)
		return true
	})
	if err != nil {
		return 0, fmt.Errorf("foreach failed: %w", err)
	}

	return 1 - float64(validSize)/float64(totalSize), nil
}

func (db *Bitcask) StartMerge(interval time.Duration) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.mergeTicker != nil {
		db.mergeTicker.Stop()
	}
	db.mergeTicker = time.NewTicker(interval)
	go db.autoMerge()
}

func (db *Bitcask) StopMerge() {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.mergeTicker != nil {
		db.mergeTicker.Stop()
		db.mergeTicker = nil
	}
	select {
	case db.mergeStopChan <- struct{}{}:
	default:
	}
}

// Sync 同步数据到磁盘
func (db *Bitcask) Sync() error {
	if db.closed {
		return err_def.ErrDBClosed
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if current := db.fm.GetActiveFile(); current != nil {
		return current.File.Sync()
	}
	return nil
}

// Close 关闭数据库
func (db *Bitcask) Close() error {
	if db.closed {
		return err_def.ErrDBClosed
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.mergeTicker != nil {
		db.mergeTicker.Stop()
		db.mergeTicker = nil
		close(db.mergeStopChan)
	}

	db.closed = true
	return db.fm.Close()
}
