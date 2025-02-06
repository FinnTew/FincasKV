package base

import (
	"context"
	"fmt"
	"github.com/FinnTew/FincasKV/err_def"
	"sort"
	"sync"
	"time"
)

type OpType uint8

const (
	OpPut OpType = iota + 1
	OpDelete
	OpExpire
	OpPersist
)

type BatchOptions struct {
	InitialSize   int
	MaxBatchSize  int
	CommitTimeout time.Duration
	AsyncCommit   bool
}

func DefaultBatchOptions() *BatchOptions {
	return &BatchOptions{
		InitialSize:   16,
		MaxBatchSize:  10000,
		CommitTimeout: time.Second * 5,
		AsyncCommit:   false,
	}
}

type operation struct {
	typ      OpType
	key      string
	value    string
	ttl      time.Duration
	created  time.Time
	priority int // 操作优先级
}

type WriteBatch struct {
	db         *DB
	operations []operation
	mu         sync.Mutex
	committed  bool
	opts       *BatchOptions
}

var batchPool = sync.Pool{
	New: func() interface{} {
		return &WriteBatch{
			operations: make([]operation, 0, DefaultBatchOptions().InitialSize),
		}
	},
}

func (wb *WriteBatch) Put(key, value string) error {
	return wb.PutWithPriority(key, value, 0)
}

func (wb *WriteBatch) PutWithPriority(key, value string, priority int) error {
	if len(key) == 0 {
		return err_def.ErrEmptyKey
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	if wb.committed {
		return fmt.Errorf("batch already committed")
	}

	if len(wb.operations) >= wb.opts.MaxBatchSize {
		return fmt.Errorf("batch size exceeded maximum limit")
	}

	wb.operations = append(wb.operations, operation{
		typ:      OpPut,
		key:      key,
		value:    value,
		created:  time.Now(),
		priority: priority,
	})

	return nil
}

func (wb *WriteBatch) Delete(key string) error {
	return wb.DeleteWithPriority(key, 0)
}

func (wb *WriteBatch) DeleteWithPriority(key string, priority int) error {
	if len(key) == 0 {
		return err_def.ErrEmptyKey
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	if wb.committed {
		return fmt.Errorf("batch already committed")
	}

	wb.operations = append(wb.operations, operation{
		typ:      OpDelete,
		key:      key,
		created:  time.Now(),
		priority: priority,
	})

	return nil
}

func (wb *WriteBatch) Expire(key string, ttl time.Duration) error {
	return wb.ExpireWithPriority(key, ttl, 0)
}

func (wb *WriteBatch) ExpireWithPriority(key string, ttl time.Duration, priority int) error {
	if len(key) == 0 {
		return err_def.ErrEmptyKey
	}
	if ttl <= 0 {
		return fmt.Errorf("invalid TTL: %v", ttl)
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	if wb.committed {
		return fmt.Errorf("batch already committed")
	}

	wb.operations = append(wb.operations, operation{
		typ:      OpExpire,
		key:      key,
		ttl:      ttl,
		created:  time.Now(),
		priority: priority,
	})

	return nil
}

func (wb *WriteBatch) Persist(key string) error {
	return wb.PersistWithPriority(key, 0)
}

func (wb *WriteBatch) PersistWithPriority(key string, priority int) error {
	if len(key) == 0 {
		return err_def.ErrEmptyKey
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	if wb.committed {
		return fmt.Errorf("batch already committed")
	}

	wb.operations = append(wb.operations, operation{
		typ:      OpPersist,
		key:      key,
		created:  time.Now(),
		priority: priority,
	})

	return nil
}

func (wb *WriteBatch) Commit() error {
	ctx, cancel := context.WithTimeout(context.Background(), wb.opts.CommitTimeout)
	defer cancel()
	return wb.CommitWithContext(ctx)
}

func (wb *WriteBatch) CommitWithContext(ctx context.Context) error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if wb.committed {
		return fmt.Errorf("batch already committed")
	}

	if len(wb.operations) == 0 {
		wb.committed = true
		return nil
	}

	// 操作排序和合并
	wb.optimizeOperations()

	// 批量写入准备，过滤非法操作
	if err := wb.prepareBatch(); err != nil {
		return err
	}

	// 异步处理
	if wb.opts.AsyncCommit {
		go wb.executeOperations(ctx)
		return nil
	}

	return wb.executeOperations(ctx)
}

func (wb *WriteBatch) optimizeOperations() {
	// 按优先级排序
	sort.Slice(wb.operations, func(i, j int) bool {
		return wb.operations[i].priority > wb.operations[j].priority
	})

	// 合并相同key的操作
	optimized := make([]operation, 0, len(wb.operations))
	keyOps := make(map[string]operation)

	for _, op := range wb.operations {
		existing, exists := keyOps[op.key]
		if !exists {
			keyOps[op.key] = op
			continue
		}

		// 合并操作
		// 下面两种组合不需要合并
		if existing.typ == OpPut && op.typ == OpExpire {
			continue
		}
		if existing.typ == OpExpire && op.typ == OpPersist {
			continue
		}
		merged := wb.mergeOperations(existing, op)
		keyOps[op.key] = merged
	}

	// 构建操作列表
	for _, op := range keyOps {
		optimized = append(optimized, op)
	}

	wb.operations = optimized
}

func (wb *WriteBatch) mergeOperations(op1, op2 operation) operation {
	// 只需要保留最后一次操作
	if op2.created.After(op1.created) {
		return op2
	}
	return op1
}

func (wb *WriteBatch) prepareBatch() error {
	keysMap := make(map[string]struct{})
	for _, op := range wb.operations {
		keysMap[op.key] = struct{}{}
	}

	for _, op := range wb.operations {
		switch op.typ {
		case OpPut:
			continue
		case OpDelete, OpExpire, OpPersist:
			exists, err := wb.db.Exists(op.key)
			if err != nil {
				return fmt.Errorf("failed to check key existence: %w", err)
			}
			if _, ok := keysMap[op.key]; !exists && !ok {
				return fmt.Errorf("key not found: %s", op.key)
			}
		}
	}
	return nil
}

func (wb *WriteBatch) executeOperations(ctx context.Context) error {
	wb.db.expireMu.Lock()
	defer wb.db.expireMu.Unlock()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("batch execution cancelled: %w", err)
	}

	for _, op := range wb.operations {
		select {
		case <-ctx.Done():
			wb.rollback(op)
			return ctx.Err()
		default:
			if err := wb.executeOperation(op); err != nil {
				wb.rollback(op)
				return err
			}
		}
	}

	if wb.db.needFlush && wb.db.dbOpts.FlushTTLOnChange {
		if err := wb.db.saveTTLMetadata(); err != nil {
			return fmt.Errorf("failed to save TTL metadata: %w", err)
		}
		wb.db.needFlush = false
	}

	wb.committed = true
	return nil
}

func (wb *WriteBatch) executeOperation(op operation) error {
	var err error
	switch op.typ {
	case OpPut:
		err = wb.db.bc.Put(op.key, []byte(op.value))
	case OpDelete:
		err = wb.db.bc.Del(op.key)
		if err == nil {
			delete(wb.db.expireMap, op.key)
		}
	case OpExpire:
		expireAt := op.created.Add(op.ttl)
		wb.db.expireMap[op.key] = expireAt
		wb.db.needFlush = true
	case OpPersist:
		delete(wb.db.expireMap, op.key)
		wb.db.needFlush = true
	}

	return err
}

func (wb *WriteBatch) rollback(failedOp operation) {
	// 倒序遍历已执行的操作回滚
	for i := len(wb.operations) - 1; i >= 0; i-- {
		op := wb.operations[i]
		if op == failedOp {
			break
		}

		switch op.typ {
		case OpPut:
			// 删除已写入的数据
			_ = wb.db.bc.Del(op.key)
		case OpDelete:
			// 恢复删除的数据
			if val, err := wb.db.Get(op.key); err == nil {
				_ = wb.db.bc.Put(op.key, []byte(val))
			}
		case OpExpire:
			// 取消过期
			delete(wb.db.expireMap, op.key)
		case OpPersist:
			// 恢复过期
			if expAt, ok := wb.db.expireMap[op.key]; ok {
				wb.db.expireMap[op.key] = expAt
			}
		}
	}
}

func (wb *WriteBatch) Clear() {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	wb.operations = wb.operations[:0]
	wb.committed = false
}

func (wb *WriteBatch) Size() int {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	return len(wb.operations)
}

func (wb *WriteBatch) Release() {
	wb.Clear()
	// 将WriteBatch放回池中
	batchPool.Put(wb)
}
