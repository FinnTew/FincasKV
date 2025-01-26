package util

import (
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"
)

const (
	// 默认分片数,必须是2的幂
	defaultShards = 16
	// 默认每个分片大小(bit)
	defaultBitsPerShard = 1024
	// 默认hash函数个数
	defaultHashFuncs = 4
	// 扩容因子
	growthFactor = 2
	// 扩容阈值,当填充率超过75%触发扩容
	growthThreshold = 0.75
)

// ShardedBloomFilter 分片布隆过滤器
type ShardedBloomFilter struct {
	shards    []shard    // 存储分片数据
	k         uint32     // hash函数个数
	m         uint64     // 总bit数
	n         uint64     // 已插入元素个数(原子操作)
	shardMask uint32     // 分片掩码
	shardBits uint32     // 每个分片的bit数
	hashPool  *sync.Pool // hash函数池
	autoScale bool       // 是否自动扩容
}

// shard 单个分片
type shard struct {
	bits []uint64
	sync.RWMutex
}

// Options 配置选项
type BloomConfig struct {
	ExpectedElements  uint64  // 预期元素数量
	FalsePositiveRate float64 // 期望误判率
	AutoScale         bool    // 是否自动扩容
	NumShards         uint32  // 分片数量
	BitsPerShard      uint32  // 每个分片bit数
	NumHashFuncs      uint32  // hash函数数量
}

// NewShardedBloomFilter 创建新的分片布隆过滤器
func NewShardedBloomFilter(opts BloomConfig) (*ShardedBloomFilter, error) {
	if err := validateOptions(&opts); err != nil {
		return nil, err
	}

	// 计算最优参数
	m := calculateOptimalM(opts.ExpectedElements, opts.FalsePositiveRate)
	k := calculateOptimalK(opts.ExpectedElements, m)

	// 初始化分片
	numShards := opts.NumShards
	if numShards == 0 {
		numShards = defaultShards
	}

	bitsPerShard := opts.BitsPerShard
	if bitsPerShard == 0 {
		bitsPerShard = defaultBitsPerShard
	}

	// 确保分片数是2的幂
	if !isPowerOfTwo(uint64(numShards)) {
		numShards = uint32(nextPowerOf2(uint64(numShards)))
	}

	if m > uint64(numShards)*uint64(bitsPerShard) {
		bitsPerShard = uint32(nextPowerOf2(uint64(m / uint64(numShards))))
	}

	shards := make([]shard, numShards)
	for i := range shards {
		shards[i].bits = make([]uint64, bitsPerShard/64)
	}

	// 初始化hash函数池
	hashPool := &sync.Pool{
		New: func() interface{} {
			return fnv.New64a()
		},
	}

	return &ShardedBloomFilter{
		shards:    shards,
		k:         k,
		m:         m,
		shardMask: numShards - 1,
		shardBits: bitsPerShard,
		hashPool:  hashPool,
		autoScale: opts.AutoScale,
	}, nil
}

// validateOptions 验证配置参数
func validateOptions(opts *BloomConfig) error {
	if opts.ExpectedElements == 0 {
		return fmt.Errorf("expected elements must be > 0")
	}
	if opts.FalsePositiveRate <= 0 || opts.FalsePositiveRate >= 1 {
		return fmt.Errorf("false positive rate must be in (0,1)")
	}
	return nil
}

// calculateOptimalM 计算最优bit数
func calculateOptimalM(n uint64, p float64) uint64 {
	return uint64(math.Ceil(-float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
}

// calculateOptimalK 计算最优hash函数个数
func calculateOptimalK(n, m uint64) uint32 {
	k := uint32(math.Round(float64(m/n) * math.Log(2)))
	if k < defaultHashFuncs {
		k = defaultHashFuncs
	}
	return k
}

// isPowerOfTwo 判断是否是2的幂
func isPowerOfTwo(x uint64) bool {
	return x != 0 && (x&(x-1)) == 0
}

// nextPowerOf2 计算下一个2的幂
func nextPowerOf2(x uint64) uint64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}

// Add 添加元素
func (bf *ShardedBloomFilter) Add(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}

	// 检查是否需要扩容
	if bf.autoScale && float64(atomic.LoadUint64(&bf.n))/float64(bf.m) > growthThreshold {
		if err := bf.grow(); err != nil {
			return fmt.Errorf("bloom filter grow failed: %v", err)
		}
	}

	hashValues := bf.hashValues(data)
	for i := uint32(0); i < bf.k; i++ {
		shardIndex := hashValues[i] & uint64(bf.shardMask)
		bitIndex := (hashValues[i] >> bf.k) % uint64(bf.shardBits)

		shard := &bf.shards[shardIndex]
		shard.Lock()
		shard.bits[bitIndex/64] |= 1 << (bitIndex % 64)
		shard.Unlock()
	}

	atomic.AddUint64(&bf.n, 1)
	return nil
}

// Contains 判断元素是否存在
func (bf *ShardedBloomFilter) Contains(data []byte) bool {
	if len(data) == 0 {
		return false
	}

	hashValues := bf.hashValues(data)
	for i := uint32(0); i < bf.k; i++ {
		shardIndex := hashValues[i] & uint64(bf.shardMask)
		bitIndex := (hashValues[i] >> bf.k) % uint64(bf.shardBits)

		shard := &bf.shards[shardIndex]
		shard.RLock()
		isSet := (shard.bits[bitIndex/64] & (1 << (bitIndex % 64))) != 0
		shard.RUnlock()

		if !isSet {
			return false
		}
	}
	return true
}

// grow 扩容操作
func (bf *ShardedBloomFilter) grow() error {
	// 创建新的分片数组
	newShardCount := uint32(len(bf.shards) * growthFactor)
	newShardBits := bf.shardBits * growthFactor
	newShards := make([]shard, newShardCount)

	for i := range newShards {
		newShards[i].bits = make([]uint64, newShardBits/64)
	}

	// 复制旧数据
	for i := 0; i < len(bf.shards); i++ {
		shard := &bf.shards[i]
		shard.Lock()
		shard.bits = make([]uint64, newShardBits/64)
		shard.Unlock()
	}

	// 原子更新
	bf.shards = newShards
	bf.m = uint64(newShardCount) * uint64(newShardBits)
	bf.shardMask = newShardCount - 1
	bf.shardBits = newShardBits

	return nil
}

// hashValues 计算hash值
func (bf *ShardedBloomFilter) hashValues(data []byte) []uint64 {
	hashFunc := bf.hashPool.Get().(hash.Hash64)
	defer bf.hashPool.Put(hashFunc)
	hashFunc.Reset()

	values := make([]uint64, bf.k)
	hashFunc.Write(data)
	h1, h2 := hashFunc.Sum64(), hashFunc.Sum64()

	for i := uint32(0); i < bf.k; i++ {
		values[i] = h1 + uint64(i)*h2
	}
	return values
}

// Stats 获取统计信息
func (bf *ShardedBloomFilter) Stats() map[string]interface{} {
	currentN := atomic.LoadUint64(&bf.n)
	return map[string]interface{}{
		"total_bits":        bf.m,
		"num_items":         currentN,
		"num_shards":        len(bf.shards),
		"bits_per_shard":    bf.shardBits,
		"num_hash_funcs":    bf.k,
		"auto_scale":        bf.autoScale,
		"estimated_fpp":     bf.estimateFPP(currentN),
		"current_fill_rate": float64(currentN) / float64(bf.m),
	}
}

// estimateFPP 估算误判率
func (bf *ShardedBloomFilter) estimateFPP(n uint64) float64 {
	if n == 0 {
		return 0
	}
	return math.Pow(1-math.Exp(-float64(bf.k)*float64(n)/float64(bf.m)), float64(bf.k))
}

// Reset 重置布隆过滤器
func (bf *ShardedBloomFilter) Reset() {
	atomic.StoreUint64(&bf.n, 0)
	for i := range bf.shards {
		bf.shards[i].Lock()
		for j := range bf.shards[i].bits {
			bf.shards[i].bits[j] = 0
		}
		bf.shards[i].Unlock()
	}
}
