package util

import (
	"strconv"
	"testing"
)

func TestNewShardedBloomFilter(t *testing.T) {
	tests := []struct {
		name    string
		config  BloomConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: BloomConfig{
				ExpectedElements:  1000,
				FalsePositiveRate: 0.01,
				AutoScale:         true,
				NumShards:         16,
				BitsPerShard:      1024,
				NumHashFuncs:      4,
			},
			wantErr: false,
		},
		{
			name: "zero expected elements",
			config: BloomConfig{
				ExpectedElements:  0,
				FalsePositiveRate: 0.01,
			},
			wantErr: true,
		},
		{
			name: "invalid false positive rate",
			config: BloomConfig{
				ExpectedElements:  1000,
				FalsePositiveRate: 0,
			},
			wantErr: true,
		},
		{
			name: "non power of 2 shards",
			config: BloomConfig{
				ExpectedElements:  1000,
				FalsePositiveRate: 0.01,
				NumShards:         10,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf, err := NewShardedBloomFilter(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewShardedBloomFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && bf == nil {
				t.Error("NewShardedBloomFilter() returned nil without error")
			}
		})
	}
}

func TestShardedBloomFilter_AddAndContains(t *testing.T) {
	tests := []struct {
		name     string
		elements [][]byte
		check    []byte
		want     bool
	}{
		{
			name:     "add and find element",
			elements: [][]byte{[]byte("test1")},
			check:    []byte("test1"),
			want:     true,
		},
		{
			name:     "element not found",
			elements: [][]byte{[]byte("test1")},
			check:    []byte("test2"),
			want:     false,
		},
		{
			name:     "empty element",
			elements: [][]byte{[]byte("test1")},
			check:    []byte{},
			want:     false,
		},
		{
			name:     "multiple elements",
			elements: [][]byte{[]byte("test1"), []byte("test2"), []byte("test3")},
			check:    []byte("test2"),
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf, _ := NewShardedBloomFilter(BloomConfig{
				ExpectedElements:  1000,
				FalsePositiveRate: 0.01,
			})

			for _, elem := range tt.elements {
				err := bf.Add(elem)
				if err != nil {
					t.Errorf("Add() error = %v", err)
				}
			}

			if got := bf.Contains(tt.check); got != tt.want {
				t.Errorf("Contains() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShardedBloomFilter_Reset(t *testing.T) {
	tests := []struct {
		name     string
		elements [][]byte
	}{
		{
			name:     "reset with elements",
			elements: [][]byte{[]byte("test1"), []byte("test2")},
		},
		{
			name:     "reset empty filter",
			elements: [][]byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf, _ := NewShardedBloomFilter(BloomConfig{
				ExpectedElements:  1000,
				FalsePositiveRate: 0.01,
			})

			for _, elem := range tt.elements {
				_ = bf.Add(elem)
			}

			bf.Reset()

			// 验证所有元素都被清除
			for _, elem := range tt.elements {
				if bf.Contains(elem) {
					t.Errorf("After Reset(), element %s still exists", elem)
				}
			}

			// 验证计数器被重置
			stats := bf.Stats()
			if stats["num_items"].(uint64) != 0 {
				t.Errorf("After Reset(), num_items = %v, want 0", stats["num_items"])
			}
		})
	}
}

func TestShardedBloomFilter_AutoScale(t *testing.T) {
	tests := []struct {
		name           string
		numElements    int
		initialShards  uint32
		expectedGrowth bool
	}{
		{
			name:           "should grow",
			numElements:    1000,
			initialShards:  16,
			expectedGrowth: true,
		},
		{
			name:           "should not grow",
			numElements:    10,
			initialShards:  16,
			expectedGrowth: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf, _ := NewShardedBloomFilter(BloomConfig{
				ExpectedElements:  100,
				FalsePositiveRate: 0.01,
				AutoScale:         true,
				NumShards:         tt.initialShards,
			})

			initialShards := len(bf.shards)

			// 添加元素直到触发扩容
			for i := 0; i < tt.numElements; i++ {
				_ = bf.Add([]byte(strconv.Itoa(i)))
			}

			finalShards := len(bf.shards)
			if tt.expectedGrowth && finalShards <= initialShards {
				t.Errorf("Expected growth, but got shards: initial=%d, final=%d", initialShards, finalShards)
			}
			if !tt.expectedGrowth && finalShards > initialShards {
				t.Errorf("Unexpected growth, got shards: initial=%d, final=%d", initialShards, finalShards)
			}
		})
	}
}

func TestShardedBloomFilter_Stats(t *testing.T) {
	tests := []struct {
		name      string
		elements  [][]byte
		checkKeys []string
	}{
		{
			name:     "empty filter stats",
			elements: [][]byte{},
			checkKeys: []string{
				"total_bits", "num_items", "num_shards", "bits_per_shard",
				"num_hash_funcs", "auto_scale", "estimated_fpp", "current_fill_rate",
			},
		},
		{
			name:     "filled filter stats",
			elements: [][]byte{[]byte("test1"), []byte("test2")},
			checkKeys: []string{
				"total_bits", "num_items", "num_shards", "bits_per_shard",
				"num_hash_funcs", "auto_scale", "estimated_fpp", "current_fill_rate",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf, _ := NewShardedBloomFilter(BloomConfig{
				ExpectedElements:  1000,
				FalsePositiveRate: 0.01,
			})

			for _, elem := range tt.elements {
				_ = bf.Add(elem)
			}

			stats := bf.Stats()

			// 检查所有必需的键是否存在
			for _, key := range tt.checkKeys {
				if _, exists := stats[key]; !exists {
					t.Errorf("Stats() missing key %s", key)
				}
			}

			if stats["num_items"].(uint64) != uint64(len(tt.elements)) {
				t.Errorf("Stats() num_items = %v, want %v", stats["num_items"], len(tt.elements))
			}

			fillRate := stats["current_fill_rate"].(float64)
			if fillRate < 0 || fillRate > 1 {
				t.Errorf("Stats() fill_rate = %v, want between 0 and 1", fillRate)
			}
		})
	}
}

func TestHelperFunctions(t *testing.T) {
	tests := []struct {
		name string
		fn   func() bool
	}{
		{
			name: "isPowerOfTwo",
			fn: func() bool {
				return isPowerOfTwo(16) && !isPowerOfTwo(7)
			},
		},
		{
			name: "nextPowerOf2",
			fn: func() bool {
				return nextPowerOf2(7) == 8 && nextPowerOf2(8) == 8
			},
		},
		{
			name: "calculateOptimalM",
			fn: func() bool {
				m := calculateOptimalM(1000, 0.01)
				return m > 0
			},
		},
		{
			name: "calculateOptimalK",
			fn: func() bool {
				k := calculateOptimalK(1000, 10000)
				return k >= defaultHashFuncs
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.fn() {
				t.Errorf("%s failed", tt.name)
			}
		})
	}
}
