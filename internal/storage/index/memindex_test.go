package index

import (
	"github.com/FinnTew/FincasKV/internal/storage"
	"testing"
)

func TestMemIndexShard(t *testing.T) {
	btreeLessFunc := func(a, b int) bool {
		return a < b
	}

	skipListLessFunc := func(a, b int) int {
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0
	}

	tests := []struct {
		name         string
		memIndexType storage.MemIndexType
		shardCount   int
		operations   []struct {
			op    string
			key   int
			value string
			want  string
		}
	}{
		{
			name:         "BTree operations",
			memIndexType: storage.BTree,
			shardCount:   2,
			operations: []struct {
				op    string
				key   int
				value string
				want  string
			}{
				{"put", 1, "value1", ""},
				{"get", 1, "", "value1"},
				{"put", 2, "value2", ""},
				{"get", 2, "", "value2"},
				{"del", 1, "", ""},
				{"get", 1, "", ""},
			},
		},
		{
			name:         "SkipList operations",
			memIndexType: storage.SkipList,
			shardCount:   2,
			operations: []struct {
				op    string
				key   int
				value string
				want  string
			}{
				{"put", 1, "value1", ""},
				{"get", 1, "", "value1"},
				{"put", 2, "value2", ""},
				{"get", 2, "", "value2"},
				{"del", 1, "", ""},
				{"get", 1, "", ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexShard := NewMemIndexShard[int, string](tt.memIndexType, tt.shardCount, 2, btreeLessFunc, nil, skipListLessFunc, 1<<10)

			for _, op := range tt.operations {
				switch op.op {
				case "put":
					if err := indexShard.Put(op.key, op.value); err != nil {
						t.Errorf("Put(%d, %s) = %v; want no error", op.key, op.value, err)
					}
				case "get":
					got, err := indexShard.Get(op.key)
					if err != nil && op.want != "" {
						t.Errorf("Get(%d) = %v; want %s", op.key, err, op.want)
					} else if err == nil && got != op.want {
						t.Errorf("Get(%d) = %s; want %s", op.key, got, op.want)
					}
				case "del":
					if err := indexShard.Del(op.key); err != nil {
						t.Errorf("Del(%d) = %v; want no error", op.key, err)
					}
				}
			}

			err := indexShard.Foreach(func(k int, v string) bool {
				t.Logf("key: %d, value: %s", k, v)
				return true
			})
			if err != nil {
				t.Fatalf("Foreach() = %v; want no error", err)
			}

			err = indexShard.Clear()
			if err != nil {
				t.Fatalf("Clear() = %v; want no error", err)
			}
		})
	}
}
