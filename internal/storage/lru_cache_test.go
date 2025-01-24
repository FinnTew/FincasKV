package storage

import (
	"testing"
)

func TestLRUCache(t *testing.T) {
	cache := NewLRUCache[string, int](2)

	tests := []struct {
		name     string
		action   string
		key      string
		value    int
		expected int
		wantErr  bool
	}{
		{"Insert1", "insert", "key1", 1, 1, false},
		{"Insert2", "insert", "key2", 2, 2, false},
		{"FindKey1", "find", "key1", 0, 1, false},
		{"FindKey2", "find", "key2", 0, 2, false},
		{"InsertEvict", "insert", "key3", 3, 0, false},  // key1 should be evicted
		{"FindEvictedKey1", "find", "key1", 0, 0, true}, // should return error
		{"FindKey3", "find", "key3", 0, 3, false},
		{"DeleteKey2", "delete", "key2", 0, 0, false},
		{"FindDeletedKey2", "find", "key2", 0, 0, true}, // should return error
		{"ExistKey3", "exist", "key3", 0, 0, false},
		{"ExistDeletedKey2", "exist", "key2", 0, 0, true}, // should return false
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			switch tt.action {
			case "insert":
				err = cache.Insert(tt.key, tt.value)
			case "find":
				_, err = cache.Find(tt.key)
			case "delete":
				err = cache.Delete(tt.key)
			case "exist":
				exists := cache.Exist(tt.key)
				if (exists && tt.wantErr) || (!exists && !tt.wantErr) {
					t.Errorf("Exist() = %v, wantErr %v", exists, tt.wantErr)
				}
				return
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("Unexpected error: %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
