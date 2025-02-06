package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTreeIndex(t *testing.T) {
	lessFunc := func(a, b int) bool {
		return a < b
	}
	btreeIndex := NewBTreeIndex[int, string](2, lessFunc)

	tests := []struct {
		name        string
		key         int
		value       string
		expected    string
		expectError bool
	}{
		{"Insert and Get", 1, "value1", "value1", false},
		{"Insert and Get another", 2, "value2", "value2", false},
		{"Get non-existing key", 3, "", "", true},
		{"Delete existing key", 1, "", "", false},
		{"Get deleted key", 1, "", "", true},
		{"Delete non-existing key", 3, "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value != "" {
				err := btreeIndex.Put(tt.key, tt.value)
				assert.NoError(t, err, "failed to insert")
			}

			if tt.name == "Delete existing key" || tt.name == "Delete non-existing key" {
				err := btreeIndex.Del(tt.key)
				if tt.expectError {
					assert.Error(t, err, "expected error when deleting key")
				} else {
					assert.NoError(t, err, "unexpected error when deleting key")
				}
			} else {
				if tt.expectError {
					_, err := btreeIndex.Get(tt.key)
					assert.Error(t, err, "expected error for key %d", tt.key)
				} else {
					value, err := btreeIndex.Get(tt.key)
					assert.NoError(t, err, "failed to get")
					assert.Equal(t, tt.expected, value, "expected value mismatch")
				}
			}
		})
	}

	count := 0
	err := btreeIndex.Foreach(func(key int, value string) bool {
		count++
		return true
	})
	assert.NoError(t, err, "unexpected error during Foreach")
	assert.Equal(t, 1, count, "expected 1 item in Foreach")

	err = btreeIndex.Clear()
	assert.NoError(t, err, "unexpected error during Clear")

	count = 0
	err = btreeIndex.Foreach(func(key int, value string) bool {
		count++
		return true
	})
	assert.NoError(t, err, "unexpected error during Foreach after Clear")
	assert.Equal(t, 0, count, "expected 0 items after Clear")
}
