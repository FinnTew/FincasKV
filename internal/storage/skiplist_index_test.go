package storage

import (
	"fmt"
	"testing"
)

func TestSkipList(t *testing.T) {
	compareFunc := func(a, b int) int {
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0
	}

	tests := []struct {
		name        string
		actions     func(sl *SkipListIndex[int, string]) error
		expected    map[int]string
		expectError bool
	}{
		{
			name: "Insert and Retrieve",
			actions: func(sl *SkipListIndex[int, string]) error {
				sl.Put(1, "one")
				sl.Put(2, "two")
				sl.Put(3, "three")
				return nil
			},
			expected: map[int]string{1: "one", 2: "two", 3: "three"},
		},
		{
			name: "Retrieve Non-Existent Key",
			actions: func(sl *SkipListIndex[int, string]) error {
				_, err := sl.Get(4)
				return err
			},
			expected:    nil,
			expectError: true,
		},
		{
			name: "Update Existing Key",
			actions: func(sl *SkipListIndex[int, string]) error {
				sl.Put(1, "one")
				sl.Put(1, "uno") // Update value
				value, err := sl.Get(1)
				if err != nil {
					return err
				}
				if value != "uno" {
					return fmt.Errorf("expected 'uno', got '%s'", value)
				}
				return nil
			},
			expected: map[int]string{1: "uno"},
		},
		{
			name: "Delete Key",
			actions: func(sl *SkipListIndex[int, string]) error {
				sl.Put(1, "one")
				sl.Put(2, "two")
				sl.Del(1)
				_, err := sl.Get(1)
				return err
			},
			expected:    map[int]string{2: "two"},
			expectError: true,
		},
		{
			name: "Clear List",
			actions: func(sl *SkipListIndex[int, string]) error {
				sl.Put(1, "one")
				sl.Put(2, "two")
				sl.Clear()
				if sl.Size() != 0 {
					return fmt.Errorf("expected size 0, got %d", sl.Size())
				}
				return nil
			},
			expected: map[int]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sl := NewSkipListIndex[int, string](compareFunc)
			err := tt.actions(sl)
			if err != nil && !tt.expectError {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.expectError && err == nil {
				t.Fatal("expected error but got none")
			}

			for key, expectedValue := range tt.expected {
				actualValue, err := sl.Get(key)
				if err != nil {
					t.Fatalf("unexpected error getting key %d: %v", key, err)
				}
				if actualValue != expectedValue {
					t.Errorf("for key %d, expected %s, got %s", key, expectedValue, actualValue)
				}
			}
		})
	}
}
