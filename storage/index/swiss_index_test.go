package index

import (
	"errors"
	"fmt"
	"testing"
)

func TestSwissIndex(t *testing.T) {
	tests := []struct {
		name        string
		key         int
		value       string
		expectedErr error
		testFunc    func(s *SwissIndex[int, string]) error
	}{
		{
			name:        "Put and Get",
			key:         1,
			value:       "value1",
			expectedErr: nil,
			testFunc: func(s *SwissIndex[int, string]) error {
				if err := s.Put(1, "value1"); err != nil {
					return err
				}
				value, err := s.Get(1)
				if err != nil {
					return err
				}
				if value != "value1" {
					return errors.New("value mismatch")
				}
				return nil
			},
		},
		{
			name:        "Get non-existent key",
			key:         2,
			value:       "",
			expectedErr: fmt.Errorf("no value found for key 2"),
			testFunc: func(s *SwissIndex[int, string]) error {
				_, err := s.Get(2)
				return err
			},
		},
		{
			name:        "Delete existing key",
			key:         3,
			value:       "value3",
			expectedErr: fmt.Errorf("no value found for key 3"),
			testFunc: func(s *SwissIndex[int, string]) error {
				if err := s.Put(3, "value3"); err != nil {
					return err
				}
				if err := s.Del(3); err != nil {
					return err
				}
				_, err := s.Get(3)
				return err
			},
		},
		{
			name:        "Delete non-existent key",
			key:         4,
			value:       "",
			expectedErr: fmt.Errorf("delete failed"),
			testFunc: func(s *SwissIndex[int, string]) error {
				return s.Del(4)
			},
		},
		{
			name:        "Foreach",
			key:         5,
			value:       "value5",
			expectedErr: nil,
			testFunc: func(s *SwissIndex[int, string]) error {
				if err := s.Put(5, "value5"); err != nil {
					return err
				}
				if err := s.Put(6, "value6"); err != nil {
					return err
				}
				var count int
				s.Foreach(func(key int, value string) bool {
					count++
					return true
				})
				if count != 2 {
					return errors.New("foreach count mismatch")
				}
				return nil
			},
		},
		{
			name:        "Clear",
			key:         7,
			value:       "value7",
			expectedErr: fmt.Errorf("no value found for key 7"),
			testFunc: func(s *SwissIndex[int, string]) error {
				if err := s.Put(7, "value7"); err != nil {
					return err
				}
				if err := s.Clear(); err != nil {
					return err
				}
				_, err := s.Get(7)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSwissIndex[int, string](10)
			err := tt.testFunc(s)
			if (err != nil && tt.expectedErr == nil) || (err == nil && tt.expectedErr != nil) || (err != nil && tt.expectedErr != nil && err.Error() != tt.expectedErr.Error()) {
				t.Errorf("%s: expected error %v, got %v", tt.name, tt.expectedErr, err)
			}
		})
	}
}
