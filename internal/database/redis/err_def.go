package redis

import "fmt"

var (
	ErrEmptyKey        = fmt.Errorf("empty key")
	ErrKeyNotFound     = fmt.Errorf("key not found")
	ErrValueNotInteger = fmt.Errorf("value is not an integer")
	ErrValueNotFloat   = fmt.Errorf("value is not a float")
)
