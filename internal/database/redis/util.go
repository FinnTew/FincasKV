package redis

import (
	"fmt"
	"math"
)

var (
	StringPrefix = "string"
	HashPrefix   = "hash"
	ListPrefix   = "list"
	SetPrefix    = "set"
	ZSetPrefix   = "zset"
)

func GetStringKey(key string) string {
	return fmt.Sprintf("%s:%s", StringPrefix, key)
}

func GetHashFieldKey(key, field string) string {
	return fmt.Sprintf("%s:%s:%s", HashPrefix, key, field)
}

func GetHashLenKey(key string) string {
	return fmt.Sprintf("%s:%s:_len_", HashPrefix, key)
}

func GetListItemKey(key string, index int64) string {
	return fmt.Sprintf("%s:%s:%d", ListPrefix, key, index)
}

func GetListLenKey(key string) string {
	return fmt.Sprintf("%s:%s:_len_", ListPrefix, key)
}

func GetListHeadKey(key string) string {
	return fmt.Sprintf("%s:%s:_head_", ListPrefix, key)
}

func GetListTailKey(key string) string {
	return fmt.Sprintf("%s:%s:_tail_", ListPrefix, key)
}

func GetSetMemberKey(key, member string) string {
	return fmt.Sprintf("%s:%s:%s", SetPrefix, key, member)
}

func GetSetLenKey(key string) string {
	return fmt.Sprintf("%s:%s:_len_", SetPrefix, key)
}

func GetZSetMemberScoreKey(key, member string) string {
	return fmt.Sprintf("%s:%s:%s", ZSetPrefix, key, member)
}

func GetZSetSortKey(key string, score float64, member string) string {
	return fmt.Sprintf("%s:%s:s:%s:%s", ZSetPrefix, key, float64ToOrderedString(score), member)
}

func float64ToOrderedString(score float64) string {
	bits := math.Float64bits(score)
	if (bits & (1 << 63)) != 0 {
		bits = ^bits
	} else {
		bits = bits | (1 << 63)
	}
	return fmt.Sprintf("%016x", bits)
}
