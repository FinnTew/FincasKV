package redis

import (
	"errors"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/err_def"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type ZMember struct {
	Member string
	Score  float64
}

type RZSet struct {
	dw       *DBWrapper
	zsetLock sync.RWMutex
}

var zsetPool = sync.Pool{
	New: func() interface{} {
		return &RZSet{
			dw: &DBWrapper{},
		}
	},
}

func NewRZSet(dw *DBWrapper) *RZSet {
	rz := zsetPool.Get().(*RZSet)
	rz.dw = dw
	return rz
}

func (rz *RZSet) Release() {
	zsetPool.Put(rz)
}

func (rz *RZSet) getKeyExists(key string) (bool, error) {
	if len(key) == 0 {
		return false, err_def.ErrEmptyKey
	}
	return rz.dw.GetDB().Exists(GetZSetMemberScoreKey(key, ""))
}

func (rz *RZSet) getMemberScore(key, member string) (float64, bool, error) {
	val, err := rz.dw.GetDB().Get(GetZSetMemberScoreKey(key, member))
	if err != nil {
		if errors.Is(err, err_def.ErrKeyNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	score, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, false, err
	}
	return score, true, nil
}

func (rz *RZSet) ZAdd(key string, members ...ZMember) (int64, error) {
	if len(key) == 0 {
		return 0, err_def.ErrEmptyKey
	}
	if len(members) == 0 {
		return 0, nil
	}

	//rz.zsetLock.Lock()
	//defer rz.zsetLock.Unlock()

	wb := rz.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	var added int64
	for _, m := range members {
		if len(m.Member) == 0 {
			continue
		}

		oldScore, exists, err := rz.getMemberScore(key, m.Member)
		if err != nil {
			return 0, err
		}

		if !exists {
			added++
		} else if oldScore == m.Score {
			continue
		}

		if exists {
			oldSortKey := GetZSetSortKey(key, oldScore, m.Member)
			if err := wb.Delete(oldSortKey); err != nil {
				return 0, err
			}
		}

		memberScoreKey := GetZSetMemberScoreKey(key, m.Member)
		if err := wb.Put(memberScoreKey, strconv.FormatFloat(m.Score, 'f', -1, 64)); err != nil {
			return 0, err
		}

		sortKey := GetZSetSortKey(key, m.Score, m.Member)
		if err := wb.Put(sortKey, ""); err != nil {
			return 0, err
		}
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return added, nil
}

func (rz *RZSet) ZRange(key string, start, stop int) ([]ZMember, error) {
	return rz.zrangeGeneric(key, start, stop, false, false)
}

func (rz *RZSet) ZRevRange(key string, start, stop int) ([]ZMember, error) {
	return rz.zrangeGeneric(key, start, stop, true, false)
}

func (rz *RZSet) ZRangeWithScores(key string, start, stop int) ([]ZMember, error) {
	return rz.zrangeGeneric(key, start, stop, false, true)
}

func (rz *RZSet) ZRevRangeWithScores(key string, start, stop int) ([]ZMember, error) {
	return rz.zrangeGeneric(key, start, stop, true, true)
}

func (rz *RZSet) zrangeGeneric(key string, start, stop int, reverse, withScores bool) ([]ZMember, error) {
	if len(key) == 0 {
		return nil, err_def.ErrEmptyKey
	}

	rz.zsetLock.RLock()
	defer rz.zsetLock.RUnlock()

	// 获取所有键
	prefix := fmt.Sprintf("%s:%s:s:", ZSetPrefix, key)
	keys, err := rz.dw.GetDB().Keys(prefix + "*")
	if err != nil {
		return nil, err
	}

	// 处理负索引
	size := len(keys)
	if start < 0 {
		start = size + start
	}
	if stop < 0 {
		stop = size + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= size {
		stop = size - 1
	}
	if start > stop {
		return []ZMember{}, nil
	}

	sort.Strings(keys)

	if reverse {
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}

	result := make([]ZMember, 0, stop-start+1)
	for i := start; i <= stop && i < len(keys); i++ {
		parts := strings.Split(keys[i], ":")
		if len(parts) < 2 {
			continue
		}

		member := parts[len(parts)-1]
		if withScores {
			score, _, err := rz.getMemberScore(key, member)
			if err != nil {
				return nil, err
			}
			result = append(result, ZMember{Member: member, Score: score})
		} else {
			result = append(result, ZMember{Member: member})
		}
	}

	return result, nil
}

func (rz *RZSet) ZRank(key, member string) (int64, error) {
	return rz.zrankGeneric(key, member, false)
}

func (rz *RZSet) ZRevRank(key, member string) (int64, error) {
	return rz.zrankGeneric(key, member, true)
}

func (rz *RZSet) zrankGeneric(key, member string, reverse bool) (int64, error) {
	if len(key) == 0 || len(member) == 0 {
		return -1, err_def.ErrEmptyKey
	}

	rz.zsetLock.RLock()
	defer rz.zsetLock.RUnlock()

	score, exists, err := rz.getMemberScore(key, member)
	if err != nil {
		return -1, err
	}
	if !exists {
		return -1, nil
	}

	prefix := fmt.Sprintf("%s:%s:s:", ZSetPrefix, key)
	keys, err := rz.dw.GetDB().Keys(prefix + "*")
	if err != nil {
		return -1, err
	}

	sort.Strings(keys)

	targetKey := GetZSetSortKey(key, score, member)
	index := sort.SearchStrings(keys, targetKey)
	if index == len(keys) || keys[index] != targetKey {
		return -1, nil
	}
	if reverse {
		return int64(len(keys) - index - 1), nil
	}
	return int64(index), nil
}

func (rz *RZSet) ZRem(key string, members ...string) (int64, error) {
	if len(key) == 0 {
		return 0, err_def.ErrEmptyKey
	}
	if len(members) == 0 {
		return 0, nil
	}

	rz.zsetLock.Lock()
	defer rz.zsetLock.Unlock()

	wb := rz.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	var removed int64
	for _, member := range members {
		if len(member) == 0 {
			continue
		}

		score, exists, err := rz.getMemberScore(key, member)
		if err != nil {
			return 0, err
		}
		if !exists {
			continue
		}

		memberScoreKey := GetZSetMemberScoreKey(key, member)
		sortKey := GetZSetSortKey(key, score, member)

		if err := wb.Delete(memberScoreKey); err != nil {
			return 0, err
		}
		if err := wb.Delete(sortKey); err != nil {
			return 0, err
		}

		removed++
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return removed, nil
}

func (rz *RZSet) ZCard(key string) (int64, error) {
	if len(key) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	rz.zsetLock.RLock()
	defer rz.zsetLock.RUnlock()

	prefix := fmt.Sprintf("%s:%s:", ZSetPrefix, key)
	keys, err := rz.dw.GetDB().Keys(prefix + "*")
	if err != nil {
		return 0, err
	}

	var count int64
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) && !strings.Contains(k, ":s:") {
			count++
		}
	}

	return count, nil
}

func (rz *RZSet) ZScore(key, member string) (float64, error) {
	if len(key) == 0 || len(member) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	rz.zsetLock.RLock()
	defer rz.zsetLock.RUnlock()

	score, exists, err := rz.getMemberScore(key, member)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, err_def.ErrKeyNotFound
	}

	return score, nil
}

func (rz *RZSet) ZIncrBy(key, member string, increment float64) (float64, error) {
	if len(key) == 0 || len(member) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	rz.zsetLock.Lock()
	defer rz.zsetLock.Unlock()

	oldScore, exists, err := rz.getMemberScore(key, member)
	if err != nil {
		return 0, err
	}

	newScore := increment
	if exists {
		newScore += oldScore
	}

	_, err = rz.ZAdd(key, ZMember{Member: member, Score: newScore})
	if err != nil {
		return 0, err
	}

	return newScore, nil
}

func (rz *RZSet) ZRangeByScore(key string, min, max float64) ([]ZMember, error) {
	return rz.zrangeByScoreGeneric(key, min, max, false)
}

func (rz *RZSet) ZRangeByScoreWithScores(key string, min, max float64) ([]ZMember, error) {
	return rz.zrangeByScoreGeneric(key, min, max, true)
}

func (rz *RZSet) zrangeByScoreGeneric(key string, min, max float64, withScores bool) ([]ZMember, error) {
	if len(key) == 0 {
		return nil, err_def.ErrEmptyKey
	}

	rz.zsetLock.RLock()
	defer rz.zsetLock.RUnlock()

	prefix := fmt.Sprintf("%s:%s:s:", ZSetPrefix, key)
	keys, err := rz.dw.GetDB().Keys(prefix + "*")
	if err != nil {
		return nil, err
	}

	result := make([]ZMember, 0)
	for _, k := range keys {
		parts := strings.Split(k, ":")
		if len(parts) < 2 {
			continue
		}

		member := parts[len(parts)-1]
		score, _, err := rz.getMemberScore(key, member)
		if err != nil {
			return nil, err
		}

		if score >= min && score <= max {
			if withScores {
				result = append(result, ZMember{Member: member, Score: score})
			} else {
				result = append(result, ZMember{Member: member})
			}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Score == result[j].Score {
			return result[i].Member < result[j].Member
		}
		return result[i].Score < result[j].Score
	})

	return result, nil
}

func (rz *RZSet) ZCount(key string, min, max float64) (int64, error) {
	members, err := rz.ZRangeByScore(key, min, max)
	if err != nil {
		return 0, err
	}
	return int64(len(members)), nil
}

func (rz *RZSet) ZRemRangeByRank(key string, start, stop int) (int64, error) {
	members, err := rz.ZRange(key, start, stop)
	if err != nil {
		return 0, err
	}

	membersList := make([]string, len(members))
	for i, m := range members {
		membersList[i] = m.Member
	}

	return rz.ZRem(key, membersList...)
}

func (rz *RZSet) ZRemRangeByScore(key string, min, max float64) (int64, error) {
	members, err := rz.ZRangeByScore(key, min, max)
	if err != nil {
		return 0, err
	}

	membersList := make([]string, len(members))
	for i, m := range members {
		membersList[i] = m.Member
	}

	return rz.ZRem(key, membersList...)
}
