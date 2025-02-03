package redis

import (
	"errors"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/err_def"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RSet struct {
	dw *DBWrapper
}

var setPool = sync.Pool{
	New: func() interface{} {
		return &RSet{
			dw: &DBWrapper{},
		}
	},
}

func NewRSet() *RSet {
	return setPool.Get().(*RSet)
}

func (rs *RSet) Release() {
	setPool.Put(rs)
}

func (rs *RSet) SAdd(key string, members ...string) (int64, error) {
	if len(key) == 0 {
		return 0, err_def.ErrEmptyKey
	}
	if len(members) == 0 {
		return 0, nil
	}

	wb := rs.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	var added int64
	setLenKey := GetSetLenKey(key)

	currLen := int64(0)
	if val, err := rs.dw.GetDB().Get(setLenKey); err == nil {
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			currLen = n
		}
	}

	uniqueMembers := make(map[string]struct{}, len(members))
	for _, member := range members {
		uniqueMembers[member] = struct{}{}
	}

	for member := range uniqueMembers {
		memberKey := GetSetMemberKey(key, member)
		exists, err := rs.dw.GetDB().Exists(memberKey)
		if err != nil {
			return 0, err
		}
		if !exists {
			if err := wb.Put(memberKey, "1"); err != nil {
				return 0, err
			}
			added++
		}
	}

	if added > 0 {
		if err := wb.Put(setLenKey, strconv.FormatInt(currLen+added, 10)); err != nil {
			return 0, err
		}
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return added, nil
}

func (rs *RSet) SRem(key string, members ...string) (int64, error) {
	if len(key) == 0 {
		return 0, err_def.ErrEmptyKey
	}
	if len(members) == 0 {
		return 0, nil
	}

	wb := rs.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	var removed int64
	setLenKey := GetSetLenKey(key)

	currLen := int64(0)
	if val, err := rs.dw.GetDB().Get(setLenKey); err == nil {
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			currLen = n
		}
	}

	for _, member := range members {
		memberKey := GetSetMemberKey(key, member)
		exists, err := rs.dw.GetDB().Exists(memberKey)
		if err != nil {
			return 0, err
		}
		if exists {
			if err := wb.Delete(memberKey); err != nil {
				return 0, err
			}
			removed++
		}
	}

	if removed > 0 {
		newLen := currLen - removed
		if newLen > 0 {
			if err := wb.Put(setLenKey, strconv.FormatInt(newLen, 10)); err != nil {
				return 0, err
			}
		} else {
			if err := wb.Delete(setLenKey); err != nil {
				return 0, err
			}
		}
	}

	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return removed, nil
}

func (rs *RSet) SIsMember(key, member string) (bool, error) {
	if len(key) == 0 {
		return false, err_def.ErrEmptyKey
	}
	memberKey := GetSetMemberKey(key, member)
	return rs.dw.GetDB().Exists(memberKey)
}

func (rs *RSet) SMembers(key string) ([]string, error) {
	if len(key) == 0 {
		return nil, err_def.ErrEmptyKey
	}

	prefix := fmt.Sprintf("%s:%s:", SetPrefix, key)
	keys, err := rs.dw.GetDB().Keys(prefix + "*")
	if err != nil {
		return nil, err
	}

	members := make([]string, 0, len(keys))
	for _, k := range keys {
		if strings.HasSuffix(k, "_len_") {
			continue
		}
		member := strings.TrimPrefix(k, prefix)
		members = append(members, member)
	}

	return members, nil
}

func (rs *RSet) SCard(key string) (int64, error) {
	if len(key) == 0 {
		return 0, err_def.ErrEmptyKey
	}

	val, err := rs.dw.GetDB().Get(GetSetLenKey(key))
	if err != nil {
		if errors.Is(err, err_def.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}

	return strconv.ParseInt(val, 10, 64)
}

func (rs *RSet) SPop(key string) (string, error) {
	members, err := rs.SPopN(key, 1)
	if err != nil {
		return "", err
	}
	if len(members) == 0 {
		return "", nil
	}
	return members[0], nil
}

func (rs *RSet) SPopN(key string, count int) ([]string, error) {
	if len(key) == 0 {
		return nil, err_def.ErrEmptyKey
	}
	if count <= 0 {
		return []string{}, nil
	}

	members, err := rs.SMembers(key)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return []string{}, nil
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(members), func(i, j int) {
		members[i], members[j] = members[j], members[i]
	})

	if count > len(members) {
		count = len(members)
	}

	popped := members[:count]
	if _, err := rs.SRem(key, popped...); err != nil {
		return nil, err
	}

	return popped, nil
}

func (rs *RSet) SRandMember(key string, count int) ([]string, error) {
	if len(key) == 0 {
		return nil, err_def.ErrEmptyKey
	}
	if count == 0 {
		return []string{}, nil
	}

	members, err := rs.SMembers(key)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return []string{}, nil
	}

	allowRepeat := count < 0
	if allowRepeat {
		count = -count
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	result := make([]string, 0, count)

	if allowRepeat {
		for i := 0; i < count; i++ {
			idx := r.Intn(len(members))
			result = append(result, members[idx])
		}
	} else {
		if count > len(members) {
			count = len(members)
		}
		perm := r.Perm(len(members))
		for i := 0; i < count; i++ {
			result = append(result, members[perm[i]])
		}
	}

	return result, nil
}

func (rs *RSet) SDiff(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	members, err := rs.SMembers(keys[0])
	if err != nil {
		return nil, err
	}

	if len(keys) == 1 {
		return members, nil
	}

	result := make(map[string]struct{})
	for _, member := range members {
		result[member] = struct{}{}
	}

	for _, key := range keys[1:] {
		otherMembers, err := rs.SMembers(key)
		if err != nil {
			return nil, err
		}
		for _, member := range otherMembers {
			delete(result, member)
		}
	}

	diff := make([]string, 0, len(result))
	for member := range result {
		diff = append(diff, member)
	}

	return diff, nil
}

func (rs *RSet) SUnion(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	result := make(map[string]struct{})

	for _, key := range keys {
		members, err := rs.SMembers(key)
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			result[member] = struct{}{}
		}
	}

	union := make([]string, 0, len(result))
	for member := range result {
		union = append(union, member)
	}

	return union, nil
}

func (rs *RSet) SInter(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	members, err := rs.SMembers(keys[0])
	if err != nil {
		return nil, err
	}

	if len(keys) == 1 {
		return members, nil
	}

	result := make(map[string]int)
	for _, member := range members {
		result[member] = 1
	}

	for _, key := range keys[1:] {
		otherMembers, err := rs.SMembers(key)
		if err != nil {
			return nil, err
		}
		for _, member := range otherMembers {
			if count := result[member]; count > 0 {
				result[member]++
			}
		}
	}

	inter := make([]string, 0)
	for member, count := range result {
		if count == len(keys) {
			inter = append(inter, member)
		}
	}

	return inter, nil
}

func (rs *RSet) SMove(source, destination, member string) (bool, error) {
	if len(source) == 0 || len(destination) == 0 {
		return false, err_def.ErrEmptyKey
	}

	wb := rs.dw.GetDB().NewWriteBatch(nil)
	defer wb.Release()

	srcMemberKey := GetSetMemberKey(source, member)
	exists, err := rs.dw.GetDB().Exists(srcMemberKey)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	srcLenKey := GetSetLenKey(source)
	srcLen := int64(0)
	if val, err := rs.dw.GetDB().Get(srcLenKey); err == nil {
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			srcLen = n
		}
	}

	if err := wb.Delete(srcMemberKey); err != nil {
		return false, err
	}

	if srcLen > 1 {
		if err := wb.Put(srcLenKey, strconv.FormatInt(srcLen-1, 10)); err != nil {
			return false, err
		}
	} else {
		if err := wb.Delete(srcLenKey); err != nil {
			return false, err
		}
	}

	dstMemberKey := GetSetMemberKey(destination, member)
	exists, err = rs.dw.GetDB().Exists(dstMemberKey)
	if err != nil {
		return false, err
	}

	if !exists {
		dstLenKey := GetSetLenKey(destination)
		dstLen := int64(0)
		if val, err := rs.dw.GetDB().Get(dstLenKey); err == nil {
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				dstLen = n
			}
		}

		if err := wb.Put(dstMemberKey, "1"); err != nil {
			return false, err
		}
		if err := wb.Put(dstLenKey, strconv.FormatInt(dstLen+1, 10)); err != nil {
			return false, err
		}
	}

	if err := wb.Commit(); err != nil {
		return false, err
	}

	return true, nil
}
