package handler

import (
	"errors"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/database"
	"github.com/FinnTew/FincasKV/internal/database/redis"
	"github.com/FinnTew/FincasKV/internal/network/conn"
	"github.com/FinnTew/FincasKV/internal/network/protocol"
	"strconv"
	"strings"
	"time"
)

var (
	ErrWrongArgCount = errors.New("wrong number of arguments")
	ErrSyntax        = errors.New("syntax error")
)

type Handler struct {
	db *database.FincasDB
}

func New(db *database.FincasDB) *Handler {
	return &Handler{
		db: db,
	}
}

func (h *Handler) Handle(conn *conn.Connection, cmd *protocol.Command) error {
	switch strings.ToUpper(cmd.Name) {
	// String commands
	case "PING":
		return h.handlePing(conn, cmd)
	case "SET":
		return h.handleSet(conn, cmd)
	case "GET":
		return h.handleGet(conn, cmd)
	case "DEL":
		return h.handleDel(conn, cmd)
	case "INCR":
		return h.handleIncr(conn, cmd)
	case "INCRBY":
		return h.handleIncrBy(conn, cmd)
	case "DECR":
		return h.handleDecr(conn, cmd)
	case "DECRBY":
		return h.handleDecrBy(conn, cmd)
	case "APPEND":
		return h.handleAppend(conn, cmd)
	case "GETSET":
		return h.handleGetSet(conn, cmd)
	case "SETNX":
		return h.handleSetNX(conn, cmd)
	case "MSET":
		return h.handleMSet(conn, cmd)
	case "MGET":
		return h.handleMGet(conn, cmd)
	case "STRLEN":
		return h.handleStrLen(conn, cmd)
	// Hash commands
	case "HSET":
		return h.handleHSet(conn, cmd)
	case "HGET":
		return h.handleHGet(conn, cmd)
	case "HMSET":
		return h.handleHMSet(conn, cmd)
	case "HMGET":
		return h.handleHMGet(conn, cmd)
	case "HDEL":
		return h.handleHDel(conn, cmd)
	case "HEXISTS":
		return h.handleHExists(conn, cmd)
	case "HKEYS":
		return h.handleHKeys(conn, cmd)
	case "HVALS":
		return h.handleHVals(conn, cmd)
	case "HGETALL":
		return h.handleHGetAll(conn, cmd)
	case "HLEN":
		return h.handleHLen(conn, cmd)
	case "HINCRBY":
		return h.handleHIncrBy(conn, cmd)
	case "HINCRBYFLOAT":
		return h.handleHIncrByFloat(conn, cmd)
	case "HSETNX":
		return h.handleHSetNX(conn, cmd)
	case "HSTRLEN":
		return h.handleHStrLen(conn, cmd)
	// List commands
	case "LPUSH":
		return h.handleLPush(conn, cmd)
	case "RPUSH":
		return h.handleRPush(conn, cmd)
	case "LPOP":
		return h.handleLPop(conn, cmd)
	case "RPOP":
		return h.handleRPop(conn, cmd)
	case "LLEN":
		return h.handleLLen(conn, cmd)
	case "LRANGE":
		return h.handleLRange(conn, cmd)
	case "LTRIM":
		return h.handleLTrim(conn, cmd)
	case "BLPOP":
		return h.handleBLPop(conn, cmd)
	case "BRPOP":
		return h.handleBRPop(conn, cmd)
	case "LINSERT":
		return h.handleLInsert(conn, cmd)
	// Set commands
	case "SADD":
		return h.handleSAdd(conn, cmd)
	case "SREM":
		return h.handleSRem(conn, cmd)
	case "SISMEMBER":
		return h.handleSIsMember(conn, cmd)
	case "SMEMBERS":
		return h.handleSMembers(conn, cmd)
	case "SCARD":
		return h.handleSCard(conn, cmd)
	case "SPOP":
		return h.handleSPop(conn, cmd)
	case "SRANDMEMBER":
		return h.handleSRandMember(conn, cmd)
	case "SDIFF":
		return h.handleSDiff(conn, cmd)
	case "SUNION":
		return h.handleSUnion(conn, cmd)
	case "SINTER":
		return h.handleSInter(conn, cmd)
	case "SMOVE":
		return h.handleSMove(conn, cmd)
	// ZSet commands
	case "ZADD":
		return h.handleZAdd(conn, cmd)
	case "ZRANGE":
		return h.handleZRange(conn, cmd)
	case "ZREVRANGE":
		return h.handleZRevRange(conn, cmd)
	case "ZRANK":
		return h.handleZRank(conn, cmd)
	case "ZREVRANK":
		return h.handleZRevRank(conn, cmd)
	case "ZREM":
		return h.handleZRem(conn, cmd)
	case "ZCARD":
		return h.handleZCard(conn, cmd)
	case "ZSCORE":
		return h.handleZScore(conn, cmd)
	case "ZINCRBY":
		return h.handleZIncrBy(conn, cmd)
	case "ZRANGEBYSCORE":
		return h.handleZRangeByScore(conn, cmd)
	case "ZCOUNT":
		return h.handleZCount(conn, cmd)
	case "ZREMRANGEBYRANK":
		return h.handleZRemRangeByRank(conn, cmd)
	case "ZREMRANGEBYSCORE":
		return h.handleZRemRangeByScore(conn, cmd)
	default:
		return conn.WriteError(errors.New("unknown command"))
	}
}

func (h *Handler) handlePing(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) > 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	if len(cmd.Args) == 1 {
		return conn.WriteString(fmt.Sprintf("PONG %s", string(cmd.Args[0])))
	}

	return conn.WriteString("PONG")
}

func (h *Handler) handleSet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	err := h.db.Set(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString("OK")
}

func (h *Handler) handleGet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := h.db.Get(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString(val)
}

func (h *Handler) handleDel(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 1 {
		return conn.WriteError(ErrSyntax)
	}

	keys := make([]string, 0, len(cmd.Args))
	for _, arg := range cmd.Args {
		keys = append(keys, string(arg))
	}

	err := h.db.Del(keys...)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString("OK")
}

func (h *Handler) handleIncr(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.Incr(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleIncrBy(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid integer value: %s", string(cmd.Args[1])))
	}

	n, err := h.db.IncrBy(string(cmd.Args[0]), val)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleDecr(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.Decr(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleDecrBy(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid integer value: %s", string(cmd.Args[1])))
	}

	n, err := h.db.DecrBy(string(cmd.Args[0]), val)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleAppend(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.Append(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleGetSet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := h.db.GetSet(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString(val)
}

func (h *Handler) handleSetNX(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	ok, err := h.db.SetNX(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	if ok {
		return conn.WriteInteger(1)
	}
	return conn.WriteInteger(0)
}

func (h *Handler) handleMSet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	if len(cmd.Args)%2 != 0 {
		return conn.WriteError(ErrSyntax)
	}

	kvPairs := make(map[string]string, len(cmd.Args)/2)
	for i := 0; i < len(cmd.Args); i += 2 {
		kvPairs[string(cmd.Args[i])] = string(cmd.Args[i+1])
	}

	err := h.db.MSet(kvPairs)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString("OK")
}

func (h *Handler) handleMGet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	keys := make([]string, 0, len(cmd.Args))
	for _, arg := range cmd.Args {
		keys = append(keys, string(arg))
	}

	kvMap, err := h.db.MGet(keys...)
	fmt.Println(kvMap)
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, key := range keys {
		res = append(res, []byte(kvMap[key]))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleStrLen(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.StrLen(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleHSet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	err := h.db.HSet(string(cmd.Args[0]), string(cmd.Args[1]), string(cmd.Args[2]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString("OK")
}

func (h *Handler) handleHGet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := h.db.HGet(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString(val)
}

func (h *Handler) handleHMSet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	if (len(cmd.Args)-1)%2 != 0 {
		return conn.WriteError(ErrSyntax)
	}

	kvPairs := make(map[string]string, (len(cmd.Args)-1)/2)
	for i := 1; i < len(cmd.Args); i += 2 {
		kvPairs[string(cmd.Args[i])] = string(cmd.Args[i+1])
	}

	err := h.db.HMSet(string(cmd.Args[0]), kvPairs)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString("OK")
}

func (h *Handler) handleHMGet(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	fields := make([]string, 0, len(cmd.Args))
	for _, arg := range cmd.Args {
		fields = append(fields, string(arg))
	}

	kvMap, err := h.db.HMGet(fields[0], fields[1:]...)
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for i := 1; i < len(fields); i++ {
		res = append(res, []byte(kvMap[fields[i]]))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleHDel(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	fields := make([]string, 0, len(cmd.Args))
	for _, arg := range cmd.Args {
		fields = append(fields, string(arg))
	}

	n, err := h.db.HDel(fields[0], fields[1:]...)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleHExists(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	ok, err := h.db.HExists(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	if ok {
		return conn.WriteInteger(1)
	}
	return conn.WriteInteger(0)
}

func (h *Handler) handleHKeys(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	keys, err := h.db.HKeys(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, key := range keys {
		res = append(res, []byte(key))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleHVals(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	vals, err := h.db.HVals(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, val := range vals {
		res = append(res, []byte(val))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleHGetAll(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	kvMap, err := h.db.HGetAll(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for key, val := range kvMap {
		res = append(res, []byte(key))
		res = append(res, []byte(val))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleHLen(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.HLen(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleHIncrBy(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid value %s", string(cmd.Args[2])))
	}

	n, err := h.db.HIncrBy(string(cmd.Args[0]), string(cmd.Args[1]), val)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleHIncrByFloat(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid value %s", string(cmd.Args[2])))
	}

	n, err := h.db.HIncrByFloat(string(cmd.Args[0]), string(cmd.Args[1]), val)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString(strconv.FormatFloat(n, 'f', -1, 64))
}

func (h *Handler) handleHSetNX(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	ok, err := h.db.HSetNX(string(cmd.Args[0]), string(cmd.Args[1]), string(cmd.Args[2]))
	if err != nil {
		return conn.WriteError(err)
	}

	if ok {
		return conn.WriteInteger(1)
	}
	return conn.WriteInteger(0)
}

func (h *Handler) handleHStrLen(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.HStrLen(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleLPush(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	var vals []string
	for _, arg := range cmd.Args[1:] {
		vals = append(vals, string(arg))
	}

	n, err := h.db.LPush(key, vals...)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleRPush(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	var vals []string
	for _, arg := range cmd.Args[1:] {
		vals = append(vals, string(arg))
	}

	n, err := h.db.RPush(key, vals...)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleLPop(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := h.db.LPop(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString(val)
}

func (h *Handler) handleRPop(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	val, err := h.db.RPop(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString(val)
}

func (h *Handler) handleLLen(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.LLen(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleLRange(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	start, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid start value %s", string(cmd.Args[1])))
	}
	end, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid end value %s", string(cmd.Args[2])))
	}

	vals, err := h.db.LRange(string(cmd.Args[0]), int(start), int(end))
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, val := range vals {
		res = append(res, []byte(val))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleLTrim(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	start, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid start value %s", string(cmd.Args[0])))
	}
	end, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid end value %s", string(cmd.Args[1])))
	}

	err = h.db.LTrim(string(cmd.Args[2]), int(start), int(end))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString("OK")
}

func (h *Handler) handleBLPop(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	timeout, err := strconv.ParseInt(string(cmd.Args[len(cmd.Args)-1]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid timeout value %s", string(cmd.Args[0])))
	}

	var keys []string
	for _, arg := range cmd.Args[:len(cmd.Args)-1] {
		keys = append(keys, string(arg))
	}

	kvMap, err := h.db.BLPop(time.Duration(timeout), keys...)
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for key, val := range kvMap {
		res = append(res, []byte(key))
		res = append(res, []byte(val))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleBRPop(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	timeout, err := strconv.ParseInt(string(cmd.Args[len(cmd.Args)-1]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid timeout value %s", string(cmd.Args[0])))
	}

	var keys []string
	for _, arg := range cmd.Args[:len(cmd.Args)-1] {
		keys = append(keys, string(arg))
	}

	kvMap, err := h.db.BRPop(time.Duration(timeout), keys...)
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for key, val := range kvMap {
		res = append(res, []byte(key))
		res = append(res, []byte(val))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleLInsert(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 4 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	before := string(cmd.Args[1])
	pivot := string(cmd.Args[2])
	elem := string(cmd.Args[3])

	var (
		n   int64
		err error
	)
	switch strings.ToUpper(before) {
	case "BEFORE":
		n, err = h.db.LInsertBefore(key, pivot, elem)
	case "AFTER":
		n, err = h.db.LInsertAfter(key, pivot, elem)
	default:
		return conn.WriteError(ErrSyntax)
	}
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleSAdd(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	var members []string
	for _, arg := range cmd.Args[1:] {
		members = append(members, string(arg))
	}

	n, err := h.db.SAdd(key, members...)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleSRem(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	var members []string
	for _, arg := range cmd.Args[1:] {
		members = append(members, string(arg))
	}

	n, err := h.db.SRem(key, members...)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleSIsMember(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	ok, err := h.db.SIsMember(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	if ok {
		return conn.WriteInteger(1)
	}
	return conn.WriteInteger(0)
}

func (h *Handler) handleSMembers(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	members, err := h.db.SMembers(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, member := range members {
		res = append(res, []byte(member))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleSCard(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.SCard(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleSPop(conn *conn.Connection, cmd *protocol.Command) error {
	switch len(cmd.Args) {
	case 1:
		val, err := h.db.SPop(string(cmd.Args[0]))
		if err != nil {
			return conn.WriteError(err)
		}
		return conn.WriteArray([][]byte{[]byte(val)})
	case 2:
		count, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
		if err != nil {
			return conn.WriteError(fmt.Errorf("invalid count value %s", string(cmd.Args[1])))
		}
		vals, err := h.db.SPopN(string(cmd.Args[0]), int(count))
		if err != nil {
			return conn.WriteError(err)
		}
		var res [][]byte
		for _, val := range vals {
			res = append(res, []byte(val))
		}
		return conn.WriteArray(res)
	default:
		return conn.WriteError(ErrWrongArgCount)
	}
}

func (h *Handler) handleSRandMember(conn *conn.Connection, cmd *protocol.Command) error {
	var (
		vals []string
		err  error
	)
	switch len(cmd.Args) {
	case 1:
		vals, err = h.db.SRandMember(string(cmd.Args[0]), 1)
	case 2:
		count, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
		if err != nil {
			return conn.WriteError(fmt.Errorf("invalid count value %s", string(cmd.Args[1])))
		}
		vals, err = h.db.SRandMember(string(cmd.Args[0]), int(count))
	default:
		return conn.WriteError(ErrWrongArgCount)
	}

	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, val := range vals {
		res = append(res, []byte(val))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleSDiff(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) == 0 {
		return conn.WriteArray(nil)
	}

	var keys []string
	for _, arg := range cmd.Args {
		keys = append(keys, string(arg))
	}

	vals, err := h.db.SDiff(keys...)
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, val := range vals {
		res = append(res, []byte(val))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleSUnion(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) == 0 {
		return conn.WriteArray(nil)
	}

	var keys []string
	for _, arg := range cmd.Args {
		keys = append(keys, string(arg))
	}

	vals, err := h.db.SUnion(keys...)
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, val := range vals {
		res = append(res, []byte(val))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleSInter(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) == 0 {
		return conn.WriteArray(nil)
	}

	var keys []string
	for _, arg := range cmd.Args {
		keys = append(keys, string(arg))
	}

	vals, err := h.db.SInter(keys...)
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, val := range vals {
		res = append(res, []byte(val))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleSMove(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	ok, err := h.db.SMove(string(cmd.Args[0]), string(cmd.Args[1]), string(cmd.Args[2]))
	if err != nil {
		return conn.WriteError(err)
	}

	if ok {
		return conn.WriteInteger(1)
	}
	return conn.WriteInteger(0)
}

func (h *Handler) handleZAdd(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	var members []redis.ZMember
	for i := 1; i < len(cmd.Args); i += 2 {
		score, err := strconv.ParseFloat(string(cmd.Args[i]), 64)
		if err != nil {
			return conn.WriteError(fmt.Errorf("invalid score value %s", string(cmd.Args[i])))
		}
		members = append(members, redis.ZMember{
			Score:  score,
			Member: string(cmd.Args[i+1]),
		})
	}

	n, err := h.db.ZAdd(key, members...)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleZRange(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 3 || len(cmd.Args) > 4 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	start, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid start value %s", string(cmd.Args[1])))
	}
	stop, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid stop value %s", string(cmd.Args[2])))
	}

	if len(cmd.Args) == 4 {
		if strings.ToUpper(string(cmd.Args[3])) != "WITHSCORES" {
			return conn.WriteError(ErrSyntax)
		} else {
			members, err := h.db.ZRangeWithScores(key, int(start), int(stop))
			if err != nil {
				return conn.WriteError(err)
			}
			var res [][]byte
			for _, val := range members {
				res = append(res, []byte(val.Member))
				res = append(res, []byte(strconv.FormatFloat(val.Score, 'f', -1, 64)))
			}
			return conn.WriteArray(res)
		}
	}

	members, err := h.db.ZRange(key, int(start), int(stop))
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, val := range members {
		res = append(res, []byte(val.Member))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleZRevRange(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 3 || len(cmd.Args) > 4 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	start, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid start value %s", string(cmd.Args[1])))
	}
	stop, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid stop value %s", string(cmd.Args[2])))
	}

	if len(cmd.Args) == 4 {
		if strings.ToUpper(string(cmd.Args[3])) != "WITHSCORES" {
			return conn.WriteError(ErrSyntax)
		} else {
			members, err := h.db.ZRevRangeWithScores(key, int(start), int(stop))
			if err != nil {
				return conn.WriteError(err)
			}
			var res [][]byte
			for _, val := range members {
				res = append(res, []byte(val.Member))
				res = append(res, []byte(strconv.FormatFloat(val.Score, 'f', -1, 64)))
			}
			return conn.WriteArray(res)
		}
	}

	members, err := h.db.ZRevRange(key, int(start), int(stop))
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, val := range members {
		res = append(res, []byte(val.Member))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleZRank(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.ZRank(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleZRevRank(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.ZRevRank(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleZRem(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	var members []string
	for _, arg := range cmd.Args[1:] {
		members = append(members, string(arg))
	}

	n, err := h.db.ZRem(key, strings.Join(members, ","))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleZCard(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 1 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.ZCard(string(cmd.Args[0]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleZScore(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 2 {
		return conn.WriteError(ErrWrongArgCount)
	}

	n, err := h.db.ZScore(string(cmd.Args[0]), string(cmd.Args[1]))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteString(strconv.FormatFloat(n, 'f', -1, 64))
}

func (h *Handler) handleZIncrBy(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	increment, err := strconv.ParseFloat(string(cmd.Args[1]), 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid increment value %s", string(cmd.Args[2])))
	}
	member := string(cmd.Args[2])

	fmt.Println(key, increment, member)
	n, err := h.db.ZIncrBy(key, member, increment)
	if err != nil {
		return conn.WriteError(err)
	}
	fmt.Println(n)

	return conn.WriteString(strconv.FormatFloat(n, 'f', -1, 64))
}

func (h *Handler) handleZRangeByScore(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) < 3 || len(cmd.Args) > 4 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	minScore, err := strconv.ParseFloat(string(cmd.Args[1]), 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid min score value %s", string(cmd.Args[1])))
	}
	maxScore, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid max score value %s", string(cmd.Args[2])))
	}

	if len(cmd.Args) == 4 {
		if strings.ToUpper(string(cmd.Args[3])) != "WITHSCORES" {
			return conn.WriteError(ErrSyntax)
		} else {
			members, err := h.db.ZRangeByScoreWithScores(key, minScore, maxScore)
			if err != nil {
				return conn.WriteError(err)
			}
			var res [][]byte
			for _, val := range members {
				res = append(res, []byte(val.Member))
				res = append(res, []byte(strconv.FormatFloat(val.Score, 'f', -1, 64)))
			}
			return conn.WriteArray(res)
		}
	}

	members, err := h.db.ZRangeByScore(key, minScore, maxScore)
	if err != nil {
		return conn.WriteError(err)
	}

	var res [][]byte
	for _, val := range members {
		res = append(res, []byte(val.Member))
	}

	return conn.WriteArray(res)
}

func (h *Handler) handleZCount(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	minScore, err := strconv.ParseFloat(string(cmd.Args[1]), 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid min score value %s", string(cmd.Args[1])))
	}
	maxScore, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid max score value %s", string(cmd.Args[2])))
	}

	n, err := h.db.ZCount(key, minScore, maxScore)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleZRemRangeByRank(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	start, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid start value %s", string(cmd.Args[1])))
	}
	stop, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid stop value %s", string(cmd.Args[2])))
	}

	n, err := h.db.ZRemRangeByRank(key, int(start), int(stop))
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}

func (h *Handler) handleZRemRangeByScore(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 3 {
		return conn.WriteError(ErrWrongArgCount)
	}

	key := string(cmd.Args[0])
	minScore, err := strconv.ParseFloat(string(cmd.Args[1]), 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid min score value %s", string(cmd.Args[1])))
	}
	maxScore, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		return conn.WriteError(fmt.Errorf("invalid max score value %s", string(cmd.Args[2])))
	}

	n, err := h.db.ZRemRangeByScore(key, minScore, maxScore)
	if err != nil {
		return conn.WriteError(err)
	}

	return conn.WriteInteger(n)
}
