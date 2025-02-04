package handler

import (
	"errors"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/database"
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
	//
	// TODO: add more cmd here
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

	res := [][]byte{}
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

	res := [][]byte{}
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

	res := [][]byte{}
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

	res := [][]byte{}
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

	res := [][]byte{}
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
	vals := []string{}
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
	vals := []string{}
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

	res := [][]byte{}
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

	keys := []string{}
	for _, arg := range cmd.Args[:len(cmd.Args)-1] {
		keys = append(keys, string(arg))
	}

	kvMap, err := h.db.BLPop(time.Duration(timeout), keys...)
	if err != nil {
		return conn.WriteError(err)
	}

	res := [][]byte{}
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

	keys := []string{}
	for _, arg := range cmd.Args[:len(cmd.Args)-1] {
		keys = append(keys, string(arg))
	}

	kvMap, err := h.db.BRPop(time.Duration(timeout), keys...)
	if err != nil {
		return conn.WriteError(err)
	}

	res := [][]byte{}
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
