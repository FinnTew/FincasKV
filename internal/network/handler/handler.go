package handler

import (
	"errors"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/database"
	"github.com/FinnTew/FincasKV/internal/network/conn"
	"github.com/FinnTew/FincasKV/internal/network/protocol"
	"strconv"
	"strings"
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
