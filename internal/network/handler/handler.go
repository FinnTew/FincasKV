package handler

import (
	"errors"
	"github.com/FinnTew/FincasKV/internal/database"
	"github.com/FinnTew/FincasKV/internal/network/conn"
	"github.com/FinnTew/FincasKV/internal/network/protocol"
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
	// TODO: add more cmd here
	default:
		return conn.WriteError(errors.New("unknown command"))
	}
}

func (h *Handler) handlePing(conn *conn.Connection, cmd *protocol.Command) error {
	if len(cmd.Args) != 0 {
		return conn.WriteError(ErrWrongArgCount)
	}

	return conn.WriteString("PONG")
}
