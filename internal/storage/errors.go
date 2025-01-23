package storage

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrDBClosed    = errors.New("database is closed")
	// TODO: add more errors here
)
