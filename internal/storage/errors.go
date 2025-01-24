package storage

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrNilKey      = errors.New("key is nil")
	ErrDBClosed    = errors.New("database is closed")
	// TODO: add more errors here
)
