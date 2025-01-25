package storage

import "errors"

var (
	ErrKeyNotFound       = errors.New("key not found")
	ErrChecksumInvalid   = errors.New("checksum invalid")
	ErrNilKey            = errors.New("key is nil")
	ErrDBClosed          = errors.New("database is closed")
	ErrFileClosed        = errors.New("file is closed")
	ErrWriteFailed       = errors.New("write failed")
	ErrReadFailed        = errors.New("read failed")
	ErrFileNotFound      = errors.New("file not found")
	ErrInvalidRecord     = errors.New("invalid record")
	ErrNilRecord         = errors.New("nil record")
	ErrKeyTooLarge       = errors.New("key too large")
	ErrValueTooLarge     = errors.New("value too large")
	ErrEmptyKey          = errors.New("empty key")
	ErrCorruptedData     = errors.New("corrupted data")
	ErrChecksumMismatch  = errors.New("checksum mismatch")
	ErrInsufficientData  = errors.New("insufficient data")
	ErrDataLengthInvalid = errors.New("invalid data length")
	// TODO: add more errors here
)
