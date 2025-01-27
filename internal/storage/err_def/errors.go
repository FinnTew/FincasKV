package err_def

import "errors"

var (
	ErrKeyNotFound       = errors.New("key not found")
	ErrChecksumInvalid   = errors.New("checksum invalid")
	ErrDBClosed          = errors.New("database is closed")
	ErrWriteFailed       = errors.New("write failed")
	ErrReadFailed        = errors.New("read failed")
	ErrFileNotFound      = errors.New("file not found")
	ErrNilRecord         = errors.New("nil record")
	ErrKeyTooLarge       = errors.New("key too large")
	ErrValueTooLarge     = errors.New("value too large")
	ErrEmptyKey          = errors.New("empty key")
	ErrChecksumMismatch  = errors.New("checksum mismatch")
	ErrInsufficientData  = errors.New("insufficient data")
	ErrDataLengthInvalid = errors.New("invalid data length")
)
