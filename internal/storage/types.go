package storage

type BitcaskItem[K comparable, V any] struct {
	Key   K
	Value V
}

type Entry struct {
	// TODO: add fields
}
