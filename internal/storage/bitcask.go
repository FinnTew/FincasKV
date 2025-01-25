package storage

type Bitcask struct {
	fm       FileManager
	memIndex MemIndexShard[string, Entry]
	memCache MemCache[string, []byte]
}
