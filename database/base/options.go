package base

import "time"

type BaseDBOptions struct {
	ExpireCheckInterval time.Duration
	TTLMetadataFile     string
	FlushTTLOnChange    bool
}

func DefaultBaseDBOptions() *BaseDBOptions {
	return &BaseDBOptions{
		ExpireCheckInterval: 1 * time.Minute,
		TTLMetadataFile:     "ttl.data",
		FlushTTLOnChange:    false,
	}
}
