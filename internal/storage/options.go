package storage

type Options struct {
	// TODO: add config options here
}

type Option func(opt *Options)

func DefaultOptions() *Options {
	return &Options{
		// TODO: add default options here
	}
}
