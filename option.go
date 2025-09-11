package dsqueue

import (
	"time"
)

const (
	DefaultBufferSize     = 16 * 1024
	DefaultDedupCacheSize = 2 * 1024
	DefaultIdleWriteTime  = time.Minute
	DefaultCloseTimeout   = 10 * time.Second
)

// config contains all options for DSQueue.
type config struct {
	bufferSize     int
	dedupCacheSize int
	idleWriteTime  time.Duration
	closeTimeout   time.Duration
}

// Option is a function that sets a value in a config.
type Option func(*config)

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) config {
	cfg := config{
		bufferSize:     DefaultBufferSize,
		dedupCacheSize: DefaultDedupCacheSize,
		idleWriteTime:  DefaultIdleWriteTime,
		closeTimeout:   DefaultCloseTimeout,
	}

	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// WithBufferSize sets the limit on number of items kept in input buffer
// memory, at which they are all written to the datastore. A value of 0 means
// items are only written to the datastore at shutdown, and read from the
// datastore at start.
func WithBufferSize(n int) Option {
	return func(c *config) {
		if n < 0 {
			n = 0
		}
		c.bufferSize = n
	}
}

// WithDedupCacheSize sets the size of the LRU cache used to deduplicate items
// in the queue. A value of 0 disables the dedup cache.
func WithDedupCacheSize(n int) Option {
	return func(c *config) {
		if n < 0 {
			n = 0
		}
		c.dedupCacheSize = n
	}
}

// WithIdleWriteTime sets the amout of time that the queue must be idle (no
// input or output) before all buffered input items are written to the
// datastore. A value of zero means that buffered input items are not
// automatically flushed to the datastore. This value must be greater than one
// second.
func WithIdleWriteTime(d time.Duration) Option {
	return func(c *config) {
		if d < time.Second {
			d = time.Second
		}
		c.idleWriteTime = d
	}
}

// WithCloseTimeout sets the duration that Close waits to finish writing items
// to the datastore. A value of 0 means wait until finished with no timeout.
func WithCloseTimeout(d time.Duration) Option {
	return func(c *config) {
		if d < 0 {
			d = 0
		}
		c.closeTimeout = d
	}
}
