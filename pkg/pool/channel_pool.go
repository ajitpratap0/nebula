package pool

import (
	"sync"
)

// ChannelPool provides pooling for channels to reduce allocations
type ChannelPool[T any] struct {
	pool sync.Pool
	size int
}

// NewChannelPool creates a new channel pool with specified buffer size
func NewChannelPool[T any](size int) *ChannelPool[T] {
	return &ChannelPool[T]{
		size: size,
		pool: sync.Pool{
			New: func() interface{} {
				return make(chan T, size)
			},
		},
	}
}

// Get retrieves a channel from the pool
func (p *ChannelPool[T]) Get() chan T {
	ch, ok := p.pool.Get().(chan T)
	if !ok {
		// Create new channel if type assertion fails
		return make(chan T, p.size)
	}
	return ch
}

// Put returns a channel to the pool after draining it
func (p *ChannelPool[T]) Put(ch chan T) {
	if ch == nil {
		return
	}

	// Drain channel before returning to pool
drainLoop:
	for len(ch) > 0 {
		select {
		case <-ch:
		default:
			break drainLoop
		}
	}

	p.pool.Put(ch)
}

// Global channel pools for common types
var (
	// RecordChannelPool for single record channels
	RecordChannelPool = NewChannelPool[*Record](10000)

	// BatchChannelPool for batch channels
	BatchChannelPool = NewChannelPool[[]*Record](100)

	// SmallRecordChannelPool for smaller buffers
	SmallRecordChannelPool = NewChannelPool[*Record](100)

	// ErrorChannelPool for error channels
	ErrorChannelPool = NewChannelPool[error](10)
)

// GetRecordChannel gets a record channel from the pool
func GetRecordChannel(size int) chan *Record {
	if size <= 100 {
		return SmallRecordChannelPool.Get()
	}
	return RecordChannelPool.Get()
}

// PutRecordChannel returns a record channel to the pool
func PutRecordChannel(ch chan *Record) {
	if ch == nil {
		return
	}

	// Determine which pool based on capacity
	if cap(ch) <= 100 {
		SmallRecordChannelPool.Put(ch)
	} else {
		RecordChannelPool.Put(ch)
	}
}

// GetBatchChannel gets a batch channel from the pool
func GetBatchChannel() chan []*Record {
	return BatchChannelPool.Get()
}

// PutBatchChannel returns a batch channel to the pool
func PutBatchChannel(ch chan []*Record) {
	BatchChannelPool.Put(ch)
}

// GetErrorChannel gets an error channel from the pool
func GetErrorChannel() chan error {
	return ErrorChannelPool.Get()
}

// PutErrorChannel returns an error channel to the pool
func PutErrorChannel(ch chan error) {
	ErrorChannelPool.Put(ch)
}
