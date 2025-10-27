// Package lockfree provides lock-free data structures for high-performance concurrent processing
package lockfree

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// Queue implements a lock-free multi-producer single-consumer queue
// optimized for 100K+ records/sec throughput
type Queue struct {
	// Separate head and tail on different cache lines to avoid false sharing
	head      atomic.Uint64
	_padding1 [7]uint64 //nolint:unused // 56 bytes padding to separate cache lines

	tail      atomic.Uint64
	_padding2 [7]uint64 //nolint:unused // 56 bytes padding

	buffer   []unsafe.Pointer
	capacity uint64
	mask     uint64
}

// NewQueue creates a new lock-free queue with given capacity.
// Capacity will be rounded up to the next power of 2 for efficient masking.
func NewQueue(capacity int) *Queue {
	// Round up to next power of 2
	cap := uint64(1)
	for cap < uint64(capacity) {
		cap <<= 1
	}

	return &Queue{
		buffer:   make([]unsafe.Pointer, cap),
		capacity: cap,
		mask:     cap - 1,
	}
}

// Enqueue adds an item to the queue in a thread-safe manner.
// Returns true if successful, false if the queue is full.
// This method is safe for multiple concurrent producers.
func (q *Queue) Enqueue(item interface{}) bool {
	for {
		tail := q.tail.Load()
		next := (tail + 1) & q.mask

		// Check if queue is full
		if next == q.head.Load() {
			return false
		}

		// Try to claim the slot
		if q.tail.CompareAndSwap(tail, next) {
			// We own this slot, store the item
			atomic.StorePointer(&q.buffer[tail], unsafe.Pointer(&item)) // #nosec G103 - safe atomic pointer store
			return true
		}

		// Another producer beat us, retry
		runtime.Gosched()
	}
}

// Dequeue removes and returns an item from the queue.
// Returns the item and true if successful, nil and false if empty.
// This method is designed for single-consumer use only.
func (q *Queue) Dequeue() (interface{}, bool) {
	head := q.head.Load()

	// Check if queue is empty
	if head == q.tail.Load() {
		return nil, false
	}

	// Load the item
	item := (*interface{})(atomic.LoadPointer(&q.buffer[head]))
	if item == nil {
		return nil, false
	}

	// Clear the slot and advance head
	atomic.StorePointer(&q.buffer[head], nil)
	q.head.Store((head + 1) & q.mask)

	return *item, true
}

// Size returns the current number of items in the queue.
// This is an approximation in concurrent scenarios.
func (q *Queue) Size() int {
	head := q.head.Load()
	tail := q.tail.Load()

	if tail >= head {
		return int(tail - head)
	}

	return int(q.capacity - head + tail)
}

// IsEmpty returns true if the queue is empty.
// This check is atomic but may be stale in concurrent scenarios.
func (q *Queue) IsEmpty() bool {
	return q.head.Load() == q.tail.Load()
}

// IsFull returns true if the queue is full.
// This check is atomic but may be stale in concurrent scenarios.
func (q *Queue) IsFull() bool {
	head := q.head.Load()
	tail := q.tail.Load()
	return ((tail + 1) & q.mask) == head
}

// MPMCQueue implements a lock-free multi-producer multi-consumer queue
// using sequence numbers for ordering and cache-line padding to avoid false sharing.
type MPMCQueue struct {
	buffer   []slot
	capacity uint64
	mask     uint64

	// Separate enqueue and dequeue indices on different cache lines
	enqueuePos atomic.Uint64
	_padding1  [7]uint64 //nolint:unused

	dequeuePos atomic.Uint64
	_padding2  [7]uint64 //nolint:unused
}

// slot represents a queue slot with sequence number for ordering
type slot struct {
	sequence atomic.Uint64
	data     unsafe.Pointer
}

// NewMPMCQueue creates a new multi-producer multi-consumer queue with the given capacity.
// Capacity will be rounded up to the next power of 2 for efficient masking.
func NewMPMCQueue(capacity int) *MPMCQueue {
	// Round up to next power of 2
	cap := uint64(1)
	for cap < uint64(capacity) {
		cap <<= 1
	}

	q := &MPMCQueue{
		buffer:   make([]slot, cap),
		capacity: cap,
		mask:     cap - 1,
	}

	// Initialize sequence numbers
	for i := uint64(0); i < cap; i++ {
		q.buffer[i].sequence.Store(i)
	}

	return q
}

// Enqueue adds an item to the MPMC queue
func (q *MPMCQueue) Enqueue(item interface{}) bool {
	for {
		pos := q.enqueuePos.Load()
		slot := &q.buffer[pos&q.mask]
		seq := slot.sequence.Load()

		diff := int64(seq) - int64(pos)

		if diff == 0 {
			// Slot is ready for enqueue
			if q.enqueuePos.CompareAndSwap(pos, pos+1) {
				// We own this slot
				atomic.StorePointer(&slot.data, unsafe.Pointer(&item)) // #nosec G103 - safe atomic pointer store
				slot.sequence.Store(pos + 1)
				return true
			}
		} else if diff < 0 {
			// Queue is full
			return false
		}

		// Slot not ready yet, retry
		runtime.Gosched()
	}
}

// Dequeue removes an item from the MPMC queue
func (q *MPMCQueue) Dequeue() (interface{}, bool) {
	for {
		pos := q.dequeuePos.Load()
		slot := &q.buffer[pos&q.mask]
		seq := slot.sequence.Load()

		diff := int64(seq) - int64(pos+1)

		if diff == 0 {
			// Slot is ready for dequeue
			if q.dequeuePos.CompareAndSwap(pos, pos+1) {
				// We own this slot
				data := (*interface{})(atomic.LoadPointer(&slot.data))
				atomic.StorePointer(&slot.data, nil)
				slot.sequence.Store(pos + q.capacity)
				return *data, true
			}
		} else if diff < 0 {
			// Queue is empty
			return nil, false
		}

		// Slot not ready yet, retry
		runtime.Gosched()
	}
}

// AtomicCounter provides a lock-free counter for statistics and metrics collection
// with atomic operations for thread-safe updates.
type AtomicCounter struct {
	value atomic.Uint64
}

// NewAtomicCounter creates a new atomic counter initialized to zero.
func NewAtomicCounter() *AtomicCounter {
	return &AtomicCounter{}
}

// Increment atomically increments the counter by one.
func (c *AtomicCounter) Increment() {
	c.value.Add(1)
}

// Add atomically adds the given delta value to the counter.
func (c *AtomicCounter) Add(delta uint64) {
	c.value.Add(delta)
}

// Get returns the current value of the counter atomically.
func (c *AtomicCounter) Get() uint64 {
	return c.value.Load()
}

// Reset atomically resets the counter to zero.
func (c *AtomicCounter) Reset() {
	c.value.Store(0)
}

// RingBuffer implements a lock-free single-producer single-consumer ring buffer
// for efficient byte stream processing with cache-line padding.
type RingBuffer struct {
	buffer   []byte
	capacity uint64
	mask     uint64

	// Separate read and write positions
	writePos  atomic.Uint64
	_padding1 [7]uint64 //nolint:unused

	readPos   atomic.Uint64
	_padding2 [7]uint64 //nolint:unused
}

// NewRingBuffer creates a new lock-free ring buffer with the given capacity.
// Capacity will be rounded up to the next power of 2 for efficient masking.
func NewRingBuffer(capacity int) *RingBuffer {
	// Round up to next power of 2
	cap := uint64(1)
	for cap < uint64(capacity) {
		cap <<= 1
	}

	return &RingBuffer{
		buffer:   make([]byte, cap),
		capacity: cap,
		mask:     cap - 1,
	}
}

// Write writes data to the ring buffer.
// Returns the number of bytes written, which may be less than len(data) if the buffer is full.
func (rb *RingBuffer) Write(data []byte) int {
	writePos := rb.writePos.Load()
	readPos := rb.readPos.Load()

	// Calculate available space
	available := rb.capacity - (writePos - readPos)
	if available == 0 {
		return 0
	}

	// Write as much as we can
	toWrite := uint64(len(data))
	if toWrite > available {
		toWrite = available
	}

	// Copy data (may wrap around)
	writeIdx := writePos & rb.mask
	if writeIdx+toWrite <= rb.capacity {
		// Single copy
		copy(rb.buffer[writeIdx:], data[:toWrite])
	} else {
		// Two copies (wrap around)
		firstPart := rb.capacity - writeIdx
		copy(rb.buffer[writeIdx:], data[:firstPart])
		copy(rb.buffer[0:], data[firstPart:toWrite])
	}

	// Update write position
	rb.writePos.Store(writePos + toWrite)

	return int(toWrite)
}

// Read reads data from the ring buffer
func (rb *RingBuffer) Read(data []byte) int {
	readPos := rb.readPos.Load()
	writePos := rb.writePos.Load()

	// Calculate available data
	available := writePos - readPos
	if available == 0 {
		return 0
	}

	// Read as much as requested
	toRead := uint64(len(data))
	if toRead > available {
		toRead = available
	}

	// Copy data (may wrap around)
	readIdx := readPos & rb.mask
	if readIdx+toRead <= rb.capacity {
		// Single copy
		copy(data[:toRead], rb.buffer[readIdx:])
	} else {
		// Two copies (wrap around)
		firstPart := rb.capacity - readIdx
		copy(data[:firstPart], rb.buffer[readIdx:])
		copy(data[firstPart:toRead], rb.buffer[0:])
	}

	// Update read position
	rb.readPos.Store(readPos + toRead)

	return int(toRead)
}

// Available returns the number of bytes available to read
func (rb *RingBuffer) Available() int {
	return int(rb.writePos.Load() - rb.readPos.Load())
}

// Free returns the number of bytes available to write
func (rb *RingBuffer) Free() int {
	return int(rb.capacity - (rb.writePos.Load() - rb.readPos.Load()))
}
