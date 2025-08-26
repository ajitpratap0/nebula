// Package pool provides unified high-performance object pooling for Nebula.
// This is the SINGLE pool implementation that replaces all other pool packages.
// It offers zero-allocation memory management with automatic object recycling,
// significantly reducing garbage collection pressure and improving performance.
//
// The package provides:
//   - Generic type-safe object pooling with Pool[T]
//   - Pre-configured global pools for common types (Records, Maps, Slices)
//   - Buffer pooling with size-based buckets
//   - Arena allocation for bulk memory management
//   - Comprehensive statistics and monitoring
//
// Example usage:
//
//	// Using the global Record pool
//	record := pool.GetRecord()
//	defer record.Release()
//
//	record.SetData("name", "John")
//	record.SetData("age", 30)
//
//	// Using custom pools
//	myPool := pool.New(
//	    func() *MyType { return &MyType{} },
//	    func(obj *MyType) { obj.Reset() },
//	)
//	obj := myPool.Get()
//	defer myPool.Put(obj)
package pool

import (
	"sync"
	"sync/atomic"
	"time"
)

// Pool represents a generic object pool with type safety.
// It wraps sync.Pool with additional features like statistics tracking
// and automatic reset functionality. The pool is safe for concurrent use.
//
// Type parameter T can be any type, but pointer types are recommended
// for efficiency. The pool maintains statistics on allocations, usage,
// and hit/miss rates for monitoring and optimization.
type Pool[T any] struct {
	pool  sync.Pool
	new   func() T
	reset func(T)
	stats struct {
		allocated int64
		inUse     int64
		hits      int64
		misses    int64
	}
}

// New creates a new typed pool with custom allocation and reset functions.
// The new function is called when the pool is empty and a new object is needed.
// The reset function is called before returning an object to the pool, allowing
// for efficient cleanup and reuse.
//
// Parameters:
//   - new: Factory function to create new instances of type T
//   - reset: Optional cleanup function called before returning objects to pool
//
// Example:
//
//	pool := New(
//	    func() *Buffer { return &Buffer{data: make([]byte, 0, 1024)} },
//	    func(b *Buffer) { b.data = b.data[:0] },
//	)
func New[T any](new func() T, reset func(T)) *Pool[T] {
	p := &Pool[T]{
		new:   new,
		reset: reset,
	}
	p.pool.New = func() interface{} {
		atomic.AddInt64(&p.stats.allocated, 1)
		return new()
	}
	return p
}

// Get retrieves an object from the pool. If the pool is empty, it creates
// a new object using the factory function provided in New. The method is
// safe for concurrent use and updates pool statistics.
//
// The returned object should be returned to the pool using Put when no
// longer needed to enable reuse and reduce allocations.
func (p *Pool[T]) Get() T {
	atomic.AddInt64(&p.stats.inUse, 1)
	obj := p.pool.Get().(T)
	atomic.AddInt64(&p.stats.hits, 1)
	return obj
}

// Put returns an object to the pool for reuse. If a reset function was
// provided during pool creation, it is called to clean up the object
// before returning it to the pool. The method is safe for concurrent use.
//
// It is safe to call Put with the zero value of T, but it's more efficient
// to avoid putting nil pointers into the pool.
func (p *Pool[T]) Put(obj T) {
	if p.reset != nil {
		p.reset(obj)
	}
	atomic.AddInt64(&p.stats.inUse, -1)
	p.pool.Put(obj)
}

// Stats returns current pool statistics including allocation count,
// objects currently in use, cache hits, and cache misses. These metrics
// are useful for monitoring pool efficiency and tuning performance.
//
// Returns:
//   - allocated: Total number of objects created by the pool
//   - inUse: Number of objects currently checked out from the pool
//   - hits: Number of successful Get operations
//   - misses: Number of times a new object had to be created
func (p *Pool[T]) Stats() (allocated, inUse, hits, misses int64) {
	return atomic.LoadInt64(&p.stats.allocated),
		atomic.LoadInt64(&p.stats.inUse),
		atomic.LoadInt64(&p.stats.hits),
		atomic.LoadInt64(&p.stats.misses)
}

// RecordMetadata contains structured metadata for records, supporting various
// data integration patterns including CDC (Change Data Capture), streaming,
// and batch processing. All fields are optional to support different use cases.
type RecordMetadata struct {
	// Source identifies the origin system or connector
	Source string `json:"source,omitempty"`
	// Table name for database sources
	Table string `json:"table,omitempty"`
	// Operation type for CDC records (INSERT, UPDATE, DELETE)
	Operation string `json:"operation,omitempty"`
	// Offset position for streaming sources
	Offset int64 `json:"offset,omitempty"`
	// StreamID identifies the stream for multi-stream sources
	StreamID string `json:"stream_id,omitempty"`
	// Timestamp when the record was created or captured
	Timestamp time.Time `json:"timestamp"`
	// Database name for database sources
	Database string `json:"database,omitempty"`
	// Schema name for database sources
	Schema string `json:"schema,omitempty"`
	// Position in the replication log for CDC sources
	Position string `json:"position,omitempty"`
	// Transaction ID for transactional sources
	Transaction string `json:"transaction,omitempty"`
	// Before state for UPDATE/DELETE operations in CDC
	Before map[string]interface{} `json:"before,omitempty"`
	// Custom metadata fields for extensibility
	Custom map[string]interface{} `json:"custom,omitempty"`
}

// Record represents the unified record type used throughout Nebula for data processing.
// It provides a consistent interface for all data sources and destinations, supporting
// various integration patterns including batch, streaming, and CDC. Records are designed
// to be pooled for maximum performance.
//
// The Record type is compatible with models.Record and should be obtained from the
// global pool using GetRecord() rather than created directly.
type Record struct {
	// ID is a unique identifier for the record
	ID string `json:"id"`
	// Data contains the actual record payload
	Data map[string]interface{} `json:"data"`
	// Metadata contains source, timing, and processing information
	Metadata RecordMetadata `json:"metadata"`
	// Schema optionally describes the structure of the data
	Schema interface{} `json:"schema,omitempty"`
	// RawData stores the original raw bytes if needed (not serialized)
	RawData []byte `json:"-"`
}

// Global unified pools for the entire system.
// These pre-configured pools provide optimized object recycling for common types
// used throughout Nebula, significantly reducing memory allocations and GC pressure.
var (
	// RecordPool provides optimized pooling for Record objects.
	// Records are pre-allocated with a 16-capacity map for data fields.
	// The reset function ensures all fields are cleared before reuse.
	RecordPool = New(
		func() *Record {
			return &Record{
				Data: make(map[string]interface{}, 16),
			}
		},
		func(r *Record) {
			r.ID = ""
			r.Schema = nil
			r.RawData = nil
			// Clear maps
			for k := range r.Data {
				delete(r.Data, k)
			}
			if r.Metadata.Custom != nil {
				for k := range r.Metadata.Custom {
					delete(r.Metadata.Custom, k)
				}
			}
			if r.Metadata.Before != nil {
				for k := range r.Metadata.Before {
					delete(r.Metadata.Before, k)
				}
			}
			// Reset metadata struct
			r.Metadata = RecordMetadata{}
		},
	)

	// MapPool provides pooling for map[string]interface{} objects.
	// Maps are pre-allocated with capacity 16 and cleared on return.
	MapPool = New(
		func() map[string]interface{} {
			return make(map[string]interface{}, 16)
		},
		func(m map[string]interface{}) {
			for k := range m {
				delete(m, k)
			}
		},
	)

	// StringSlicePool provides pooling for []string slices.
	// Slices are pre-allocated with capacity 32 and reset to zero length on return.
	StringSlicePool = New(
		func() []string {
			return make([]string, 0, 32)
		},
		func(s []string) {
			for i := range s {
				s[i] = ""
			}
			// Reset slice length (assignment not needed)
		},
	)

	// ByteSlicePool provides pooling for general-purpose byte slices.
	// Slices are pre-allocated with 1KB capacity and reset to zero length on return.
	ByteSlicePool = New(
		func() []byte {
			return make([]byte, 0, 1024)
		},
		func(b []byte) {
			// Reset slice length (assignment not needed)
		},
	)

	// IDBufferPool provides pooling for ID generation buffers.
	// Buffers are pre-allocated with 64 byte capacity for efficient ID creation.
	IDBufferPool = New(
		func() []byte {
			return make([]byte, 0, 64)
		},
		func(b []byte) {
			// Reset slice length (assignment not needed)
		},
	)

	// BatchSlicePool provides pooling for record batches used in pipeline processing.
	// Batches are pre-allocated with 1000 record capacity and cleared on return.
	BatchSlicePool = New(
		func() []*Record {
			return make([]*Record, 0, 1000) // Default batch size
		},
		func(s []*Record) {
			// Clear references to allow GC
			for i := range s {
				s[i] = nil
			}
			// Reset slice length (assignment not needed)
		},
	)
)

// idCounter provides atomic unique ID generation
var idCounter uint64

// Unified pool access functions provide convenient access to the global pools.
// These functions handle proper initialization and cleanup of pooled objects.

// GetRecord retrieves a Record from the global pool with automatic initialization.
// The returned record has a fresh timestamp and an initialized Custom metadata map.
// Records must be returned to the pool using PutRecord or record.Release() when done.
//
// Example:
//
//	record := pool.GetRecord()
//	defer record.Release()
//
//	record.SetData("key", "value")
//	// Use record...
func GetRecord() *Record {
	r := RecordPool.Get()
	r.Metadata.Timestamp = time.Now()
	if r.Metadata.Custom == nil {
		r.Metadata.Custom = GetMap()
	}
	return r
}

// PutRecord returns a Record to the global pool for reuse.
// It properly cleans up nested maps by returning them to their respective pools.
// This function is safe to call with nil records.
//
// Note: Prefer using record.Release() for cleaner code.
func PutRecord(record *Record) {
	if record != nil {
		// Return nested maps to their pools
		if record.Metadata.Before != nil {
			PutMap(record.Metadata.Before)
			record.Metadata.Before = nil
		}
		if record.Metadata.Custom != nil {
			PutMap(record.Metadata.Custom)
			record.Metadata.Custom = nil
		}
		RecordPool.Put(record)
	}
}

// GetMap retrieves a map[string]interface{} from the global pool.
// The returned map is empty and ready for use with capacity 16.
func GetMap() map[string]interface{} {
	return MapPool.Get()
}

// PutMap returns a map to the global pool for reuse.
// The map is automatically cleared before being pooled.
// This function is safe to call with nil maps.
func PutMap(m map[string]interface{}) {
	if m != nil {
		MapPool.Put(m)
	}
}

// GetStringSlice retrieves a string slice from the global pool.
// The returned slice has zero length and capacity 32.
func GetStringSlice() []string {
	return StringSlicePool.Get()
}

// PutStringSlice returns a string slice to the global pool for reuse.
// The slice is automatically cleared before being pooled.
// This function is safe to call with nil slices.
func PutStringSlice(s []string) {
	if s != nil {
		StringSlicePool.Put(s)
	}
}

// GetByteSlice retrieves a byte slice from the global pool.
// The returned slice has zero length and capacity 1024.
func GetByteSlice() []byte {
	return ByteSlicePool.Get()
}

// PutByteSlice returns a byte slice to the global pool for reuse.
// The slice is automatically reset to zero length before being pooled.
// This function is safe to call with nil slices.
func PutByteSlice(b []byte) {
	if b != nil {
		ByteSlicePool.Put(b)
	}
}

// GetBatchSlice retrieves a record batch slice from the global pool.
// If the requested capacity exceeds the pooled slice capacity, a new slice
// is allocated. The returned slice always has zero length.
//
// Parameters:
//   - capacity: Desired capacity for the batch slice
//
// Example:
//
//	batch := pool.GetBatchSlice(5000)
//	defer pool.PutBatchSlice(batch)
//
//	for _, record := range records {
//	    batch = append(batch, record)
//	}
func GetBatchSlice(capacity int) []*Record {
	batch := BatchSlicePool.Get()
	// If we need more capacity than the pooled slice, grow it
	if cap(batch) < capacity {
		batch = make([]*Record, 0, capacity)
	}
	return batch[:0] // Always return with length 0
}

// PutBatchSlice returns a batch slice to the global pool for reuse.
// All record references are cleared to allow garbage collection.
// This function is safe to call with nil slices.
func PutBatchSlice(batch []*Record) {
	if batch != nil {
		BatchSlicePool.Put(batch)
	}
}

// GenerateID generates a unique ID with the specified prefix using pooled buffers.
// The ID format is "prefix-number" where number is an atomic counter.
// This function is safe for concurrent use.
//
// Parameters:
//   - prefix: String prefix for the ID (e.g., "rec", "batch", "job")
//
// Example:
//
//	id := pool.GenerateID("rec")  // Returns "rec-1", "rec-2", etc.
func GenerateID(prefix string) string {
	buf := IDBufferPool.Get()
	defer IDBufferPool.Put(buf)

	// Use atomic counter for uniqueness
	id := atomic.AddUint64(&idCounter, 1)

	// Build ID efficiently
	buf = append(buf, prefix...)
	buf = append(buf, '-')
	buf = appendUint64(buf, id)

	return string(buf)
}

// appendUint64 efficiently appends uint64 to byte slice
func appendUint64(buf []byte, n uint64) []byte {
	if n == 0 {
		return append(buf, '0')
	}

	// Calculate digits
	temp := n
	digits := 0
	for temp > 0 {
		temp /= 10
		digits++
	}

	// Extend buffer
	start := len(buf)
	buf = buf[:start+digits]

	// Fill digits from right to left
	for i := digits - 1; i >= 0; i-- {
		buf[start+i] = byte('0' + n%10)
		n /= 10
	}

	return buf
}

// BufferPool manages byte buffer pooling with size-based buckets.
// It maintains multiple pools for different buffer sizes, automatically
// selecting the appropriate pool based on requested size. This reduces
// memory fragmentation and improves allocation performance for I/O operations.
type BufferPool struct {
	pools []*Pool[[]byte]
	sizes []int
}

// NewBufferPool creates a new buffer pool with predefined size buckets.
// The pool uses power-of-2 sizes from 512 bytes to 16MB, covering most
// common buffer requirements. Buffers larger than 16MB are allocated
// directly without pooling.
//
// The predefined sizes are:
//   - 512B, 1KB, 4KB, 16KB, 64KB, 256KB, 1MB, 4MB, 16MB
func NewBufferPool() *BufferPool {
	// Common buffer sizes (powers of 2)
	sizes := []int{
		512,      // 512B
		1024,     // 1KB
		4096,     // 4KB
		16384,    // 16KB
		65536,    // 64KB
		262144,   // 256KB
		1048576,  // 1MB
		4194304,  // 4MB
		16777216, // 16MB
	}

	pools := make([]*Pool[[]byte], len(sizes))
	for i, size := range sizes {
		size := size // capture loop variable
		pools[i] = New(
			func() []byte {
				return make([]byte, size)
			},
			func(b []byte) {
				// Reset slice length (assignment not needed)
			},
		)
	}

	return &BufferPool{
		pools: pools,
		sizes: sizes,
	}
}

// Get returns a buffer of at least the requested size from the pool.
// It selects the smallest available buffer that can accommodate the request.
// For sizes larger than 16MB, a new buffer is allocated directly.
//
// The returned buffer's length is set to the requested size, but its
// capacity may be larger.
//
// Example:
//
//	buf := bufferPool.Get(2048)  // Returns a 4KB buffer with length 2048
//	defer bufferPool.Put(buf)
func (p *BufferPool) Get(size int) []byte {
	// Find the smallest buffer that fits
	for i, s := range p.sizes {
		if s >= size {
			buf := p.pools[i].Get()
			return buf[:size]
		}
	}

	// Fallback to allocation for very large buffers
	return make([]byte, size)
}

// Put returns a buffer to the pool for reuse.
// The buffer is matched to its appropriate size pool based on capacity.
// Buffers that don't match any pool size are released to garbage collection.
//
// The buffer's content is not cleared; it will be reset to zero length
// when retrieved from the pool.
func (p *BufferPool) Put(buf []byte) {
	size := cap(buf)

	// Find the matching pool
	for i, s := range p.sizes {
		if s == size {
			p.pools[i].Put(buf)
			return
		}
	}

	// Buffer doesn't match any pool size, let GC handle it
}

// ArenaPool provides arena-style allocation for bulk memory management.
// It pre-allocates large chunks of memory and serves smaller allocations
// from these chunks, reducing the number of system allocations. This is
// particularly efficient for scenarios with many small, short-lived allocations.
type ArenaPool struct {
	mu        sync.Mutex
	arenas    []*Arena
	chunkSize int
	maxArenas int
}

// Arena represents a memory arena - a large pre-allocated chunk of memory
// from which smaller allocations are served.
type Arena struct {
	data   []byte
	offset int
}

// NewArenaPool creates a new arena pool with specified chunk size and maximum arenas.
// Each arena allocates chunkSize bytes, and up to maxArenas can be created.
// When all arenas are full, allocations fall back to direct allocation.
//
// Parameters:
//   - chunkSize: Size of each arena chunk in bytes
//   - maxArenas: Maximum number of arenas to maintain
//
// Example:
//
//	// Create pool with 16MB chunks, max 10 arenas (160MB total)
//	arenaPool := NewArenaPool(16*1024*1024, 10)
func NewArenaPool(chunkSize, maxArenas int) *ArenaPool {
	return &ArenaPool{
		chunkSize: chunkSize,
		maxArenas: maxArenas,
		arenas:    make([]*Arena, 0, maxArenas),
	}
}

// Alloc allocates memory from the arena pool.
// It finds the first arena with sufficient space or creates a new arena
// if needed. Allocations larger than chunkSize are allocated directly.
// The method is thread-safe.
//
// Parameters:
//   - size: Number of bytes to allocate
//
// Returns:
//   - Byte slice of the requested size
//
// Note: Memory allocated from arenas cannot be individually freed;
// use Reset() to reclaim all arena memory at once.
func (p *ArenaPool) Alloc(size int) []byte {
	if size > p.chunkSize {
		// Too large for arena, allocate directly
		return make([]byte, size)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Find arena with enough space
	for _, arena := range p.arenas {
		if arena.offset+size <= len(arena.data) {
			start := arena.offset
			arena.offset += size
			return arena.data[start:arena.offset]
		}
	}

	// Need new arena
	if len(p.arenas) < p.maxArenas {
		arena := &Arena{
			data:   make([]byte, p.chunkSize),
			offset: 0,
		}
		p.arenas = append(p.arenas, arena)
		arena.offset = size
		return arena.data[0:size]
	}

	// All arenas full, allocate directly
	return make([]byte, size)
}

// Reset resets all arenas, making all previously allocated memory available again.
// This is an efficient way to reclaim memory when all allocations from the pool
// are no longer needed. The method is thread-safe.
//
// Warning: After calling Reset, all previously allocated byte slices from
// this pool should no longer be used.
func (p *ArenaPool) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, arena := range p.arenas {
		arena.offset = 0
	}
}

// Record methods for unified record management

// SetData sets a data field in the record, automatically initializing
// the data map if needed. This method uses pooled maps for efficiency.
//
// Parameters:
//   - key: Field name
//   - value: Field value (any type)
func (r *Record) SetData(key string, value interface{}) {
	if r.Data == nil {
		r.Data = GetMap()
	}
	r.Data[key] = value
}

// GetData retrieves a data field from the record.
//
// Parameters:
//   - key: Field name to retrieve
//
// Returns:
//   - value: The field value if present
//   - ok: true if the field exists, false otherwise
func (r *Record) GetData(key string) (interface{}, bool) {
	if r.Data == nil {
		return nil, false
	}
	val, ok := r.Data[key]
	return val, ok
}

// SetMetadata sets a custom metadata field, automatically initializing
// the metadata map if needed. Custom metadata allows extending records
// with connector-specific or processing-specific information.
//
// Parameters:
//   - key: Metadata field name
//   - value: Metadata value (any type)
func (r *Record) SetMetadata(key string, value interface{}) {
	if r.Metadata.Custom == nil {
		r.Metadata.Custom = GetMap()
	}
	r.Metadata.Custom[key] = value
}

// GetMetadata retrieves a custom metadata field from the record.
//
// Parameters:
//   - key: Metadata field name to retrieve
//
// Returns:
//   - value: The metadata value if present
//   - ok: true if the field exists, false otherwise
func (r *Record) GetMetadata(key string) (interface{}, bool) {
	if r.Metadata.Custom == nil {
		return nil, false
	}
	val, ok := r.Metadata.Custom[key]
	return val, ok
}

// Release returns the record and all its resources to the appropriate pools.
// This method should be called when the record is no longer needed, typically
// using defer immediately after obtaining the record.
//
// Example:
//
//	record := pool.GetRecord()
//	defer record.Release()
func (r *Record) Release() {
	PutRecord(r)
}

// SetTimestamp sets the record's timestamp.
// This is typically set automatically when the record is created.
func (r *Record) SetTimestamp(t time.Time) {
	r.Metadata.Timestamp = t
}

// GetTimestamp returns the record's timestamp.
func (r *Record) GetTimestamp() time.Time {
	return r.Metadata.Timestamp
}

// CDC-specific convenience methods provide easy access to Change Data Capture fields

// SetCDCOperation sets the CDC operation type.
// Valid values are typically "INSERT", "UPDATE", "DELETE".
func (r *Record) SetCDCOperation(operation string) {
	r.Metadata.Operation = operation
}

// GetCDCOperation returns the CDC operation type.
// Returns empty string if not a CDC record.
func (r *Record) GetCDCOperation() string {
	return r.Metadata.Operation
}

// SetCDCTable sets the table name for CDC records.
func (r *Record) SetCDCTable(table string) {
	r.Metadata.Table = table
}

// GetCDCTable returns the table name for CDC records.
func (r *Record) GetCDCTable() string {
	return r.Metadata.Table
}

// SetCDCDatabase sets the database name for CDC records.
func (r *Record) SetCDCDatabase(database string) {
	r.Metadata.Database = database
}

// GetCDCDatabase returns the database name for CDC records.
func (r *Record) GetCDCDatabase() string {
	return r.Metadata.Database
}

// SetCDCBefore sets the before state for UPDATE/DELETE operations.
// The before state represents the record values before the change.
// This method copies values from the provided map to avoid external mutations.
func (r *Record) SetCDCBefore(before map[string]interface{}) {
	if r.Metadata.Before == nil {
		r.Metadata.Before = GetMap()
	}
	for k, v := range before {
		r.Metadata.Before[k] = v
	}
}

// GetCDCBefore returns the before state for UPDATE/DELETE operations.
// Returns nil if no before state is set.
func (r *Record) GetCDCBefore() map[string]interface{} {
	return r.Metadata.Before
}

// SetCDCPosition sets the replication position for CDC sources.
// The position format is source-specific (e.g., MySQL binlog position, PostgreSQL LSN).
func (r *Record) SetCDCPosition(position string) {
	r.Metadata.Position = position
}

// GetCDCPosition returns the replication position.
func (r *Record) GetCDCPosition() string {
	return r.Metadata.Position
}

// SetCDCTransaction sets the transaction ID for transactional CDC sources.
func (r *Record) SetCDCTransaction(txID string) {
	r.Metadata.Transaction = txID
}

// GetCDCTransaction returns the transaction ID.
func (r *Record) GetCDCTransaction() string {
	return r.Metadata.Transaction
}

// Streaming-specific convenience methods provide easy access to streaming fields

// SetStreamID sets the stream identifier for multi-stream sources.
func (r *Record) SetStreamID(streamID string) {
	r.Metadata.StreamID = streamID
}

// GetStreamID returns the stream identifier.
func (r *Record) GetStreamID() string {
	return r.Metadata.StreamID
}

// SetOffset sets the stream offset position.
// The offset represents the record's position in the stream.
func (r *Record) SetOffset(offset int64) {
	r.Metadata.Offset = offset
}

// GetOffset returns the stream offset position.
func (r *Record) GetOffset() int64 {
	return r.Metadata.Offset
}

// Type checking convenience methods help identify record types

// IsCDCRecord returns true if this is a Change Data Capture record.
// A record is considered a CDC record if it has an operation type or table name.
func (r *Record) IsCDCRecord() bool {
	return r.Metadata.Operation != "" || r.Metadata.Table != ""
}

// IsStreamingRecord returns true if this is a streaming record.
// A record is considered a streaming record if it has a stream ID or offset.
func (r *Record) IsStreamingRecord() bool {
	return r.Metadata.StreamID != "" || r.Metadata.Offset > 0
}

// IsInsert returns true if this is a CDC INSERT operation.
func (r *Record) IsInsert() bool {
	return r.Metadata.Operation == "INSERT"
}

// IsUpdate returns true if this is a CDC UPDATE operation.
func (r *Record) IsUpdate() bool {
	return r.Metadata.Operation == "UPDATE"
}

// IsDelete returns true if this is a CDC DELETE operation.
func (r *Record) IsDelete() bool {
	return r.Metadata.Operation == "DELETE"
}

// Constructor functions create new records with proper initialization

// NewRecord creates a new record with the given source and data.
// The record is obtained from the pool and initialized with a unique ID
// and current timestamp. The provided data map is used directly.
//
// Parameters:
//   - source: Identifies the data source
//   - data: The record's data fields
//
// Note: The caller should call record.Release() when done.
func NewRecord(source string, data map[string]interface{}) *Record {
	r := GetRecord()
	r.ID = GenerateID("rec")
	r.Data = data
	r.Metadata.Source = source
	r.Metadata.Timestamp = time.Now()
	if r.Metadata.Custom == nil {
		r.Metadata.Custom = GetMap()
	}
	return r
}

// NewRecordFromPool creates a new record using entirely pooled resources.
// Unlike NewRecord, this creates a new pooled map for data fields.
// This is the most efficient way to create records when building data incrementally.
//
// Parameters:
//   - source: Identifies the data source
//
// Note: The caller should call record.Release() when done.
func NewRecordFromPool(source string) *Record {
	r := GetRecord()
	r.ID = GenerateID("rec")
	r.Data = GetMap()
	r.Metadata.Source = source
	r.Metadata.Timestamp = time.Now()
	r.Metadata.Custom = GetMap()
	return r
}

// NewCDCRecord creates a new record specifically for Change Data Capture.
// It automatically sets the source to "cdc" and populates CDC-specific metadata.
//
// Parameters:
//   - database: The database name
//   - table: The table name
//   - operation: The CDC operation (INSERT, UPDATE, DELETE)
//   - before: The record state before the change (nil for INSERT)
//   - after: The record state after the change (nil for DELETE)
//
// Note: The caller should call record.Release() when done.
func NewCDCRecord(database, table, operation string, before, after map[string]interface{}) *Record {
	r := GetRecord()
	r.Metadata.Source = "cdc"
	r.Metadata.Database = database
	r.Metadata.Table = table
	r.Metadata.Operation = operation
	r.Metadata.Before = before
	r.Data = after
	return r
}

// NewStreamingRecord creates a new record for streaming data sources.
// It extends a regular record with streaming-specific metadata.
//
// Parameters:
//   - streamID: Identifier for the stream
//   - offset: Position in the stream
//   - data: The record's data fields
//
// Note: The caller should call record.Release() when done.
func NewStreamingRecord(streamID string, offset int64, data map[string]interface{}) *Record {
	r := NewRecord("streaming", data)
	r.Metadata.StreamID = streamID
	r.Metadata.Offset = offset
	return r
}

// Global pools for advanced use cases provide shared resources across the application

var (
	// GlobalBufferPool provides size-based byte buffer pooling for I/O operations.
	// It manages buffers from 512B to 16MB with automatic size selection.
	GlobalBufferPool = NewBufferPool()

	// GlobalArenaPool provides arena-style allocation for bulk memory operations.
	// Configured with 16MB chunks and maximum 10 arenas (160MB total).
	GlobalArenaPool = NewArenaPool(16*1024*1024, 10) // 16MB chunks, 10 arenas
)

// Stats represents pool statistics for monitoring and optimization.
type Stats struct {
	// Allocated is the total number of objects created by the pool
	Allocated int64
	// InUse is the current number of objects checked out from the pool
	InUse int64
	// Hits is the number of successful pool retrievals
	Hits int64
	// Misses is the number of times a new object had to be created
	Misses int64
}

// GetGlobalStats returns comprehensive statistics for all global pools.
// This is useful for monitoring pool efficiency and detecting memory leaks.
//
// The returned map contains stats for:
//   - "record": Record object pool
//   - "map": Generic map pool
//   - "string_slice": String slice pool
//   - "byte_slice": Byte slice pool
//
// Example:
//
//	stats := pool.GetGlobalStats()
//	for name, stat := range stats {
//	    fmt.Printf("%s pool: %d in use, %.2f%% hit rate\n",
//	        name, stat.InUse, float64(stat.Hits)/float64(stat.Hits+stat.Misses)*100)
//	}
func GetGlobalStats() map[string]Stats {
	recordAlloc, recordInUse, recordHits, recordMisses := RecordPool.Stats()
	mapAlloc, mapInUse, mapHits, mapMisses := MapPool.Stats()
	stringAlloc, stringInUse, stringHits, stringMisses := StringSlicePool.Stats()
	byteAlloc, byteInUse, byteHits, byteMisses := ByteSlicePool.Stats()

	return map[string]Stats{
		"record": {
			Allocated: recordAlloc,
			InUse:     recordInUse,
			Hits:      recordHits,
			Misses:    recordMisses,
		},
		"map": {
			Allocated: mapAlloc,
			InUse:     mapInUse,
			Hits:      mapHits,
			Misses:    mapMisses,
		},
		"string_slice": {
			Allocated: stringAlloc,
			InUse:     stringInUse,
			Hits:      stringHits,
			Misses:    stringMisses,
		},
		"byte_slice": {
			Allocated: byteAlloc,
			InUse:     byteInUse,
			Hits:      byteHits,
			Misses:    byteMisses,
		},
	}
}
