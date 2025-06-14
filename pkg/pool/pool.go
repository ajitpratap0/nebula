// Package pool provides unified high-performance object pooling for Nebula
// This is the SINGLE pool implementation that replaces all other pool packages
package pool

import (
	"sync"
	"sync/atomic"
	"time"
)

// Pool represents a generic object pool with type safety
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

// New creates a new typed pool
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

// Get retrieves an object from the pool
func (p *Pool[T]) Get() T {
	atomic.AddInt64(&p.stats.inUse, 1)
	obj := p.pool.Get().(T)
	atomic.AddInt64(&p.stats.hits, 1)
	return obj
}

// Put returns an object to the pool
func (p *Pool[T]) Put(obj T) {
	if p.reset != nil {
		p.reset(obj)
	}
	atomic.AddInt64(&p.stats.inUse, -1)
	p.pool.Put(obj)
}

// Stats returns pool statistics
func (p *Pool[T]) Stats() (allocated, inUse, hits, misses int64) {
	return atomic.LoadInt64(&p.stats.allocated),
		atomic.LoadInt64(&p.stats.inUse),
		atomic.LoadInt64(&p.stats.hits),
		atomic.LoadInt64(&p.stats.misses)
}

// RecordMetadata contains structured metadata (forward declaration for models.Record compatibility)
type RecordMetadata struct {
	Source      string                 `json:"source,omitempty"`
	Table       string                 `json:"table,omitempty"`
	Operation   string                 `json:"operation,omitempty"`
	Offset      int64                  `json:"offset,omitempty"`
	StreamID    string                 `json:"stream_id,omitempty"`
	Timestamp   time.Time              `json:"timestamp"`
	Database    string                 `json:"database,omitempty"`
	Schema      string                 `json:"schema,omitempty"`
	Position    string                 `json:"position,omitempty"`
	Transaction string                 `json:"transaction,omitempty"`
	Before      map[string]interface{} `json:"before,omitempty"`
	Custom      map[string]interface{} `json:"custom,omitempty"`
}

// Record represents the unified record type (compatible with models.Record)
type Record struct {
	ID       string                 `json:"id"`
	Data     map[string]interface{} `json:"data"`
	Metadata RecordMetadata         `json:"metadata"`
	Schema   interface{}            `json:"schema,omitempty"`
	RawData  []byte                 `json:"-"`
}

// Global unified pools for the entire system
var (
	// RecordPool provides optimized pooling for Record objects
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

	// MapPool for map[string]interface{} objects
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

	// StringSlicePool for []string objects
	StringSlicePool = New(
		func() []string {
			return make([]string, 0, 32)
		},
		func(s []string) {
			for i := range s {
				s[i] = ""
			}
			s = s[:0]
		},
	)

	// ByteSlicePool for []byte objects
	ByteSlicePool = New(
		func() []byte {
			return make([]byte, 0, 1024)
		},
		func(b []byte) {
			b = b[:0]
		},
	)

	// IDBufferPool for ID generation
	IDBufferPool = New(
		func() []byte {
			return make([]byte, 0, 64)
		},
		func(b []byte) {
			b = b[:0]
		},
	)

	// BatchSlicePool for []*Record objects (used in pipeline batching)
	BatchSlicePool = New(
		func() []*Record {
			return make([]*Record, 0, 1000) // Default batch size
		},
		func(s []*Record) {
			// Clear references to allow GC
			for i := range s {
				s[i] = nil
			}
			s = s[:0]
		},
	)
)

// Global ID counter
var idCounter uint64

// Unified pool access functions

// GetRecord gets a record from the global pool
func GetRecord() *Record {
	r := RecordPool.Get()
	r.Metadata.Timestamp = time.Now()
	if r.Metadata.Custom == nil {
		r.Metadata.Custom = GetMap()
	}
	return r
}

// PutRecord returns a record to the global pool
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

// GetMap gets a map from the global pool
func GetMap() map[string]interface{} {
	return MapPool.Get()
}

// PutMap returns a map to the global pool
func PutMap(m map[string]interface{}) {
	if m != nil {
		MapPool.Put(m)
	}
}

// GetStringSlice gets a string slice from the global pool
func GetStringSlice() []string {
	return StringSlicePool.Get()
}

// PutStringSlice returns a string slice to the global pool
func PutStringSlice(s []string) {
	if s != nil {
		StringSlicePool.Put(s)
	}
}

// GetByteSlice gets a byte slice from the global pool
func GetByteSlice() []byte {
	return ByteSlicePool.Get()
}

// PutByteSlice returns a byte slice to the global pool
func PutByteSlice(b []byte) {
	if b != nil {
		ByteSlicePool.Put(b)
	}
}

// GetBatchSlice gets a batch slice from the global pool with specified capacity
func GetBatchSlice(capacity int) []*Record {
	batch := BatchSlicePool.Get()
	// If we need more capacity than the pooled slice, grow it
	if cap(batch) < capacity {
		batch = make([]*Record, 0, capacity)
	}
	return batch[:0] // Always return with length 0
}

// PutBatchSlice returns a batch slice to the global pool
func PutBatchSlice(batch []*Record) {
	if batch != nil {
		BatchSlicePool.Put(batch)
	}
}

// GenerateID generates a unique ID using pooled buffers
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

// BufferPool manages byte buffer pooling with size-based buckets
type BufferPool struct {
	pools []*Pool[[]byte]
	sizes []int
}

// NewBufferPool creates a new buffer pool with predefined sizes
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
				b = b[:0]
			},
		)
	}

	return &BufferPool{
		pools: pools,
		sizes: sizes,
	}
}

// Get returns a buffer of at least the requested size
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

// Put returns a buffer to the pool
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

// ArenaPool provides arena-style allocation
type ArenaPool struct {
	mu        sync.Mutex
	arenas    []*Arena
	chunkSize int
	maxArenas int
}

// Arena represents a memory arena
type Arena struct {
	data   []byte
	offset int
}

// NewArenaPool creates a new arena pool
func NewArenaPool(chunkSize, maxArenas int) *ArenaPool {
	return &ArenaPool{
		chunkSize: chunkSize,
		maxArenas: maxArenas,
		arenas:    make([]*Arena, 0, maxArenas),
	}
}

// Alloc allocates memory from the arena
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

// Reset resets all arenas
func (p *ArenaPool) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, arena := range p.arenas {
		arena.offset = 0
	}
}

// Record methods for unified record management

// SetData efficiently sets a data field
func (r *Record) SetData(key string, value interface{}) {
	if r.Data == nil {
		r.Data = GetMap()
	}
	r.Data[key] = value
}

// GetData efficiently gets a data field
func (r *Record) GetData(key string) (interface{}, bool) {
	if r.Data == nil {
		return nil, false
	}
	val, ok := r.Data[key]
	return val, ok
}

// SetMetadata efficiently sets a custom metadata field
func (r *Record) SetMetadata(key string, value interface{}) {
	if r.Metadata.Custom == nil {
		r.Metadata.Custom = GetMap()
	}
	r.Metadata.Custom[key] = value
}

// GetMetadata efficiently gets a custom metadata field
func (r *Record) GetMetadata(key string) (interface{}, bool) {
	if r.Metadata.Custom == nil {
		return nil, false
	}
	val, ok := r.Metadata.Custom[key]
	return val, ok
}

// Release returns the record and its resources to the pool
func (r *Record) Release() {
	PutRecord(r)
}

// SetTimestamp sets the timestamp
func (r *Record) SetTimestamp(t time.Time) {
	r.Metadata.Timestamp = t
}

// GetTimestamp returns the timestamp
func (r *Record) GetTimestamp() time.Time {
	return r.Metadata.Timestamp
}

// CDC-specific convenience methods

// SetCDCOperation sets the CDC operation type (INSERT, UPDATE, DELETE)
func (r *Record) SetCDCOperation(operation string) {
	r.Metadata.Operation = operation
}

// GetCDCOperation returns the CDC operation type
func (r *Record) GetCDCOperation() string {
	return r.Metadata.Operation
}

// SetCDCTable sets the table name for CDC records
func (r *Record) SetCDCTable(table string) {
	r.Metadata.Table = table
}

// GetCDCTable returns the table name for CDC records
func (r *Record) GetCDCTable() string {
	return r.Metadata.Table
}

// SetCDCDatabase sets the database name for CDC records
func (r *Record) SetCDCDatabase(database string) {
	r.Metadata.Database = database
}

// GetCDCDatabase returns the database name for CDC records
func (r *Record) GetCDCDatabase() string {
	return r.Metadata.Database
}

// SetCDCBefore sets the before state for UPDATE/DELETE operations
func (r *Record) SetCDCBefore(before map[string]interface{}) {
	if r.Metadata.Before == nil {
		r.Metadata.Before = GetMap()
	}
	for k, v := range before {
		r.Metadata.Before[k] = v
	}
}

// GetCDCBefore returns the before state for UPDATE/DELETE operations
func (r *Record) GetCDCBefore() map[string]interface{} {
	return r.Metadata.Before
}

// SetCDCPosition sets the replication position
func (r *Record) SetCDCPosition(position string) {
	r.Metadata.Position = position
}

// GetCDCPosition returns the replication position
func (r *Record) GetCDCPosition() string {
	return r.Metadata.Position
}

// SetCDCTransaction sets the transaction ID
func (r *Record) SetCDCTransaction(txID string) {
	r.Metadata.Transaction = txID
}

// GetCDCTransaction returns the transaction ID
func (r *Record) GetCDCTransaction() string {
	return r.Metadata.Transaction
}

// Streaming-specific convenience methods

// SetStreamID sets the stream identifier
func (r *Record) SetStreamID(streamID string) {
	r.Metadata.StreamID = streamID
}

// GetStreamID returns the stream identifier
func (r *Record) GetStreamID() string {
	return r.Metadata.StreamID
}

// SetOffset sets the stream offset
func (r *Record) SetOffset(offset int64) {
	r.Metadata.Offset = offset
}

// GetOffset returns the stream offset
func (r *Record) GetOffset() int64 {
	return r.Metadata.Offset
}

// Type checking convenience methods

// IsCDCRecord returns true if this is a CDC record
func (r *Record) IsCDCRecord() bool {
	return r.Metadata.Operation != "" || r.Metadata.Table != ""
}

// IsStreamingRecord returns true if this is a streaming record
func (r *Record) IsStreamingRecord() bool {
	return r.Metadata.StreamID != "" || r.Metadata.Offset > 0
}

// IsInsert returns true if this is a CDC INSERT operation
func (r *Record) IsInsert() bool {
	return r.Metadata.Operation == "INSERT"
}

// IsUpdate returns true if this is a CDC UPDATE operation
func (r *Record) IsUpdate() bool {
	return r.Metadata.Operation == "UPDATE"
}

// IsDelete returns true if this is a CDC DELETE operation
func (r *Record) IsDelete() bool {
	return r.Metadata.Operation == "DELETE"
}

// Constructor functions

// NewRecord creates a new record with the given source and data
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

// NewRecordFromPool creates a new record using pooled resources
func NewRecordFromPool(source string) *Record {
	r := GetRecord()
	r.ID = GenerateID("rec")
	r.Data = GetMap()
	r.Metadata.Source = source
	r.Metadata.Timestamp = time.Now()
	r.Metadata.Custom = GetMap()
	return r
}

// NewCDCRecord creates a new record for Change Data Capture
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

// NewStreamingRecord creates a new record for streaming
func NewStreamingRecord(streamID string, offset int64, data map[string]interface{}) *Record {
	r := NewRecord("streaming", data)
	r.Metadata.StreamID = streamID
	r.Metadata.Offset = offset
	return r
}

// Global pools for advanced use cases
var (
	// GlobalBufferPool for byte buffer pooling
	GlobalBufferPool = NewBufferPool()

	// GlobalArenaPool for arena allocation
	GlobalArenaPool = NewArenaPool(16*1024*1024, 10) // 16MB chunks, 10 arenas
)

// Stats represents pool statistics
type Stats struct {
	Allocated int64
	InUse     int64
	Hits      int64
	Misses    int64
}

// GetGlobalStats returns statistics for all global pools
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