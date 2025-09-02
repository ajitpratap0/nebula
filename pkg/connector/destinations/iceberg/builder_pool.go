package iceberg

import (
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"
)

// ArrowBuilderPool manages a pool of Arrow RecordBuilders to eliminate allocation overhead
type ArrowBuilderPool struct {
	pool        sync.Pool
	schemaCache map[string]*arrow.Schema
	schemaMutex sync.RWMutex
	allocator   memory.Allocator
	logger      *zap.Logger

	// Pool statistics for monitoring
	stats struct {
		hits   int64
		misses int64
		resets int64
	}
	statsMutex sync.RWMutex
}

// PooledBuilder wraps a RecordBuilder with additional pooling metadata
type PooledBuilder struct {
	builder    *array.RecordBuilder
	schema     *arrow.Schema
	lastUsed   time.Time
	resetCount int
	pool       *ArrowBuilderPool
}

// NewArrowBuilderPool creates a new builder pool with the given allocator
func NewArrowBuilderPool(allocator memory.Allocator, logger *zap.Logger) *ArrowBuilderPool {
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}

	pool := &ArrowBuilderPool{
		schemaCache: make(map[string]*arrow.Schema),
		allocator:   allocator,
		logger:      logger,
	}

	pool.pool = sync.Pool{
		New: func() interface{} {
			pool.incrementMisses()
			// Return nil - actual builder creation happens in Get()
			return nil
		},
	}

	return pool
}

// Get retrieves a builder for the given schema, creating one if necessary
func (p *ArrowBuilderPool) Get(schema *arrow.Schema) *PooledBuilder {
	schemaKey := p.getSchemaKey(schema)

	// Try to get from pool first
	if item := p.pool.Get(); item != nil {
		// Safe type assertion with ok check
		if pooled, ok := item.(*PooledBuilder); ok && pooled != nil {
			// Check if schema matches
			if p.schemasEqual(pooled.schema, schema) {
				p.incrementHits()
				pooled.lastUsed = time.Now()
				pooled.resetBuilder()
				return pooled
			}
			// Schema mismatch - release and create new
			pooled.release()
		} else {
			// Unexpected type in pool - cleanup and log warning
			if releaser, ok := item.(interface{ Release() }); ok {
				releaser.Release()
			}
			p.logger.Warn("Unexpected item type in builder pool", 
				zap.String("type", fmt.Sprintf("%T", item)))
		}
	}

	// Cache schema for reuse
	p.cacheSchema(schemaKey, schema)

	// Create new builder
	builder := array.NewRecordBuilder(p.allocator, schema)
	pooled := &PooledBuilder{
		builder:  builder,
		schema:   schema,
		lastUsed: time.Now(),
		pool:     p,
	}

	p.incrementMisses()
	return pooled
}

// Put returns a builder to the pool for reuse
func (p *ArrowBuilderPool) Put(pooled *PooledBuilder) {
	if pooled == nil || pooled.builder == nil {
		return
	}

	// Reset the builder for next use
	pooled.resetBuilder()
	pooled.lastUsed = time.Now()

	// Return to pool
	p.pool.Put(pooled)
}

// GetStats returns pool statistics
func (p *ArrowBuilderPool) GetStats() (hits, misses, resets int64) {
	p.statsMutex.RLock()
	defer p.statsMutex.RUnlock()
	return p.stats.hits, p.stats.misses, p.stats.resets
}

// Clear empties the pool and clears schema cache
func (p *ArrowBuilderPool) Clear() {
	// Create new pool to clear all items
	p.pool = sync.Pool{
		New: func() interface{} {
			p.incrementMisses()
			return nil
		},
	}

	p.schemaMutex.Lock()
	p.schemaCache = make(map[string]*arrow.Schema)
	p.schemaMutex.Unlock()

	p.logger.Debug("Arrow builder pool cleared")
}

// PooledBuilder methods

// Builder returns the underlying RecordBuilder
func (pb *PooledBuilder) Builder() *array.RecordBuilder {
	return pb.builder
}

// NewRecord creates a new record and automatically returns the builder to the pool
func (pb *PooledBuilder) NewRecord() arrow.Record {
	if pb.builder == nil {
		return nil
	}

	record := pb.builder.NewRecord()
	record.Retain() // Ensure record stays alive after builder returns to pool

	// Return builder to pool
	pb.pool.Put(pb)

	return record
}

// Release releases the builder without returning to pool (for cleanup)
func (pb *PooledBuilder) Release() {
	pb.release()
}

// resetBuilder resets the builder state for reuse
func (pb *PooledBuilder) resetBuilder() {
	if pb.builder == nil {
		return
	}

	// Reset all field builders to empty state
	schema := pb.builder.Schema()
	for i := 0; i < len(schema.Fields()); i++ {
		fieldBuilder := pb.builder.Field(i)
		if fieldBuilder != nil {
			// Clear the builder data without releasing memory
			fieldBuilder.Resize(0)
		}
	}

	pb.resetCount++
	pb.pool.incrementResets()
}

// release releases the builder resources
func (pb *PooledBuilder) release() {
	if pb.builder != nil {
		pb.builder.Release()
		pb.builder = nil
	}
}

// Helper methods

func (p *ArrowBuilderPool) getSchemaKey(schema *arrow.Schema) string {
	if schema == nil {
		return ""
	}
	// Use schema fingerprint as key for efficient comparison
	return schema.Fingerprint()
}

func (p *ArrowBuilderPool) cacheSchema(key string, schema *arrow.Schema) {
	p.schemaMutex.Lock()
	defer p.schemaMutex.Unlock()
	p.schemaCache[key] = schema
}

func (p *ArrowBuilderPool) schemasEqual(a, b *arrow.Schema) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Fingerprint() == b.Fingerprint()
}

func (p *ArrowBuilderPool) incrementHits() {
	p.statsMutex.Lock()
	p.stats.hits++
	p.statsMutex.Unlock()
}

func (p *ArrowBuilderPool) incrementMisses() {
	p.statsMutex.Lock()
	p.stats.misses++
	p.statsMutex.Unlock()
}

func (p *ArrowBuilderPool) incrementResets() {
	p.statsMutex.Lock()
	p.stats.resets++
	p.statsMutex.Unlock()
}
