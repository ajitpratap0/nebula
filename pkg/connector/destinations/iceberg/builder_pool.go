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
// Optimized for single-schema usage (one table per destination)
type ArrowBuilderPool struct {
	pool      sync.Pool
	schema    *arrow.Schema // Single schema per pool (one table per destination)
	allocator memory.Allocator
	logger    *zap.Logger
}

// PooledBuilder wraps a RecordBuilder with pooling metadata
type PooledBuilder struct {
	builder  *array.RecordBuilder
	lastUsed time.Time
	pool     *ArrowBuilderPool
}

// NewArrowBuilderPool creates a new builder pool with the given allocator
func NewArrowBuilderPool(allocator memory.Allocator, logger *zap.Logger) *ArrowBuilderPool {
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}

	pool := &ArrowBuilderPool{
		allocator: allocator,
		logger:    logger,
	}

	pool.pool = sync.Pool{
		New: func() interface{} {
			// Return nil if no schema is set yet - builder creation happens in Get()
			// This prevents creating builders with the wrong schema
			return nil
		},
	}

	return pool
}

// Get retrieves a builder for the given schema, creating one if necessary
func (p *ArrowBuilderPool) Get(schema *arrow.Schema) *PooledBuilder {
	// Validate input
	if schema == nil {
		p.logger.Error("Cannot get builder with nil schema")
		return nil
	}

	// Set schema on first use (single schema per pool)
	if p.schema == nil {
		p.schema = schema
	}

	// Verify schema consistency (single schema per pool design)
	if !p.schema.Equal(schema) {
		p.logger.Warn("Schema mismatch in builder pool - creating new builder",
			zap.String("pool_schema", p.schema.String()),
			zap.String("requested_schema", schema.String()))
	}

	// Try to get from pool first
	if item := p.pool.Get(); item != nil {
		// Safe type assertion with ok check
		if pooled, ok := item.(*PooledBuilder); ok && pooled != nil && pooled.builder != nil {
			// Verify builder is still valid
			if pooled.builder.Schema().Equal(schema) {
				// Reuse pooled builder (all builders have same schema)
				pooled.lastUsed = time.Now()
				pooled.resetBuilder()
				return pooled
			} else {
				// Schema mismatch - release old builder
				pooled.release()
			}
		} else {
			// Unexpected type in pool - cleanup and log warning
			if releaser, ok := item.(interface{ Release() }); ok {
				releaser.Release()
			}
			p.logger.Warn("Unexpected item type in builder pool", 
				zap.String("type", fmt.Sprintf("%T", item)))
		}
	}

	// Create new builder - handle potential allocation failure
	builder := array.NewRecordBuilder(p.allocator, schema)
	if builder == nil {
		p.logger.Error("Failed to create new RecordBuilder")
		return nil
	}

	pooled := &PooledBuilder{
		builder:  builder,
		lastUsed: time.Now(),
		pool:     p,
	}

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


// Clear empties the pool 
func (p *ArrowBuilderPool) Clear() {
	// Create new pool to clear all items
	p.pool = sync.Pool{
		New: func() interface{} {
			return nil
		},
	}

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
}

// release releases the builder resources
func (pb *PooledBuilder) release() {
	if pb.builder != nil {
		pb.builder.Release()
		pb.builder = nil
	}
}

