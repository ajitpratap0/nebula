// Package models provides data models and structures for Nebula.
// It re-exports the unified Record types from the pool package for
// backward compatibility and adds schema definitions for data structure.
//
// This package serves as a compatibility layer during the transition
// to the unified pool-based record system. New code should import
// directly from the pool package.
package models

import (
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// Record is a type alias for pool.Record, providing backward compatibility.
// New code should use pool.Record directly.
type Record = pool.Record

// RecordMetadata is a type alias for pool.RecordMetadata.
// New code should use pool.RecordMetadata directly.
type RecordMetadata = pool.RecordMetadata

// Schema defines the structure of record data.
// It provides a way to describe the expected format of records,
// enabling validation and schema evolution capabilities.
type Schema struct {
	// Name identifies the schema (e.g., table name, event type)
	Name string `json:"name"`

	// Version tracks schema changes for evolution support
	Version string `json:"version"`

	// Fields defines the structure of the data
	Fields []Field `json:"fields"`
}

// Field represents a single field in the schema.
// It supports nested structures through the Fields property.
type Field struct {
	// Name is the field identifier
	Name string `json:"name"`

	// Type specifies the data type (string, integer, float, boolean, object, array)
	Type string `json:"type"`

	// Description provides human-readable field information
	Description string `json:"description,omitempty"`

	// Required indicates if the field must be present
	Required bool `json:"required"`

	// Fields defines nested structure for object types
	Fields []Field `json:"fields,omitempty"`
}

// Re-export pool functions for backward compatibility.
// These are provided to ease migration to the unified pool system.

// NewRecord creates a new record with the given source and data.
// Deprecated: Use pool.NewRecord directly.
var NewRecord = pool.NewRecord

// NewRecordFromPool creates a new record using pooled resources.
// Deprecated: Use pool.NewRecordFromPool directly.
var NewRecordFromPool = pool.NewRecordFromPool

// NewCDCRecord creates a new record for Change Data Capture.
// Deprecated: Use pool.NewCDCRecord directly.
var NewCDCRecord = pool.NewCDCRecord

// NewStreamingRecord creates a new record for streaming.
// Deprecated: Use pool.NewStreamingRecord directly.
var NewStreamingRecord = pool.NewStreamingRecord

// AsIRecord converts a Record to IRecord interface for backward compatibility.
// This function helps bridge old code that expects the IRecord interface.
//
// Deprecated: New code should work directly with *pool.Record.
func AsIRecord(r *Record) IRecord {
	return &RecordWrapper{record: r}
}

// IRecord interface for backward compatibility.
// This interface was used before the unified pool system.
//
// Deprecated: Use *pool.Record directly.
type IRecord interface {
	// ToModelsRecord returns the underlying Record
	ToModelsRecord() *Record
	// Release returns the record to the pool
	Release()
}

// RecordWrapper implements IRecord for backward compatibility.
// It wraps a pool.Record to provide the old interface.
//
// Deprecated: Use *pool.Record directly.
type RecordWrapper struct {
	record *Record
}

// ToModelsRecord returns the underlying Record.
func (w *RecordWrapper) ToModelsRecord() *Record {
	return w.record
}

// Release releases the record back to the pool.
func (w *RecordWrapper) Release() {
	if w.record != nil {
		w.record.Release()
	}
}

// RecordBatch represents a batch of records for efficient bulk processing.
// It provides methods for managing collections of records with minimal allocations.
type RecordBatch struct {
	// Records holds the actual record pointers
	Records []*Record
	// size tracks the current number of records
	size int
}

// NewRecordBatch creates a new record batch with the specified capacity.
// The batch will grow as needed but pre-allocating improves performance.
func NewRecordBatch(capacity int) *RecordBatch {
	return &RecordBatch{
		Records: make([]*Record, 0, capacity),
		size:    0,
	}
}

// AddRecord appends a record to the batch.
// The batch will automatically grow if needed.
func (rb *RecordBatch) AddRecord(r *Record) {
	rb.Records = append(rb.Records, r)
	rb.size++
}

// Reset clears the batch for reuse without deallocating memory.
// This is more efficient than creating a new batch.
func (rb *RecordBatch) Reset() {
	rb.Records = rb.Records[:0]
	rb.size = 0
}

// Size returns the current number of records in the batch.
func (rb *RecordBatch) Size() int {
	return rb.size
}
