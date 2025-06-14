package models

import (
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// Re-export unified pool types for backward compatibility
type Record = pool.Record
type RecordMetadata = pool.RecordMetadata

// Schema defines the structure of record data
type Schema struct {
	// Name of the schema
	Name string `json:"name"`

	// Version of the schema
	Version string `json:"version"`

	// Fields defines the structure of the data
	Fields []Field `json:"fields"`
}

// Field represents a single field in the schema
type Field struct {
	// Name of the field
	Name string `json:"name"`

	// Type of the field (string, integer, float, boolean, object, array)
	Type string `json:"type"`

	// Description of the field (optional)
	Description string `json:"description,omitempty"`

	// Required indicates if the field is required
	Required bool `json:"required"`

	// Fields for nested objects
	Fields []Field `json:"fields,omitempty"`
}

// Re-export pool functions for backward compatibility

// NewRecord creates a new record with the given source and data
var NewRecord = pool.NewRecord

// NewRecordFromPool creates a new record using pooled resources
var NewRecordFromPool = pool.NewRecordFromPool

// NewCDCRecord creates a new record for Change Data Capture
var NewCDCRecord = pool.NewCDCRecord

// NewStreamingRecord creates a new record for streaming
var NewStreamingRecord = pool.NewStreamingRecord

// AsIRecord converts a Record to IRecord interface for backward compatibility
func AsIRecord(r *Record) IRecord {
	return &RecordWrapper{record: r}
}

// IRecord interface for backward compatibility
type IRecord interface {
	ToModelsRecord() *Record
	Release()
}

// RecordWrapper implements IRecord for backward compatibility
type RecordWrapper struct {
	record *Record
}

// ToModelsRecord returns the underlying Record
func (w *RecordWrapper) ToModelsRecord() *Record {
	return w.record
}

// Release releases the record back to the pool
func (w *RecordWrapper) Release() {
	if w.record != nil {
		w.record.Release()
	}
}

// RecordBatch represents a batch of records
type RecordBatch struct {
	Records []*Record
	size    int
}

// NewRecordBatch creates a new record batch
func NewRecordBatch(capacity int) *RecordBatch {
	return &RecordBatch{
		Records: make([]*Record, 0, capacity),
		size:    0,
	}
}

// AddRecord adds a record to the batch
func (rb *RecordBatch) AddRecord(r *Record) {
	rb.Records = append(rb.Records, r)
	rb.size++
}

// Reset resets the batch for reuse
func (rb *RecordBatch) Reset() {
	rb.Records = rb.Records[:0]
	rb.size = 0
}

// Size returns the number of records in the batch
func (rb *RecordBatch) Size() int {
	return rb.size
}