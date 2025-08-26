package pool

import (
	"sync"
	"time"
)

// TypedRecord provides specialized record types to avoid interface{} boxing
type TypedRecord struct {
	*Record

	// Typed fields for common data types
	StringFields map[string]string
	IntFields    map[string]int64
	FloatFields  map[string]float64
	BoolFields   map[string]bool
	TimeFields   map[string]time.Time
	BytesFields  map[string][]byte
}

// Global typed record pool
var typedRecordPool = &sync.Pool{
	New: func() interface{} {
		return &TypedRecord{
			Record:       &Record{},
			StringFields: make(map[string]string, 16),
			IntFields:    make(map[string]int64, 8),
			FloatFields:  make(map[string]float64, 8),
			BoolFields:   make(map[string]bool, 4),
			TimeFields:   make(map[string]time.Time, 4),
			BytesFields:  make(map[string][]byte, 4),
		}
	},
}

// GetTypedRecord gets a typed record from the pool
func GetTypedRecord() *TypedRecord {
	tr := typedRecordPool.Get().(*TypedRecord)
	// Initialize the base record
	tr.Record = GetRecord()
	return tr
}

// PutTypedRecord returns a typed record to the pool
func PutTypedRecord(tr *TypedRecord) {
	if tr == nil {
		return
	}

	// Clear typed fields
	for k := range tr.StringFields {
		delete(tr.StringFields, k)
	}
	for k := range tr.IntFields {
		delete(tr.IntFields, k)
	}
	for k := range tr.FloatFields {
		delete(tr.FloatFields, k)
	}
	for k := range tr.BoolFields {
		delete(tr.BoolFields, k)
	}
	for k := range tr.TimeFields {
		delete(tr.TimeFields, k)
	}
	for k := range tr.BytesFields {
		delete(tr.BytesFields, k)
	}

	// Release base record
	if tr.Record != nil {
		tr.Record.Release()
		tr.Record = nil
	}

	typedRecordPool.Put(tr)
}

// SetString sets a string field without boxing
func (tr *TypedRecord) SetString(key, value string) {
	tr.StringFields[key] = value
	// Also set in the generic map for compatibility
	tr.Record.SetData(key, value)
}

// SetInt sets an int field without boxing
func (tr *TypedRecord) SetInt(key string, value int64) {
	tr.IntFields[key] = value
	// Also set in the generic map for compatibility
	tr.Record.SetData(key, value)
}

// SetFloat sets a float field without boxing
func (tr *TypedRecord) SetFloat(key string, value float64) {
	tr.FloatFields[key] = value
	// Also set in the generic map for compatibility
	tr.Record.SetData(key, value)
}

// SetBool sets a bool field without boxing
func (tr *TypedRecord) SetBool(key string, value bool) {
	tr.BoolFields[key] = value
	// Also set in the generic map for compatibility
	tr.Record.SetData(key, value)
}

// SetTime sets a time field without boxing
func (tr *TypedRecord) SetTime(key string, value time.Time) {
	tr.TimeFields[key] = value
	// Also set in the generic map for compatibility
	tr.Record.SetData(key, value)
}

// SetBytes sets a bytes field without boxing
func (tr *TypedRecord) SetBytes(key string, value []byte) {
	tr.BytesFields[key] = value
	// Also set in the generic map for compatibility
	tr.Record.SetData(key, value)
}

// GetString gets a string field without unboxing
func (tr *TypedRecord) GetString(key string) (string, bool) {
	val, ok := tr.StringFields[key]
	return val, ok
}

// GetInt gets an int field without unboxing
func (tr *TypedRecord) GetInt(key string) (int64, bool) {
	val, ok := tr.IntFields[key]
	return val, ok
}

// GetFloat gets a float field without unboxing
func (tr *TypedRecord) GetFloat(key string) (float64, bool) {
	val, ok := tr.FloatFields[key]
	return val, ok
}

// GetBool gets a bool field without unboxing
func (tr *TypedRecord) GetBool(key string) (bool, bool) {
	val, ok := tr.BoolFields[key]
	return val, ok
}

// GetTime gets a time field without unboxing
func (tr *TypedRecord) GetTime(key string) (time.Time, bool) {
	val, ok := tr.TimeFields[key]
	return val, ok
}

// GetBytes gets a bytes field without unboxing
func (tr *TypedRecord) GetBytes(key string) ([]byte, bool) {
	val, ok := tr.BytesFields[key]
	return val, ok
}

// Release releases the typed record
func (tr *TypedRecord) Release() {
	PutTypedRecord(tr)
}
