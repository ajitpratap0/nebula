package columnar

import (
	"fmt"
	"sync"
)

// Schema defines the structure of a columnar store
type Schema struct {
	Fields []FieldSchema
}

// FieldSchema defines a single field in the schema
type FieldSchema struct {
	Name string
	Type ColumnType
}

// ColumnStore provides columnar storage for records
type ColumnStore struct {
	mu       sync.RWMutex
	columns  map[string]Column
	schema   *Schema
	rowCount int
}

// NewColumnStore creates a new column store
func NewColumnStore() *ColumnStore {
	return &ColumnStore{
		columns: make(map[string]Column),
	}
}

// NewColumnStoreWithSchema creates a new column store with predefined schema
func NewColumnStoreWithSchema(schema *Schema) *ColumnStore {
	store := &ColumnStore{
		columns: make(map[string]Column),
		schema:  schema,
	}
	
	// Pre-create columns based on schema
	for _, field := range schema.Fields {
		store.columns[field.Name] = createColumn(field.Type)
	}
	
	return store
}

// createColumn creates a new column of the specified type
func createColumn(colType ColumnType) Column {
	switch colType {
	case ColumnTypeString:
		return NewStringColumn()
	case ColumnTypeInt:
		return NewIntColumn()
	case ColumnTypeFloat:
		return NewFloatColumn()
	case ColumnTypeBool:
		return NewBoolColumn()
	case ColumnTypeTimestamp:
		return NewTimestampColumn()
	default:
		return NewStringColumn() // Default to string
	}
}

// AddColumn adds a new column to the store
func (s *ColumnStore) AddColumn(name string, colType ColumnType) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if _, exists := s.columns[name]; exists {
		return fmt.Errorf("column %q already exists", name)
	}
	
	col := createColumn(colType)
	
	// If we already have data, fill with nulls
	if s.rowCount > 0 {
		for i := 0; i < s.rowCount; i++ {
			col.Append("")
		}
	}
	
	s.columns[name] = col
	return nil
}

// AppendRow adds a new row to the store
func (s *ColumnStore) AppendRow(data map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Auto-create columns if they don't exist
	for key, value := range data {
		if _, exists := s.columns[key]; !exists {
			// Infer type from value
			colType := inferColumnType(value)
			s.columns[key] = createColumn(colType)
			
			// Fill with nulls for existing rows
			for i := 0; i < s.rowCount; i++ {
				s.columns[key].Append("")
			}
		}
	}
	
	// Append values to each column
	for name, col := range s.columns {
		if value, exists := data[name]; exists {
			if err := col.Append(value); err != nil {
				return fmt.Errorf("error appending to column %q: %w", name, err)
			}
		} else {
			// Append null/empty value
			col.Append("")
		}
	}
	
	s.rowCount++
	return nil
}

// AppendBatch adds multiple rows efficiently
func (s *ColumnStore) AppendBatch(rows []map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	for _, row := range rows {
		// Auto-create columns
		for key, value := range row {
			if _, exists := s.columns[key]; !exists {
				colType := inferColumnType(value)
				s.columns[key] = createColumn(colType)
				
				// Fill with nulls
				for i := 0; i < s.rowCount; i++ {
					s.columns[key].Append("")
				}
			}
		}
	}
	
	// Append all rows
	for _, row := range rows {
		for name, col := range s.columns {
			if value, exists := row[name]; exists {
				col.Append(value)
			} else {
				col.Append("")
			}
		}
		s.rowCount++
	}
	
	return nil
}

// GetRow retrieves a row by index
func (s *ColumnStore) GetRow(index int) (map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if index < 0 || index >= s.rowCount {
		return nil, fmt.Errorf("index %d out of range [0, %d)", index, s.rowCount)
	}
	
	row := make(map[string]interface{})
	for name, col := range s.columns {
		row[name] = col.Get(index)
	}
	
	return row, nil
}

// GetColumn retrieves a column by name
func (s *ColumnStore) GetColumn(name string) (Column, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	col, exists := s.columns[name]
	return col, exists
}

// RowCount returns the number of rows
func (s *ColumnStore) RowCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rowCount
}

// ColumnCount returns the number of columns
func (s *ColumnStore) ColumnCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.columns)
}

// ColumnNames returns all column names
func (s *ColumnStore) ColumnNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	names := make([]string, 0, len(s.columns))
	for name := range s.columns {
		names = append(names, name)
	}
	return names
}

// MemoryUsage returns total memory usage in bytes
func (s *ColumnStore) MemoryUsage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	var total int64
	
	// Overhead for the store itself
	total += 64 // Base struct overhead
	total += int64(len(s.columns) * 32) // Map overhead
	
	// Memory for each column
	for name, col := range s.columns {
		total += int64(len(name)) // Column name
		total += col.MemoryUsage()
	}
	
	return total
}

// MemoryPerRecord returns average memory usage per record
func (s *ColumnStore) MemoryPerRecord() float64 {
	if s.rowCount == 0 {
		return 0
	}
	return float64(s.MemoryUsage()) / float64(s.rowCount)
}

// Clear removes all data from the store
func (s *ColumnStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	for _, col := range s.columns {
		col.Clear()
	}
	s.rowCount = 0
}

// inferColumnType attempts to determine column type from a value
func inferColumnType(value interface{}) ColumnType {
	switch value.(type) {
	case string:
		return ColumnTypeString
	case int, int32, int64, uint, uint32, uint64:
		return ColumnTypeInt
	case float32, float64:
		return ColumnTypeFloat
	case bool:
		return ColumnTypeBool
	default:
		return ColumnTypeString // Default to string
	}
}

// Iterator provides sequential access to rows
type Iterator struct {
	store  *ColumnStore
	index  int
	buffer map[string]interface{}
}

// NewIterator creates a new iterator over the store
func (s *ColumnStore) NewIterator() *Iterator {
	return &Iterator{
		store:  s,
		index:  -1,
		buffer: make(map[string]interface{}),
	}
}

// Next advances to the next row
func (it *Iterator) Next() bool {
	it.index++
	return it.index < it.store.rowCount
}

// Row returns the current row
func (it *Iterator) Row() map[string]interface{} {
	// Clear buffer
	for k := range it.buffer {
		delete(it.buffer, k)
	}
	
	// Fill buffer with current row data
	it.store.mu.RLock()
	for name, col := range it.store.columns {
		it.buffer[name] = col.Get(it.index)
	}
	it.store.mu.RUnlock()
	
	return it.buffer
}

// BatchIterator provides batch access to rows
type BatchIterator struct {
	store     *ColumnStore
	batchSize int
	index     int
}

// NewBatchIterator creates a new batch iterator
func (s *ColumnStore) NewBatchIterator(batchSize int) *BatchIterator {
	return &BatchIterator{
		store:     s,
		batchSize: batchSize,
		index:     0,
	}
}

// NextBatch returns the next batch of rows
func (it *BatchIterator) NextBatch() ([]map[string]interface{}, bool) {
	if it.index >= it.store.rowCount {
		return nil, false
	}
	
	endIndex := it.index + it.batchSize
	if endIndex > it.store.rowCount {
		endIndex = it.store.rowCount
	}
	
	batch := make([]map[string]interface{}, 0, endIndex-it.index)
	
	it.store.mu.RLock()
	for i := it.index; i < endIndex; i++ {
		row := make(map[string]interface{})
		for name, col := range it.store.columns {
			row[name] = col.Get(i)
		}
		batch = append(batch, row)
	}
	it.store.mu.RUnlock()
	
	it.index = endIndex
	return batch, true
}