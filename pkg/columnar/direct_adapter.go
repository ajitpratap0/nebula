package columnar

import (
	"encoding/csv"
	"io"
	"strconv"
)

// DirectCSVToColumnar provides zero-intermediate CSV to columnar conversion
type DirectCSVToColumnar struct {
	store     *ColumnStore
	headers   []string
	headerMap map[string]int
	types     []ColumnType
	rowCount  int
}

// NewDirectCSVToColumnar creates an optimized CSV to columnar converter
func NewDirectCSVToColumnar() *DirectCSVToColumnar {
	return &DirectCSVToColumnar{
		store:     NewColumnStore(),
		headerMap: make(map[string]int),
	}
}

// ProcessCSV reads and converts CSV directly to columnar storage
func (d *DirectCSVToColumnar) ProcessCSV(reader *csv.Reader) error {
	// Read headers
	headers, err := reader.Read()
	if err != nil {
		return err
	}

	d.headers = headers
	d.types = make([]ColumnType, len(headers))

	// Create columns
	for i, header := range headers {
		d.headerMap[header] = i
		d.types[i] = ColumnTypeString // Start with string, optimize later
		_ = d.store.AddColumn(header, ColumnTypeString) // Error ignored - column addition is expected to succeed
	}

	// Process rows directly
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Direct append to columns - no intermediate maps!
		d.store.mu.Lock()
		for i, value := range row {
			if i < len(d.headers) && i < len(d.store.columns) {
				col, exists := d.store.columns[d.headers[i]]
				if exists && col != nil {
					_ = col.Append(value)
				}
			}
		}
		d.store.rowCount++
		d.store.mu.Unlock()

		d.rowCount++
	}

	return nil
}

// OptimizeTypes converts string columns to appropriate types
func (d *DirectCSVToColumnar) OptimizeTypes() error {
	d.store.mu.Lock()
	defer d.store.mu.Unlock()

	for i, header := range d.headers {
		col := d.store.columns[header]
		strCol, ok := col.(*StringColumn)
		if !ok {
			continue
		}

		// Try to infer type from first 100 values
		sampleSize := 100
		if strCol.Len() < sampleSize {
			sampleSize = strCol.Len()
		}

		colType := d.inferColumnType(strCol, sampleSize)
		if colType != ColumnTypeString {
			// Convert column
			newCol := d.convertColumn(strCol, colType)
			if newCol != nil {
				d.store.columns[header] = newCol
				d.types[i] = colType
			}
		}
	}

	return nil
}

// inferColumnType samples values to determine optimal type
func (d *DirectCSVToColumnar) inferColumnType(col *StringColumn, sampleSize int) ColumnType {
	allInt := true
	allFloat := true
	allBool := true

	for i := 0; i < sampleSize; i++ {
		val := col.Get(i).(string)

		// Skip empty values
		if val == "" {
			continue
		}

		// Check int
		if allInt {
			_, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				allInt = false
			}
		}

		// Check float
		if allFloat && !allInt {
			_, err := strconv.ParseFloat(val, 64)
			if err != nil {
				allFloat = false
			}
		}

		// Check bool
		if allBool {
			if val != "true" && val != "false" && val != "0" && val != "1" && val != "yes" && val != "no" {
				allBool = false
			}
		}
	}

	if allInt {
		return ColumnTypeInt
	}
	if allFloat {
		return ColumnTypeFloat
	}
	if allBool {
		return ColumnTypeBool
	}

	return ColumnTypeString
}

// convertColumn converts a string column to the specified type
func (d *DirectCSVToColumnar) convertColumn(strCol *StringColumn, newType ColumnType) Column {
	switch newType {
	case ColumnTypeInt:
		intCol := NewIntColumn()
		for i := 0; i < strCol.Len(); i++ {
			val := strCol.Get(i).(string)
			if val == "" {
				_ = 				intCol.Append(int64(0))
			} else {
				intVal, _ := strconv.ParseInt(val, 10, 64)
				_ = 				intCol.Append(intVal)
			}
		}
		return intCol

	case ColumnTypeFloat:
		floatCol := NewFloatColumn()
		for i := 0; i < strCol.Len(); i++ {
			val := strCol.Get(i).(string)
			if val == "" {
				_ = 				floatCol.Append(0.0)
			} else {
				floatVal, _ := strconv.ParseFloat(val, 64)
				_ = 				floatCol.Append(floatVal)
			}
		}
		return floatCol

	case ColumnTypeBool:
		boolCol := NewBoolColumn()
		for i := 0; i < strCol.Len(); i++ {
			val := strCol.Get(i).(string)
			if val == "" {
				_ = 				boolCol.Append(false)
			} else {
				boolVal := val == "true" || val == "1" || val == "yes"
				_ = 				boolCol.Append(boolVal)
			}
		}
		return boolCol
	}

	return nil
}

// GetStore returns the columnar store
func (d *DirectCSVToColumnar) GetStore() *ColumnStore {
	return d.store
}

// GetRowCount returns the number of rows processed
func (d *DirectCSVToColumnar) GetRowCount() int {
	return d.rowCount
}

// StreamingDirectCSVToColumnar provides streaming CSV to columnar conversion
type StreamingDirectCSVToColumnar struct {
	store   *ColumnStore
	headers []string
	buffer  [][]string
	bufSize int
}

// NewStreamingDirectCSVToColumnar creates a streaming converter
func NewStreamingDirectCSVToColumnar(bufferSize int) *StreamingDirectCSVToColumnar {
	return &StreamingDirectCSVToColumnar{
		store:   NewColumnStore(),
		bufSize: bufferSize,
		buffer:  make([][]string, 0, bufferSize),
	}
}

// SetHeaders initializes columns from headers
func (s *StreamingDirectCSVToColumnar) SetHeaders(headers []string) {
	s.headers = make([]string, len(headers))
	copy(s.headers, headers)

	// Pre-create columns
	for _, header := range headers {
		_ = s.store.AddColumn(header, ColumnTypeString) // Column already exists check
	}
}

// AddRow adds a single row with buffering
func (s *StreamingDirectCSVToColumnar) AddRow(row []string) error {
	// Make a copy since CSV reader reuses the slice
	rowCopy := make([]string, len(row))
	copy(rowCopy, row)

	s.buffer = append(s.buffer, rowCopy)

	if len(s.buffer) >= s.bufSize {
	return s.Flush() // Ignore flush error
	}

	return nil
}

// Flush writes buffered rows to columnar store
func (s *StreamingDirectCSVToColumnar) Flush() error {
	if len(s.buffer) == 0 {
		return nil
	}

	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	// Process each buffered row
	for _, row := range s.buffer {
		// Direct column append - no maps!
		for i, value := range row {
			if i < len(s.headers) {
				col := s.store.columns[s.headers[i]]
				_ = col.Append(value)
			}
		}
		s.store.rowCount++
	}

	// Clear buffer
	s.buffer = s.buffer[:0]

	return nil
}

// GetStore returns the columnar store
func (s *StreamingDirectCSVToColumnar) GetStore() *ColumnStore {
	// Ensure all buffered data is written
	_ = s.Flush() // Best effort flush
	return s.store
}
