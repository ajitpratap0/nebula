// Package columnar provides columnar storage for ultra-low memory footprint
package columnar

import (
	"fmt"
	"time"
)

// ColumnType represents the data type of a column
type ColumnType int

const (
	ColumnTypeString ColumnType = iota
	ColumnTypeInt
	ColumnTypeFloat
	ColumnTypeBool
	ColumnTypeTimestamp
	ColumnTypeBytes
)

// Column is the base interface for all column types
type Column interface {
	Type() ColumnType
	Len() int
	Get(i int) interface{}
	Append(value interface{}) error
	Clear()
	MemoryUsage() int64
}

// StringColumn stores string values efficiently
type StringColumn struct {
	values []string
	// Dictionary encoding for repeated values
	dict      map[string]uint32
	codes     []uint32
	dictMode  bool
	threshold float64 // Switch to dictionary when repetition > threshold
}

// NewStringColumn creates a new string column
func NewStringColumn() *StringColumn {
	return &StringColumn{
		values:    make([]string, 0, 1024),
		dict:      make(map[string]uint32),
		codes:     make([]uint32, 0, 1024),
		threshold: 0.5, // Use dict if >50% values are repeated
	}
}

func (c *StringColumn) Type() ColumnType { return ColumnTypeString }
func (c *StringColumn) Len() int {
	if c.dictMode {
		return len(c.codes)
	}
	return len(c.values)
}

func (c *StringColumn) Get(i int) interface{} {
	if c.dictMode {
		// Reverse lookup from dictionary
		for val, code := range c.dict {
			if code == c.codes[i] {
				return val
			}
		}
		return ""
	}
	return c.values[i]
}

func (c *StringColumn) Append(value interface{}) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string, got %T", value)
	}

	if c.dictMode {
		// Dictionary mode
		if code, exists := c.dict[str]; exists {
			c.codes = append(c.codes, code)
		} else {
			newCode := uint32(len(c.dict))
			c.dict[str] = newCode
			c.codes = append(c.codes, newCode)
		}
	} else {
		// Regular mode
		c.values = append(c.values, str)

		// Check if we should switch to dictionary mode
		if len(c.values) > 100 && c.shouldUseDictionary() {
			c.convertToDictionary()
		}
	}

	return nil
}

func (c *StringColumn) shouldUseDictionary() bool {
	unique := make(map[string]struct{})
	for _, v := range c.values {
		unique[v] = struct{}{}
	}
	ratio := float64(len(unique)) / float64(len(c.values))
	return ratio < c.threshold
}

func (c *StringColumn) convertToDictionary() {
	c.dictMode = true
	c.dict = make(map[string]uint32)
	c.codes = make([]uint32, 0, len(c.values))

	for _, v := range c.values {
		if code, exists := c.dict[v]; exists {
			c.codes = append(c.codes, code)
		} else {
			newCode := uint32(len(c.dict))
			c.dict[v] = newCode
			c.codes = append(c.codes, newCode)
		}
	}

	// Clear values to free memory
	c.values = nil
}

func (c *StringColumn) Clear() {
	c.values = c.values[:0]
	c.codes = c.codes[:0]
	c.dict = make(map[string]uint32)
	c.dictMode = false
}

func (c *StringColumn) MemoryUsage() int64 {
	var total int64

	if c.dictMode {
		// Dictionary storage
		for k := range c.dict {
			total += int64(len(k)) // String bytes
			total += 4             // uint32 code
		}
		total += int64(len(c.codes) * 4) // codes array
	} else {
		// Regular storage
		for _, v := range c.values {
			total += int64(len(v))
			total += 16 // string header overhead
		}
	}

	return total
}

// IntColumn stores integer values efficiently
type IntColumn struct {
	values   []int64
	min, max int64
	packed   bool //nolint:unused // Reserved for packed storage optimization
	bitWidth int  //nolint:unused // Reserved for bit packing optimization
}

// NewIntColumn creates a new integer column
func NewIntColumn() *IntColumn {
	return &IntColumn{
		values: make([]int64, 0, 1024),
	}
}

func (c *IntColumn) Type() ColumnType { return ColumnTypeInt }
func (c *IntColumn) Len() int         { return len(c.values) }

func (c *IntColumn) Get(i int) interface{} {
	return c.values[i]
}

func (c *IntColumn) Append(value interface{}) error {
	var intVal int64
	switch v := value.(type) {
	case int:
		intVal = int64(v)
	case int64:
		intVal = v
	case int32:
		intVal = int64(v)
	case string:
		// Try to parse string as int
		_, err := fmt.Sscanf(v, "%d", &intVal)
		if err != nil {
			return fmt.Errorf("cannot parse %q as int: %w", v, err)
		}
	default:
		return fmt.Errorf("expected int, got %T", value)
	}

	if len(c.values) == 0 {
		c.min = intVal
		c.max = intVal
	} else {
		if intVal < c.min {
			c.min = intVal
		}
		if intVal > c.max {
			c.max = intVal
		}
	}

	c.values = append(c.values, intVal)
	return nil
}

func (c *IntColumn) Clear() {
	c.values = c.values[:0]
	c.min = 0
	c.max = 0
}

func (c *IntColumn) MemoryUsage() int64 {
	return int64(len(c.values) * 8) // 8 bytes per int64
}

// FloatColumn stores floating point values
type FloatColumn struct {
	values []float64
}

// NewFloatColumn creates a new float column
func NewFloatColumn() *FloatColumn {
	return &FloatColumn{
		values: make([]float64, 0, 1024),
	}
}

func (c *FloatColumn) Type() ColumnType { return ColumnTypeFloat }
func (c *FloatColumn) Len() int         { return len(c.values) }

func (c *FloatColumn) Get(i int) interface{} {
	return c.values[i]
}

func (c *FloatColumn) Append(value interface{}) error {
	var floatVal float64
	switch v := value.(type) {
	case float64:
		floatVal = v
	case float32:
		floatVal = float64(v)
	case string:
		_, err := fmt.Sscanf(v, "%f", &floatVal)
		if err != nil {
			return fmt.Errorf("cannot parse %q as float: %w", v, err)
		}
	default:
		return fmt.Errorf("expected float, got %T", value)
	}

	c.values = append(c.values, floatVal)
	return nil
}

func (c *FloatColumn) Clear() {
	c.values = c.values[:0]
}

func (c *FloatColumn) MemoryUsage() int64 {
	return int64(len(c.values) * 8) // 8 bytes per float64
}

// BoolColumn stores boolean values efficiently
type BoolColumn struct {
	values []uint64 // Bit-packed storage: 64 bools per uint64
	count  int
}

// NewBoolColumn creates a new boolean column
func NewBoolColumn() *BoolColumn {
	return &BoolColumn{
		values: make([]uint64, 0, 16),
	}
}

func (c *BoolColumn) Type() ColumnType { return ColumnTypeBool }
func (c *BoolColumn) Len() int         { return c.count }

func (c *BoolColumn) Get(i int) interface{} {
	wordIndex := i / 64
	bitIndex := i % 64
	return (c.values[wordIndex] & (1 << bitIndex)) != 0
}

func (c *BoolColumn) Append(value interface{}) error {
	var boolVal bool
	switch v := value.(type) {
	case bool:
		boolVal = v
	case string:
		boolVal = v == "true" || v == "1" || v == "yes"
	default:
		return fmt.Errorf("expected bool, got %T", value)
	}

	wordIndex := c.count / 64
	bitIndex := c.count % 64

	// Grow if needed
	if wordIndex >= len(c.values) {
		c.values = append(c.values, 0)
	}

	if boolVal {
		c.values[wordIndex] |= (1 << bitIndex)
	}

	c.count++
	return nil
}

func (c *BoolColumn) Clear() {
	c.values = c.values[:0]
	c.count = 0
}

func (c *BoolColumn) MemoryUsage() int64 {
	return int64(len(c.values) * 8) // 8 bytes per uint64
}

// TimestampColumn stores timestamp values
type TimestampColumn struct {
	values []int64 // Unix timestamps
}

// NewTimestampColumn creates a new timestamp column
func NewTimestampColumn() *TimestampColumn {
	return &TimestampColumn{
		values: make([]int64, 0, 1024),
	}
}

func (c *TimestampColumn) Type() ColumnType { return ColumnTypeTimestamp }
func (c *TimestampColumn) Len() int         { return len(c.values) }

func (c *TimestampColumn) Get(i int) interface{} {
	return time.Unix(c.values[i], 0)
}

func (c *TimestampColumn) Append(value interface{}) error {
	var timestamp int64
	switch v := value.(type) {
	case time.Time:
		timestamp = v.Unix()
	case int64:
		timestamp = v
	case string:
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return fmt.Errorf("cannot parse %q as timestamp: %w", v, err)
		}
		timestamp = t.Unix()
	default:
		return fmt.Errorf("expected timestamp, got %T", value)
	}

	c.values = append(c.values, timestamp)
	return nil
}

func (c *TimestampColumn) Clear() {
	c.values = c.values[:0]
}

func (c *TimestampColumn) MemoryUsage() int64 {
	return int64(len(c.values) * 8) // 8 bytes per int64
}
