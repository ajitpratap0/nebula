package columnar

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ajitpratap0/nebula/pkg/compression"
)

// CompressedColumnStore wraps a ColumnStore with compression
type CompressedColumnStore struct {
	store      *ColumnStore
	compressor compression.Compressor
	compressed map[string][]byte // compressed column data
}

// NewCompressedColumnStore creates a new compressed column store
func NewCompressedColumnStore(algorithm compression.Algorithm) (*CompressedColumnStore, error) {
	config := &compression.Config{
		Algorithm:  algorithm,
		Level:      compression.Default,
		BufferSize: 64 * 1024,
	}
	compressor, err := compression.NewCompressor(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create compressor: %w", err)
	}

	return &CompressedColumnStore{
		store:      NewColumnStore(),
		compressor: compressor,
		compressed: make(map[string][]byte),
	}, nil
}

// CompressColumn compresses a single column
func (c *CompressedColumnStore) CompressColumn(name string) error {
	col, exists := c.store.GetColumn(name)
	if !exists {
		return fmt.Errorf("column %q not found", name)
	}

	// Serialize column data based on type
	data, err := c.serializeColumn(col)
	if err != nil {
		return fmt.Errorf("failed to serialize column %q: %w", name, err)
	}

	// Compress the serialized data
	compressed, err := c.compressor.Compress(data)
	if err != nil {
		return fmt.Errorf("failed to compress column %q: %w", name, err)
	}

	c.compressed[name] = compressed
	return nil
}

// CompressAll compresses all columns
func (c *CompressedColumnStore) CompressAll() error {
	for name := range c.store.columns {
		if err := c.CompressColumn(name); err != nil {
			return err
		}
	}
	return nil
}

// serializeColumn converts column data to bytes for compression
func (c *CompressedColumnStore) serializeColumn(col Column) ([]byte, error) {
	var buf bytes.Buffer

	// Write column type
	if err := binary.Write(&buf, binary.LittleEndian, uint8(col.Type())); err != nil { //nolint:gosec // G115: ColumnType values are small and fit in uint8
		return nil, err
	}

	// Write column length
	if err := binary.Write(&buf, binary.LittleEndian, uint32(col.Len())); err != nil { //nolint:gosec // G115: Column lengths are expected to fit in uint32
		return nil, err
	}

	switch col.Type() {
	case ColumnTypeInt:
		intCol := col.(*IntColumn)
		// Write min/max for delta encoding
		if err := binary.Write(&buf, binary.LittleEndian, intCol.min); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, intCol.max); err != nil {
			return nil, err
		}

		// Delta encode integers
		if intCol.Len() > 0 {
			prev := intCol.values[0]
			if err := binary.Write(&buf, binary.LittleEndian, prev); err != nil {
				return nil, err
			}

			for i := 1; i < intCol.Len(); i++ {
				delta := intCol.values[i] - prev
				if err := writeVarint(&buf, delta); err != nil {
					return nil, err
				}
				prev = intCol.values[i]
			}
		}

	case ColumnTypeFloat:
		floatCol := col.(*FloatColumn)
		// Write all floats directly (compression will handle redundancy)
		for i := 0; i < floatCol.Len(); i++ {
			if err := binary.Write(&buf, binary.LittleEndian, floatCol.values[i]); err != nil {
				return nil, err
			}
		}

	case ColumnTypeBool:
		boolCol := col.(*BoolColumn)
		// Already bit-packed, write directly
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(boolCol.values))); err != nil { //nolint:gosec // G115: Bool column values length expected to fit in uint32
			return nil, err
		}
		for _, word := range boolCol.values {
			if err := binary.Write(&buf, binary.LittleEndian, word); err != nil {
				return nil, err
			}
		}

	case ColumnTypeString:
		strCol := col.(*StringColumn)
		if strCol.dictMode {
			// Write dictionary mode flag
			if err := binary.Write(&buf, binary.LittleEndian, true); err != nil {
				return nil, err
			}

			// Write dictionary size
			dictLen := len(strCol.dict)
			if dictLen > 4294967295 { // math.MaxUint32
				return nil, fmt.Errorf("dictionary size %d exceeds maximum uint32 value", dictLen)
			}
			if err := binary.Write(&buf, binary.LittleEndian, uint32(dictLen)); err != nil {
				return nil, err
			}

			// Write dictionary entries
			for str, code := range strCol.dict {
				// Write string length and data
				strLen := len(str)
				if strLen > 4294967295 { // math.MaxUint32
					return nil, fmt.Errorf("string length %d exceeds maximum uint32 value", strLen)
				}
				if err := binary.Write(&buf, binary.LittleEndian, uint32(strLen)); err != nil {
					return nil, err
				}
				if _, err := buf.WriteString(str); err != nil {
					return nil, err
				}
				if err := binary.Write(&buf, binary.LittleEndian, code); err != nil {
					return nil, err
				}
			}

			// Write codes
			for _, code := range strCol.codes {
				if err := binary.Write(&buf, binary.LittleEndian, code); err != nil {
					return nil, err
				}
			}
		} else {
			// Write regular mode flag
			if err := binary.Write(&buf, binary.LittleEndian, false); err != nil {
				return nil, err
			}

			// Write each string
			for _, str := range strCol.values {
				strLen := len(str)
				if strLen > 4294967295 { // math.MaxUint32
					return nil, fmt.Errorf("string length %d exceeds maximum uint32 value", strLen)
				}
				if err := binary.Write(&buf, binary.LittleEndian, uint32(strLen)); err != nil {
					return nil, err
				}
				if _, err := buf.WriteString(str); err != nil {
					return nil, err
				}
			}
		}

	default:
		return nil, fmt.Errorf("unsupported column type: %v", col.Type())
	}

	return buf.Bytes(), nil
}

// writeVarint writes a variable-length integer
func writeVarint(buf *bytes.Buffer, v int64) error {
	// Zigzag encoding: intentional conversion from signed to unsigned
	uv := uint64(v<<1) ^ uint64(v>>63) // #nosec G115 - intentional zigzag encoding
	for uv >= 0x80 {
		buf.WriteByte(byte(uv) | 0x80)
		uv >>= 7
	}
	buf.WriteByte(byte(uv))
	return nil
}

// GetCompressedSize returns the total compressed size
func (c *CompressedColumnStore) GetCompressedSize() int64 {
	var total int64
	for _, data := range c.compressed {
		total += int64(len(data))
	}
	return total
}

// GetCompressionRatio returns the compression ratio
func (c *CompressedColumnStore) GetCompressionRatio() float64 {
	original := c.store.MemoryUsage()
	compressed := c.GetCompressedSize()
	if compressed == 0 {
		return 1.0
	}
	return float64(original) / float64(compressed)
}

// ColumnarCompressionConfig configures columnar compression
type ColumnarCompressionConfig struct {
	Algorithm       compression.Algorithm
	Level           compression.Level
	CompressStrings bool
	CompressNumbers bool
	MinColumnSize   int // Minimum column size to compress
}

// DefaultColumnarCompressionConfig returns default compression settings
func DefaultColumnarCompressionConfig() ColumnarCompressionConfig {
	return ColumnarCompressionConfig{
		Algorithm:       compression.Zstd,
		Level:           compression.Default,
		CompressStrings: true,
		CompressNumbers: true,
		MinColumnSize:   100, // Don't compress tiny columns
	}
}
