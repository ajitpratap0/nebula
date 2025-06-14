// Package columnar provides columnar format support for Nebula
package columnar

import (
	"fmt"
	"io"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
)

// Format represents a columnar storage format
type Format string

const (
	// Parquet is Apache Parquet format
	Parquet Format = "parquet"
	// ORC is Apache ORC format
	ORC Format = "orc"
	// Arrow is Apache Arrow format
	Arrow Format = "arrow"
	// Avro is Apache Avro format
	Avro Format = "avro"
)

// Writer provides columnar format writing capabilities
type Writer interface {
	// WriteRecords writes records in columnar format
	WriteRecords(records []*models.Record) error
	// WriteRecord writes a single record
	WriteRecord(record *models.Record) error
	// Flush flushes any buffered data
	Flush() error
	// Close closes the writer
	Close() error
	// Format returns the columnar format
	Format() Format
	// BytesWritten returns bytes written
	BytesWritten() int64
	// RecordsWritten returns records written
	RecordsWritten() int64
}

// Reader provides columnar format reading capabilities
type Reader interface {
	// ReadRecords reads all records
	ReadRecords() ([]*models.Record, error)
	// Next reads the next record
	Next() (*models.Record, error)
	// HasNext checks if more records exist
	HasNext() bool
	// Close closes the reader
	Close() error
	// Format returns the columnar format
	Format() Format
	// Schema returns the schema if available
	Schema() (*core.Schema, error)
}

// WriterConfig configures columnar writers
type WriterConfig struct {
	Format            Format
	Schema            *core.Schema
	Compression       string
	BatchSize         int
	PageSize          int
	RowGroupSize      int
	DictionarySize    int
	EnableStats       bool
	EnableBloomFilter bool
}

// DefaultWriterConfig returns default writer configuration
func DefaultWriterConfig() *WriterConfig {
	return &WriterConfig{
		Format:            Parquet,
		Compression:       "snappy",
		BatchSize:         10000,
		PageSize:          8192,
		RowGroupSize:      128 * 1024 * 1024, // 128MB
		DictionarySize:    1024 * 1024,       // 1MB
		EnableStats:       true,
		EnableBloomFilter: true,
	}
}

// ReaderConfig configures columnar readers
type ReaderConfig struct {
	Format      Format
	BatchSize   int
	Projection  []string // Column projection
	Filter      string   // Filter expression
	EnableStats bool
}

// DefaultReaderConfig returns default reader configuration
func DefaultReaderConfig() *ReaderConfig {
	return &ReaderConfig{
		Format:      Parquet,
		BatchSize:   10000,
		EnableStats: true,
	}
}

// NewWriter creates a new columnar writer
func NewWriter(w io.Writer, config *WriterConfig) (Writer, error) {
	if config == nil {
		config = DefaultWriterConfig()
	}

	switch config.Format {
	case Parquet:
		return newParquetWriter(w, config)
	case ORC:
		return newORCWriter(w, config)
	case Arrow:
		return newArrowWriter(w, config)
	case Avro:
		return newAvroWriter(w, config)
	default:
		return nil, fmt.Errorf("unsupported columnar format: %s", config.Format)
	}
}

// NewReader creates a new columnar reader
func NewReader(r io.Reader, config *ReaderConfig) (Reader, error) {
	if config == nil {
		config = DefaultReaderConfig()
	}

	switch config.Format {
	case Parquet:
		return newParquetReader(r, config)
	case ORC:
		return newORCReader(r, config)
	case Arrow:
		return newArrowReader(r, config)
	case Avro:
		return newAvroReader(r, config)
	default:
		return nil, fmt.Errorf("unsupported columnar format: %s", config.Format)
	}
}

// FormatInfo provides information about columnar formats
type FormatInfo struct {
	Format           Format
	Name             string
	Description      string
	FileExtension    string
	MIMEType         string
	SupportsSchema   bool
	SupportsCompress bool
	SupportsIndex    bool
	SupportsStats    bool
}

// GetFormatInfo returns information about a columnar format
func GetFormatInfo(format Format) *FormatInfo {
	switch format {
	case Parquet:
		return &FormatInfo{
			Format:           Parquet,
			Name:             "Apache Parquet",
			Description:      "Columnar storage format optimized for analytics",
			FileExtension:    ".parquet",
			MIMEType:         "application/x-parquet",
			SupportsSchema:   true,
			SupportsCompress: true,
			SupportsIndex:    true,
			SupportsStats:    true,
		}
	case ORC:
		return &FormatInfo{
			Format:           ORC,
			Name:             "Apache ORC",
			Description:      "Optimized Row Columnar format",
			FileExtension:    ".orc",
			MIMEType:         "application/x-orc",
			SupportsSchema:   true,
			SupportsCompress: true,
			SupportsIndex:    true,
			SupportsStats:    true,
		}
	case Arrow:
		return &FormatInfo{
			Format:           Arrow,
			Name:             "Apache Arrow",
			Description:      "In-memory columnar format",
			FileExtension:    ".arrow",
			MIMEType:         "application/x-arrow",
			SupportsSchema:   true,
			SupportsCompress: true,
			SupportsIndex:    false,
			SupportsStats:    false,
		}
	case Avro:
		return &FormatInfo{
			Format:           Avro,
			Name:             "Apache Avro",
			Description:      "Row-oriented data serialization format",
			FileExtension:    ".avro",
			MIMEType:         "application/x-avro",
			SupportsSchema:   true,
			SupportsCompress: true,
			SupportsIndex:    false,
			SupportsStats:    false,
		}
	default:
		return nil
	}
}

// SchemaConverter converts between Nebula schema and columnar schema
type SchemaConverter interface {
	// ToColumnar converts Nebula schema to columnar format
	ToColumnar(schema *core.Schema) (interface{}, error)
	// FromColumnar converts columnar schema to Nebula format
	FromColumnar(schema interface{}) (*core.Schema, error)
}

// Statistics provides columnar file statistics
type Statistics struct {
	RowCount      int64
	ColumnCount   int
	FileSize      int64
	Compressed    bool
	MinValues     map[string]interface{}
	MaxValues     map[string]interface{}
	NullCounts    map[string]int64
	DistinctCount map[string]int64
}

// Metadata provides columnar file metadata
type Metadata struct {
	Format      Format
	Version     string
	CreatedBy   string
	Schema      *core.Schema
	RowGroups   int
	Compression string
	Statistics  *Statistics
	CustomMeta  map[string]string
}
