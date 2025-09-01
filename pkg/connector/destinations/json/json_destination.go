package json

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ajitpratap0/nebula/pkg/compression"
	nebulaConfig "github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
)

// JSONFormat represents the JSON file format
type JSONFormat string

const (
	// JSONArray represents a file containing a JSON array of objects
	JSONArray JSONFormat = "array"
	// JSONLines represents line-delimited JSON (JSONL/NDJSON)
	JSONLines JSONFormat = "lines"
)

// JSONDestination writes data to JSON files
type JSONDestination struct {
	config         *nebulaConfig.BaseConfig
	file           *os.File
	writer         *bufio.Writer
	streamEncoder  *jsonpool.StreamingEncoder
	recordsWritten int64
	bytesWritten   int64
	mu             sync.Mutex
	isArray        bool
	arrayStarted   bool // Reserved for future array formatting support
	firstRecord    bool
	schema         *core.Schema
	filePath       string
	format         JSONFormat
	bufferSize     int
	pretty         bool
	indent         string

	// Compression fields
	compressionEnabled   bool
	compressionAlgorithm compression.Algorithm
	compressionLevel     compression.Level
	compressor           compression.Compressor
	compressionWriter    io.WriteCloser
}

// NewJSONDestination creates a new JSON destination factory function
func NewJSONDestination(config *nebulaConfig.BaseConfig) (core.Destination, error) {
	return &JSONDestination{
		config:      config,
		firstRecord: true,
	}, nil
}

// Initialize initializes the JSON destination
func (d *JSONDestination) Initialize(ctx context.Context, config *nebulaConfig.BaseConfig) error {
	d.config = config

	// Get file path from credentials
	if config.Security.Credentials == nil || config.Security.Credentials["path"] == "" {
		return fmt.Errorf("missing required file path in security.credentials")
	}
	d.filePath = config.Security.Credentials["path"]

	// Get format from credentials or use default
	format := JSONLines // default
	if f, ok := config.Security.Credentials["format"]; ok && f != "" {
		format = JSONFormat(f)
	}
	d.format = format
	d.isArray = format == JSONArray

	// Get pretty print settings from credentials
	d.pretty = false
	if p, ok := config.Security.Credentials["pretty"]; ok && p == "true" {
		d.pretty = true
	}

	d.indent = "  "
	if i, ok := config.Security.Credentials["indent"]; ok && i != "" {
		d.indent = i
	}

	// Use buffer size from config
	d.bufferSize = config.Performance.BufferSize
	if d.bufferSize <= 0 {
		d.bufferSize = 64 * 1024 // default 64KB
	}

	// Extract compression configuration from new config structure
	d.compressionEnabled = config.Advanced.EnableCompression

	if config.Advanced.CompressionAlgorithm != "" {
		d.compressionAlgorithm = compression.Algorithm(config.Advanced.CompressionAlgorithm)
	} else {
		d.compressionAlgorithm = compression.Snappy // default
	}

	// Map compression level from int to compression.Level
	switch config.Advanced.CompressionLevel {
	case 1:
		d.compressionLevel = compression.Fastest
	case 2:
		d.compressionLevel = compression.Better
	case 3:
		d.compressionLevel = compression.Best
	default:
		d.compressionLevel = compression.Default
	}

	// Initialize compressor if compression is enabled
	if d.compressionEnabled {
		compressionConfig := &compression.Config{
			Algorithm: d.compressionAlgorithm,
			Level:     d.compressionLevel,
		}

		var err error
		d.compressor, err = compression.NewCompressor(compressionConfig)
		if err != nil {
			// Log warning and disable compression
			d.compressionEnabled = false
		}
	}

	// Add compression extension if enabled
	if d.compressionEnabled && d.compressor != nil {
		ext := d.getCompressionExtension()
		if ext != "" && !strings.HasSuffix(d.filePath, ext) {
			d.filePath = d.filePath + ext
		}
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(d.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Open file for writing
	file, err := os.Create(d.filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", d.filePath, err)
	}

	d.file = file

	// Wrap with compression if enabled
	var writer io.Writer = file
	if d.compressionEnabled && d.compressor != nil {
		// Create a compression writer that wraps the file with pooled builder
		compressedWriter := &compressionWriter{
			dest:       file,
			compressor: d.compressor,
			builder:    stringpool.GetBuilder(stringpool.Medium),
		}
		writer = compressedWriter
		d.compressionWriter = compressedWriter // Store for cleanup
	}

	d.writer = bufio.NewWriterSize(writer, d.bufferSize)

	// Create streaming encoder
	d.streamEncoder = jsonpool.NewStreamingEncoder(d.writer, d.isArray)
	if d.pretty {
		d.streamEncoder.SetPretty(true, d.indent)
	}

	return nil
}

// CreateSchema creates a schema in the destination
func (d *JSONDestination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	d.schema = schema
	return nil
}

// Write writes records to the JSON file
func (d *JSONDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				// No more records
				if d.isArray {
					// Close the array
					if err := d.closeArray(); err != nil {
						return fmt.Errorf("failed to close array: %w", err)
					}
				}
				if err := d.writer.Flush(); err != nil {
					// Flush errors at stream end are typically not critical
				}
				return nil
			}

			// Write record
			_, err := d.writeRecord(record)
			if err != nil {
				return fmt.Errorf("failed to write record: %w", err)
			}

		case err := <-stream.Errors:
			return err

		case <-ctx.Done():
			if err := d.writer.Flush(); err != nil {
				// Flush errors on context done are typically not critical
			}
			return ctx.Err()
		}
	}
}

// WriteBatch writes a batch of records to the JSON file
func (d *JSONDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	for {
		select {
		case batch, ok := <-stream.Batches:
			if !ok {
				// No more batches
				if d.isArray {
					// Close the array
					if err := d.closeArray(); err != nil {
						return fmt.Errorf("failed to close array: %w", err)
					}
				}
				if err := d.writer.Flush(); err != nil {
					// Flush errors at stream end are typically not critical
				}
				return nil
			}

			// Write batch
			_, _, err := d.writeBatch(ctx, batch)
			if err != nil {
				return fmt.Errorf("failed to write batch: %w", err)
			}

		case err := <-stream.Errors:
			return err

		case <-ctx.Done():
			if err := d.writer.Flush(); err != nil {
				// Flush errors on context done are typically not critical
			}
			return ctx.Err()
		}
	}
}

// writeBatch writes a single batch of records
func (d *JSONDestination) writeBatch(ctx context.Context, records []*models.Record) (int, int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	written := 0
	bytesWritten := int64(0)

	for _, record := range records {
		select {
		case <-ctx.Done():
			return written, bytesWritten, ctx.Err()
		default:
			n, err := d.writeRecord(record)
			if err != nil {
				return written, bytesWritten, fmt.Errorf("failed to write record: %w", err)
			}
			written++
			bytesWritten += int64(n)
		}
	}

	return written, bytesWritten, nil
}

// writeRecord writes a single record
func (d *JSONDestination) writeRecord(record *models.Record) (int, error) {
	// Use the streaming encoder for both array and line-delimited formats
	if err := d.streamEncoder.Encode(record.Data); err != nil {
		return 0, fmt.Errorf("failed to encode record: %w", err)
	}

	// Track first record for array format
	if d.isArray && d.firstRecord {
		d.firstRecord = false
	}

	// Estimate bytes written (actual tracking would require buffering)
	estimatedBytes := len(record.ID) + 100 // Rough estimate
	for k, v := range record.Data {
		estimatedBytes += len(k)
		if s, ok := v.(string); ok {
			estimatedBytes += len(s)
		} else {
			estimatedBytes += 20 // Estimate for other types
		}
	}

	return estimatedBytes, nil
}

// closeArray writes the closing bracket for array format
func (d *JSONDestination) closeArray() error {
	// The streaming encoder handles closing the array
	if d.streamEncoder != nil {
		_ = d.streamEncoder.Close()
		return nil
	}
	return nil
}

// SupportsBulkLoad returns whether the destination supports bulk loading
func (d *JSONDestination) SupportsBulkLoad() bool {
	return false
}

// SupportsTransactions returns whether the destination supports transactions
func (d *JSONDestination) SupportsTransactions() bool {
	return false
}

// SupportsUpsert returns whether the destination supports upsert
func (d *JSONDestination) SupportsUpsert() bool {
	return false
}

// SupportsBatch returns whether the destination supports batch writing
func (d *JSONDestination) SupportsBatch() bool {
	return true
}

// SupportsStreaming returns whether the destination supports streaming
func (d *JSONDestination) SupportsStreaming() bool {
	return true
}

// BulkLoad performs bulk loading
func (d *JSONDestination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	return fmt.Errorf("bulk load not supported")
}

// BeginTransaction begins a transaction
func (d *JSONDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return nil, fmt.Errorf("transactions not supported")
}

// Upsert performs an upsert operation
func (d *JSONDestination) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	return fmt.Errorf("upsert not supported")
}

// AlterSchema alters the schema
func (d *JSONDestination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	return fmt.Errorf("schema alteration not supported")
}

// DropSchema drops a schema
func (d *JSONDestination) DropSchema(ctx context.Context, schema *core.Schema) error {
	return fmt.Errorf("schema drop not supported")
}

// Health checks the health of the destination
func (d *JSONDestination) Health(ctx context.Context) error {
	if d.file == nil {
		return fmt.Errorf("file not opened")
	}
	return nil
}

// Close closes the JSON file
func (d *JSONDestination) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Close streaming encoder first
	if d.streamEncoder != nil {
		d.streamEncoder = nil
	}

	if d.writer != nil {
		// Flush any remaining data  
		_ = d.writer.Flush() // Error ignored during cleanup
	}

	// Close compression writer first if it exists
	if d.compressionWriter != nil {
		d.compressionWriter = nil
	}

	if d.file != nil {
		if err := d.file.Close(); err != nil {
			return err
		}
		d.file = nil
	}

	return nil
}

// Metrics returns metrics for the destination
func (d *JSONDestination) Metrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"type":            "json",
		"format":          string(d.format),
		"records_written": atomic.LoadInt64(&d.recordsWritten),
		"bytes_written":   atomic.LoadInt64(&d.bytesWritten),
	}

	if d.compressionEnabled {
		metrics["compression_enabled"] = true
		metrics["compression_algorithm"] = string(d.compressionAlgorithm)
	}

	return metrics
}

// getCompressionExtension returns the file extension for the compression algorithm
func (d *JSONDestination) getCompressionExtension() string {
	switch d.compressionAlgorithm {
	case compression.Gzip:
		return ".gz"
	case compression.Snappy:
		return ".snappy"
	case compression.LZ4:
		return ".lz4"
	case compression.Zstd:
		return ".zst"
	case compression.S2:
		return ".s2"
	case compression.Deflate:
		return ".deflate"
	default:
		return ""
	}
}

// compressionWriter wraps a writer with compression
type compressionWriter struct {
	dest       io.Writer
	compressor compression.Compressor
	builder    *stringpool.Builder
}

func (cw *compressionWriter) Write(p []byte) (n int, err error) {
	// Write to pooled builder first
	return cw.builder.Write(p)
}

func (cw *compressionWriter) Close() error {
	// Compress all buffered data
	if cw.builder.Len() > 0 {
		// Get the data as bytes for compression
		data := stringpool.StringToBytes(cw.builder.String())
		compressed, err := cw.compressor.Compress(data)
		if err != nil {
			return err
		}
		_, err = cw.dest.Write(compressed)
		if err != nil {
			return err
		}
	}

	// Return builder to pool
	if cw.builder != nil {
		stringpool.PutBuilder(cw.builder, stringpool.Medium)
		cw.builder = nil
	}

	// Don't close underlying writer - let the main Close() method handle it
	return nil
}
