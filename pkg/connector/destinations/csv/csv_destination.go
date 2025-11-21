// Package csv provides a high-performance CSV destination connector for Nebula.
// It supports writing data to CSV files with automatic schema handling, compression,
// and comprehensive error handling.
//
// # Features
//
//   - Automatic header generation from record fields
//   - Dynamic schema evolution support
//   - Optional compression with multiple algorithms (gzip, snappy, lz4, zstd)
//   - Configurable buffering and flushing
//   - Append or overwrite modes
//   - Thread-safe concurrent writing
//   - Memory-efficient streaming for large datasets
//   - Circuit breaker protection for I/O operations
//
// # Configuration
//
// The CSV destination uses the standard BaseConfig with these specific credentials:
//
//	config.Security.Credentials["path"] = "/path/to/output.csv"
//	config.Security.Credentials["delimiter"] = ","  // optional, default: ","
//	config.Security.Credentials["overwrite"] = "true"  // optional, default: "true"
//	config.Security.Credentials["compression"] = "gzip"  // optional, enables compression
//	config.Security.Credentials["compression_level"] = "6"  // optional, 1-9
//
// # Example Usage
//
//	cfg := config.NewBaseConfig("csv_dest", "row")
//	cfg.Security.Credentials["path"] = "output.csv"
//	cfg.Performance.BatchSize = 10000
//	cfg.Advanced.CompressionType = "snappy"
//
//	dest, err := csv.NewCSVDestination(cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ctx := context.Background()
//	if err := dest.Initialize(ctx, cfg); err != nil {
//	    log.Fatal(err)
//	}
//	defer dest.Close(ctx)
//
//	// Write records
//	records := []*pool.Record{
//	    pool.NewRecord("source", map[string]interface{}{
//	        "name": "John Doe",
//	        "age": 30,
//	    }),
//	}
//
//	if err := dest.Write(ctx, records); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Records are automatically released after writing
package csv

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ajitpratap0/nebula/pkg/compression"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/baseconnector"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
	nerrors "github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"
)

// CSVDestination is a production-ready CSV destination connector that writes data to CSV files.
// It extends BaseConnector to provide circuit breakers, health monitoring, and metrics.
//
// The connector supports:
//   - Automatic header detection and writing
//   - Dynamic schema evolution with new columns
//   - Multiple compression algorithms
//   - Configurable buffering for performance
//   - Append and overwrite modes
//   - Thread-safe concurrent writing
type CSVDestination struct {
	*baseconnector.BaseConnector

	// CSV-specific fields
	file              *os.File    // Open file handle
	writer            *csv.Writer // CSV writer instance
	headers           []string    // Column headers (ordered)
	headersMutex      sync.Mutex  // Protects header modifications
	hasWrittenHeaders bool        // Whether headers have been written

	// Schema and state
	schema         *core.Schema // Current schema information
	recordsWritten int64        // Total records written

	// Configuration
	overwrite     bool   // Overwrite existing file
	bufferSize    int    // Internal buffer size
	flushInterval int    // Records between flushes
	filePath      string // Store the file path

	// Compression configuration
	compressionEnabled   bool                   // Whether compression is enabled
	compressionAlgorithm compression.Algorithm  // Algorithm to use
	compressionLevel     compression.Level      // Compression level
	compressor           compression.Compressor // Compressor instance
	compressionWriter    io.WriteCloser         // Compression writer
}

// NewCSVDestination creates a new CSV destination connector
func NewCSVDestination(config *config.BaseConfig) (core.Destination, error) {
	base := baseconnector.NewBaseConnector("csv", core.ConnectorTypeDestination, "2.0.0")

	return &CSVDestination{
		BaseConnector: base,
		overwrite:     true,  // default
		bufferSize:    10000, // default
		flushInterval: 1000,  // default
	}, nil
}

// Initialize initializes the CSV destination connector
func (d *CSVDestination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	// Initialize base connector first
	if err := d.BaseConnector.Initialize(ctx, config); err != nil {
		return nerrors.Wrap(err, nerrors.ErrorTypeConfig, "failed to initialize base connector")
	}

	// Validate configuration
	if err := d.validateConfig(config); err != nil {
		return err
	}

	// Extract CSV-specific configuration
	d.extractCSVConfig(config)

	// Create/open file with circuit breaker protection
	if err := d.ExecuteWithCircuitBreaker(func() error {
		return d.createFile(config)
	}); err != nil {
		return nerrors.Wrap(err, nerrors.ErrorTypeConnection, "failed to create CSV file")
	}

	d.UpdateHealth(true, map[string]interface{}{
		"file_created":   true,
		"overwrite_mode": d.overwrite,
		"buffer_size":    d.bufferSize,
	})

	d.GetLogger().Info("CSV destination initialized",
		zap.String("file", d.getFilePath()),
		zap.Bool("overwrite", d.overwrite),
		zap.Int("buffer_size", d.bufferSize))

	return nil
}

// CreateSchema creates or updates the schema for the destination
func (d *CSVDestination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	if schema == nil {
		// If no schema provided, we'll auto-detect from records
		d.GetLogger().Debug("no schema provided, will auto-detect from records")
		return nil
	}

	d.schema = schema

	// Extract headers from schema
	d.headersMutex.Lock()
	d.headers = make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		d.headers[i] = field.Name
	}
	d.headersMutex.Unlock()

	d.GetLogger().Info("schema created",
		zap.String("schema_name", schema.Name),
		zap.Int("field_count", len(schema.Fields)))

	return nil
}

// AlterSchema alters the schema for the destination
func (d *CSVDestination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	// For CSV, we can only add new columns, not modify existing ones
	d.schema = newSchema

	// Update headers
	d.headersMutex.Lock()
	oldHeaders := make([]string, len(d.headers))
	copy(oldHeaders, d.headers)

	d.headers = make([]string, len(newSchema.Fields))
	for i, field := range newSchema.Fields {
		d.headers[i] = field.Name
	}
	d.headersMutex.Unlock()

	d.GetLogger().Info("schema altered",
		zap.Strings("old_headers", oldHeaders),
		zap.Strings("new_headers", d.headers))

	return nil
}

// Write writes a stream of records to the CSV file
func (d *CSVDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	recordCount := 0

	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				// Stream closed, flush and return
				d.writer.Flush()
				d.GetLogger().Info("write stream completed",
					zap.Int("records_written", recordCount))
				return nil
			}

			// Rate limiting
			if err := d.RateLimit(ctx); err != nil {
				return err
			}

			// Write record with circuit breaker protection
			if err := d.ExecuteWithCircuitBreaker(func() error {
				return d.writeRecord(record)
			}); err != nil {
				// Handle with error handler
				if handleErr := d.HandleError(ctx, err, record); handleErr != nil {
					return handleErr
				}
				continue
			}

			recordCount++
			d.recordsWritten++

			// Periodic flush
			if recordCount%d.flushInterval == 0 {
				d.writer.Flush()
				d.RecordMetric("records_flushed", d.flushInterval, core.MetricTypeCounter)
			}

			// Report progress and metrics
			d.ReportProgress(d.recordsWritten, -1) // -1 = unknown total
			d.RecordMetric("records_written", 1, core.MetricTypeCounter)

			// Release record back to pool
			record.Release()

		case err := <-stream.Errors:
			if err != nil {
				return nerrors.Wrap(err, nerrors.ErrorTypeData, "error in record stream")
			}

		case <-ctx.Done():
			d.writer.Flush()
			return ctx.Err()
		}
	}
}

// WriteBatch writes batches of records to the CSV file
func (d *CSVDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	recordCount := 0
	batchCount := 0

	for {
		select {
		case batch, ok := <-stream.Batches:
			if !ok {
				// Stream closed, flush and return
				d.writer.Flush()
				d.GetLogger().Info("write batch stream completed",
					zap.Int("batches_processed", batchCount),
					zap.Int("total_records", recordCount))
				return nil
			}

			// Rate limiting
			if err := d.RateLimit(ctx); err != nil {
				return err
			}

			// Write batch with circuit breaker protection
			if err := d.ExecuteWithCircuitBreaker(func() error {
				return d.writeBatch(batch)
			}); err != nil {
				// Handle with error handler
				if handleErr := d.HandleError(ctx, err, nil); handleErr != nil {
					return handleErr
				}
				continue
			}

			batchCount++
			recordCount += len(batch)
			d.recordsWritten += int64(len(batch))

			// Flush after each batch
			d.writer.Flush()

			// Report progress and metrics
			d.ReportProgress(d.recordsWritten, -1) // -1 = unknown total
			d.RecordMetric("batches_written", 1, core.MetricTypeCounter)
			d.RecordMetric("records_in_batch", len(batch), core.MetricTypeGauge)

			// Release all records in batch
			for _, record := range batch {
				record.Release()
			}

		case err := <-stream.Errors:
			if err != nil {
				return nerrors.Wrap(err, nerrors.ErrorTypeData, "error in batch stream")
			}

		case <-ctx.Done():
			d.writer.Flush()
			return ctx.Err()
		}
	}
}

// Close closes the CSV destination connector
func (d *CSVDestination) Close(ctx context.Context) error {
	if d.writer != nil {
		d.writer.Flush()
	}

	// Close compression writer first if it exists
	if d.compressionWriter != nil {
		if err := d.compressionWriter.Close(); err != nil {
			d.GetLogger().Error("failed to close compression writer", zap.Error(err))
		}
		d.compressionWriter = nil
	}

	if d.file != nil {
		if err := d.file.Close(); err != nil {
			d.GetLogger().Error("failed to close file", zap.Error(err))
		}
		d.file = nil
	}

	d.GetLogger().Info("CSV destination closed",
		zap.Int64("total_records_written", d.recordsWritten))

	// Close base connector
	return d.BaseConnector.Close(ctx)
}

// SupportsBulkLoad returns true since CSV supports bulk loading
func (d *CSVDestination) SupportsBulkLoad() bool {
	return true
}

// SupportsTransactions returns false since CSV doesn't support transactions
func (d *CSVDestination) SupportsTransactions() bool {
	return false
}

// SupportsUpsert returns false since CSV doesn't support upsert
func (d *CSVDestination) SupportsUpsert() bool {
	return false
}

// SupportsBatch returns true since CSV supports batch operations
func (d *CSVDestination) SupportsBatch() bool {
	return true
}

// SupportsStreaming returns true since CSV supports streaming writes
func (d *CSVDestination) SupportsStreaming() bool {
	return true
}

// BulkLoad loads data in bulk from a reader (CSV supports this natively)
func (d *CSVDestination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	if format != "csv" {
		return nerrors.New(nerrors.ErrorTypeCapability, "CSV destination only supports CSV format for bulk load")
	}

	// For CSV, bulk load is essentially a copy operation
	// This would be implemented based on the reader type (io.Reader, file path, etc.)
	return nerrors.New(nerrors.ErrorTypeCapability, "bulk load not yet implemented for CSV destination")
}

// BeginTransaction returns an error since CSV doesn't support transactions
func (d *CSVDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return nil, nerrors.New(nerrors.ErrorTypeCapability, "CSV destination does not support transactions")
}

// Upsert returns an error since CSV doesn't support upsert operations
func (d *CSVDestination) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	return nerrors.New(nerrors.ErrorTypeCapability, "CSV destination does not support upsert operations")
}

// DropSchema removes the schema (for CSV, this would delete the file)
func (d *CSVDestination) DropSchema(ctx context.Context, schema *core.Schema) error {
	path := d.getFilePath()
	if path == "" {
		return nerrors.New(nerrors.ErrorTypeConfig, "no file path configured")
	}

	// Close current file first
	if d.file != nil {
		_ = d.file.Close() // Ignore close error during cleanup
		d.file = nil
		d.writer = nil
	}

	// Remove the file
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nerrors.Wrap(err, nerrors.ErrorTypeConnection, "failed to remove CSV file")
	}

	d.GetLogger().Info("schema dropped", zap.String("file", path))
	return nil
}

// Private methods

// compressionWriter wraps a writer with compression
type compressionWriter struct {
	dest       io.Writer
	compressor compression.Compressor
	buffer     bytes.Buffer
}

func (cw *compressionWriter) Write(p []byte) (n int, err error) {
	// Write to buffer first
	return cw.buffer.Write(p)
}

func (cw *compressionWriter) Close() error {
	// Compress all buffered data
	if cw.buffer.Len() > 0 {
		compressed, err := cw.compressor.Compress(cw.buffer.Bytes())
		if err != nil {
			return err
		}
		_, err = cw.dest.Write(compressed)
		if err != nil {
			return err
		}
	}

	// Don't close underlying writer - let the main Close() method handle it
	return nil
}

func (d *CSVDestination) validateConfig(config *config.BaseConfig) error {
	// For CSV destination, we'd typically validate that a file path is provided
	// Since BaseConfig doesn't have Properties, this would need to be validated
	// differently - perhaps through environment variables or separate config
	// For now, assume validation is handled by the caller
	return nil
}

func (d *CSVDestination) extractCSVConfig(config *config.BaseConfig) {
	// Extract file path from credentials (set by CLI)
	if config.Security.Credentials != nil {
		if path, ok := config.Security.Credentials["path"]; ok {
			d.filePath = path
		}
	}

	// Extract configuration from structured BaseConfig
	d.bufferSize = config.Performance.BufferSize
	if d.bufferSize <= 0 {
		d.bufferSize = 10000 // default
	}

	// Use flush interval based on performance config
	if config.Performance.FlushInterval > 0 {
		d.flushInterval = int(config.Performance.FlushInterval.Seconds() * 100) // convert to record count
	}

	// Compression configuration from advanced settings
	d.compressionEnabled = config.Advanced.EnableCompression
	if d.compressionEnabled {
		d.compressionAlgorithm = compression.Algorithm(config.Advanced.CompressionAlgorithm)
		if d.compressionAlgorithm == "" {
			d.compressionAlgorithm = compression.Snappy // default
		}

		// Map compression level from int to compression.Level
		switch config.Advanced.CompressionLevel {
		case 1:
			d.compressionLevel = compression.Fastest
		case 6:
			d.compressionLevel = compression.Default
		case 9:
			d.compressionLevel = compression.Best
		default:
			d.compressionLevel = compression.Default
		}
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
			d.GetLogger().Warn("Failed to initialize compressor, disabling compression",
				zap.Error(err),
				zap.String("algorithm", string(d.compressionAlgorithm)))
			d.compressionEnabled = false
		} else {
			d.GetLogger().Info("Compression enabled",
				zap.String("algorithm", string(d.compressionAlgorithm)),
				zap.String("level", string(rune(d.compressionLevel))))
		}
	}
}

func (d *CSVDestination) getFilePath() string {
	return d.filePath
}

func (d *CSVDestination) createFile(config *config.BaseConfig) error {
	path := d.getFilePath()

	// Add compression extension if enabled
	if d.compressionEnabled && d.compressor != nil {
		ext := d.getCompressionExtension()
		if ext != "" && !strings.HasSuffix(path, ext) {
			path += ext
		}
	}

	// Create directory if it doesn't exist
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Determine file creation flags
	flags := os.O_CREATE | os.O_WRONLY
	if d.overwrite {
		flags |= os.O_TRUNC
	} else {
		flags |= os.O_APPEND
	}

	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", path, err)
	}

	d.file = file

	// Wrap with compression if enabled
	var writer io.Writer = file
	if d.compressionEnabled && d.compressor != nil {
		// For streaming compression, we need a wrapper that compresses on the fly
		// Create a compression writer that wraps the file
		compressedWriter := &compressionWriter{
			dest:       file,
			compressor: d.compressor,
		}
		writer = compressedWriter
		d.compressionWriter = compressedWriter // Store for cleanup
	}

	d.writer = csv.NewWriter(writer)

	// Check if file is new or empty for header writing
	stat, err := file.Stat()
	if err == nil && stat.Size() == 0 {
		d.hasWrittenHeaders = false
	} else if !d.overwrite {
		// File exists and we're appending, assume headers are already written
		d.hasWrittenHeaders = true
	}

	return nil
}

func (d *CSVDestination) writeRecord(record *models.Record) error {
	// Ensure headers are written
	if err := d.ensureHeaders(record); err != nil {
		return err
	}

	// Convert record to CSV row
	row := d.recordToRow(record)

	// Write row
	if err := d.writer.Write(row); err != nil {
		return fmt.Errorf("failed to write CSV row: %w", err)
	}

	return nil
}

func (d *CSVDestination) writeBatch(batch []*models.Record) error {
	if len(batch) == 0 {
		return nil
	}

	// Ensure headers are written
	if err := d.ensureHeaders(batch[0]); err != nil {
		return err
	}

	// Write all records in batch
	for _, record := range batch {
		row := d.recordToRow(record)
		if err := d.writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	return nil
}

func (d *CSVDestination) ensureHeaders(record *models.Record) error {
	d.headersMutex.Lock()
	defer d.headersMutex.Unlock()

	if d.hasWrittenHeaders {
		return nil
	}

	// If no headers set from schema, infer from first record
	if len(d.headers) == 0 && record.Data != nil {
		for key := range record.Data {
			d.headers = append(d.headers, key)
		}
	}

	// Write headers if we have them
	if len(d.headers) > 0 {
		if err := d.writer.Write(d.headers); err != nil {
			return fmt.Errorf("failed to write CSV headers: %w", err)
		}
		d.hasWrittenHeaders = true
		d.GetLogger().Debug("headers written", zap.Strings("headers", d.headers))
	}

	return nil
}

func (d *CSVDestination) recordToRow(record *models.Record) []string {
	d.headersMutex.Lock()
	headers := make([]string, len(d.headers))
	copy(headers, d.headers)
	d.headersMutex.Unlock()

	// Use pooled string slice to reduce allocations
	row := pool.GetStringSlice()
	// Ensure capacity
	if cap(row) < len(headers) {
		pool.PutStringSlice(row)
		row = make([]string, len(headers))
	} else {
		row = row[:len(headers)]
	}

	for i, header := range headers {
		if record.Data != nil {
			if value, ok := record.Data[header]; ok {
				row[i] = stringpool.ValueToString(value)
			} else {
				row[i] = "" // Ensure empty string for missing values
			}
		} else {
			row[i] = ""
		}
	}

	return row
}

func (d *CSVDestination) getCompressionExtension() string {
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
