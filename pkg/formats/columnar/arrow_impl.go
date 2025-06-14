// Package columnar provides Arrow format implementation
package columnar

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
)

// arrowWriter implements Writer for Arrow format
type arrowWriter struct {
	writer         io.Writer
	config         *WriterConfig
	arrowSchema    *arrow.Schema
	fileWriter     *ipc.FileWriter
	recordBuilder  *array.RecordBuilder
	bytesWritten   int64
	recordsWritten int64
	currentBatch   int
	mu             sync.Mutex
	pool           memory.Allocator
}

func newArrowWriter(w io.Writer, config *WriterConfig) (*arrowWriter, error) {
	if config.Schema == nil {
		return nil, fmt.Errorf("schema is required for Arrow writer")
	}

	// Convert schema to Arrow schema
	arrowSchema, err := nebulaToArrowSchema(config.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert schema: %w", err)
	}

	pool := memory.NewGoAllocator()

	aw := &arrowWriter{
		writer:      w,
		config:      config,
		arrowSchema: arrowSchema,
		pool:        pool,
	}

	// Create Arrow record builder
	aw.recordBuilder = array.NewRecordBuilder(pool, arrowSchema)

	// Create Arrow file writer
	fw, err := ipc.NewFileWriter(w, ipc.WithSchema(arrowSchema), ipc.WithAllocator(pool))
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow writer: %w", err)
	}
	aw.fileWriter = fw

	return aw, nil
}

func (aw *arrowWriter) WriteRecord(record *models.Record) error {
	aw.mu.Lock()
	defer aw.mu.Unlock()

	// Add record to builder
	for i, field := range aw.arrowSchema.Fields() {
		value := record.Data[field.Name]
		if err := aw.appendValue(i, value, field.Type); err != nil {
			return fmt.Errorf("failed to append value for field %s: %w", field.Name, err)
		}
	}

	aw.currentBatch++

	// Flush if batch is full
	if aw.currentBatch >= aw.config.BatchSize {
		return aw.flushBatch()
	}

	return nil
}

func (aw *arrowWriter) WriteRecords(records []*models.Record) error {
	for _, record := range records {
		if err := aw.WriteRecord(record); err != nil {
			return err
		}
	}
	return nil
}

func (aw *arrowWriter) Flush() error {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	return aw.flushBatch()
}

func (aw *arrowWriter) Close() error {
	// Flush any remaining records
	if err := aw.Flush(); err != nil {
		return err
	}

	aw.mu.Lock()
	defer aw.mu.Unlock()

	// Close Arrow writer
	if err := aw.fileWriter.Close(); err != nil {
		return fmt.Errorf("failed to close Arrow writer: %w", err)
	}

	return nil
}

func (aw *arrowWriter) Format() Format {
	return Arrow
}

func (aw *arrowWriter) BytesWritten() int64 {
	return aw.bytesWritten
}

func (aw *arrowWriter) RecordsWritten() int64 {
	return aw.recordsWritten
}

func (aw *arrowWriter) flushBatch() error {
	if aw.currentBatch == 0 {
		return nil
	}

	// Build record batch
	record := aw.recordBuilder.NewRecord()
	defer record.Release()

	// Write to Arrow file
	if err := aw.fileWriter.Write(record); err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	// Update counters
	aw.recordsWritten += int64(aw.currentBatch)
	aw.currentBatch = 0

	// Track bytes written
	aw.bytesWritten += int64(record.NumRows() * 64) // Estimate

	return nil
}

func (aw *arrowWriter) appendValue(colIdx int, value interface{}, dataType arrow.DataType) error {
	builder := aw.recordBuilder.Field(colIdx)

	if value == nil {
		builder.AppendNull()
		return nil
	}

	// Use the same type conversions as Parquet
	return appendArrowValue(builder, value)
}

// arrowReader implements Reader for Arrow format
type arrowReader struct {
	reader       io.Reader
	config       *ReaderConfig
	fileReader   *ipc.FileReader
	currentBatch arrow.Record
	currentRow   int
	batchIndex   int
	schema       *core.Schema
	mu           sync.Mutex
}

func newArrowReader(r io.Reader, config *ReaderConfig) (*arrowReader, error) {
	// Read all data for Arrow (needs seekable reader in production)
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read Arrow data: %w", err)
	}

	// Create Arrow file reader
	reader, err := ipc.NewFileReader(bytes.NewReader(data), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow reader: %w", err)
	}

	ar := &arrowReader{
		reader:     r,
		config:     config,
		fileReader: reader,
		batchIndex: -1,
	}

	// Convert schema
	ar.schema = arrowToNebulaSchema(reader.Schema())

	return ar, nil
}

func (ar *arrowReader) ReadRecords() ([]*models.Record, error) {
	records := make([]*models.Record, 0)

	for ar.HasNext() {
		record, err := ar.Next()
		if err != nil {
			return nil, err
		}
		if record == nil {
			break
		}
		records = append(records, record)
	}

	return records, nil
}

func (ar *arrowReader) Next() (*models.Record, error) {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	// Load next batch if needed
	if ar.currentBatch == nil || ar.currentRow >= int(ar.currentBatch.NumRows()) {
		if err := ar.loadNextBatch(); err != nil {
			return nil, err
		}
		if ar.currentBatch == nil {
			return nil, nil // EOF
		}
	}

	// Convert Arrow record to Nebula record
	record := ar.rowToRecord(ar.currentRow)
	ar.currentRow++

	return record, nil
}

func (ar *arrowReader) HasNext() bool {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	if ar.currentBatch != nil && ar.currentRow < int(ar.currentBatch.NumRows()) {
		return true
	}

	// Try to load next batch
	if err := ar.loadNextBatch(); err != nil {
		return false
	}

	return ar.currentBatch != nil
}

func (ar *arrowReader) Close() error {
	if ar.currentBatch != nil {
		ar.currentBatch.Release()
	}
	return ar.fileReader.Close()
}

func (ar *arrowReader) Format() Format {
	return Arrow
}

func (ar *arrowReader) Schema() (*core.Schema, error) {
	return ar.schema, nil
}

func (ar *arrowReader) loadNextBatch() error {
	if ar.currentBatch != nil {
		ar.currentBatch.Release()
		ar.currentBatch = nil
	}

	ar.batchIndex++
	if ar.batchIndex >= ar.fileReader.NumRecords() {
		return nil // EOF
	}

	// Read next record batch
	ar.currentBatch, _ = ar.fileReader.Record(ar.batchIndex)
	ar.currentRow = 0

	return nil
}

func (ar *arrowReader) rowToRecord(rowIdx int) *models.Record {
	data := pool.GetMap()

	defer pool.PutMap(data)

	for i := 0; i < int(ar.currentBatch.NumCols()); i++ {
		col := ar.currentBatch.Column(i)
		field := ar.currentBatch.Schema().Field(i)

		value := getArrowColumnValue(col, rowIdx)
		data[field.Name] = value
	}

	return &models.Record{
		Data: data,
		Metadata: models.RecordMetadata{
			Timestamp: time.Now(),
			Custom: map[string]interface{}{
				"format": "arrow",
				"batch":  ar.batchIndex,
				"row":    rowIdx,
			},
		},
	}
}

// Helper function to append values to Arrow builders
func appendArrowValue(builder array.Builder, value interface{}) error {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if v, ok := value.(bool); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}

	case *array.Int64Builder:
		switch v := value.(type) {
		case int:
			b.Append(int64(v))
		case int32:
			b.Append(int64(v))
		case int64:
			b.Append(v)
		default:
			b.AppendNull()
		}

	case *array.Float64Builder:
		switch v := value.(type) {
		case float32:
			b.Append(float64(v))
		case float64:
			b.Append(v)
		default:
			b.AppendNull()
		}

	case *array.StringBuilder:
		if v, ok := value.(string); ok {
			b.Append(v)
		} else {
			b.Append(fmt.Sprintf("%v", value))
		}

	case *array.BinaryBuilder:
		switch v := value.(type) {
		case []byte:
			b.Append(v)
		case string:
			b.Append([]byte(v))
		default:
			b.AppendNull()
		}

	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}

// Helper function to get Arrow column values
func getArrowColumnValue(col arrow.Array, rowIdx int) interface{} {
	if col.IsNull(rowIdx) {
		return nil
	}

	switch c := col.(type) {
	case *array.Boolean:
		return c.Value(rowIdx)
	case *array.Int64:
		return c.Value(rowIdx)
	case *array.Float64:
		return c.Value(rowIdx)
	case *array.String:
		return c.Value(rowIdx)
	case *array.Binary:
		return c.Value(rowIdx)
	default:
		return nil
	}
}
