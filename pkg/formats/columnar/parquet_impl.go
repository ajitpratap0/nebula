// Package columnar provides Parquet implementation
package columnar

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// parquetWriter implements Writer for Parquet format
type parquetWriter struct {
	writer         io.Writer
	config         *WriterConfig
	arrowSchema    *arrow.Schema
	fileWriter     *pqarrow.FileWriter
	recordBuilder  *array.RecordBuilder
	bytesWritten   int64
	recordsWritten int64
	currentBatch   int
	mu             sync.Mutex
}

func newParquetWriter(w io.Writer, config *WriterConfig) (*parquetWriter, error) {
	if config.Schema == nil {
		return nil, fmt.Errorf("schema is required for Parquet writer")
	}

	// Convert schema to Arrow schema
	arrowSchema, err := nebulaToArrowSchema(config.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert schema: %w", err)
	}

	pw := &parquetWriter{
		writer:      w,
		config:      config,
		arrowSchema: arrowSchema,
	}

	// Create Arrow record builder
	pool := memory.NewGoAllocator()
	pw.recordBuilder = array.NewRecordBuilder(pool, arrowSchema)

	// Create Parquet writer properties
	props := parquet.NewWriterProperties(
		parquet.WithCompression(getParquetCompression(config.Compression)),
		parquet.WithDictionaryDefault(config.DictionarySize > 0),
		parquet.WithDataPageSize(int64(config.PageSize)),
	)

	// Create Arrow writer properties
	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithAllocator(pool),
	)

	// Create Parquet file writer
	fw, err := pqarrow.NewFileWriter(arrowSchema, w, props, arrowProps)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	pw.fileWriter = fw

	return pw, nil
}

func (pw *parquetWriter) WriteRecord(record *models.Record) error {
	pw.mu.Lock()
	defer pw.mu.Unlock()

	// Add record to builder
	for i, field := range pw.arrowSchema.Fields() {
		value := record.Data[field.Name]
		if err := pw.appendValue(i, value, field.Type); err != nil {
			return fmt.Errorf("failed to append value for field %s: %w", field.Name, err)
		}
	}

	pw.currentBatch++

	// Flush if batch is full
	if pw.currentBatch >= pw.config.BatchSize {
		return pw.flushBatch()
	}

	return nil
}

func (pw *parquetWriter) WriteRecords(records []*models.Record) error {
	for _, record := range records {
		if err := pw.WriteRecord(record); err != nil {
			return err
		}
	}
	return nil
}

func (pw *parquetWriter) Flush() error {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	return pw.flushBatch()
}

func (pw *parquetWriter) Close() error {
	// Flush any remaining records
	if err := pw.Flush(); err != nil {
		return err
	}

	pw.mu.Lock()
	defer pw.mu.Unlock()

	// Close Parquet writer
	if err := pw.fileWriter.Close(); err != nil {
		return fmt.Errorf("failed to close Parquet writer: %w", err)
	}

	return nil
}

func (pw *parquetWriter) Format() Format {
	return Parquet
}

func (pw *parquetWriter) BytesWritten() int64 {
	return pw.bytesWritten
}

func (pw *parquetWriter) RecordsWritten() int64 {
	return pw.recordsWritten
}

func (pw *parquetWriter) flushBatch() error {
	if pw.currentBatch == 0 {
		return nil
	}

	// Build record batch
	record := pw.recordBuilder.NewRecord()
	defer record.Release()

	// Write to Parquet
	if err := pw.fileWriter.WriteBuffered(record); err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	// Update counters
	pw.recordsWritten += int64(pw.currentBatch)
	pw.currentBatch = 0

	// TODO: Track bytes written accurately
	pw.bytesWritten += int64(record.NumRows() * 100) // Estimate

	return nil
}

func (pw *parquetWriter) appendValue(colIdx int, value interface{}, dataType arrow.DataType) error {
	builder := pw.recordBuilder.Field(colIdx)

	if value == nil {
		builder.AppendNull()
		return nil
	}

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

	case *array.TimestampBuilder:
		switch v := value.(type) {
		case time.Time:
			b.Append(arrow.Timestamp(v.UnixNano()))
		case string:
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				b.Append(arrow.Timestamp(t.UnixNano()))
			} else {
				b.AppendNull()
			}
		default:
			b.AppendNull()
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

// parquetReader implements Reader for Parquet format
type parquetReader struct {
	reader       io.Reader
	config       *ReaderConfig
	fileReader   *file.Reader
	arrowReader  *pqarrow.FileReader
	currentBatch arrow.Record
	currentRow   int
	totalRows    int64
	schema       *core.Schema
	mu           sync.Mutex
}

func newParquetReader(r io.Reader, config *ReaderConfig) (*parquetReader, error) {
	// Read all data for Parquet (needs seekable reader in production)
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read Parquet data: %w", err)
	}

	// Create Parquet file reader
	fr, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet reader: %w", err)
	}

	// Create Arrow reader
	pool := memory.NewGoAllocator()
	arrowReader, err := pqarrow.NewFileReader(fr, pqarrow.ArrowReadProperties{}, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow reader: %w", err)
	}

	pr := &parquetReader{
		reader:      r,
		config:      config,
		fileReader:  fr,
		arrowReader: arrowReader,
		totalRows:   fr.NumRows(),
	}

	// Convert schema
	arrowSchema, err := arrowReader.Schema()
	if err != nil {
		return nil, fmt.Errorf("failed to get Arrow schema: %w", err)
	}

	pr.schema = arrowToNebulaSchema(arrowSchema)

	return pr, nil
}

func (pr *parquetReader) ReadRecords() ([]*models.Record, error) {
	records := pool.GetBatchSlice(int(pr.totalRows))

	defer pool.PutBatchSlice(records)

	for pr.HasNext() {
		record, err := pr.Next()
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

func (pr *parquetReader) Next() (*models.Record, error) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	// Load next batch if needed
	if pr.currentBatch == nil || pr.currentRow >= int(pr.currentBatch.NumRows()) {
		if err := pr.loadNextBatch(); err != nil {
			return nil, err
		}
		if pr.currentBatch == nil {
			return nil, nil // EOF
		}
	}

	// Convert Arrow record to Nebula record
	record := pr.rowToRecord(pr.currentRow)
	pr.currentRow++

	return record, nil
}

func (pr *parquetReader) HasNext() bool {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if pr.currentBatch != nil && pr.currentRow < int(pr.currentBatch.NumRows()) {
		return true
	}

	// Try to load next batch
	if err := pr.loadNextBatch(); err != nil {
		return false
	}

	return pr.currentBatch != nil
}

func (pr *parquetReader) Close() error {
	if pr.currentBatch != nil {
		pr.currentBatch.Release()
	}
	return pr.fileReader.Close()
}

func (pr *parquetReader) Format() Format {
	return Parquet
}

func (pr *parquetReader) Schema() (*core.Schema, error) {
	return pr.schema, nil
}

func (pr *parquetReader) loadNextBatch() error {
	if pr.currentBatch != nil {
		pr.currentBatch.Release()
		pr.currentBatch = nil
	}

	// Read next row group
	rr, err := pr.arrowReader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		return err
	}

	if rr.Next() {
		pr.currentBatch = rr.Record()
		pr.currentRow = 0
		return nil
	}

	return nil // EOF
}

func (pr *parquetReader) rowToRecord(rowIdx int) *models.Record {
	data := pool.GetMap()

	defer pool.PutMap(data)

	for i := 0; i < int(pr.currentBatch.NumCols()); i++ {
		col := pr.currentBatch.Column(i)
		field := pr.currentBatch.Schema().Field(i)

		value := pr.getColumnValue(col, rowIdx)
		data[field.Name] = value
	}

	return &models.Record{
		Data: data,
		Metadata: models.RecordMetadata{
			Timestamp: time.Now(),
			Custom: map[string]interface{}{
				"format": "parquet",
				"row":    rowIdx,
			},
		},
	}
}

func (pr *parquetReader) getColumnValue(col arrow.Array, rowIdx int) interface{} {
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
	case *array.Timestamp:
		return time.Unix(0, int64(c.Value(rowIdx)))
	default:
		return nil
	}
}

// Schema conversion helpers

func nebulaToArrowSchema(schema *core.Schema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(schema.Fields))

	for _, field := range schema.Fields {
		arrowType, err := nebulaToArrowType(field.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", field.Name, err)
		}

		arrowField := arrow.Field{
			Name:     field.Name,
			Type:     arrowType,
			Nullable: field.Nullable,
		}
		fields = append(fields, arrowField)
	}

	return arrow.NewSchema(fields, nil), nil
}

func nebulaToArrowType(fieldType core.FieldType) (arrow.DataType, error) {
	switch fieldType {
	case core.FieldTypeString:
		return arrow.BinaryTypes.String, nil
	case core.FieldTypeInt:
		return arrow.PrimitiveTypes.Int64, nil
	case core.FieldTypeFloat:
		return arrow.PrimitiveTypes.Float64, nil
	case core.FieldTypeBool:
		return arrow.FixedWidthTypes.Boolean, nil
	case core.FieldTypeTimestamp:
		return arrow.FixedWidthTypes.Timestamp_ns, nil
	case core.FieldTypeDate:
		return arrow.FixedWidthTypes.Date32, nil
	case core.FieldTypeTime:
		return arrow.FixedWidthTypes.Time64ns, nil
	case core.FieldTypeBinary:
		return arrow.BinaryTypes.Binary, nil
	case core.FieldTypeJSON:
		return arrow.BinaryTypes.String, nil // JSON as string
	default:
		return nil, fmt.Errorf("unsupported field type: %s", fieldType)
	}
}

func arrowToNebulaSchema(arrowSchema *arrow.Schema) *core.Schema {
	fields := make([]core.Field, 0, arrowSchema.NumFields())

	for i := 0; i < arrowSchema.NumFields(); i++ {
		field := arrowSchema.Field(i)
		nebulaType := arrowToNebulaType(field.Type)

		fields = append(fields, core.Field{
			Name:     field.Name,
			Type:     nebulaType,
			Nullable: field.Nullable,
		})
	}

	return &core.Schema{
		Name:   "parquet_schema",
		Fields: fields,
	}
}

func arrowToNebulaType(arrowType arrow.DataType) core.FieldType {
	switch arrowType.ID() {
	case arrow.BOOL:
		return core.FieldTypeBool
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return core.FieldTypeInt
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return core.FieldTypeFloat
	case arrow.STRING, arrow.LARGE_STRING:
		return core.FieldTypeString
	case arrow.BINARY, arrow.LARGE_BINARY:
		return core.FieldTypeBinary
	case arrow.DATE32, arrow.DATE64:
		return core.FieldTypeDate
	case arrow.TIMESTAMP:
		return core.FieldTypeTimestamp
	case arrow.TIME32, arrow.TIME64:
		return core.FieldTypeTime
	default:
		return core.FieldTypeString // Default to string
	}
}

func getParquetCompression(compression string) compress.Compression {
	// Note: Using file.WithCompressionFor() when creating writer instead
	// This function is kept for compatibility but compression is set differently
	return compress.Codecs.Snappy
}
