// Package columnar provides Avro implementation
package columnar

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
)

// avroWriter implements Writer for Avro format
type avroWriter struct {
	writer         io.Writer
	config         *WriterConfig
	codec          *goavro.Codec
	ocfWriter      *goavro.OCFWriter
	buffer         []*models.Record
	bytesWritten   int64
	recordsWritten int64
	mu             sync.Mutex
}

func newAvroWriter(w io.Writer, config *WriterConfig) (*avroWriter, error) {
	if config.Schema == nil {
		return nil, fmt.Errorf("schema is required for Avro writer")
	}

	// Convert Nebula schema to Avro schema
	avroSchema := nebulaToAvroSchema(config.Schema)

	// Create Avro codec
	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro codec: %w", err)
	}

	// Create OCF writer
	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               w,
		Codec:           codec,
		CompressionName: getAvroCompression(config.Compression),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro writer: %w", err)
	}

	return &avroWriter{
		writer:    w,
		config:    config,
		codec:     codec,
		ocfWriter: ocfWriter,
		buffer: pool.GetBatchSlice(config.BatchSize),
	}, nil
}

func (aw *avroWriter) WriteRecord(record *models.Record) error {
	aw.mu.Lock()
	defer aw.mu.Unlock()

	aw.buffer = append(aw.buffer, record)

	// Flush if batch is full
	if len(aw.buffer) >= aw.config.BatchSize {
		return aw.flushBatch()
	}

	return nil
}

func (aw *avroWriter) WriteRecords(records []*models.Record) error {
	for _, record := range records {
		if err := aw.WriteRecord(record); err != nil {
			return err
		}
	}
	return nil
}

func (aw *avroWriter) Flush() error {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	return aw.flushBatch()
}

func (aw *avroWriter) Close() error {
	// Flush any remaining records
	if err := aw.Flush(); err != nil {
		return err
	}

	aw.mu.Lock()
	defer aw.mu.Unlock()

	// OCFWriter doesn't have a Close method, just flush remaining data
	return aw.flushBatch()
}

func (aw *avroWriter) Format() Format {
	return Avro
}

func (aw *avroWriter) BytesWritten() int64 {
	return aw.bytesWritten
}

func (aw *avroWriter) RecordsWritten() int64 {
	return aw.recordsWritten
}

func (aw *avroWriter) flushBatch() error {
	if len(aw.buffer) == 0 {
		return nil
	}

	// Convert records to Avro native format
	for _, record := range aw.buffer {
		native := recordToAvroNative(record)
		if err := aw.ocfWriter.Append([]interface{}{native}); err != nil {
			return fmt.Errorf("failed to write Avro record: %w", err)
		}
	}

	// Update counters
	aw.recordsWritten += int64(len(aw.buffer))
	aw.bytesWritten += int64(len(aw.buffer) * 100) // Estimate

	// Clear buffer
	aw.buffer = aw.buffer[:0]

	return nil
}

// avroReader implements Reader for Avro format
type avroReader struct {
	reader    io.Reader
	config    *ReaderConfig
	codec     *goavro.Codec
	ocfReader *goavro.OCFReader
	schema    *core.Schema
	mu        sync.Mutex
}

func newAvroReader(r io.Reader, config *ReaderConfig) (*avroReader, error) {
	// Read all data for Avro
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read Avro data: %w", err)
	}

	// Create OCF reader
	ocfReader, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro reader: %w", err)
	}

	// Get codec from OCF reader
	codec := ocfReader.Codec()

	// Convert Avro schema to Nebula schema
	schema := avroToNebulaSchema(codec.Schema())

	return &avroReader{
		reader:    r,
		config:    config,
		codec:     codec,
		ocfReader: ocfReader,
		schema:    schema,
	}, nil
}

func (ar *avroReader) ReadRecords() ([]*models.Record, error) {
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

func (ar *avroReader) Next() (*models.Record, error) {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	datum, err := ar.ocfReader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}

	// Convert Avro datum to Nebula record
	record := avroNativeToRecord(datum)

	return record, nil
}

func (ar *avroReader) HasNext() bool {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	// Check if more data is available
	return ar.ocfReader.RemainingBlockItems() > 0 || ar.ocfReader.Scan()
}

func (ar *avroReader) Close() error {
	// OCF reader doesn't need explicit close
	return nil
}

func (ar *avroReader) Format() Format {
	return Avro
}

func (ar *avroReader) Schema() (*core.Schema, error) {
	return ar.schema, nil
}

// Schema conversion helpers

func nebulaToAvroSchema(schema *core.Schema) string {
	fields := make([]map[string]interface{}, 0, len(schema.Fields))

	for _, field := range schema.Fields {
		avroType := nebulaToAvroType(field.Type)

		avroField := map[string]interface{}{
			"name": field.Name,
			"type": avroType,
		}

		if field.Nullable {
			avroField["type"] = []interface{}{"null", avroType}
		}

		fields = append(fields, avroField)
	}

	schema_map := map[string]interface{}{
		"type":   "record",
		"name":   schema.Name,
		"fields": fields,
	}

	// Convert to JSON string
	schemaBytes, _ := jsonpool.Marshal(schema_map)
	return string(schemaBytes)
}

func nebulaToAvroType(fieldType core.FieldType) string {
	switch fieldType {
	case core.FieldTypeString:
		return "string"
	case core.FieldTypeInt:
		return "long"
	case core.FieldTypeFloat:
		return "double"
	case core.FieldTypeBool:
		return "boolean"
	case core.FieldTypeTimestamp:
		return "long" // Unix timestamp
	case core.FieldTypeDate:
		return "int" // Days since epoch
	case core.FieldTypeTime:
		return "long" // Nanoseconds since midnight
	case core.FieldTypeBinary:
		return "bytes"
	case core.FieldTypeJSON:
		return "string"
	default:
		return "string"
	}
}

func avroToNebulaSchema(avroSchema string) *core.Schema {
	// Parse Avro schema JSON
	var schemaMap map[string]interface{}
	jsonpool.Unmarshal([]byte(avroSchema), &schemaMap)

	fields := make([]core.Field, 0)

	if fieldsData, ok := schemaMap["fields"].([]interface{}); ok {
		for _, fieldData := range fieldsData {
			if fieldMap, ok := fieldData.(map[string]interface{}); ok {
				field := core.Field{
					Name:     fieldMap["name"].(string),
					Type:     avroTypeToNebula(fieldMap["type"]),
					Nullable: isNullableAvroType(fieldMap["type"]),
				}
				fields = append(fields, field)
			}
		}
	}

	return &core.Schema{
		Name:   schemaMap["name"].(string),
		Fields: fields,
	}
}

func avroTypeToNebula(avroType interface{}) core.FieldType {
	switch t := avroType.(type) {
	case string:
		return avroStringTypeToNebula(t)
	case []interface{}:
		// Union type - find non-null type
		for _, unionType := range t {
			if str, ok := unionType.(string); ok && str != "null" {
				return avroStringTypeToNebula(str)
			}
		}
	}
	return core.FieldTypeString
}

func avroStringTypeToNebula(avroType string) core.FieldType {
	switch avroType {
	case "string":
		return core.FieldTypeString
	case "int", "long":
		return core.FieldTypeInt
	case "float", "double":
		return core.FieldTypeFloat
	case "boolean":
		return core.FieldTypeBool
	case "bytes":
		return core.FieldTypeBinary
	default:
		return core.FieldTypeString
	}
}

func isNullableAvroType(avroType interface{}) bool {
	if arr, ok := avroType.([]interface{}); ok {
		for _, t := range arr {
			if str, ok := t.(string); ok && str == "null" {
				return true
			}
		}
	}
	return false
}

func getAvroCompression(compression string) string {
	switch compression {
	case "snappy":
		return "snappy"
	case "deflate":
		return "deflate"
	case "none":
		return "null"
	default:
		return "snappy"
	}
}

func recordToAvroNative(record *models.Record) map[string]interface{} {
	native := pool.GetMap()

	defer pool.PutMap(native)
	for k, v := range record.Data {
		native[k] = v
	}
	return native
}

func avroNativeToRecord(datum interface{}) *models.Record {
	data := pool.GetMap()

	defer pool.PutMap(data)

	if m, ok := datum.(map[string]interface{}); ok {
		for k, v := range m {
			data[k] = v
		}
	}

	return &models.Record{
		Data: data,
		Metadata: models.RecordMetadata{
			Timestamp: time.Now(),
			Custom: map[string]interface{}{
				"format": "avro",
			},
		},
	}
}
