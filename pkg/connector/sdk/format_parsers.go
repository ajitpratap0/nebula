package sdk

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	gojson "github.com/goccy/go-json"
	"go.uber.org/zap"

	"github.com/ajitpratap0/nebula/pkg/compression"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
)

// FormatParser provides parsing capabilities for different file formats
type FormatParser struct {
	logger     *zap.Logger
	compressor compression.Compressor
}

// NewFormatParser creates a new format parser
func NewFormatParser() *FormatParser {
	compressor, err := compression.NewCompressor(&compression.Config{})
	if err != nil {
		// Use no compression as fallback
		compressor, _ = compression.NewCompressor(&compression.Config{Algorithm: compression.None})
	}
	return &FormatParser{
		logger:     GetGlobalSDK().logger.With(zap.String("component", "format_parser")),
		compressor: compressor,
	}
}

// ParseResult contains the result of parsing operations
type ParseResult struct {
	Records  []*models.Record       `json:"records"`
	Schema   *core.Schema           `json:"schema"`
	Metadata map[string]interface{} `json:"metadata"`
	Errors   []error                `json:"errors"`
}

// Release returns the pooled resources used by ParseResult
func (pr *ParseResult) Release() {
	// Return the metadata map to the pool if it's not nil
	if pr.Metadata != nil {
		pool.PutMap(pr.Metadata)
		pr.Metadata = nil
	}
	// Clear slices to help GC (but don't return them to pool as they may have varying sizes)
	pr.Records = nil
	pr.Errors = nil
}

// CSVParserConfig configures CSV parsing behavior
type CSVParserConfig struct {
	Delimiter           rune                 `json:"delimiter"`
	Comment             rune                 `json:"comment"`
	HasHeader           bool                 `json:"has_header"`
	SkipRows            int                  `json:"skip_rows"`
	MaxRows             int                  `json:"max_rows"`
	BufferSize          int                  `json:"buffer_size"`
	EnableTypeInference bool                 `json:"enable_type_inference"`
	TypeInferenceConfig *TypeInferenceConfig `json:"type_inference_config"`
	CompressionConfig   *compression.Config  `json:"compression_config"`
	Encoding            string               `json:"encoding"`
}

// DefaultCSVParserConfig returns default CSV parser configuration
func DefaultCSVParserConfig() *CSVParserConfig {
	return &CSVParserConfig{
		Delimiter:           ',',
		Comment:             '#',
		HasHeader:           true,
		SkipRows:            0,
		MaxRows:             0, // 0 means no limit
		BufferSize:          64 * 1024,
		EnableTypeInference: true,
		TypeInferenceConfig: DefaultTypeInferenceConfig(),
		CompressionConfig:   &compression.Config{Algorithm: compression.None},
		Encoding:            "utf-8",
	}
}

// CSVParser provides CSV parsing capabilities with support for
// large files, streaming, and automatic schema inference.
type CSVParser struct {
	*FormatParser
	config   *CSVParserConfig
	reader   *csv.Reader
	headers  []string
	rowCount int64
}

// NewCSVParser creates a new CSV parser with the given configuration.
// Uses default configuration if nil is provided.
func NewCSVParser(config *CSVParserConfig) *CSVParser {
	if config == nil {
		config = DefaultCSVParserConfig()
	}

	return &CSVParser{
		FormatParser: NewFormatParser(),
		config:       config,
	}
}

// ParseFile parses a CSV file and returns records and schema
func (cp *CSVParser) ParseFile(ctx context.Context, filePath string) (*ParseResult, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeFile, "failed to open CSV file")
	}
	defer file.Close() // Ignore close error //nolint:errcheck // File close errors in defer are usually not actionable

	return cp.ParseReader(ctx, file)
}

// ParseReader parses CSV from an io.Reader
func (cp *CSVParser) ParseReader(ctx context.Context, reader io.Reader) (*ParseResult, error) {
	// Handle decompression if configured
	if cp.config.CompressionConfig.Algorithm != compression.None {
		// Create a pipe for decompression
		pr, pw := io.Pipe()

		// Start decompression in a goroutine
		errChan := make(chan error, 1)
		go func() {
			defer func() {
				if err := pw.Close(); err != nil {
					// Pipe writer close errors can be ignored in this context
					// as the decompression process is completing
				}
			}()
			err := cp.compressor.DecompressStream(pw, reader)
			errChan <- err
		}()

		// Check for decompression errors
		select {
		case err := <-errChan:
			if err != nil {
				_ = pr.Close() // Close reader on error
				return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to decompress CSV data")
			}
		default:
			// Continue with decompressed reader
		}

		reader = pr
	}

	// Create buffered reader for performance
	bufferedReader := bufio.NewReaderSize(reader, cp.config.BufferSize)

	// Setup CSV reader
	cp.reader = csv.NewReader(bufferedReader)
	cp.reader.Comma = cp.config.Delimiter
	cp.reader.Comment = cp.config.Comment
	cp.reader.ReuseRecord = true // Optimize memory usage

	// Pre-allocate slices and use pooled map for better performance
	result := &ParseResult{
		Records:  pool.GetBatchSlice(100), // Pre-allocate for typical batch size
		Metadata: pool.GetMap(),           // Use pooled map
		Errors:   make([]error, 0, 10),    // Pre-allocate for common error count
	}

	// Skip initial rows if configured
	for i := 0; i < cp.config.SkipRows; i++ {
		if _, err := cp.reader.Read(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to skip CSV row")
		}
	}

	// Read headers if configured
	if cp.config.HasHeader {
		headers, err := cp.reader.Read()
		if err != nil {
			return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to read CSV headers")
		}
		cp.headers = make([]string, len(headers))
		copy(cp.headers, headers)
	}

	// Parse data rows
	var sampleRecords []*models.Record
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		record, err := cp.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			result.Errors = append(result.Errors, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to read CSV row"))
			continue
		}

		modelRecord, err := cp.rowToRecord(record, cp.rowCount)
		if err != nil {
			result.Errors = append(result.Errors, err)
			continue
		}

		result.Records = append(result.Records, modelRecord)

		// Collect samples for type inference
		if cp.config.EnableTypeInference && len(sampleRecords) < cp.config.TypeInferenceConfig.SampleSize {
			sampleRecords = append(sampleRecords, modelRecord)
		}

		cp.rowCount++

		// Check max rows limit
		if cp.config.MaxRows > 0 && cp.rowCount >= int64(cp.config.MaxRows) {
			break
		}
	}

	// Perform type inference if enabled and we have samples
	if cp.config.EnableTypeInference && len(sampleRecords) > 0 {
		schema, err := cp.inferSchema(sampleRecords)
		if err != nil {
			cp.logger.Warn("Failed to infer schema", zap.Error(err))
		} else {
			result.Schema = schema
		}
	}

	// Add metadata
	result.Metadata["format"] = "csv"
	result.Metadata["rows_parsed"] = cp.rowCount
	result.Metadata["headers"] = cp.headers
	result.Metadata["delimiter"] = string(cp.config.Delimiter)
	result.Metadata["has_header"] = cp.config.HasHeader

	cp.logger.Info("CSV parsing completed",
		zap.Int64("rows_parsed", cp.rowCount),
		zap.Int("records_created", len(result.Records)),
		zap.Int("errors", len(result.Errors)))

	return result, nil
}

// rowToRecord converts a CSV row to a models.Record
func (cp *CSVParser) rowToRecord(row []string, rowIndex int64) (*models.Record, error) {
	// Use pooled record
	record := models.NewRecordFromPool("csv_parser")
	record.ID = fmt.Sprintf("csv_row_%d", rowIndex)

	// Populate data using SetData to preserve pooled map
	for i, value := range row {
		var fieldName string
		if cp.config.HasHeader && i < len(cp.headers) {
			fieldName = cp.headers[i]
		} else {
			fieldName = fmt.Sprintf("column_%d", i)
		}
		record.SetData(fieldName, value)
	}

	// Set metadata
	record.SetTimestamp(time.Now())
	record.SetMetadata("row_index", rowIndex)
	record.SetMetadata("format", "csv")

	return record, nil
}

// inferSchema infers schema from sample records
func (cp *CSVParser) inferSchema(sampleRecords []*models.Record) (*core.Schema, error) {
	samples := make([]map[string]interface{}, len(sampleRecords))
	for i, record := range sampleRecords {
		samples[i] = record.Data
	}

	inference := NewDataSampleInference(samples)
	inference.inferenceConfig = cp.config.TypeInferenceConfig

	return inference.InferSchema("csv_inferred_schema")
}

// JSONParserConfig configures JSON parsing behavior
type JSONParserConfig struct {
	Format              JSONFormat           `json:"format"`
	BufferSize          int                  `json:"buffer_size"`
	MaxRecords          int                  `json:"max_records"`
	EnableTypeInference bool                 `json:"enable_type_inference"`
	TypeInferenceConfig *TypeInferenceConfig `json:"type_inference_config"`
	CompressionConfig   *compression.Config  `json:"compression_config"`
	StreamingThreshold  int64                `json:"streaming_threshold"`
}

// JSONFormat represents different JSON file formats
type JSONFormat string

const (
	JSONFormatArray JSONFormat = "array" // Single JSON array: [{"a":1}, {"b":2}]
	JSONFormatLines JSONFormat = "lines" // Line-delimited: {"a":1}\n{"b":2}
	JSONFormatAuto  JSONFormat = "auto"  // Auto-detect format
)

// DefaultJSONParserConfig returns default JSON parser configuration
func DefaultJSONParserConfig() *JSONParserConfig {
	return &JSONParserConfig{
		Format:              JSONFormatAuto,
		BufferSize:          64 * 1024,
		MaxRecords:          0, // 0 means no limit
		EnableTypeInference: true,
		TypeInferenceConfig: DefaultTypeInferenceConfig(),
		CompressionConfig:   &compression.Config{Algorithm: compression.None},
		StreamingThreshold:  10 * 1024 * 1024, // 10MB
	}
}

// JSONParser provides JSON parsing capabilities
type JSONParser struct {
	*FormatParser
	config      *JSONParserConfig
	recordCount int64
}

// NewJSONParser creates a new JSON parser
func NewJSONParser(config *JSONParserConfig) *JSONParser {
	if config == nil {
		config = DefaultJSONParserConfig()
	}

	return &JSONParser{
		FormatParser: NewFormatParser(),
		config:       config,
	}
}

// ParseFile parses a JSON file and returns records and schema
func (jp *JSONParser) ParseFile(ctx context.Context, filePath string) (*ParseResult, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeFile, "failed to open JSON file")
	}
	defer file.Close() // Ignore close error //nolint:errcheck // File close errors in defer are usually not actionable

	// Auto-detect format if needed
	if jp.config.Format == JSONFormatAuto {
		format, err := jp.detectFormat(filePath)
		if err != nil {
			return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to detect JSON format")
		}
		jp.config.Format = format
	}

	return jp.ParseReader(ctx, file)
}

// ParseReader parses JSON from an io.Reader
func (jp *JSONParser) ParseReader(ctx context.Context, reader io.Reader) (*ParseResult, error) {
	// Handle decompression if configured
	if jp.config.CompressionConfig.Algorithm != compression.None {
		// Create a pipe for decompression
		pr, pw := io.Pipe()

		// Start decompression in a goroutine
		errChan := make(chan error, 1)
		go func() {
			defer func() {
				if err := pw.Close(); err != nil {
					// Pipe writer close errors can be ignored in this context
					// as the decompression process is completing
				}
			}()
			err := jp.compressor.DecompressStream(pw, reader)
			errChan <- err
		}()

		// Check for decompression errors
		select {
		case err := <-errChan:
			if err != nil {
				_ = pr.Close() // Close reader on error
				return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to decompress JSON data")
			}
		default:
			// Continue with decompressed reader
		}

		reader = pr
	}

	switch jp.config.Format {
	case JSONFormatArray:
		return jp.parseJSONArray(ctx, reader)
	case JSONFormatLines:
		return jp.parseJSONLines(ctx, reader)
	default:
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeData, fmt.Sprintf("unsupported JSON format: %s", jp.config.Format))
	}
}

// parseJSONArray parses a JSON array format
func (jp *JSONParser) parseJSONArray(ctx context.Context, reader io.Reader) (*ParseResult, error) {
	bufferedReader := bufio.NewReaderSize(reader, jp.config.BufferSize)
	decoder := jsonpool.GetDecoder(bufferedReader)

	// Pre-allocate slices and use pooled map for better performance
	result := &ParseResult{
		Records:  pool.GetBatchSlice(100), // Pre-allocate for typical batch size
		Metadata: pool.GetMap(),           // Use pooled map
		Errors:   make([]error, 0, 10),    // Pre-allocate for common error count
	}

	// Expect opening bracket
	token, err := decoder.Token()
	if err != nil {
		return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to read JSON array start")
	}
	if delim, ok := token.(gojson.Delim); !ok || delim != '[' {
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeData, "expected JSON array to start with '['")
	}

	var sampleRecords []*models.Record

	// Parse array elements
	for decoder.More() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		var obj map[string]interface{}
		if err := decoder.Decode(&obj); err != nil {
			result.Errors = append(result.Errors, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to decode JSON object"))
			continue
		}

		modelRecord, err := jp.objectToRecord(obj, jp.recordCount)
		if err != nil {
			result.Errors = append(result.Errors, err)
			continue
		}

		result.Records = append(result.Records, modelRecord)

		// Collect samples for type inference
		if jp.config.EnableTypeInference && len(sampleRecords) < jp.config.TypeInferenceConfig.SampleSize {
			sampleRecords = append(sampleRecords, modelRecord)
		}

		jp.recordCount++

		// Check max records limit
		if jp.config.MaxRecords > 0 && jp.recordCount >= int64(jp.config.MaxRecords) {
			break
		}
	}

	// Perform type inference if enabled
	if jp.config.EnableTypeInference && len(sampleRecords) > 0 {
		schema, err := jp.inferSchema(sampleRecords)
		if err != nil {
			jp.logger.Warn("Failed to infer schema", zap.Error(err))
		} else {
			result.Schema = schema
		}
	}

	// Add metadata
	result.Metadata["format"] = "json_array"
	result.Metadata["records_parsed"] = jp.recordCount

	jp.logger.Info("JSON array parsing completed",
		zap.Int64("records_parsed", jp.recordCount),
		zap.Int("records_created", len(result.Records)),
		zap.Int("errors", len(result.Errors)))

	return result, nil
}

// parseJSONLines parses line-delimited JSON format
func (jp *JSONParser) parseJSONLines(ctx context.Context, reader io.Reader) (*ParseResult, error) {
	bufferedReader := bufio.NewReaderSize(reader, jp.config.BufferSize)
	scanner := bufio.NewScanner(bufferedReader)

	// Pre-allocate slices and use pooled map for better performance
	result := &ParseResult{
		Records:  pool.GetBatchSlice(100), // Pre-allocate for typical batch size
		Metadata: pool.GetMap(),           // Use pooled map
		Errors:   make([]error, 0, 10),    // Pre-allocate for common error count
	}

	var sampleRecords []*models.Record

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		line := scanner.Text()
		line = strings.TrimSpace(line)

		// Skip empty lines
		if line == "" {
			continue
		}

		var obj map[string]interface{}
		if err := jsonpool.Unmarshal([]byte(line), &obj); err != nil {
			result.Errors = append(result.Errors, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to parse JSON line"))
			continue
		}

		modelRecord, err := jp.objectToRecord(obj, jp.recordCount)
		if err != nil {
			result.Errors = append(result.Errors, err)
			continue
		}

		result.Records = append(result.Records, modelRecord)

		// Collect samples for type inference
		if jp.config.EnableTypeInference && len(sampleRecords) < jp.config.TypeInferenceConfig.SampleSize {
			sampleRecords = append(sampleRecords, modelRecord)
		}

		jp.recordCount++

		// Check max records limit
		if jp.config.MaxRecords > 0 && jp.recordCount >= int64(jp.config.MaxRecords) {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "error reading JSON lines")
	}

	// Perform type inference if enabled
	if jp.config.EnableTypeInference && len(sampleRecords) > 0 {
		schema, err := jp.inferSchema(sampleRecords)
		if err != nil {
			jp.logger.Warn("Failed to infer schema", zap.Error(err))
		} else {
			result.Schema = schema
		}
	}

	// Add metadata
	result.Metadata["format"] = "json_lines"
	result.Metadata["records_parsed"] = jp.recordCount

	jp.logger.Info("JSON lines parsing completed",
		zap.Int64("records_parsed", jp.recordCount),
		zap.Int("records_created", len(result.Records)),
		zap.Int("errors", len(result.Errors)))

	return result, nil
}

// objectToRecord converts a JSON object to a models.Record
func (jp *JSONParser) objectToRecord(obj map[string]interface{}, recordIndex int64) (*models.Record, error) {
	record := &models.Record{
		ID:   fmt.Sprintf("json_record_%d", recordIndex),
		Data: obj,
		Metadata: models.RecordMetadata{
			Source:    "json_parser",
			Timestamp: time.Now(),
			Custom: map[string]interface{}{
				"record_index": recordIndex,
				"format":       "json",
			},
		},
	}

	return record, nil
}

// inferSchema infers schema from sample records
func (jp *JSONParser) inferSchema(sampleRecords []*models.Record) (*core.Schema, error) {
	samples := make([]map[string]interface{}, len(sampleRecords))
	for i, record := range sampleRecords {
		samples[i] = record.Data
	}

	inference := NewDataSampleInference(samples)
	inference.inferenceConfig = jp.config.TypeInferenceConfig

	return inference.InferSchema("json_inferred_schema")
}

// detectFormat auto-detects JSON format by reading file header
func (jp *JSONParser) detectFormat(filePath string) (JSONFormat, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeFile, "failed to open file for format detection")
	}
	defer file.Close() // Ignore close error //nolint:errcheck // File close errors in defer are usually not actionable

	// Read first few bytes to determine format
	header := pool.GetByteSlice()

	if cap(header) < 1024 {

		header = make([]byte, 1024)

	}

	defer pool.PutByteSlice(header)
	n, err := file.Read(header)
	if err != nil && err != io.EOF {
		return "", nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeFile, "failed to read file header")
	}

	headerStr := strings.TrimSpace(string(header[:n]))

	if strings.HasPrefix(headerStr, "[") {
		return JSONFormatArray, nil
	} else if strings.HasPrefix(headerStr, "{") {
		return JSONFormatLines, nil
	}

	return "", nebulaerrors.New(nebulaerrors.ErrorTypeData, "unable to detect JSON format")
}

// FormatWriter provides writing capabilities for different file formats
type FormatWriter struct {
	logger     *zap.Logger
	compressor compression.Compressor
}

// NewFormatWriter creates a new format writer
func NewFormatWriter() *FormatWriter {
	compressor, err := compression.NewCompressor(&compression.Config{})
	if err != nil {
		// Use no compression as fallback
		compressor, _ = compression.NewCompressor(&compression.Config{Algorithm: compression.None})
	}
	return &FormatWriter{
		logger:     GetGlobalSDK().logger.With(zap.String("component", "format_writer")),
		compressor: compressor,
	}
}

// CSVWriter writes records in CSV format
type CSVWriter struct {
	*FormatWriter
	config      *CSVParserConfig
	writer      *csv.Writer
	headers     []string
	recordCount int64
}

// NewCSVWriter creates a new CSV writer
func NewCSVWriter(config *CSVParserConfig) *CSVWriter {
	if config == nil {
		config = DefaultCSVParserConfig()
	}

	return &CSVWriter{
		FormatWriter: NewFormatWriter(),
		config:       config,
	}
}

// WriteToFile writes records to a CSV file
func (cw *CSVWriter) WriteToFile(ctx context.Context, filePath string, records []*models.Record, schema *core.Schema) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeFile, "failed to create directory")
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeFile, "failed to create CSV file")
	}
	defer file.Close() // Ignore close error //nolint:errcheck // File close errors in defer are usually not actionable

	return cw.WriteToWriter(ctx, file, records, schema)
}

// WriteToWriter writes records to an io.Writer
func (cw *CSVWriter) WriteToWriter(ctx context.Context, writer io.Writer, records []*models.Record, schema *core.Schema) error {
	// Handle compression if configured
	if cw.config.CompressionConfig.Algorithm != compression.None {
		// Create a pipe for compression
		pr, pw := io.Pipe()

		// Start compression in a goroutine
		errChan := make(chan error, 1)
		go func() {
			defer func() {
				if err := pr.Close(); err != nil {
					// Pipe reader close errors can be ignored
				}
			}()
			err := cw.compressor.CompressStream(writer, pr)
			errChan <- err
		}()

		// Use the pipe writer for CSV output
		writer = pw

		// Ensure we close the pipe writer at the end
		defer func() {
			if err := pw.Close(); err != nil {
				// Pipe writer close errors can be ignored
			}
			// Wait for compression to complete
			if err := <-errChan; err != nil {
				cw.logger.Error("compression error", zap.Error(err))
			}
		}()
	}

	// Setup CSV writer
	cw.writer = csv.NewWriter(writer)
	cw.writer.Comma = cw.config.Delimiter

	// Determine headers
	if schema != nil {
		cw.headers = make([]string, len(schema.Fields))
		for i, field := range schema.Fields {
			cw.headers[i] = field.Name
		}
	} else if len(records) > 0 {
		// Infer headers from first record
		for key := range records[0].Data {
			cw.headers = append(cw.headers, key)
		}
	}

	// Write headers if configured
	if cw.config.HasHeader && len(cw.headers) > 0 {
		if err := cw.writer.Write(cw.headers); err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to write CSV headers")
		}
	}

	// Write records
	for _, record := range records {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		row := make([]string, len(cw.headers))
		for i, header := range cw.headers {
			if value, exists := record.Data[header]; exists {
				row[i] = stringpool.ValueToString(value)
			}
		}

		if err := cw.writer.Write(row); err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to write CSV row")
		}
		cw.recordCount++
	}

	cw.writer.Flush()
	if err := cw.writer.Error(); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "CSV writer error")
	}

	cw.logger.Info("CSV writing completed",
		zap.Int64("records_written", cw.recordCount),
		zap.Int("headers", len(cw.headers)))

	return nil
}

// JSONWriter writes records in JSON format
type JSONWriter struct {
	*FormatWriter
	config      *JSONParserConfig
	recordCount int64
}

// NewJSONWriter creates a new JSON writer
func NewJSONWriter(config *JSONParserConfig) *JSONWriter {
	if config == nil {
		config = DefaultJSONParserConfig()
	}

	return &JSONWriter{
		FormatWriter: NewFormatWriter(),
		config:       config,
	}
}

// WriteToFile writes records to a JSON file
func (jw *JSONWriter) WriteToFile(ctx context.Context, filePath string, records []*models.Record) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeFile, "failed to create directory")
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeFile, "failed to create JSON file")
	}
	defer file.Close() // Ignore close error //nolint:errcheck // File close errors in defer are usually not actionable

	return jw.WriteToWriter(ctx, file, records)
}

// WriteToWriter writes records to an io.Writer
func (jw *JSONWriter) WriteToWriter(ctx context.Context, writer io.Writer, records []*models.Record) error {
	// Handle compression if configured
	if jw.config.CompressionConfig.Algorithm != compression.None {
		// Create a pipe for compression
		pr, pw := io.Pipe()

		// Start compression in a goroutine
		errChan := make(chan error, 1)
		go func() {
			defer func() {
				if err := pr.Close(); err != nil {
					// Pipe reader close errors can be ignored
				}
			}()
			err := jw.compressor.CompressStream(writer, pr)
			errChan <- err
		}()

		// Use the pipe writer for JSON output
		writer = pw

		// Ensure we close the pipe writer at the end
		defer func() {
			if err := pw.Close(); err != nil {
				// Pipe writer close errors can be ignored
			}
			// Wait for compression to complete
			if err := <-errChan; err != nil {
				jw.logger.Error("compression error", zap.Error(err))
			}
		}()
	}

	switch jw.config.Format {
	case JSONFormatArray:
		return jw.writeJSONArray(ctx, writer, records)
	case JSONFormatLines:
		return jw.writeJSONLines(ctx, writer, records)
	default:
		return nebulaerrors.New(nebulaerrors.ErrorTypeData, fmt.Sprintf("unsupported JSON format: %s", jw.config.Format))
	}
}

// writeJSONArray writes records as a JSON array
func (jw *JSONWriter) writeJSONArray(ctx context.Context, writer io.Writer, records []*models.Record) error {
	buf := &bytes.Buffer{}
	buf.WriteString("[\n")

	for i, record := range records {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if i > 0 {
			buf.WriteString(",\n")
		}

		jsonData, err := jsonpool.MarshalIndent(record.Data, "  ", "  ")
		if err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to marshal record to JSON")
		}

		buf.WriteString("  ")
		_, _ = buf.Write(jsonData) // Ignore write error
		jw.recordCount++
	}

	buf.WriteString("\n]")

	if _, err := writer.Write(buf.Bytes()); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to write JSON array")
	}

	jw.logger.Info("JSON array writing completed",
		zap.Int64("records_written", jw.recordCount))

	return nil
}

// writeJSONLines writes records as line-delimited JSON
func (jw *JSONWriter) writeJSONLines(ctx context.Context, writer io.Writer, records []*models.Record) error {
	buf := &bytes.Buffer{}

	for _, record := range records {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		jsonData, err := jsonpool.Marshal(record.Data)
		if err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to marshal record to JSON")
		}

		_, _ = buf.Write(jsonData) // Ignore write error
		buf.WriteString("\n")
		jw.recordCount++
	}

	if _, err := writer.Write(buf.Bytes()); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to write JSON lines")
	}

	jw.logger.Info("JSON lines writing completed",
		zap.Int64("records_written", jw.recordCount))

	return nil
}
