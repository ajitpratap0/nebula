package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/base"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/errors"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

// CSVSource is a production-ready CSV source connector using BaseConnector
type CSVSource struct {
	*base.BaseConnector

	// CSV-specific fields
	file           *os.File
	reader         *csv.Reader
	headers        []string
	currentRow     int
	totalRows      int
	hasHeader      bool
	enableParallel bool // Enable parallel CSV parsing
	filePath       string // Store the file path

	// Schema and state
	schema       *core.Schema
	lastPosition int64
	eof          bool
	
	// Parallel parser (when enabled)
	parallelParser *ParallelCSVParser
}

// NewCSVSource creates a new CSV source connector
func NewCSVSource(config *config.BaseConfig) (core.Source, error) {
	base := base.NewBaseConnector("csv", core.ConnectorTypeSource, "2.0.0")

	return &CSVSource{
		BaseConnector: base,
		hasHeader:     true, // default
	}, nil
}

// Initialize initializes the CSV source connector
func (s *CSVSource) Initialize(ctx context.Context, config *config.BaseConfig) error {
	// Initialize base connector first
	if err := s.BaseConnector.Initialize(ctx, config); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "failed to initialize base connector")
	}

	// Validate configuration
	if err := s.validateConfig(config); err != nil {
		return err
	}

	// Extract CSV-specific configuration
	s.extractCSVConfig(config)

	// Open file with circuit breaker protection
	if err := s.ExecuteWithCircuitBreaker(func() error {
		return s.openFile(config)
	}); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to open CSV file")
	}

	// Discover schema
	if err := s.discoverSchema(); err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to discover schema")
	}

	// Count total rows for progress tracking
	if err := s.countTotalRows(); err != nil {
		s.GetLogger().Warn("failed to count total rows", zap.Error(err))
		s.totalRows = -1 // unknown
	}

	s.UpdateHealth(true, map[string]interface{}{
		"file_opened":       true,
		"schema_discovered": true,
		"total_rows":        s.totalRows,
	})

	s.GetLogger().Info("CSV source initialized",
		zap.String("file", s.getFilePath()),
		zap.Int("total_rows", s.totalRows),
		zap.Bool("has_header", s.hasHeader))

	return nil
}

// Discover returns the schema of the CSV file
func (s *CSVSource) Discover(ctx context.Context) (*core.Schema, error) {
	if s.schema == nil {
		return nil, errors.New(errors.ErrorTypeData, "schema not discovered yet")
	}

	return s.schema, nil
}

// Read returns a stream of records from the CSV file
func (s *CSVSource) Read(ctx context.Context) (*core.RecordStream, error) {
	// Use parallel parsing if enabled for better performance
	if s.enableParallel {
		return s.readParallel(ctx)
	}
	
	recordChan := pool.GetRecordChannel(s.GetConfig().Performance.BufferSize)
	defer pool.PutRecordChannel(recordChan)
	errorChan := make(chan error, 1)

	stream := &core.RecordStream{
		Records: recordChan,
		Errors:  errorChan,
	}

	// Start reading in background goroutine
	go func() {
		defer close(recordChan)
		defer close(errorChan)

		s.readRecords(ctx, recordChan, errorChan)
	}()

	return stream, nil
}

// ReadBatch returns batches of records from the CSV file
func (s *CSVSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	batchChan := pool.GetBatchChannel()
	defer pool.PutBatchChannel(batchChan)
	errorChan := make(chan error, 1)

	stream := &core.BatchStream{
		Batches: batchChan,
		Errors:  errorChan,
	}

	// Use optimized batch size from BaseConnector
	optimizedBatchSize := s.OptimizeBatchSize()
	if batchSize > 0 && batchSize < optimizedBatchSize {
		optimizedBatchSize = batchSize
	}

	// Start reading in background goroutine
	go func() {
		defer close(batchChan)
		defer close(errorChan)

		s.readBatches(ctx, optimizedBatchSize, batchChan, errorChan)
	}()

	return stream, nil
}

// SupportsIncremental returns true since CSV can support incremental reads
func (s *CSVSource) SupportsIncremental() bool {
	return true
}

// SupportsRealtime returns false since CSV is not real-time
func (s *CSVSource) SupportsRealtime() bool {
	return false
}

// SupportsBatch returns true since CSV supports batch reads
func (s *CSVSource) SupportsBatch() bool {
	return true
}

// Subscribe is not supported by CSV files (CDC-only method)
func (s *CSVSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	return nil, errors.New(errors.ErrorTypeCapability, "CSV source does not support real-time subscriptions")
}

// Close closes the CSV source connector
func (s *CSVSource) Close(ctx context.Context) error {
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			s.GetLogger().Error("failed to close file", zap.Error(err))
		}
		s.file = nil
	}

	// Close base connector
	return s.BaseConnector.Close(ctx)
}

// Private methods

func (s *CSVSource) validateConfig(config *config.BaseConfig) error {
	// For CSV source, we'd typically validate that a file path is provided
	// Since BaseConfig doesn't have Properties, this would need to be validated
	// differently - perhaps through environment variables or separate config
	// For now, assume validation is handled by the caller
	return nil
}

func (s *CSVSource) extractCSVConfig(config *config.BaseConfig) {
	// Extract file path from credentials (set by CLI)
	if config.Security.Credentials != nil {
		if path, ok := config.Security.Credentials["path"]; ok {
			s.filePath = path
		}
	}
	
	// CSV-specific configuration defaults
	s.hasHeader = true // default
	
	// Enable parallel parsing for better performance on large files
	// This could be configured via credentials or advanced settings
	if config.Performance.Workers > 1 && config.Performance.BatchSize > 100 {
		s.enableParallel = true
	}
}

func (s *CSVSource) getFilePath() string {
	return s.filePath
}

func (s *CSVSource) openFile(config *config.BaseConfig) error {
	path := s.getFilePath()

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", path, err)
	}

	s.file = file
	s.reader = csv.NewReader(file)
	s.reader.FieldsPerRecord = -1 // Allow variable number of fields

	return nil
}

func (s *CSVSource) discoverSchema() error {
	if s.hasHeader {
		// Read header row
		headers, err := s.reader.Read()
		if err != nil {
			return fmt.Errorf("failed to read headers: %w", err)
		}
		s.headers = headers
		s.currentRow++
	}

	// Read first data row to infer types
	dataRow, err := s.reader.Read()
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read first data row: %w", err)
	}

	// Reset file position
	s.file.Seek(0, 0)
	s.reader = csv.NewReader(s.file)
	s.reader.FieldsPerRecord = -1
	s.currentRow = 0

	// Create schema
	fields := make([]core.Field, len(s.headers))
	for i, header := range s.headers {
		fieldType := core.FieldTypeString // default

		if dataRow != nil && i < len(dataRow) {
			fieldType = s.inferFieldType(dataRow[i])
		}

		fields[i] = core.Field{
			Name:        header,
			Type:        fieldType,
			Description: "Field " + header + " from CSV", // Avoid fmt.Sprintf allocation
			Nullable:    true,
		}
	}

	s.schema = &core.Schema{
		Name:        fmt.Sprintf("csv_%s", strings.ReplaceAll(s.getFilePath(), "/", "_")),
		Description: fmt.Sprintf("Schema for CSV file %s", s.getFilePath()),
		Fields:      fields,
		Version:     1,
	}

	return nil
}

func (s *CSVSource) inferFieldType(value string) core.FieldType {
	value = strings.TrimSpace(value)

	if value == "" {
		return core.FieldTypeString
	}

	// Try integer
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return core.FieldTypeInt
	}

	// Try float
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return core.FieldTypeFloat
	}

	// Try boolean
	if value == "true" || value == "false" || value == "TRUE" || value == "FALSE" {
		return core.FieldTypeBool
	}

	return core.FieldTypeString
}

func (s *CSVSource) countTotalRows() error {
	currentPos, err := s.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// Seek to beginning
	_, err = s.file.Seek(0, 0)
	if err != nil {
		return err
	}

	tempReader := csv.NewReader(s.file)
	tempReader.FieldsPerRecord = -1

	count := 0
	for {
		_, err := tempReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		count++
	}

	// Subtract header if present
	if s.hasHeader {
		count--
	}

	s.totalRows = count

	// Restore position
	_, err = s.file.Seek(currentPos, 0)
	if err != nil {
		return err
	}

	s.reader = csv.NewReader(s.file)
	s.reader.FieldsPerRecord = -1

	return nil
}

func (s *CSVSource) readRecords(ctx context.Context, recordChan chan<- *models.Record, errorChan chan<- error) {
	// Skip header if present and not already skipped
	if s.hasHeader && s.currentRow == 0 {
		if _, err := s.reader.Read(); err != nil {
			errorChan <- errors.Wrap(err, errors.ErrorTypeData, "failed to skip header row")
			return
		}
		s.currentRow++
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Rate limiting
			if err := s.RateLimit(ctx); err != nil {
				errorChan <- err
				return
			}

			// Read row with circuit breaker protection
			var row []string
			var err error

			readErr := s.ExecuteWithCircuitBreaker(func() error {
				row, err = s.reader.Read()
				return err
			})

			if readErr != nil {
				if err == io.EOF {
					s.eof = true
					return
				}

				// Handle with error handler
				if handleErr := s.HandleError(ctx, err, nil); handleErr != nil {
					errorChan <- handleErr
				}
				continue
			}

			// Convert to record
			record := s.rowToRecord(row)
			if record == nil {
				continue
			}

			// Update position
			s.currentRow++
			s.lastPosition = int64(s.currentRow)

			// Report progress
			if s.totalRows > 0 {
				s.ReportProgress(s.lastPosition, int64(s.totalRows))
			}

			// Record metrics
			s.RecordMetric("records_read", 1, core.MetricTypeCounter)

			select {
			case recordChan <- record:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *CSVSource) readBatches(ctx context.Context, batchSize int, batchChan chan<- []*models.Record, errorChan chan<- error) {
	batch := pool.GetBatchSlice(batchSize)
	defer pool.PutBatchSlice(batch)

	// Skip header if present and not already skipped
	if s.hasHeader && s.currentRow == 0 {
		if _, err := s.reader.Read(); err != nil {
			errorChan <- errors.Wrap(err, errors.ErrorTypeData, "failed to skip header row")
			return
		}
		s.currentRow++
	}

	for {
		select {
		case <-ctx.Done():
			// Send remaining batch
			if len(batch) > 0 {
				batchChan <- batch
			}
			return
		default:
			// Rate limiting
			if err := s.RateLimit(ctx); err != nil {
				errorChan <- err
				return
			}

			// Read row with circuit breaker protection
			var row []string
			var err error

			readErr := s.ExecuteWithCircuitBreaker(func() error {
				row, err = s.reader.Read()
				return err
			})

			if readErr != nil {
				if err == io.EOF {
					// Send remaining batch
					if len(batch) > 0 {
						batchChan <- batch
					}
					s.eof = true
					return
				}

				// Handle with error handler
				if handleErr := s.HandleError(ctx, err, nil); handleErr != nil {
					errorChan <- handleErr
				}
				continue
			}

			// Convert to record
			record := s.rowToRecord(row)
			if record == nil {
				continue
			}

			// Add to batch
			batch = append(batch, record)
			s.currentRow++
			s.lastPosition = int64(s.currentRow)

			// Send batch when full
			if len(batch) >= batchSize {
				// Report progress
				if s.totalRows > 0 {
					s.ReportProgress(s.lastPosition, int64(s.totalRows))
				}

				// Record metrics
				s.RecordMetric("batches_read", 1, core.MetricTypeCounter)
				s.RecordMetric("records_in_batch", len(batch), core.MetricTypeGauge)

				select {
				case batchChan <- batch:
					// Reset batch - no need to allocate new one
					batch = batch[:0]
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (s *CSVSource) rowToRecord(row []string) *models.Record {
	if len(row) == 0 {
		return nil
	}

	// Create record - this already gets a pooled record with pooled maps
	record := models.NewRecordFromPool(s.Name())

	// Populate data using the existing pooled map
	for i, value := range row {
		var fieldName string
		if i < len(s.headers) {
			// Use interned string for header field names
			fieldName = pool.InternString(s.headers[i])
		} else {
			// Use strconv for better performance (no allocations for small numbers)
			// and intern the synthetic field name
			fieldName = pool.InternString("field_" + strconv.Itoa(i))
		}

		// Convert based on schema if available
		if s.schema != nil && i < len(s.schema.Fields) {
			record.SetData(fieldName, s.convertValue(value, s.schema.Fields[i].Type))
		} else {
			record.SetData(fieldName, value)
		}
	}

	// Add metadata using interned keys
	record.SetMetadata(pool.InternString("source_file"), s.getFilePath())
	record.SetMetadata(pool.InternString("row_number"), s.currentRow)
	record.SetMetadata(pool.InternString("connector_version"), s.Version())

	return record
}

func (s *CSVSource) convertValue(value string, fieldType core.FieldType) interface{} {
	value = strings.TrimSpace(value)

	if value == "" {
		return nil
	}

	switch fieldType {
	case core.FieldTypeInt:
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intVal
		}
	case core.FieldTypeFloat:
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
	case core.FieldTypeBool:
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}

	return value
}

// readParallel reads CSV file using parallel parsing for better performance
func (s *CSVSource) readParallel(ctx context.Context) (*core.RecordStream, error) {
	// Create parallel parser configuration
	config := ParallelCSVConfig{
		NumWorkers: s.GetConfig().Performance.Workers,
		ChunkSize:  s.GetConfig().Performance.BatchSize,
		Headers:    s.headers,
		SkipHeader: false, // We already read headers
		Delimiter:  ',',   // Default CSV delimiter
		ParseFunc: func(fields []string) (*models.Record, error) {
			// Use existing row parsing logic
			return s.rowToRecord(fields), nil
		},
	}
	
	// Create parallel parser
	s.parallelParser = NewParallelCSVParser(config, s.GetLogger())
	
	// Parse file in parallel
	recordChan, errorChan := s.parallelParser.ParseFile(s.file)
	
	// Create stream wrapper that tracks metrics
	wrappedRecordChan := pool.GetRecordChannel(s.GetConfig().Performance.BufferSize)
	defer pool.PutRecordChannel(wrappedRecordChan)
	wrappedErrorChan := make(chan error, 1)
	
	go func() {
		defer close(wrappedRecordChan)
		defer close(wrappedErrorChan)
		defer s.parallelParser.Stop()
		
		for {
			select {
			case record, ok := <-recordChan:
				if !ok {
					return
				}
				
				// Update metrics
				s.currentRow++
				s.lastPosition = int64(s.currentRow)
				s.RecordMetric("records_read", 1, core.MetricTypeCounter)
				
				// Report progress
				if s.totalRows > 0 {
					s.ReportProgress(s.lastPosition, int64(s.totalRows))
				}
				
				select {
				case wrappedRecordChan <- record:
				case <-ctx.Done():
					return
				}
				
			case err, ok := <-errorChan:
				if !ok {
					continue
				}
				select {
				case wrappedErrorChan <- err:
				case <-ctx.Done():
					return
				}
				
			case <-ctx.Done():
				return
			}
		}
	}()
	
	return &core.RecordStream{
		Records: wrappedRecordChan,
		Errors:  wrappedErrorChan,
	}, nil
}
