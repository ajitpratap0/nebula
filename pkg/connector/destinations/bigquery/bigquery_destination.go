package bigquery

import (
	"context"
	"encoding/csv"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/ajitpratap0/nebula/pkg/compression"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/base"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"
	"google.golang.org/api/option"
)

// BigQueryDestination is a high-performance BigQuery destination connector
type BigQueryDestination struct {
	*base.BaseConnector

	// Connection configuration
	projectID       string
	datasetID       string
	tableID         string
	credentialsPath string
	location        string

	// BigQuery client
	client   *bigquery.Client
	dataset  *bigquery.Dataset
	table    *bigquery.Table
	inserter *bigquery.Inserter

	// Performance configuration
	batchSize            int
	batchTimeout         time.Duration
	maxConcurrentBatches int
	streamingBuffer      int
	enableStreaming      bool
	autoDetectSchema     bool
	createDisposition    string
	writeDisposition     string

	// Partitioning configuration
	partitionField         string
	partitionType          string // DAY, HOUR, MONTH, YEAR
	clusteringFields       []string
	requirePartitionFilter bool

	// Compression configuration
	compressionType string
	compressor      compression.Compressor

	// Removed optimization layer - simplified for compilation

	// State management
	currentSchema     *core.Schema
	recordsWritten    int64
	bytesWritten      int64
	batchesProcessed  int64
	errorsEncountered int64

	// Micro-batching
	microBatch      []*models.Record
	microBatchMutex sync.Mutex
	microBatchTimer *time.Timer

	// Worker pool
	workerCount  int
	batchChan    chan *RecordBatch
	workerWG     sync.WaitGroup
	shutdownOnce sync.Once

	// Statistics
	stats      *BigQueryStats
	statsMutex sync.RWMutex
}

// RecordBatch represents a batch of records to be inserted
type RecordBatch struct {
	Records   []*models.Record
	Timestamp time.Time
	BatchID   string
}

// BigQueryStats provides real-time statistics
type BigQueryStats struct {
	RecordsWritten    int64     `json:"records_written"`
	BytesWritten      int64     `json:"bytes_written"`
	BatchesProcessed  int64     `json:"batches_processed"`
	ErrorsEncountered int64     `json:"errors_encountered"`
	AverageLatency    int64     `json:"average_latency_ms"`
	StreamingEnabled  bool      `json:"streaming_enabled"`
	LastBatchTime     time.Time `json:"last_batch_time"`
}

// NewBigQueryDestination creates a new BigQuery destination connector
func NewBigQueryDestination(name string, config *config.BaseConfig) (core.Destination, error) {
	base := base.NewBaseConnector(name, core.ConnectorTypeDestination, "1.0.0")

	dest := &BigQueryDestination{
		BaseConnector: base,

		// Default configuration
		batchSize:            10000,
		batchTimeout:         5 * time.Second,
		maxConcurrentBatches: 10,
		streamingBuffer:      50000,
		enableStreaming:      true,
		autoDetectSchema:     true,
		createDisposition:    "CREATE_IF_NEEDED",
		writeDisposition:     "WRITE_APPEND",
		compressionType:      "NONE", // BigQuery handles compression internally
		workerCount:          4,

		microBatch: pool.GetBatchSlice(10000),
		stats:      &BigQueryStats{},
	}

	return dest, nil
}

// Initialize initializes the BigQuery destination connector
func (b *BigQueryDestination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	// Initialize base connector first
	if err := b.BaseConnector.Initialize(ctx, config); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig, "failed to initialize base connector")
	}

	// Validate and extract configuration
	if err := b.validateConfig(config); err != nil {
		return err
	}

	b.extractBigQueryConfig(config)

	// Optimization layer removed for compilation - can be re-added later

	// Initialize BigQuery client
	if err := b.ExecuteWithCircuitBreaker(func() error {
		return b.initializeClient(ctx)
	}); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to initialize BigQuery client")
	}

	// Initialize dataset and table
	if err := b.ExecuteWithCircuitBreaker(func() error {
		return b.initializeDatasetAndTable(ctx)
	}); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to initialize dataset and table")
	}

	// Start worker pool
	b.startWorkerPool(ctx)

	b.UpdateHealth(true, map[string]interface{}{
		"project_id":        b.projectID,
		"dataset_id":        b.datasetID,
		"table_id":          b.tableID,
		"batch_size":        b.batchSize,
		"streaming_enabled": b.enableStreaming,
		"worker_count":      b.workerCount,
	})

	b.GetLogger().Info("BigQuery destination initialized",
		zap.String("project", b.projectID),
		zap.String("dataset", b.datasetID),
		zap.String("table", b.tableID),
		zap.Int("batch_size", b.batchSize),
		zap.Bool("streaming", b.enableStreaming))

	return nil
}

// CreateSchema creates or updates the schema for the destination
func (b *BigQueryDestination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	b.currentSchema = schema

	// Convert core schema to BigQuery schema
	bqSchema := b.convertToBigQuerySchema(schema)

	// Get table metadata
	metadata, err := b.table.Metadata(ctx)
	if err != nil {
		// Table doesn't exist, create it
		if err := b.createTable(ctx, bqSchema); err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to create table")
		}
	} else {
		// Table exists, update schema if needed
		if !b.schemasMatch(metadata.Schema, bqSchema) {
			if err := b.updateTableSchema(ctx, bqSchema); err != nil {
				return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to update table schema")
			}
		}
	}

	b.GetLogger().Info("schema created/updated",
		zap.String("schema_name", schema.Name),
		zap.String("table", b.tableID),
		zap.Int("field_count", len(schema.Fields)))

	return nil
}

// AlterSchema alters the schema for the destination
func (b *BigQueryDestination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	b.currentSchema = newSchema

	// Convert to BigQuery schema
	bqSchema := b.convertToBigQuerySchema(newSchema)

	// Update table schema
	if err := b.ExecuteWithCircuitBreaker(func() error {
		return b.updateTableSchema(ctx, bqSchema)
	}); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to alter schema")
	}

	b.GetLogger().Info("schema altered",
		zap.String("table", b.tableID),
		zap.Int("new_fields", len(newSchema.Fields)-len(oldSchema.Fields)))

	return nil
}

// Write writes a stream of records to BigQuery
func (b *BigQueryDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	recordCount := 0
	startTime := time.Now()

	// Start micro-batch timer
	b.startMicroBatchTimer(ctx)

	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				// Stream closed, flush remaining micro-batch
				if err := b.flushMicroBatch(ctx); err != nil {
					return err
				}
				b.GetLogger().Info("write stream completed",
					zap.Int("total_records", recordCount),
					zap.Duration("duration", time.Since(startTime)))
				return nil
			}

			// Rate limiting
			if err := b.RateLimit(ctx); err != nil {
				return err
			}

			// Process the record
			if err := b.addToMicroBatch(ctx, record); err != nil {
				if handleErr := b.HandleError(ctx, err, record); handleErr != nil {
					return handleErr
				}
				continue
			}
			recordCount++

			// Report progress
			b.ReportProgress(int64(recordCount), -1)
			b.RecordMetric("records_processed", 1, core.MetricTypeCounter)

		case err := <-stream.Errors:
			if err != nil {
				return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "error in record stream")
			}

		case <-ctx.Done():
			// Flush remaining micro-batch
			if err := b.flushMicroBatch(ctx); err != nil {
				b.GetLogger().Error("failed to flush micro-batch on context cancellation", zap.Error(err))
			}
			return ctx.Err()
		}
	}
}

// WriteBatch writes batches of records to BigQuery
func (b *BigQueryDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	recordCount := 0
	batchCount := 0
	startTime := time.Now()

	for {
		select {
		case batch, ok := <-stream.Batches:
			if !ok {
				// Stream closed, flush remaining micro-batch
				if err := b.flushMicroBatch(ctx); err != nil {
					return err
				}
				b.GetLogger().Info("write batch stream completed",
					zap.Int("batches_processed", batchCount),
					zap.Int("total_records", recordCount),
					zap.Duration("duration", time.Since(startTime)))
				return nil
			}

			// Rate limiting
			if err := b.RateLimit(ctx); err != nil {
				return err
			}

			// Process batch directly
			if err := b.processBatch(ctx, batch); err != nil {
				if handleErr := b.HandleError(ctx, err, nil); handleErr != nil {
					return handleErr
				}
				continue
			}

			batchCount++
			recordCount += len(batch)

			// Report progress
			b.ReportProgress(int64(recordCount), -1)
			b.RecordMetric("batches_processed", 1, core.MetricTypeCounter)
			b.RecordMetric("records_in_batch", len(batch), core.MetricTypeGauge)

		case err := <-stream.Errors:
			if err != nil {
				return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "error in batch stream")
			}

		case <-ctx.Done():
			if err := b.flushMicroBatch(ctx); err != nil {
				b.GetLogger().Error("failed to flush micro-batch on context cancellation", zap.Error(err))
			}
			return ctx.Err()
		}
	}
}

// Close closes the BigQuery destination connector
func (b *BigQueryDestination) Close(ctx context.Context) error {
	// Flush any remaining micro-batch
	if err := b.flushMicroBatch(ctx); err != nil {
		b.GetLogger().Error("failed to flush micro-batch during close", zap.Error(err))
	}

	// Stop worker pool
	b.stopWorkerPool()

	// Close BigQuery client
	if b.client != nil {
		if err := b.client.Close(); err != nil {
			b.GetLogger().Error("failed to close BigQuery client", zap.Error(err))
		}
	}

	// Optimization layer removed for compilation

	b.GetLogger().Info("BigQuery destination closed",
		zap.Int64("total_records_written", atomic.LoadInt64(&b.recordsWritten)),
		zap.Int64("total_bytes_written", atomic.LoadInt64(&b.bytesWritten)),
		zap.Int64("total_batches_processed", atomic.LoadInt64(&b.batchesProcessed)))

	// Close base connector
	return b.BaseConnector.Close(ctx)
}

// SupportsBulkLoad returns true since BigQuery supports bulk loading
func (b *BigQueryDestination) SupportsBulkLoad() bool {
	return true
}

// SupportsTransactions returns false since BigQuery doesn't support traditional transactions
func (b *BigQueryDestination) SupportsTransactions() bool {
	return false
}

// SupportsUpsert returns true since BigQuery supports MERGE operations
func (b *BigQueryDestination) SupportsUpsert() bool {
	return true
}

// SupportsBatch returns true since BigQuery supports batch operations
func (b *BigQueryDestination) SupportsBatch() bool {
	return true
}

// SupportsStreaming returns true since this connector supports streaming
func (b *BigQueryDestination) SupportsStreaming() bool {
	return b.enableStreaming
}

// BulkLoad loads data in bulk (not implemented yet)
func (b *BigQueryDestination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	if b.table == nil {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "no table configured for bulk load")
	}

	// Start timing for performance metrics
	start := time.Now()
	defer func() {
		b.GetMetricsCollector().RecordHistogram("bulk_load_duration", float64(time.Since(start).Milliseconds()))
	}()

	// Handle different reader types
	var records []*models.Record
	var err error

	switch r := reader.(type) {
	case []*models.Record:
		records = r
	case chan *models.Record:
		records, err = b.collectFromChannel(ctx, r)
		if err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to collect records from channel")
		}
	case *core.RecordStream:
		records, err = b.collectFromStream(ctx, r)
		if err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to collect records from stream")
		}
	default:
		return nebulaerrors.New(nebulaerrors.ErrorTypeValidation, "unsupported reader type for bulk load")
	}

	if len(records) == 0 {
		b.GetLogger().Info("No records to bulk load")
		return nil
	}

	b.GetLogger().Info("Starting BigQuery bulk load operation",
		zap.Int("total_records", len(records)),
		zap.String("format", format))

	// Use BigQuery Load Jobs for bulk loading (more efficient than streaming for large datasets)
	return b.executeBulkLoadJob(ctx, records, format)
}

// executeBulkLoadJob executes a BigQuery Load Job for bulk loading
func (b *BigQueryDestination) executeBulkLoadJob(ctx context.Context, records []*models.Record, format string) error {
	// Create a temporary data source for the load job
	var source bigquery.LoadSource
	var err error

	switch strings.ToUpper(format) {
	case "JSON", "JSONL":
		source, err = b.createJSONSource(records)
	case "CSV":
		source, err = b.createCSVSource(records)
	default:
		return nebulaerrors.New(nebulaerrors.ErrorTypeValidation, stringpool.Sprintf("unsupported bulk load format: %s", format))
	}

	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to create load source")
	}

	// Configure the load job
	loader := b.table.LoaderFrom(source)
	loader.WriteDisposition = bigquery.WriteAppend // Or WriteEmpty/WriteTruncate based on needs

	// Configure job labels for high throughput tracking
	loader.Labels = map[string]string{
		"source":     "nebula",
		"type":       "bulk_load",
		"records":    stringpool.Sprintf("%d", len(records)),
		"created_at": stringpool.Sprintf("%d", time.Now().Unix()),
	}

	b.GetLogger().Info("Submitting BigQuery load job",
		zap.Int("record_count", len(records)),
		zap.String("format", format))

	// Submit the load job
	job, err := loader.Run(ctx)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to submit BigQuery load job")
	}

	// Wait for completion with timeout
	jobCtx, cancel := context.WithTimeout(ctx, 10*time.Minute) // 10 minute timeout for bulk jobs
	defer cancel()

	status, err := job.Wait(jobCtx)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "load job failed or timed out")
	}

	if status.Err() != nil {
		// Log detailed error information
		b.GetLogger().Error("BigQuery load job failed",
			zap.Error(status.Err()),
			zap.String("job_id", job.ID()))

		// Check for detailed errors
		if len(status.Errors) > 0 {
			for i, jobErr := range status.Errors {
				b.GetLogger().Error("Load job error detail",
					zap.Int("error_index", i),
					zap.String("message", jobErr.Message),
					zap.String("reason", jobErr.Reason),
					zap.String("location", jobErr.Location))
			}
		}

		return nebulaerrors.Wrap(status.Err(), nebulaerrors.ErrorTypeConnection, "BigQuery load job failed")
	}

	// Get job statistics
	jobStats := status.Statistics
	if loadStats, ok := jobStats.Details.(*bigquery.LoadStatistics); ok {
		b.GetLogger().Info("BigQuery load job completed successfully",
			zap.String("job_id", job.ID()),
			zap.Int64("input_file_bytes", loadStats.InputFileBytes),
			zap.Int64("output_rows", loadStats.OutputRows))

		// Update metrics
		atomic.AddInt64(&b.recordsWritten, loadStats.OutputRows)
		atomic.AddInt64(&b.bytesWritten, loadStats.InputFileBytes)
		atomic.AddInt64(&b.batchesProcessed, 1)
	} else {
		b.GetLogger().Info("BigQuery load job completed",
			zap.String("job_id", job.ID()))

		// Update basic metrics without detailed stats
		atomic.AddInt64(&b.recordsWritten, int64(len(records)))
		atomic.AddInt64(&b.batchesProcessed, 1)
	}

	return nil
}

// createJSONSource creates a JSON source for BigQuery load job
func (b *BigQueryDestination) createJSONSource(records []*models.Record) (bigquery.LoadSource, error) {
	// Create a pipe for streaming data
	reader, writer := io.Pipe()

	// Start a goroutine to write JSON data
	go func() {
		defer func() { _ = writer.Close() }() // Ignore close error in goroutine

		for _, record := range records {
			jsonData, err := jsonpool.Marshal(record.Data)
			if err != nil {
				b.GetLogger().Error("Failed to marshal record to JSON", zap.Error(err))
				writer.CloseWithError(err)
				return
			}

			if _, err := writer.Write(jsonData); err != nil {
				b.GetLogger().Error("Failed to write JSON data", zap.Error(err))
				writer.CloseWithError(err)
				return
			}

			if _, err := writer.Write([]byte("\n")); err != nil {
				b.GetLogger().Error("Failed to write newline", zap.Error(err))
				writer.CloseWithError(err)
				return
			}
		}
	}()

	// Create and configure the JSON source
	source := bigquery.NewReaderSource(reader)
	source.SourceFormat = bigquery.JSON
	source.AutoDetect = true // Let BigQuery auto-detect schema

	return source, nil
}

// createCSVSource creates a CSV source for BigQuery load job
func (b *BigQueryDestination) createCSVSource(records []*models.Record) (bigquery.LoadSource, error) {
	if len(records) == 0 {
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeData, "no records to create CSV source")
	}

	// Create a pipe for streaming data
	reader, writer := io.Pipe()

	// Start a goroutine to write CSV data
	go func() {
		defer func() { _ = writer.Close() }() // Ignore close error in goroutine

		csvWriter := csv.NewWriter(writer)
		defer csvWriter.Flush()

		// Get field names from first record for header
		var fieldNames []string
		for key := range records[0].Data {
			fieldNames = append(fieldNames, key)
		}

		// Write header
		if err := csvWriter.Write(fieldNames); err != nil {
			b.GetLogger().Error("Failed to write CSV header", zap.Error(err))
			writer.CloseWithError(err)
			return
		}

		// Write data rows
		for _, record := range records {
			var values []string
			for _, fieldName := range fieldNames {
				value := ""
				if val, exists := record.Data[fieldName]; exists && val != nil {
					value = stringpool.Sprintf("%v", val)
				}
				values = append(values, value)
			}

			if err := csvWriter.Write(values); err != nil {
				b.GetLogger().Error("Failed to write CSV row", zap.Error(err))
				writer.CloseWithError(err)
				return
			}
		}
	}()

	// Create and configure the CSV source
	source := bigquery.NewReaderSource(reader)
	source.SourceFormat = bigquery.CSV
	source.SkipLeadingRows = 1 // Skip header row
	source.AutoDetect = true   // Let BigQuery auto-detect schema

	return source, nil
}

// collectFromChannel collects all records from a channel
func (b *BigQueryDestination) collectFromChannel(ctx context.Context, ch chan *models.Record) ([]*models.Record, error) {
	var records []*models.Record

	for {
		select {
		case record, ok := <-ch:
			if !ok {
				return records, nil
			}
			records = append(records, record)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// collectFromStream collects all records from a stream
func (b *BigQueryDestination) collectFromStream(ctx context.Context, stream *core.RecordStream) ([]*models.Record, error) {
	var records []*models.Record

	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				return records, nil
			}
			records = append(records, record)
		case err := <-stream.Errors:
			if err != nil {
				return nil, err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// BeginTransaction begins a new transaction (not supported by BigQuery)
func (b *BigQueryDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return nil, nebulaerrors.New(nebulaerrors.ErrorTypeCapability, "BigQuery does not support traditional transactions")
}

// Upsert performs upsert operations using MERGE statement (not implemented yet)
func (b *BigQueryDestination) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	return nebulaerrors.New(nebulaerrors.ErrorTypeCapability, "upsert not yet implemented for BigQuery destination")
}

// DropSchema removes the table
func (b *BigQueryDestination) DropSchema(ctx context.Context, schema *core.Schema) error {
	if b.table == nil {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "no table configured")
	}

	// Delete the table
	if err := b.ExecuteWithCircuitBreaker(func() error {
		return b.table.Delete(ctx)
	}); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to drop table")
	}

	b.GetLogger().Info("table dropped",
		zap.String("table", b.tableID))

	return nil
}

// GetStats returns real-time statistics
func (b *BigQueryDestination) GetStats() *BigQueryStats {
	b.statsMutex.RLock()
	defer b.statsMutex.RUnlock()

	// Update current stats
	b.stats.RecordsWritten = atomic.LoadInt64(&b.recordsWritten)
	b.stats.BytesWritten = atomic.LoadInt64(&b.bytesWritten)
	b.stats.BatchesProcessed = atomic.LoadInt64(&b.batchesProcessed)
	b.stats.ErrorsEncountered = atomic.LoadInt64(&b.errorsEncountered)
	b.stats.StreamingEnabled = b.enableStreaming

	// Return copy to avoid race conditions
	statsCopy := *b.stats
	return &statsCopy
}

// Private method implementations

// validateConfig validates the connector configuration
func (b *BigQueryDestination) validateConfig(config *config.BaseConfig) error {
	// Basic validation - in production, would validate specific BigQuery config fields
	if config.Name == "" {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "connector name is required")
	}

	// For now, assume config contains required BigQuery fields
	// TODO: Add proper BigQuery config validation

	return nil
}

// extractBigQueryConfig extracts BigQuery-specific configuration
func (b *BigQueryDestination) extractBigQueryConfig(config *config.BaseConfig) {
	// Simplified config extraction for compilation
	// TODO: Extract from proper BigQuery-specific config structure
	b.projectID = "default-project"
	b.datasetID = "default-dataset"
	b.tableID = "default-table"
	b.location = "US"

	// Use performance config from BaseConfig
	if config.Performance.BatchSize > 0 {
		b.batchSize = config.Performance.BatchSize
		b.microBatch = pool.GetBatchSlice(config.Performance.BatchSize)
	}

	if config.Performance.Workers > 0 {
		b.workerCount = config.Performance.Workers
	}

	if config.Performance.MaxConcurrency > 0 {
		b.maxConcurrentBatches = config.Performance.MaxConcurrency
	}

	b.enableStreaming = config.Performance.EnableStreaming
}

// initializeClient creates the BigQuery client
func (b *BigQueryDestination) initializeClient(ctx context.Context) error {
	var opts []option.ClientOption

	// Add credentials if provided
	if b.credentialsPath != "" {
		opts = append(opts, option.WithCredentialsFile(b.credentialsPath))
	}

	// Create client
	client, err := bigquery.NewClient(ctx, b.projectID, opts...)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to create BigQuery client")
	}

	b.client = client
	return nil
}

// initializeDatasetAndTable initializes the dataset and table references
func (b *BigQueryDestination) initializeDatasetAndTable(ctx context.Context) error {
	// Get dataset reference
	b.dataset = b.client.Dataset(b.datasetID)

	// Check if dataset exists
	if _, err := b.dataset.Metadata(ctx); err != nil {
		// Create dataset if it doesn't exist
		if err := b.dataset.Create(ctx, &bigquery.DatasetMetadata{
			Location: b.location,
		}); err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to create dataset")
		}
	}

	// Get table reference
	b.table = b.dataset.Table(b.tableID)

	// Create inserter for streaming inserts
	b.inserter = b.table.Inserter()
	b.inserter.SkipInvalidRows = true
	b.inserter.IgnoreUnknownValues = true

	return nil
}

// convertToBigQuerySchema converts core schema to BigQuery schema
func (b *BigQueryDestination) convertToBigQuerySchema(schema *core.Schema) bigquery.Schema {
	var bqSchema bigquery.Schema

	for _, field := range schema.Fields {
		bqField := &bigquery.FieldSchema{
			Name:     field.Name,
			Type:     b.mapFieldTypeToBigQuery(field.Type),
			Required: !field.Nullable,
		}

		if field.Description != "" {
			bqField.Description = field.Description
		}

		bqSchema = append(bqSchema, bqField)
	}

	return bqSchema
}

// mapFieldTypeToBigQuery maps core field types to BigQuery types
func (b *BigQueryDestination) mapFieldTypeToBigQuery(fieldType core.FieldType) bigquery.FieldType {
	switch fieldType {
	case core.FieldTypeString:
		return bigquery.StringFieldType
	case core.FieldTypeInt:
		return bigquery.IntegerFieldType
	case core.FieldTypeFloat:
		return bigquery.FloatFieldType
	case core.FieldTypeBool:
		return bigquery.BooleanFieldType
	case core.FieldTypeTimestamp:
		return bigquery.TimestampFieldType
	case core.FieldTypeDate:
		return bigquery.DateFieldType
	case core.FieldTypeTime:
		return bigquery.TimeFieldType
	case core.FieldTypeBinary:
		return bigquery.BytesFieldType
	case core.FieldTypeJSON:
		return bigquery.JSONFieldType
	default:
		return bigquery.StringFieldType
	}
}

// createTable creates a new BigQuery table
func (b *BigQueryDestination) createTable(ctx context.Context, schema bigquery.Schema) error {
	tableMetadata := &bigquery.TableMetadata{
		Schema: schema,
	}

	// Add partitioning if configured
	if b.partitionField != "" {
		partitioning := &bigquery.TimePartitioning{
			Field: b.partitionField,
		}

		switch b.partitionType {
		case "HOUR":
			partitioning.Type = bigquery.HourPartitioningType
		case "MONTH":
			partitioning.Type = bigquery.MonthPartitioningType
		case "YEAR":
			partitioning.Type = bigquery.YearPartitioningType
		default:
			partitioning.Type = bigquery.DayPartitioningType
		}

		tableMetadata.TimePartitioning = partitioning
		tableMetadata.RequirePartitionFilter = b.requirePartitionFilter
	}

	// Add clustering if configured
	if len(b.clusteringFields) > 0 {
		tableMetadata.Clustering = &bigquery.Clustering{
			Fields: b.clusteringFields,
		}
	}

	return b.table.Create(ctx, tableMetadata)
}

// updateTableSchema updates the table schema
func (b *BigQueryDestination) updateTableSchema(ctx context.Context, schema bigquery.Schema) error {
	tableMetadata, err := b.table.Metadata(ctx)
	if err != nil {
		return err
	}

	update := bigquery.TableMetadataToUpdate{
		Schema: schema,
	}

	_, err = b.table.Update(ctx, update, tableMetadata.ETag)
	return err
}

// schemasMatch checks if two schemas match
func (b *BigQueryDestination) schemasMatch(schema1, schema2 bigquery.Schema) bool {
	if len(schema1) != len(schema2) {
		return false
	}

	// Create map for comparison
	fieldMap := make(map[string]*bigquery.FieldSchema)
	for _, field := range schema1 {
		fieldMap[field.Name] = field
	}

	for _, field := range schema2 {
		existing, ok := fieldMap[field.Name]
		if !ok || existing.Type != field.Type {
			return false
		}
	}

	return true
}

// startWorkerPool starts the worker pool
func (b *BigQueryDestination) startWorkerPool(ctx context.Context) {
	b.batchChan = make(chan *RecordBatch, b.maxConcurrentBatches)

	for i := 0; i < b.workerCount; i++ {
		b.workerWG.Add(1)
		go b.batchWorker(ctx, i)
	}

	b.GetLogger().Info("started worker pool",
		zap.Int("worker_count", b.workerCount))
}

// stopWorkerPool stops all workers
func (b *BigQueryDestination) stopWorkerPool() {
	b.shutdownOnce.Do(func() {
		if b.batchChan != nil {
			close(b.batchChan)
		}
		b.workerWG.Wait()
	})
}

// batchWorker processes batches
func (b *BigQueryDestination) batchWorker(ctx context.Context, id int) {
	defer b.workerWG.Done()

	for batch := range b.batchChan {
		select {
		case <-ctx.Done():
			return
		default:
			if err := b.insertBatch(ctx, batch); err != nil {
				atomic.AddInt64(&b.errorsEncountered, 1)
				b.GetLogger().Error("failed to insert batch",
					zap.Int("worker_id", id),
					zap.String("batch_id", batch.BatchID),
					zap.Error(err))
				if handleErr := b.HandleError(ctx, err, nil); handleErr != nil {
					b.GetLogger().Error("failed to handle error", zap.Error(handleErr))
				}
			}
		}
	}
}

// startMicroBatchTimer starts the timer for flushing micro-batches
func (b *BigQueryDestination) startMicroBatchTimer(ctx context.Context) {
	if b.microBatchTimer != nil {
		b.microBatchTimer.Stop()
	}

	b.microBatchTimer = time.AfterFunc(b.batchTimeout, func() {
		if err := b.flushMicroBatch(ctx); err != nil {
			b.GetLogger().Error("failed to flush micro-batch on timer", zap.Error(err))
		}
	})
}

// addToMicroBatch adds a record to the current micro-batch
func (b *BigQueryDestination) addToMicroBatch(ctx context.Context, record *models.Record) error {
	b.microBatchMutex.Lock()
	defer b.microBatchMutex.Unlock()

	b.microBatch = append(b.microBatch, record)

	// Check if batch is full
	if len(b.microBatch) >= b.batchSize {
		return b.flushMicroBatchLocked(ctx)
	}

	// Reset timer
	b.startMicroBatchTimer(ctx)

	return nil
}

// flushMicroBatch flushes the current micro-batch
func (b *BigQueryDestination) flushMicroBatch(ctx context.Context) error {
	b.microBatchMutex.Lock()
	defer b.microBatchMutex.Unlock()

	return b.flushMicroBatchLocked(ctx)
}

// flushMicroBatchLocked flushes the micro-batch (caller must hold lock)
func (b *BigQueryDestination) flushMicroBatchLocked(ctx context.Context) error {
	if len(b.microBatch) == 0 {
		return nil
	}

	// Stop timer
	if b.microBatchTimer != nil {
		b.microBatchTimer.Stop()
	}

	// Create batch
	batch := &RecordBatch{
		Records:   b.microBatch,
		Timestamp: time.Now(),
		BatchID:   stringpool.Sprintf("%s_%d", b.tableID, time.Now().UnixNano()),
	}

	// Send to batch channel
	select {
	case b.batchChan <- batch:
		// Update metrics
		atomic.AddInt64(&b.recordsWritten, int64(len(b.microBatch)))
		b.RecordMetric("micro_batch_flushed", 1, core.MetricTypeCounter)
		b.RecordMetric("micro_batch_size", len(b.microBatch), core.MetricTypeGauge)
	case <-ctx.Done():
		return ctx.Err()
	}

	// Clear micro-batch
	b.microBatch = pool.GetBatchSlice(b.batchSize)

	return nil
}

// processBatch processes a batch of records
func (b *BigQueryDestination) processBatch(ctx context.Context, batch []*models.Record) error {
	// Create batch directly
	recordBatch := &RecordBatch{
		Records:   batch,
		Timestamp: time.Now(),
		BatchID:   stringpool.Sprintf("%s_%d", b.tableID, time.Now().UnixNano()),
	}

	// Send to batch channel
	select {
	case b.batchChan <- recordBatch:
		atomic.AddInt64(&b.recordsWritten, int64(len(batch)))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// insertBatch inserts a batch into BigQuery
func (b *BigQueryDestination) insertBatch(ctx context.Context, batch *RecordBatch) error {
	startTime := time.Now()

	// Convert records to BigQuery format
	items := make([]interface{}, 0, len(batch.Records))
	for _, record := range batch.Records {
		// Use record.Data directly as it's already a map
		items = append(items, record.Data)
	}

	// Insert into BigQuery
	if err := b.inserter.Put(ctx, items); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to insert batch")
	}

	// Update metrics
	atomic.AddInt64(&b.batchesProcessed, 1)
	b.RecordMetric("batch_insert_latency", time.Since(startTime).Milliseconds(), core.MetricTypeGauge)

	// Calculate and update bytes written (approximate)
	var totalBytes int64
	for _, record := range batch.Records {
		if data, err := jsonpool.Marshal(record.Data); err == nil {
			totalBytes += int64(len(data))
		}
	}
	atomic.AddInt64(&b.bytesWritten, totalBytes)

	b.GetLogger().Debug("batch inserted successfully",
		zap.String("batch_id", batch.BatchID),
		zap.Int("record_count", len(batch.Records)),
		zap.Duration("latency", time.Since(startTime)))

	return nil
}
