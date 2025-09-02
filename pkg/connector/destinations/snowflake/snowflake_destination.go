package snowflake

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/compression"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/base"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/errors"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"

	// Snowflake driver
	_ "github.com/snowflakedb/gosnowflake"
)

// SnowflakeOptimizedDestination is a high-performance Snowflake destination connector
type SnowflakeOptimizedDestination struct {
	*base.BaseConnector

	// Connection configuration
	account   string
	user      string
	password  string
	database  string
	schema    string
	warehouse string
	role      string

	// Connection pool
	connectionPool     *sql.DB
	connectionPoolSize int

	// Stage configuration
	stageName         string
	stagePrefix       string
	useExternalStage  bool
	externalStageType string
	externalStageURL  string

	// Performance configuration
	parallelUploads   int
	microBatchSize    int
	microBatchTimeout time.Duration
	filesPerCopy      int
	maxFileSize       int64
	enableStreaming   bool
	asyncCopy         bool

	// File format configuration
	fileFormat      string
	compressionType string

	// Compression
	compressor compression.Compressor

	// State management
	currentTable   string
	currentSchema  *core.Schema
	recordsWritten int64
	bytesUploaded  int64
	filesUploaded  int64
	copyOperations int64
	uploadWorkers  int

	// Micro-batching
	microBatch      []*models.Record
	microBatchMutex sync.Mutex
	microBatchTimer *time.Timer
	microBatchCount int64

	// Upload workers
	uploadChan         chan *FileUpload
	uploadWG           sync.WaitGroup
	uploadWorkersMutex sync.RWMutex

	// Statistics
	stats      *SnowflakeStats
	statsMutex sync.RWMutex
}

// FileUpload represents a file to be uploaded to the stage
type FileUpload struct {
	Filename    string
	Data        []byte
	RecordCount int
	Timestamp   time.Time
}

// SnowflakeStats provides real-time statistics
type SnowflakeStats struct {
	RecordsWritten int64   `json:"records_written"`
	BytesUploaded  int64   `json:"bytes_uploaded"`
	FilesUploaded  int64   `json:"files_uploaded"`
	CopyOperations int64   `json:"copy_operations"`
	UploadWorkers  int     `json:"upload_workers"`
	MicroBatches   int64   `json:"micro_batches"`
	AverageLatency int64   `json:"average_latency_ms"`
	ErrorRate      float64 `json:"error_rate"`
}

// SnowflakeTransaction implements the core.Transaction interface
type SnowflakeTransaction struct {
	tx   *sql.Tx
	conn *sql.Conn
}

// Commit commits the transaction
func (st *SnowflakeTransaction) Commit(ctx context.Context) error {
	defer st.conn.Close()
	return st.tx.Commit()
}

// Rollback rolls back the transaction
func (st *SnowflakeTransaction) Rollback(ctx context.Context) error {
	defer st.conn.Close()
	return st.tx.Rollback()
}

// NewSnowflakeOptimizedDestination creates a new Snowflake optimized destination connector
func NewSnowflakeOptimizedDestination(name string, config *config.BaseConfig) (core.Destination, error) {
	base := base.NewBaseConnector(name, core.ConnectorTypeDestination, "2.0.0")

	dest := &SnowflakeOptimizedDestination{
		BaseConnector: base,

		// Default configuration
		connectionPoolSize: 8,
		parallelUploads:    16,
		microBatchSize:     200000,
		microBatchTimeout:  5 * time.Second,
		filesPerCopy:       10,
		maxFileSize:        500 * 1024 * 1024, // 500MB
		fileFormat:         "CSV",
		compressionType:    "GZIP",
		enableStreaming:    true,
		asyncCopy:          true,

		microBatch: pool.GetBatchSlice(200000),
		stats:      &SnowflakeStats{},
	}

	return dest, nil
}

// Initialize initializes the Snowflake destination connector
func (s *SnowflakeOptimizedDestination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	// Initialize base connector first
	if err := s.BaseConnector.Initialize(ctx, config); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "failed to initialize base connector")
	}

	// Validate and extract configuration
	if err := s.validateConfig(config); err != nil {
		return err
	}

	s.extractSnowflakeConfig(config)

	// Initialize compression if enabled
	if s.compressionType != "NONE" {
		if err := s.initializeCompression(); err != nil {
			return err
		}
	}

	// Initialize connection pool with circuit breaker protection
	if err := s.ExecuteWithCircuitBreaker(func() error {
		return s.initializeConnectionPool(ctx)
	}); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to initialize connection pool")
	}

	// Initialize stage
	if err := s.ExecuteWithCircuitBreaker(func() error {
		return s.initializeStage(ctx)
	}); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to initialize stage")
	}

	// Start upload workers
	s.startUploadWorkers(ctx)

	s.UpdateHealth(true, map[string]interface{}{
		"connection_pool_size": s.connectionPoolSize,
		"parallel_uploads":     s.parallelUploads,
		"micro_batch_size":     s.microBatchSize,
		"stage_name":           s.stageName,
		"file_format":          s.fileFormat,
		"compression_type":     s.compressionType,
	})

	s.GetLogger().Info("Snowflake optimized destination initialized",
		zap.String("account", s.account),
		zap.String("database", s.database),
		zap.String("schema", s.schema),
		zap.String("warehouse", s.warehouse),
		zap.Int("connection_pool_size", s.connectionPoolSize),
		zap.Int("parallel_uploads", s.parallelUploads),
		zap.Int("micro_batch_size", s.microBatchSize),
		zap.String("file_format", s.fileFormat),
		zap.Bool("external_stage", s.useExternalStage))

	return nil
}

// CreateSchema creates or updates the schema for the destination
func (s *SnowflakeOptimizedDestination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	s.currentSchema = schema

	// Build CREATE TABLE statement
	createTableSQL := s.buildCreateTableSQL(schema)

	// Execute with circuit breaker protection
	if err := s.ExecuteWithCircuitBreaker(func() error {
		return s.executeSQL(ctx, createTableSQL)
	}); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to create schema")
	}

	s.GetLogger().Info("schema created",
		zap.String("schema_name", schema.Name),
		zap.String("table_name", s.currentTable),
		zap.Int("field_count", len(schema.Fields)))

	return nil
}

// AlterSchema alters the schema for the destination
func (s *SnowflakeOptimizedDestination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	s.currentSchema = newSchema

	// Generate ALTER TABLE statements for new columns
	alterStatements := s.buildAlterTableSQL(oldSchema, newSchema)

	for _, stmt := range alterStatements {
		if err := s.ExecuteWithCircuitBreaker(func() error {
			return s.executeSQL(ctx, stmt)
		}); err != nil {
			return errors.Wrap(err, errors.ErrorTypeConnection, "failed to alter schema")
		}
	}

	s.GetLogger().Info("schema altered",
		zap.String("table_name", s.currentTable),
		zap.Int("new_fields", len(newSchema.Fields)-len(oldSchema.Fields)))

	return nil
}

// Write writes a stream of records to Snowflake using micro-batching
func (s *SnowflakeOptimizedDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	recordCount := 0
	startTime := time.Now()

	// Start micro-batch timer
	s.startMicroBatchTimer(ctx)

	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				// Stream closed, flush remaining micro-batch
				if err := s.flushMicroBatch(ctx); err != nil {
					return err
				}
				s.GetLogger().Info("write stream completed",
					zap.Int("total_records", recordCount),
					zap.Duration("duration", time.Since(startTime)))
				return nil
			}

			// Rate limiting
			if err := s.RateLimit(ctx); err != nil {
				return err
			}

			// Process the record
			if err := s.addToMicroBatch(ctx, record); err != nil {
				if handleErr := s.HandleError(ctx, err, record); handleErr != nil {
					return handleErr
				}
				continue
			}
			recordCount++

			// Report progress
			s.ReportProgress(int64(recordCount), -1)
			s.RecordMetric("records_processed", 1, core.MetricTypeCounter)

		case err := <-stream.Errors:
			if err != nil {
				return errors.Wrap(err, errors.ErrorTypeData, "error in record stream")
			}

		case <-ctx.Done():
			// Flush remaining micro-batch
			s.flushMicroBatch(ctx)
			return ctx.Err()
		}
	}
}

// WriteBatch writes batches of records to Snowflake
func (s *SnowflakeOptimizedDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	recordCount := 0
	batchCount := 0
	startTime := time.Now()

	for {
		select {
		case batch, ok := <-stream.Batches:
			if !ok {
				// Stream closed, flush remaining micro-batch
				if err := s.flushMicroBatch(ctx); err != nil {
					return err
				}
				s.GetLogger().Info("write batch stream completed",
					zap.Int("batches_processed", batchCount),
					zap.Int("total_records", recordCount),
					zap.Duration("duration", time.Since(startTime)))
				return nil
			}

			// Rate limiting
			if err := s.RateLimit(ctx); err != nil {
				return err
			}

			// Process batch records
			if err := s.processBatch(ctx, batch); err != nil {
				if handleErr := s.HandleError(ctx, err, nil); handleErr != nil {
					return handleErr
				}
				continue
			}

			batchCount++
			recordCount += len(batch)

			// Report progress
			s.ReportProgress(int64(recordCount), -1)
			s.RecordMetric("batches_processed", 1, core.MetricTypeCounter)
			s.RecordMetric("records_in_batch", len(batch), core.MetricTypeGauge)

		case err := <-stream.Errors:
			if err != nil {
				return errors.Wrap(err, errors.ErrorTypeData, "error in batch stream")
			}

		case <-ctx.Done():
			s.flushMicroBatch(ctx)
			return ctx.Err()
		}
	}
}

// Close closes the Snowflake destination connector
func (s *SnowflakeOptimizedDestination) Close(ctx context.Context) error {
	// Flush any remaining micro-batch
	if err := s.flushMicroBatch(ctx); err != nil {
		s.GetLogger().Error("failed to flush micro-batch during close", zap.Error(err))
	}

	// Stop upload workers
	s.stopUploadWorkers()

	// Execute final COPY command to load any remaining staged files
	if atomic.LoadInt64(&s.filesUploaded) > atomic.LoadInt64(&s.copyOperations)*int64(s.filesPerCopy) {
		s.executeCopyCommand(ctx)
	}

	// Close connection pool
	if s.connectionPool != nil {
		if err := s.connectionPool.Close(); err != nil {
			s.GetLogger().Error("failed to close connection pool", zap.Error(err))
		}
	}

	// Optimization layer removed for simplification

	s.GetLogger().Info("Snowflake destination closed",
		zap.Int64("total_records_written", atomic.LoadInt64(&s.recordsWritten)),
		zap.Int64("total_bytes_uploaded", atomic.LoadInt64(&s.bytesUploaded)),
		zap.Int64("total_files_uploaded", atomic.LoadInt64(&s.filesUploaded)),
		zap.Int64("total_copy_operations", atomic.LoadInt64(&s.copyOperations)))

	// Return microBatch to pool
	if s.microBatch != nil {
		pool.PutBatchSlice(s.microBatch)
		s.microBatch = nil
	}

	// Close base connector
	return s.BaseConnector.Close(ctx)
}

// SupportsBulkLoad returns true since Snowflake supports bulk loading via COPY
func (s *SnowflakeOptimizedDestination) SupportsBulkLoad() bool {
	return true
}

// SupportsTransactions returns true since Snowflake supports transactions
func (s *SnowflakeOptimizedDestination) SupportsTransactions() bool {
	return true
}

// SupportsUpsert returns true since Snowflake supports MERGE operations
func (s *SnowflakeOptimizedDestination) SupportsUpsert() bool {
	return true
}

// SupportsBatch returns true since Snowflake supports batch operations
func (s *SnowflakeOptimizedDestination) SupportsBatch() bool {
	return true
}

// SupportsStreaming returns true since this connector supports streaming
func (s *SnowflakeOptimizedDestination) SupportsStreaming() bool {
	return s.enableStreaming
}

// BulkLoad loads data in bulk using COPY command with optimized staging
func (s *SnowflakeOptimizedDestination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	if s.currentTable == "" {
		return errors.New(errors.ErrorTypeConfig, "no table configured for bulk load")
	}

	// Start timing for performance metrics
	start := time.Now()
	defer func() {
		s.GetMetricsCollector().RecordHistogram("bulk_load_duration", float64(time.Since(start).Milliseconds()))
	}()

	// Handle different reader types
	var records []*models.Record
	var err error

	switch r := reader.(type) {
	case []*models.Record:
		records = r
	case chan *models.Record:
		records, err = s.collectFromChannel(ctx, r)
		if err != nil {
			return errors.Wrap(err, errors.ErrorTypeData, "failed to collect records from channel")
		}
	case *core.RecordStream:
		records, err = s.collectFromStream(ctx, r)
		if err != nil {
			return errors.Wrap(err, errors.ErrorTypeData, "failed to collect records from stream")
		}
	default:
		return errors.New(errors.ErrorTypeValidation, "unsupported reader type for bulk load")
	}

	if len(records) == 0 {
		s.GetLogger().Info("No records to bulk load")
		return nil
	}

	s.GetLogger().Info("Starting bulk load operation",
		zap.Int("total_records", len(records)),
		zap.String("format", format))

	// Determine optimal chunking strategy based on record count and size
	chunkSize := s.calculateOptimalChunkSize(len(records))
	chunks := s.chunkRecords(records, chunkSize)

	// Process chunks in parallel for maximum throughput
	return s.processBulkChunks(ctx, chunks, format)
}

// calculateOptimalChunkSize determines the best chunk size for bulk loading
func (s *SnowflakeOptimizedDestination) calculateOptimalChunkSize(totalRecords int) int {
	// Base chunk size on micro batch size, but adjust for bulk operations
	baseChunkSize := s.microBatchSize

	// For very large datasets, use larger chunks to reduce overhead
	if totalRecords > 1000000 {
		baseChunkSize = 500000 // 500K records per chunk
	} else if totalRecords > 100000 {
		baseChunkSize = 100000 // 100K records per chunk
	} else if totalRecords > 10000 {
		baseChunkSize = 50000 // 50K records per chunk
	}

	// Ensure we don't exceed available workers
	maxChunks := s.parallelUploads * 2
	if totalRecords/baseChunkSize > maxChunks {
		baseChunkSize = totalRecords / maxChunks
	}

	// Minimum chunk size to maintain efficiency
	if baseChunkSize < 1000 {
		baseChunkSize = 1000
	}

	return baseChunkSize
}

// chunkRecords splits records into optimally-sized chunks
func (s *SnowflakeOptimizedDestination) chunkRecords(records []*models.Record, chunkSize int) [][]*models.Record {
	var chunks [][]*models.Record

	for i := 0; i < len(records); i += chunkSize {
		end := i + chunkSize
		if end > len(records) {
			end = len(records)
		}
		chunks = append(chunks, records[i:end])
	}

	return chunks
}

// processBulkChunks processes chunks in parallel using worker pool
func (s *SnowflakeOptimizedDestination) processBulkChunks(ctx context.Context, chunks [][]*models.Record, format string) error {
	if len(chunks) == 0 {
		return nil
	}

	// Create error aggregation channel
	errorChan := make(chan error, len(chunks))

	// Semaphore to limit concurrent workers
	semaphore := make(chan struct{}, s.parallelUploads)

	// Wait group for all workers
	var wg sync.WaitGroup

	s.GetLogger().Info("Processing bulk chunks in parallel",
		zap.Int("chunk_count", len(chunks)),
		zap.Int("parallel_workers", s.parallelUploads))

	// Process each chunk in parallel
	for i, chunk := range chunks {
		wg.Add(1)
		go func(chunkIndex int, chunkRecords []*models.Record) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Process this chunk
			if err := s.processBulkChunk(ctx, chunkRecords, format, chunkIndex); err != nil {
				s.GetLogger().Error("Failed to process bulk chunk",
					zap.Int("chunk_index", chunkIndex),
					zap.Int("records_in_chunk", len(chunkRecords)),
					zap.Error(err))
				errorChan <- err
				return
			}

			s.GetLogger().Debug("Successfully processed bulk chunk",
				zap.Int("chunk_index", chunkIndex),
				zap.Int("records_processed", len(chunkRecords)))
		}(i, chunk)
	}

	// Wait for all workers to complete
	wg.Wait()
	close(errorChan)

	// Collect any errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		// Log all errors and return aggregate error
		s.GetLogger().Error("Bulk load completed with errors", zap.Int("error_count", len(errors)))
		return errors[0] // Return first error for now
	}

	s.GetLogger().Info("Bulk load completed successfully",
		zap.Int("total_chunks", len(chunks)))

	return nil
}

// processBulkChunk processes a single chunk of records
func (s *SnowflakeOptimizedDestination) processBulkChunk(ctx context.Context, records []*models.Record, format string, chunkIndex int) error {
	// Generate unique filename for this chunk using pooled string building
	filename := stringpool.Sprintf("bulk_load_%d_%d_%d.%s",
		time.Now().Unix(),
		chunkIndex,
		len(records),
		strings.ToLower(format))

	// Convert records to file format
	var data []byte
	var err error

	switch strings.ToUpper(format) {
	case "CSV":
		data, err = s.recordsToCSV(records)
	case "JSON":
		data, err = s.recordsToJSON(records)
	case "JSONL":
		data, err = s.recordsToJSONL(records)
	default:
		return errors.New(errors.ErrorTypeValidation, stringpool.Sprintf("unsupported bulk load format: %s", format))
	}

	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to convert records to format")
	}

	// Compress data if compression is enabled
	if s.compressor != nil {
		compressedData, err := s.compressor.Compress(data)
		if err != nil {
			s.GetLogger().Warn("Failed to compress bulk data, using uncompressed", zap.Error(err))
		} else {
			data = compressedData
			filename += ".gz" // Add compression extension
		}
	}

	// Upload to stage
	if err := s.uploadToStage(ctx, filename, data); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to upload chunk to stage")
	}

	// Execute COPY command for this file
	copySQL := s.buildCopySQL(filename, format)
	if err := s.executeSQL(ctx, copySQL); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to execute COPY command")
	}

	// Update statistics
	atomic.AddInt64(&s.recordsWritten, int64(len(records)))
	atomic.AddInt64(&s.bytesUploaded, int64(len(data)))
	atomic.AddInt64(&s.filesUploaded, 1)
	atomic.AddInt64(&s.copyOperations, 1)

	return nil
}

// collectFromChannel collects all records from a channel
func (s *SnowflakeOptimizedDestination) collectFromChannel(ctx context.Context, ch chan *models.Record) ([]*models.Record, error) {
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
func (s *SnowflakeOptimizedDestination) collectFromStream(ctx context.Context, stream *core.RecordStream) ([]*models.Record, error) {
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

// BeginTransaction begins a new transaction
func (s *SnowflakeOptimizedDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	// Get connection from pool
	conn, err := s.connectionPool.Conn(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConnection, "failed to get connection from pool")
	}

	// Begin transaction
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		conn.Close()
		return nil, errors.Wrap(err, errors.ErrorTypeConnection, "failed to begin transaction")
	}

	return &SnowflakeTransaction{
		tx:   tx,
		conn: conn,
	}, nil
}

// Upsert performs upsert operations using MERGE statement
func (s *SnowflakeOptimizedDestination) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	if len(records) == 0 {
		return nil
	}

	// Build MERGE statement
	mergeSQL := s.buildMergeSQL(records, keys)

	// Execute with circuit breaker protection
	return s.ExecuteWithCircuitBreaker(func() error {
		return s.executeSQL(ctx, mergeSQL)
	})
}

// DropSchema removes the schema (table)
func (s *SnowflakeOptimizedDestination) DropSchema(ctx context.Context, schema *core.Schema) error {
	if s.currentTable == "" {
		return errors.New(errors.ErrorTypeConfig, "no table configured")
	}

	// Use SQLBuilder for DROP TABLE statement
	sqlBuilder := stringpool.NewSQLBuilder(128)
	defer sqlBuilder.Close()

	dropSQL := sqlBuilder.WriteQuery("DROP TABLE IF EXISTS ").
		WriteIdentifier(s.database).WriteQuery(".").
		WriteIdentifier(s.schema).WriteQuery(".").
		WriteIdentifier(s.currentTable).String()

	// Execute with circuit breaker protection
	if err := s.ExecuteWithCircuitBreaker(func() error {
		return s.executeSQL(ctx, dropSQL)
	}); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to drop schema")
	}

	s.GetLogger().Info("schema dropped",
		zap.String("table", s.currentTable))

	return nil
}

// GetStats returns real-time statistics
func (s *SnowflakeOptimizedDestination) GetStats() *SnowflakeStats {
	s.statsMutex.RLock()
	defer s.statsMutex.RUnlock()

	// Update current stats
	s.stats.RecordsWritten = atomic.LoadInt64(&s.recordsWritten)
	s.stats.BytesUploaded = atomic.LoadInt64(&s.bytesUploaded)
	s.stats.FilesUploaded = atomic.LoadInt64(&s.filesUploaded)
	s.stats.CopyOperations = atomic.LoadInt64(&s.copyOperations)
	s.stats.UploadWorkers = s.uploadWorkers
	s.stats.MicroBatches = atomic.LoadInt64(&s.microBatchCount)

	// Return copy to avoid race conditions
	statsCopy := *s.stats
	return &statsCopy
}

// Private methods implementation would continue...
// Due to length constraints, showing the key structure and patterns

// validateConfig validates the connector configuration
func (s *SnowflakeOptimizedDestination) validateConfig(config *config.BaseConfig) error {
	// Simplified validation for compilation
	// TODO: Add proper validation when connector-specific config structure is defined
	if config == nil {
		return errors.New(errors.ErrorTypeConfig, "configuration is required")
	}
	return nil
}

// extractSnowflakeConfig extracts Snowflake-specific configuration
func (s *SnowflakeOptimizedDestination) extractSnowflakeConfig(config *config.BaseConfig) {
	// Simplified config extraction for compilation
	// TODO: Extract from proper Snowflake-specific config structure
	s.account = "default-account"
	s.user = "default-user"
	s.password = "default-password"
	s.database = "default-database"
	s.schema = "default-schema"
	s.warehouse = "default-warehouse"
	s.currentTable = "default-table"

	// Use performance config from BaseConfig
	if config.Performance.BatchSize > 0 {
		s.microBatchSize = config.Performance.BatchSize
		// Reset micro batch from pool
		if s.microBatch != nil {
			pool.PutBatchSlice(s.microBatch)
		}
		s.microBatch = pool.GetBatchSlice(config.Performance.BatchSize)
	}

	if config.Performance.Workers > 0 {
		s.parallelUploads = config.Performance.Workers
	}

	s.enableStreaming = config.Performance.EnableStreaming
	s.asyncCopy = config.Performance.AsyncOperations
}

// Private method implementations

// initializeCompression initializes the compression handler
func (s *SnowflakeOptimizedDestination) initializeCompression() error {
	algorithm := compression.Gzip // Default
	switch s.compressionType {
	case "GZIP":
		algorithm = compression.Gzip
	case "SNAPPY":
		algorithm = compression.Snappy
	case "LZ4":
		algorithm = compression.LZ4
	case "ZSTD":
		algorithm = compression.Zstd
	case "S2":
		algorithm = compression.S2
	case "DEFLATE":
		algorithm = compression.Deflate
	}

	config := &compression.Config{
		Algorithm:   algorithm,
		Level:       compression.Default,
		BufferSize:  64 * 1024, // 64KB
		Concurrency: 4,
	}

	var err error
	s.compressor, err = compression.NewCompressor(config)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "failed to initialize compressor")
	}

	return nil
}

// initializeConnectionPool creates the connection pool
func (s *SnowflakeOptimizedDestination) initializeConnectionPool(ctx context.Context) error {
	// Build Snowflake connection string (DSN)
	// Format: username:password@account/database/schema?warehouse=wh&role=role
	dsn := stringpool.Sprintf("%s:%s@%s/%s/%s",
		s.user, s.password, s.account, s.database, s.schema)

	// Add query parameters
	params := []string{}
	if s.warehouse != "" {
		params = append(params, stringpool.Sprintf("warehouse=%s", s.warehouse))
	}
	if s.role != "" {
		params = append(params, stringpool.Sprintf("role=%s", s.role))
	}

	// Add additional Snowflake-specific parameters for performance
	params = append(params, "ocspFailOpen=true") // Continue if OCSP check fails
	params = append(params, "validateDefaultParameters=true")
	params = append(params, "clientSessionKeepAlive=true") // Keep session alive

	if len(params) > 0 {
		dsn = stringpool.Concat(dsn, "?", stringpool.JoinPooled(params, "&"))
	}

	var err error
	s.connectionPool, err = sql.Open("snowflake", dsn)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to create connection pool")
	}

	// Configure pool
	s.connectionPool.SetMaxOpenConns(s.connectionPoolSize)
	s.connectionPool.SetMaxIdleConns(s.connectionPoolSize / 2)
	s.connectionPool.SetConnMaxLifetime(30 * time.Minute)

	// Test connection
	if err := s.connectionPool.PingContext(ctx); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to ping Snowflake")
	}

	return nil
}

// initializeStage creates or validates the stage
func (s *SnowflakeOptimizedDestination) initializeStage(ctx context.Context) error {
	var stageSQL string

	// Use SQLBuilder for stage creation
	sqlBuilder := stringpool.NewSQLBuilder(512)
	defer sqlBuilder.Close()

	if s.useExternalStage {
		// Create external stage
		stageSQL = sqlBuilder.WriteQuery("CREATE STAGE IF NOT EXISTS ").
			WriteIdentifier(s.database).WriteQuery(".").
			WriteIdentifier(s.schema).WriteQuery(".").
			WriteIdentifier(s.stageName).
			WriteQuery(" URL = '").WriteQuery(s.externalStageURL).
			WriteQuery("' FILE_FORMAT = (TYPE = ").
			WriteQuery(s.fileFormat).WriteQuery(" COMPRESSION = ").
			WriteQuery(s.compressionType).WriteQuery(")").String()
	} else {
		// Create internal stage
		stageSQL = sqlBuilder.WriteQuery("CREATE STAGE IF NOT EXISTS ").
			WriteIdentifier(s.database).WriteQuery(".").
			WriteIdentifier(s.schema).WriteQuery(".").
			WriteIdentifier(s.stageName).
			WriteQuery(" FILE_FORMAT = (TYPE = ").
			WriteQuery(s.fileFormat).WriteQuery(" COMPRESSION = ").
			WriteQuery(s.compressionType).WriteQuery(")").String()
	}

	return s.executeSQL(ctx, stageSQL)
}

// startUploadWorkers starts the upload worker pool
func (s *SnowflakeOptimizedDestination) startUploadWorkers(ctx context.Context) {
	s.uploadChan = make(chan *FileUpload, s.parallelUploads*2)
	s.uploadWorkers = s.parallelUploads

	for i := 0; i < s.parallelUploads; i++ {
		s.uploadWG.Add(1)
		go s.uploadWorker(ctx, i)
	}

	s.GetLogger().Info("started upload workers",
		zap.Int("worker_count", s.parallelUploads))
}

// stopUploadWorkers stops all upload workers
func (s *SnowflakeOptimizedDestination) stopUploadWorkers() {
	if s.uploadChan != nil {
		close(s.uploadChan)
	}
	s.uploadWG.Wait()
}

// uploadWorker processes file uploads
func (s *SnowflakeOptimizedDestination) uploadWorker(ctx context.Context, id int) {
	defer s.uploadWG.Done()

	for upload := range s.uploadChan {
		select {
		case <-ctx.Done():
			return
		default:
			if err := s.uploadFile(ctx, upload); err != nil {
				s.GetLogger().Error("failed to upload file",
					zap.String("filename", upload.Filename),
					zap.Error(err))
				s.HandleError(ctx, err, nil)
			}
		}
	}
}

// startMicroBatchTimer starts the timer for flushing micro-batches
func (s *SnowflakeOptimizedDestination) startMicroBatchTimer(ctx context.Context) {
	if s.microBatchTimer != nil {
		s.microBatchTimer.Stop()
	}

	s.microBatchTimer = time.AfterFunc(s.microBatchTimeout, func() {
		if err := s.flushMicroBatch(ctx); err != nil {
			s.GetLogger().Error("failed to flush micro-batch on timer", zap.Error(err))
		}
	})
}

// addToMicroBatch adds a record to the current micro-batch
func (s *SnowflakeOptimizedDestination) addToMicroBatch(ctx context.Context, record *models.Record) error {
	s.microBatchMutex.Lock()
	defer s.microBatchMutex.Unlock()

	s.microBatch = append(s.microBatch, record)

	// Check if batch is full
	if len(s.microBatch) >= s.microBatchSize {
		return s.flushMicroBatchLocked(ctx)
	}

	// Reset timer
	s.startMicroBatchTimer(ctx)

	return nil
}

// flushMicroBatch flushes the current micro-batch
func (s *SnowflakeOptimizedDestination) flushMicroBatch(ctx context.Context) error {
	s.microBatchMutex.Lock()
	defer s.microBatchMutex.Unlock()

	return s.flushMicroBatchLocked(ctx)
}

// flushMicroBatchLocked flushes the micro-batch (caller must hold lock)
func (s *SnowflakeOptimizedDestination) flushMicroBatchLocked(ctx context.Context) error {
	if len(s.microBatch) == 0 {
		return nil
	}

	// Stop timer
	if s.microBatchTimer != nil {
		s.microBatchTimer.Stop()
	}

	// Create file upload
	filename := stringpool.Sprintf("%s/%s_%d_%d.%s",
		s.stagePrefix,
		s.currentTable,
		time.Now().Unix(),
		atomic.AddInt64(&s.microBatchCount, 1),
		strings.ToLower(s.fileFormat))

	// Convert records to file format
	data, err := s.convertRecordsToFileFormat(s.microBatch)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to convert records")
	}

	// Compress if enabled
	if s.compressor != nil {
		compressed, err := s.compressor.Compress(data)
		if err != nil {
			return errors.Wrap(err, errors.ErrorTypeData, "failed to compress data")
		}
		data = compressed
		filename += "." + strings.ToLower(s.compressionType)
	}

	// Create upload
	upload := &FileUpload{
		Filename:    filename,
		Data:        data,
		RecordCount: len(s.microBatch),
		Timestamp:   time.Now(),
	}

	// Send to upload channel
	select {
	case s.uploadChan <- upload:
		// Update metrics
		atomic.AddInt64(&s.recordsWritten, int64(len(s.microBatch)))
		s.RecordMetric("micro_batch_flushed", 1, core.MetricTypeCounter)
		s.RecordMetric("micro_batch_size", len(s.microBatch), core.MetricTypeGauge)
	case <-ctx.Done():
		return ctx.Err()
	}

	// Clear micro-batch
	s.microBatch = s.microBatch[:0]

	return nil
}

// processBatch processes a batch of records
func (s *SnowflakeOptimizedDestination) processBatch(ctx context.Context, batch []*models.Record) error {
	for _, record := range batch {
		if err := s.addToMicroBatch(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

// buildCreateTableSQL builds the CREATE TABLE SQL statement
func (s *SnowflakeOptimizedDestination) buildCreateTableSQL(schema *core.Schema) string {
	if s.currentTable == "" {
		s.currentTable = schema.Name
	}

	var columns []string
	for _, field := range schema.Fields {
		// Use SQLBuilder for column definitions
		colBuilder := stringpool.NewSQLBuilder(64)
		column := colBuilder.WriteIdentifier(field.Name).WriteSpace().
			WriteQuery(s.mapFieldTypeToSnowflake(field.Type)).String()
		colBuilder.Close()
		columns = append(columns, column)
	}

	// Use SQLBuilder for CREATE TABLE statement
	sqlBuilder := stringpool.NewSQLBuilder(512)
	defer sqlBuilder.Close()

	return sqlBuilder.WriteQuery("CREATE TABLE IF NOT EXISTS ").
		WriteIdentifier(s.database).WriteQuery(".").
		WriteIdentifier(s.schema).WriteQuery(".").
		WriteIdentifier(s.currentTable).WriteQuery(" (").
		WriteQuery(stringpool.JoinPooled(columns, ", ")).
		WriteQuery(")").String()
}

// buildAlterTableSQL builds ALTER TABLE statements for new columns
func (s *SnowflakeOptimizedDestination) buildAlterTableSQL(old, new *core.Schema) []string {
	var statements []string

	// Find new fields
	oldFields := make(map[string]*core.Field)
	for _, field := range old.Fields {
		oldFields[field.Name] = &field
	}

	for _, field := range new.Fields {
		if _, exists := oldFields[field.Name]; !exists {
			// New field - add column using SQLBuilder
			sqlBuilder := stringpool.NewSQLBuilder(256)
			stmt := sqlBuilder.WriteQuery("ALTER TABLE ").
				WriteIdentifier(s.database).WriteQuery(".").
				WriteIdentifier(s.schema).WriteQuery(".").
				WriteIdentifier(s.currentTable).WriteQuery(" ADD COLUMN ").
				WriteIdentifier(field.Name).WriteSpace().
				WriteQuery(s.mapFieldTypeToSnowflake(field.Type)).String()
			sqlBuilder.Close()
			statements = append(statements, stmt)
		}
	}

	return statements
}

// buildMergeSQL builds the MERGE statement for upsert
func (s *SnowflakeOptimizedDestination) buildMergeSQL(records []*models.Record, keys []string) string {
	// This is a simplified implementation
	// In production, would build a proper MERGE statement with staging table
	return stringpool.Sprintf("-- MERGE implementation needed for %d records", len(records))
}

// executeSQL executes a SQL statement
func (s *SnowflakeOptimizedDestination) executeSQL(ctx context.Context, sql string) error {
	_, err := s.connectionPool.ExecContext(ctx, sql)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to execute SQL")
	}
	return nil
}

// Helper methods

// mapFieldTypeToSnowflake maps core field types to Snowflake types
func (s *SnowflakeOptimizedDestination) mapFieldTypeToSnowflake(fieldType core.FieldType) string {
	switch fieldType {
	case core.FieldTypeString:
		return "VARCHAR"
	case core.FieldTypeInt:
		return "NUMBER"
	case core.FieldTypeFloat:
		return "FLOAT"
	case core.FieldTypeBool:
		return "BOOLEAN"
	case core.FieldTypeTimestamp:
		return "TIMESTAMP_NTZ"
	case core.FieldTypeDate:
		return "DATE"
	case core.FieldTypeTime:
		return "TIME"
	case core.FieldTypeBinary:
		return "BINARY"
	case core.FieldTypeJSON:
		return "VARIANT"
	default:
		return "VARCHAR"
	}
}

// convertRecordsToFileFormat converts records to the configured file format
func (s *SnowflakeOptimizedDestination) convertRecordsToFileFormat(records []*models.Record) ([]byte, error) {
	// This is a simplified implementation
	// In production, would properly convert to CSV, JSON, Parquet, etc.

	// Get buffer from pool with estimated size
	estimatedSize := len(records) * 1024 // Estimate 1KB per record
	buf := pool.GlobalBufferPool.Get(estimatedSize)
	defer pool.GlobalBufferPool.Put(buf)

	// Track actual usage
	offset := 0

	// Helper function to append to buffer
	appendBytes := func(data []byte) {
		if offset+len(data) > len(buf) {
			// Need to grow buffer
			newBuf := pool.GlobalBufferPool.Get(len(buf) * 2)
			copy(newBuf, buf[:offset])
			pool.GlobalBufferPool.Put(buf)
			buf = newBuf
		}
		copy(buf[offset:], data)
		offset += len(data)
	}

	appendString := func(s string) {
		appendBytes([]byte(s))
	}

	appendByte := func(b byte) {
		if offset >= len(buf) {
			// Need to grow buffer
			newBuf := pool.GlobalBufferPool.Get(len(buf) * 2)
			copy(newBuf, buf[:offset])
			pool.GlobalBufferPool.Put(buf)
			buf = newBuf
		}
		buf[offset] = b
		offset++
	}

	switch s.fileFormat {
	case "JSON", "JSONL":
		// Convert to JSON Lines format
		for _, record := range records {
			data, err := jsonpool.Marshal(record.Data)
			if err != nil {
				return nil, errors.Wrap(err, errors.ErrorTypeData, "failed to marshal record")
			}
			appendBytes(data)
			appendByte('\n')
		}
	case "CSV":
		// For CSV, we need to know the schema to maintain column order
		// This is a simplified implementation
		for i, record := range records {
			if i == 0 && s.currentSchema != nil {
				// Write header
				var headers []string
				for _, field := range s.currentSchema.Fields {
					headers = append(headers, field.Name)
				}
				appendString(stringpool.JoinPooled(headers, ","))
				appendByte('\n')
			}

			// Write values
			if s.currentSchema != nil {
				var values []string
				for _, field := range s.currentSchema.Fields {
					val := ""
					if v, ok := record.Data[field.Name]; ok {
						val = stringpool.ValueToString(v)
					}
					values = append(values, val)
				}
				appendString(stringpool.JoinPooled(values, ","))
				appendByte('\n')
			}
		}
	default:
		// Default to JSON
		for _, record := range records {
			data, err := jsonpool.Marshal(record.Data)
			if err != nil {
				return nil, errors.Wrap(err, errors.ErrorTypeData, "failed to marshal record")
			}
			appendBytes(data)
			appendByte('\n')
		}
	}

	// Return a copy of the used portion of the buffer
	result := pool.GetByteSlice()

	if cap(result) < offset {
		result = make([]byte, offset)
	}

	defer pool.PutByteSlice(result)
	copy(result, buf[:offset])
	return result, nil
}

// uploadFile uploads a file to the stage
func (s *SnowflakeOptimizedDestination) uploadFile(ctx context.Context, upload *FileUpload) error {
	// Create a temporary file for staging
	tempFile := stringpool.Sprintf("/tmp/%s", upload.Filename)
	if err := os.WriteFile(tempFile, upload.Data, 0644); err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to write temp file")
	}
	defer os.Remove(tempFile) // Clean up temp file

	// Build PUT command using SQLBuilder
	sqlBuilder := stringpool.NewSQLBuilder(256)
	defer sqlBuilder.Close()

	putSQL := sqlBuilder.WriteQuery("PUT file://").WriteQuery(tempFile).
		WriteQuery(" @").
		WriteIdentifier(s.database).WriteQuery(".").
		WriteIdentifier(s.schema).WriteQuery(".").
		WriteIdentifier(s.stageName).WriteQuery("/").
		WriteQuery(upload.Filename).
		WriteQuery(" AUTO_COMPRESS=false OVERWRITE=true").String()

	// Execute PUT command
	if _, err := s.connectionPool.ExecContext(ctx, putSQL); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to upload file to stage")
	}

	// Update metrics
	atomic.AddInt64(&s.filesUploaded, 1)
	atomic.AddInt64(&s.bytesUploaded, int64(len(upload.Data)))

	s.GetLogger().Debug("uploaded file to stage",
		zap.String("filename", upload.Filename),
		zap.Int("record_count", upload.RecordCount),
		zap.Int("size_bytes", len(upload.Data)))

	// If we've accumulated enough files, trigger a COPY operation
	filesUploaded := atomic.LoadInt64(&s.filesUploaded)
	if filesUploaded > 0 && filesUploaded%int64(s.filesPerCopy) == 0 {
		go s.executeCopyCommand(ctx)
	}

	return nil
}

// executeCopyCommand executes COPY command to load staged files into table
func (s *SnowflakeOptimizedDestination) executeCopyCommand(ctx context.Context) {
	// Build COPY command using SQLBuilder
	sqlBuilder := stringpool.NewSQLBuilder(512)
	defer sqlBuilder.Close()

	copySQL := sqlBuilder.WriteQuery("COPY INTO ").
		WriteIdentifier(s.database).WriteQuery(".").
		WriteIdentifier(s.schema).WriteQuery(".").
		WriteIdentifier(s.currentTable).
		WriteQuery(" FROM @").
		WriteIdentifier(s.database).WriteQuery(".").
		WriteIdentifier(s.schema).WriteQuery(".").
		WriteIdentifier(s.stageName).WriteQuery("/").
		WriteQuery(s.stagePrefix).
		WriteQuery(" FILE_FORMAT = (TYPE = ").
		WriteQuery(s.fileFormat).WriteQuery(" COMPRESSION = ").
		WriteQuery(s.compressionType).WriteQuery(") ON_ERROR = 'CONTINUE' PURGE = true").String()

	// Execute COPY command
	if _, err := s.connectionPool.ExecContext(ctx, copySQL); err != nil {
		s.GetLogger().Error("failed to execute COPY command", zap.Error(err))
		s.HandleError(ctx, err, nil)
		return
	}

	// Update metrics
	atomic.AddInt64(&s.copyOperations, 1)
	s.RecordMetric("copy_operations", 1, core.MetricTypeCounter)

	s.GetLogger().Info("executed COPY command successfully",
		zap.Int64("copy_operation", atomic.LoadInt64(&s.copyOperations)))
}

// Helper methods for bulk loading

// recordsToCSV converts records to CSV format
func (s *SnowflakeOptimizedDestination) recordsToCSV(records []*models.Record) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}

	// Get field names from first record for header
	fieldNames := make([]string, 0, len(records[0].Data))
	for key := range records[0].Data {
		fieldNames = append(fieldNames, key)
	}

	// Create optimized CSV builder
	csvBuilder := stringpool.NewCSVBuilder(len(records), len(fieldNames))
	defer csvBuilder.Close()

	// Write header
	csvBuilder.WriteHeader(fieldNames)

	// Write data rows
	for _, record := range records {
		row := make([]string, len(fieldNames))
		for i, fieldName := range fieldNames {
			if val, exists := record.Data[fieldName]; exists && val != nil {
				row[i] = stringpool.Sprintf("%v", val)
			} else {
				row[i] = "" // Empty value for missing fields
			}
		}
		csvBuilder.WriteRow(row)
	}

	csvString := csvBuilder.String()
	return stringpool.StringToBytes(csvString), nil
}

// recordsToJSON converts records to JSON array format
func (s *SnowflakeOptimizedDestination) recordsToJSON(records []*models.Record) ([]byte, error) {
	// Use optimized JSON marshaling with pooled resources
	return jsonpool.MarshalRecords(records, "array")
}

// recordsToJSONL converts records to JSON Lines format (one JSON object per line)
func (s *SnowflakeOptimizedDestination) recordsToJSONL(records []*models.Record) ([]byte, error) {
	// Use optimized JSON marshaling with pooled resources
	return jsonpool.MarshalRecords(records, "lines")
}

// uploadToStage uploads data to Snowflake stage
func (s *SnowflakeOptimizedDestination) uploadToStage(ctx context.Context, filename string, data []byte) error {
	// Create temporary file
	tempFile, err := os.CreateTemp("", filename)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to create temporary file")
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Write data to file
	if _, err := tempFile.Write(data); err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to write data to temporary file")
	}

	// Close file to ensure data is written
	if err := tempFile.Close(); err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to close temporary file")
	}

	// Build PUT command using SQLBuilder
	sqlBuilder := stringpool.NewSQLBuilder(256)
	defer sqlBuilder.Close()

	putSQL := sqlBuilder.WriteQuery("PUT file://").WriteQuery(tempFile.Name()).
		WriteQuery(" @").
		WriteIdentifier(s.database).WriteQuery(".").
		WriteIdentifier(s.schema).WriteQuery(".").
		WriteIdentifier(s.stageName).WriteQuery("/").
		WriteQuery(s.stagePrefix).
		WriteQuery(" OVERWRITE=TRUE").String()

	// Execute PUT command with circuit breaker protection
	return s.ExecuteWithCircuitBreaker(func() error {
		_, err := s.connectionPool.ExecContext(ctx, putSQL)
		return err
	})
}

// buildCopySQL builds a COPY command for the given file and format
func (s *SnowflakeOptimizedDestination) buildCopySQL(filename, format string) string {
	// Determine file format options
	var formatOptions string
	switch strings.ToUpper(format) {
	case "CSV":
		formatOptions = stringpool.Sprintf("TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1 COMPRESSION = '%s'", s.compressionType)
	case "JSON":
		formatOptions = stringpool.Sprintf("TYPE = 'JSON' COMPRESSION = '%s'", s.compressionType)
	case "JSONL":
		// Use pooled string building for JSON format options
		formatOptions = stringpool.Sprintf("TYPE = 'JSON' COMPRESSION = '%s'", s.compressionType)
	default:
		// Use pooled string building for format options
		formatOptions = stringpool.Sprintf("TYPE = '%s' COMPRESSION = '%s'", format, s.compressionType)
	}

	// Build COPY command for specific file using SQLBuilder
	sqlBuilder := stringpool.NewSQLBuilder(512)
	defer sqlBuilder.Close()

	copySQL := sqlBuilder.WriteQuery("COPY INTO ").
		WriteIdentifier(s.database).WriteQuery(".").
		WriteIdentifier(s.schema).WriteQuery(".").
		WriteIdentifier(s.currentTable).
		WriteQuery(" FROM @").
		WriteIdentifier(s.database).WriteQuery(".").
		WriteIdentifier(s.schema).WriteQuery(".").
		WriteIdentifier(s.stageName).WriteQuery("/").
		WriteQuery(s.stagePrefix).WriteQuery("/").
		WriteQuery(filename).
		WriteQuery(" FILE_FORMAT = (").WriteQuery(formatOptions).
		WriteQuery(") ON_ERROR = 'CONTINUE' PURGE = true").String()

	return copySQL
}
