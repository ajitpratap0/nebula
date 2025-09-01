package gcs

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/ajitpratap0/nebula/pkg/compression"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/base"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	"github.com/ajitpratap0/nebula/pkg/formats/columnar"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"
	"google.golang.org/api/option"
)

const (
	// Default configuration values
	defaultBatchSize         = 10000
	defaultUploadTimeout     = 5 * time.Minute
	defaultMaxConcurrency    = 10
	defaultCompressionType   = "snappy"
	defaultFileFormat        = "parquet"
	defaultPartitionStrategy = "daily"
)

// GCSDestination represents a Google Cloud Storage destination connector
type GCSDestination struct {
	*base.BaseConnector

	// Configuration
	config            *config.BaseConfig
	bucket            string
	prefix            string
	projectID         string
	credentialsFile   string
	fileFormat        string
	compressionType   string
	partitionStrategy string
	batchSize         int
	uploadTimeout     time.Duration
	maxConcurrency    int

	// GCS clients
	gcsClient    *storage.Client
	bucketHandle *storage.BucketHandle

	// State management
	currentBatch []*models.Record
	batchMutex   sync.Mutex
	uploadWG     sync.WaitGroup
	uploadErrors []error
	errorMutex   sync.Mutex

	// Columnar writer
	columnarWriter columnar.Writer

	// Memory pool
	bufferPool *pool.BufferPool

	// Metrics
	recordsWritten int64
	bytesWritten   int64
	filesCreated   int64
	uploadDuration time.Duration
}

// NewGCSDestination creates a new GCS destination
func NewGCSDestination(name string, config *config.BaseConfig) (*GCSDestination, error) {
	baseConnector := base.NewBaseConnector(name, core.ConnectorTypeDestination, "1.0.0")

	// Parse configuration
	if config.Security.Credentials == nil {
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "security credentials are required")
	}

	bucket := config.Security.Credentials["bucket"]
	if bucket == "" {
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "bucket is required")
	}

	prefix := config.Security.Credentials["prefix"]
	projectID := config.Security.Credentials["project_id"]
	if projectID == "" {
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "project_id is required")
	}

	credentialsFile := config.Security.Credentials["credentials_file"]

	fileFormat := config.Security.Credentials["file_format"]
	if fileFormat == "" {
		fileFormat = defaultFileFormat
	}

	compressionType := config.Security.Credentials["compression"]
	if compressionType == "" {
		compressionType = defaultCompressionType
	}

	partitionStrategy := config.Security.Credentials["partition_strategy"]
	if partitionStrategy == "" {
		partitionStrategy = defaultPartitionStrategy
	}

	batchSize := config.Performance.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	uploadTimeout := defaultUploadTimeout
	if ut := config.Security.Credentials["upload_timeout"]; ut != "" {
		if parsed, err := time.ParseDuration(ut); err == nil {
			uploadTimeout = parsed
		}
	}

	maxConcurrency := config.Performance.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = defaultMaxConcurrency
	}

	return &GCSDestination{
		BaseConnector:     baseConnector,
		config:            config,
		bucket:            bucket,
		prefix:            prefix,
		projectID:         projectID,
		credentialsFile:   credentialsFile,
		fileFormat:        fileFormat,
		compressionType:   compressionType,
		partitionStrategy: partitionStrategy,
		batchSize:         batchSize,
		uploadTimeout:     uploadTimeout,
		maxConcurrency:    maxConcurrency,
		currentBatch:      pool.GetBatchSlice(batchSize),
		uploadErrors:      make([]error, 0),
		bufferPool:        pool.NewBufferPool(),
	}, nil
}

// Initialize initializes the GCS destination
func (d *GCSDestination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	if err := d.BaseConnector.Initialize(ctx, config); err != nil {
		return err
	}

	// Initialize GCS client
	if err := d.initializeGCSClient(ctx); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to initialize GCS client")
	}

	// Initialize columnar writer based on format
	if err := d.initializeColumnarWriter(); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig, "failed to initialize columnar writer")
	}

	// Test bucket access
	if err := d.testBucketAccess(ctx); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to access GCS bucket")
	}

	d.GetLogger().Info("GCS destination initialized",
		zap.String("bucket", d.bucket),
		zap.String("prefix", d.prefix),
		zap.String("format", d.fileFormat),
		zap.String("compression", d.compressionType))

	return nil
}

// Write writes a stream of records to GCS
func (d *GCSDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	if stream == nil {
		return nebulaerrors.New(nebulaerrors.ErrorTypeValidation, "stream cannot be nil")
	}

	// Process records from stream
	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				// Channel closed, flush remaining batch
				if err := d.flushBatch(ctx); err != nil {
					return err
				}
				return nil
			}

			// Add record to batch
			if err := d.addToBatch(ctx, record); err != nil {
				d.GetLogger().Error("failed to add record to batch", zap.Error(err))
				d.GetMetricsCollector().RecordCounter("errors", 1, "type", "batch_add_error")
				continue
			}

		case err := <-stream.Errors:
			if err != nil {
				d.GetLogger().Error("stream error", zap.Error(err))
				d.GetMetricsCollector().RecordCounter("errors", 1, "type", "stream_error")
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// WriteBatch writes a batch of records to GCS
func (d *GCSDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	if stream == nil {
		return nebulaerrors.New(nebulaerrors.ErrorTypeValidation, "stream cannot be nil")
	}

	for {
		select {
		case batch, ok := <-stream.Batches:
			if !ok {
				// Channel closed, flush remaining batch
				if err := d.flushBatch(ctx); err != nil {
					return err
				}
				// Wait for all uploads to complete
				d.uploadWG.Wait()
				return d.checkUploadErrors()
			}

			// Process batch
			for _, record := range batch {
				if err := d.addToBatch(ctx, record); err != nil {
					d.GetLogger().Error("failed to add record to batch", zap.Error(err))
					d.GetMetricsCollector().RecordCounter("errors", 1, "type", "batch_add_error")
				}
			}

		case err := <-stream.Errors:
			if err != nil {
				d.GetLogger().Error("batch stream error", zap.Error(err))
				d.GetMetricsCollector().RecordCounter("errors", 1, "type", "batch_stream_error")
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// CreateSchema creates or updates schema in GCS (metadata file)
func (d *GCSDestination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	// Write schema metadata file
	schemaJSON, err := jsonpool.MarshalIndent(schema, "", "  ")
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to marshal schema")
	}

	// Build GCS object name using URLBuilder for optimized string handling
	ub := stringpool.NewURLBuilder(d.prefix)
	defer ub.Close() // Ignore close error
	objectName := ub.AddPath("_schema", "schema.json").String()

	obj := d.bucketHandle.Object(objectName)
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/json"

	_, err = writer.Write(schemaJSON) // Ignore write error
	if err != nil {
		_ = writer.Close() // Ignore close error
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to write schema metadata")
	}

	if err = _ = writer.Close(); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to close schema writer")
	}

	d.GetLogger().Info("schema metadata created", zap.String("object", objectName))
	return nil
}

// AlterSchema alters schema in GCS (updates metadata)
func (d *GCSDestination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	// For GCS, we just update the schema metadata
	return d.CreateSchema(ctx, newSchema)
}

// Close closes the GCS destination
func (d *GCSDestination) Close(ctx context.Context) error {
	// Flush any remaining batch
	if err := d.flushBatch(ctx); err != nil {
		d.GetLogger().Error("failed to flush final batch", zap.Error(err))
	}

	// Wait for all uploads to complete
	d.uploadWG.Wait()

	// Check for upload errors
	if err := d.checkUploadErrors(); err != nil {
		d.GetLogger().Error("upload errors detected", zap.Error(err))
	}

	// Close GCS client
	if d.gcsClient != nil {
		if err := d.gcsClient.Close(); err != nil {
			d.GetLogger().Error("failed to close GCS client", zap.Error(err))
		}
	}

	// Log final metrics
	d.GetLogger().Info("GCS destination closed",
		zap.Int64("records_written", d.recordsWritten),
		zap.Int64("bytes_written", d.bytesWritten),
		zap.Int64("files_created", d.filesCreated),
		zap.Duration("total_upload_duration", d.uploadDuration))

	return d.BaseConnector.Close(ctx)
}

// SupportsBulkLoad returns true as GCS supports bulk loading
func (d *GCSDestination) SupportsBulkLoad() bool {
	return true
}

// SupportsTransactions returns false as GCS doesn't support transactions
func (d *GCSDestination) SupportsTransactions() bool {
	return false
}

// SupportsUpsert returns false as GCS doesn't support upsert
func (d *GCSDestination) SupportsUpsert() bool {
	return false
}

// SupportsBatch returns true as GCS supports batch operations
func (d *GCSDestination) SupportsBatch() bool {
	return true
}

// SupportsStreaming returns true as GCS supports streaming
func (d *GCSDestination) SupportsStreaming() bool {
	return true
}

// BulkLoad performs bulk loading of data (not implemented for GCS)
func (d *GCSDestination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "bulk load not implemented for GCS destination")
}

// BeginTransaction begins a transaction (not supported by GCS)
func (d *GCSDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return nil, nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "transactions not supported by GCS")
}

// Upsert performs upsert operations (not supported by GCS)
func (d *GCSDestination) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "upsert not supported by GCS (append-only)")
}

// DropSchema drops the schema (not applicable for GCS)
func (d *GCSDestination) DropSchema(ctx context.Context, schema *core.Schema) error {
	// For GCS, we just remove schema metadata if it exists
	// Build GCS object name using URLBuilder for optimized string handling
	ub := stringpool.NewURLBuilder(d.prefix)
	defer ub.Close() // Ignore close error
	objectName := ub.AddPath("_schema", "schema.json").String()

	err := d.bucketHandle.Object(objectName).Delete(ctx)
	if err != nil {
		d.GetLogger().Warn("failed to delete schema metadata", zap.Error(err))
	}

	d.GetLogger().Info("schema dropped for GCS destination")
	return nil
}

// Private methods

func (d *GCSDestination) initializeGCSClient(ctx context.Context) error {
	var opts []option.ClientOption

	// Add credentials if provided
	if d.credentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(d.credentialsFile))
	}

	// Create GCS client
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return err
	}

	d.gcsClient = client
	d.bucketHandle = client.Bucket(d.bucket)

	return nil
}

func (d *GCSDestination) initializeColumnarWriter() error {
	// Select columnar writer based on format
	switch d.fileFormat {
	case "parquet":
		// For Parquet format
		d.columnarWriter = nil // Will be created when needed

	case "avro":
		// For Avro format
		d.columnarWriter = nil // Will be created when needed

	case "orc":
		// For ORC format
		d.columnarWriter = nil // Will be created when needed

	case "csv":
		// For CSV, we'll handle it differently
		d.columnarWriter = nil

	case "json", "jsonl":
		// For JSON formats, we'll handle them differently
		d.columnarWriter = nil

	default:
		return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, stringpool.Sprintf("unsupported file format: %s", d.fileFormat))
	}

	return nil
}

func (d *GCSDestination) testBucketAccess(ctx context.Context) error {
	// Test bucket access by getting bucket attributes
	_, err := d.bucketHandle.Attrs(ctx)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, stringpool.Sprintf("failed to access GCS bucket %s", d.bucket))
	}

	d.GetLogger().Info("GCS bucket access verified", zap.String("bucket", d.bucket))
	return nil
}

func (d *GCSDestination) addToBatch(ctx context.Context, record *models.Record) error {
	d.batchMutex.Lock()
	defer d.batchMutex.Unlock()

	d.currentBatch = append(d.currentBatch, record)

	// Check if batch is full
	if len(d.currentBatch) >= d.batchSize {
		// Flush batch asynchronously
		batch := d.currentBatch
		d.currentBatch = pool.GetBatchSlice(d.batchSize)

		d.uploadWG.Add(1)
		go func() {
			defer d.uploadWG.Done()
			if err := d.uploadBatch(ctx, batch); err != nil {
				d.recordUploadError(err)
			}
		}()
	}

	return nil
}

func (d *GCSDestination) flushBatch(ctx context.Context) error {
	d.batchMutex.Lock()
	defer d.batchMutex.Unlock()

	if len(d.currentBatch) == 0 {
		return nil
	}

	// Upload remaining batch
	batch := d.currentBatch
	d.currentBatch = pool.GetBatchSlice(d.batchSize)

	return d.uploadBatch(ctx, batch)
}

func (d *GCSDestination) uploadBatch(ctx context.Context, batch []*models.Record) error {
	if len(batch) == 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		d.uploadDuration += time.Since(start)
	}()

	// Generate file path with partitioning
	filePath := d.generateFilePath()

	// Get buffer from pool
	buf := d.bufferPool.Get(1024 * 1024) // 1MB buffer
	defer d.bufferPool.Put(buf)

	// Convert batch to file format
	var data io.Reader
	var size int64
	var err error

	if d.fileFormat == "parquet" || d.fileFormat == "avro" || d.fileFormat == "orc" {
		// For columnar formats, we need to handle differently
		// In production, use actual columnar libraries
		// For now, convert to JSON as placeholder
		content, err := d.batchToJSON(batch)
		if err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to convert to columnar format")
		}
		data = bytes.NewReader(content)
		size = int64(len(content))
	} else {
		// Handle CSV, JSON, JSONL formats
		var content []byte
		switch d.fileFormat {
		case "csv":
			content, err = d.batchToCSV(batch)
		case "json":
			content, err = d.batchToJSON(batch)
		case "jsonl":
			content, err = d.batchToJSONL(batch)
		default:
			return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, stringpool.Sprintf("unsupported format: %s", d.fileFormat))
		}

		if err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to convert batch")
		}

		// Apply compression if needed
		if d.compressionType != "" && d.compressionType != "none" {
			var algorithm compression.Algorithm
			switch d.compressionType {
			case "gzip":
				algorithm = compression.Gzip
			case "snappy":
				algorithm = compression.Snappy
			case "lz4":
				algorithm = compression.LZ4
			case "zstd":
				algorithm = compression.Zstd
			default:
				algorithm = compression.None
			}

			if algorithm != compression.None {
				config := &compression.Config{
					Algorithm: algorithm,
					Level:     compression.Default,
				}
				compressor, err := compression.NewCompressor(config)
				if err != nil {
					return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to create compressor")
				}
				content, err = compressor.Compress(content)
				if err != nil {
					return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to compress data")
				}
			}
		}

		data = bytes.NewReader(content)
		size = int64(len(content))
	}

	// Upload to GCS
	obj := d.bucketHandle.Object(filePath)
	writer := obj.NewWriter(ctx)
	writer.ContentType = d.getContentType()
	writer.Metadata = map[string]string{
		"records":     stringpool.Sprintf("%d", len(batch)),
		"format":      d.fileFormat,
		"compression": d.compressionType,
		"created":     time.Now().UTC().Format(time.RFC3339),
	}

	// Copy data to GCS
	_, err = io.Copy(writer, data)
	if err != nil {
		_ = writer.Close() // Ignore close error
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to write to GCS")
	}

	if err = _ = writer.Close(); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to close GCS writer")
	}

	// Update metrics
	d.recordsWritten += int64(len(batch))
	d.bytesWritten += size
	d.filesCreated++

	d.GetLogger().Info("batch uploaded to GCS",
		zap.String("object", filePath),
		zap.Int("records", len(batch)),
		zap.Int64("bytes", size),
		zap.Duration("duration", time.Since(start)))

	return nil
}

func (d *GCSDestination) generateFilePath() string {
	now := time.Now().UTC()
	var partitionPath string

	switch d.partitionStrategy {
	case "hourly":
		partitionPath = stringpool.Sprintf("year=%d/month=%02d/day=%02d/hour=%02d",
			now.Year(), now.Month(), now.Day(), now.Hour())
	case "daily":
		partitionPath = stringpool.Sprintf("year=%d/month=%02d/day=%02d",
			now.Year(), now.Month(), now.Day())
	case "monthly":
		partitionPath = stringpool.Sprintf("year=%d/month=%02d",
			now.Year(), now.Month())
	case "yearly":
		partitionPath = stringpool.Sprintf("year=%d", now.Year())
	default:
		partitionPath = ""
	}

	// Generate unique filename
	filename := stringpool.Sprintf("data_%s_%d.%s",
		now.Format("20060102_150405"),
		now.UnixNano(),
		d.getFileExtension())

	// Build GCS object name using URLBuilder for optimized string handling
	ub := stringpool.NewURLBuilder(d.prefix)
	defer ub.Close() // Ignore close error

	if partitionPath != "" {
		return ub.AddPath(partitionPath, filename).String()
	}

	return ub.AddPath(filename).String()
}

func (d *GCSDestination) getFileExtension() string {
	ext := d.fileFormat

	// Add compression extension if applicable
	switch d.compressionType {
	case "gzip":
		ext += ".gz"
	case "snappy":
		if d.fileFormat == "parquet" {
			// Snappy is internal to Parquet
			break
		}
		ext += ".snappy"
	case "lz4":
		ext += ".lz4"
	case "zstd":
		ext += ".zst"
	}

	return ext
}

func (d *GCSDestination) getContentType() string {
	switch d.fileFormat {
	case "parquet":
		return "application/x-parquet"
	case "avro":
		return "application/avro"
	case "orc":
		return "application/x-orc"
	case "csv":
		return "text/csv"
	case "json", "jsonl":
		return "application/json"
	default:
		return "application/octet-stream"
	}
}

func (d *GCSDestination) batchToCSV(batch []*models.Record) ([]byte, error) {
	if len(batch) == 0 {
		return []byte{}, nil
	}

	// Estimate size: assume average 20 chars per field
	estimatedCols := len(batch[0].Data)
	csvBuilder := stringpool.NewCSVBuilder(len(batch), estimatedCols)
	defer csvBuilder.Close() // Ignore close error

	// Write headers from first record
	headers := make([]string, 0, len(batch[0].Data))
	for key := range batch[0].Data {
		headers = append(headers, key)
	}
	csvBuilder.WriteHeader(headers)

	// Write data rows
	for _, record := range batch {
		row := make([]string, len(headers))
		for i, key := range headers {
			if value, exists := record.Data[key]; exists {
				row[i] = stringpool.Sprintf("%v", value)
			} else {
				row[i] = "" // Empty value for missing keys
			}
		}
		csvBuilder.WriteRow(row)
	}

	csvString := csvBuilder.String()
	return stringpool.StringToBytes(csvString), nil
}

func (d *GCSDestination) batchToJSON(batch []*models.Record) ([]byte, error) {
	// Convert batch to JSON array
	data := make([]map[string]interface{}, len(batch))
	for i, record := range batch {
		data[i] = record.Data
	}

	return jsonpool.Marshal(data)
}

func (d *GCSDestination) batchToJSONL(batch []*models.Record) ([]byte, error) {
	buf := jsonpool.GetBuffer()
	defer jsonpool.PutBuffer(buf)

	encoder := jsonpool.GetEncoder(buf)
	defer jsonpool.PutEncoder(encoder)

	// Write each record as a separate JSON line
	for _, record := range batch {
		if err := encoder.Encode(record.Data); err != nil {
			return nil, err
		}
	}

	// Copy data since we're returning the buffer to the pool
	result := pool.GetByteSlice()

	if cap(result) < buf.Len() {
		result = make([]byte, buf.Len())
	}
	defer pool.PutByteSlice(result)
	copy(result, buf.Bytes())
	return result, nil
}

func (d *GCSDestination) recordUploadError(err error) {
	d.errorMutex.Lock()
	defer d.errorMutex.Unlock()
	d.uploadErrors = append(d.uploadErrors, err)
}

func (d *GCSDestination) checkUploadErrors() error {
	d.errorMutex.Lock()
	defer d.errorMutex.Unlock()

	if len(d.uploadErrors) == 0 {
		return nil
	}

	// Combine all errors
	var errMsgs []string
	for _, err := range d.uploadErrors {
		errMsgs = append(errMsgs, err.Error())
	}

	return nebulaerrors.New(nebulaerrors.ErrorTypeData,
		stringpool.Sprintf("upload errors: %s", stringpool.JoinPooled(errMsgs, "; ")))
}
