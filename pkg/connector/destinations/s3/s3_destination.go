package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/compression"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/base"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/errors"
	"github.com/ajitpratap0/nebula/pkg/formats/columnar"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap"
)

const (
	// Default configuration values
	defaultBatchSize         = 10000
	defaultUploadPartSize    = 5 * 1024 * 1024 // 5MB
	defaultMaxConcurrency    = 10
	defaultCompressionType   = "snappy"
	defaultFileFormat        = "parquet"
	defaultPartitionStrategy = "daily"
)

// S3Destination represents an S3 destination connector
type S3Destination struct {
	*base.BaseConnector

	// Configuration
	config            *config.BaseConfig
	bucket            string
	prefix            string
	region            string
	fileFormat        string
	compressionType   string
	partitionStrategy string
	batchSize         int
	uploadPartSize    int64
	maxConcurrency    int

	// AWS clients
	s3Client *s3.Client
	uploader *manager.Uploader

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

// NewS3Destination creates a new S3 destination
func NewS3Destination(name string, config *config.BaseConfig) (*S3Destination, error) {
	baseConnector := base.NewBaseConnector(name, core.ConnectorTypeDestination, "1.0.0")

	// Parse configuration
	if config.Security.Credentials == nil {
		return nil, errors.New(errors.ErrorTypeConfig, "security credentials are required")
	}

	bucket := config.Security.Credentials["bucket"]
	if bucket == "" {
		return nil, errors.New(errors.ErrorTypeConfig, "bucket is required")
	}

	prefix := config.Security.Credentials["prefix"]
	region := config.Security.Credentials["region"]
	if region == "" {
		region = "us-east-1"
	}

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

	uploadPartSize := int64(defaultUploadPartSize)
	if ups, ok := config.Security.Credentials["upload_part_size"]; ok && ups != "" {
		var size int
		if _, err := fmt.Sscanf(ups, "%d", &size); err == nil && size > 0 {
			uploadPartSize = int64(size)
		}
	}

	maxConcurrency := config.Performance.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = defaultMaxConcurrency
	}

	return &S3Destination{
		BaseConnector:     baseConnector,
		config:            config,
		bucket:            bucket,
		prefix:            prefix,
		region:            region,
		fileFormat:        fileFormat,
		compressionType:   compressionType,
		partitionStrategy: partitionStrategy,
		batchSize:         batchSize,
		uploadPartSize:    uploadPartSize,
		maxConcurrency:    maxConcurrency,
		currentBatch:      pool.GetBatchSlice(batchSize),
		uploadErrors:      make([]error, 0),
		bufferPool:        pool.NewBufferPool(),
	}, nil
}

// Initialize initializes the S3 destination
func (d *S3Destination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	if err := d.BaseConnector.Initialize(ctx, config); err != nil {
		return err
	}

	// Initialize AWS clients
	if err := d.initializeAWSClients(ctx); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to initialize AWS clients")
	}

	// Initialize columnar writer based on format
	if err := d.initializeColumnarWriter(); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "failed to initialize columnar writer")
	}

	// Test bucket access
	if err := d.testBucketAccess(ctx); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to access S3 bucket")
	}

	d.GetLogger().Info("S3 destination initialized",
		zap.String("bucket", d.bucket),
		zap.String("prefix", d.prefix),
		zap.String("format", d.fileFormat),
		zap.String("compression", d.compressionType))

	return nil
}

// Write writes a stream of records to S3
func (d *S3Destination) Write(ctx context.Context, stream *core.RecordStream) error {
	if stream == nil {
		return errors.New(errors.ErrorTypeValidation, "stream cannot be nil")
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

// WriteBatch writes a batch of records to S3
func (d *S3Destination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	if stream == nil {
		return errors.New(errors.ErrorTypeValidation, "stream cannot be nil")
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

// CreateSchema creates or updates schema in S3 (metadata file)
func (d *S3Destination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	// Write schema metadata file
	schemaJSON, err := jsonpool.MarshalIndent(schema, "", "  ")
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to marshal schema")
	}

	// Build S3 key using URLBuilder for optimized string handling
	ub := stringpool.NewURLBuilder(d.prefix)
	defer ub.Close()
	key := ub.AddPath("_schema", "schema.json").String()

	_, err = d.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(d.bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(string(schemaJSON)),
		ContentType: aws.String("application/json"),
	})

	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to write schema metadata")
	}

	d.GetLogger().Info("schema metadata created", zap.String("key", key))
	return nil
}

// AlterSchema alters schema in S3 (updates metadata)
func (d *S3Destination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	// For S3, we just update the schema metadata
	return d.CreateSchema(ctx, newSchema)
}

// Close closes the S3 destination
func (d *S3Destination) Close(ctx context.Context) error {
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

	// Log final metrics
	d.GetLogger().Info("S3 destination closed",
		zap.Int64("records_written", d.recordsWritten),
		zap.Int64("bytes_written", d.bytesWritten),
		zap.Int64("files_created", d.filesCreated),
		zap.Duration("total_upload_duration", d.uploadDuration))

	return d.BaseConnector.Close(ctx)
}

// SupportsBulkLoad returns true as S3 supports bulk loading
func (d *S3Destination) SupportsBulkLoad() bool {
	return true
}

// SupportsTransactions returns false as S3 doesn't support transactions
func (d *S3Destination) SupportsTransactions() bool {
	return false
}

// SupportsUpsert returns false as S3 doesn't support upsert
func (d *S3Destination) SupportsUpsert() bool {
	return false
}

// SupportsBatch returns true as S3 supports batch operations
func (d *S3Destination) SupportsBatch() bool {
	return true
}

// SupportsStreaming returns true as S3 supports streaming
func (d *S3Destination) SupportsStreaming() bool {
	return true
}

// BulkLoad performs bulk loading of data (not implemented for S3)
func (d *S3Destination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	return errors.New(errors.ErrorTypeConfig, "bulk load not implemented for S3 destination")
}

// BeginTransaction begins a transaction (not supported by S3)
func (d *S3Destination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return nil, errors.New(errors.ErrorTypeConfig, "transactions not supported by S3")
}

// Upsert performs upsert operations (not supported by S3)
func (d *S3Destination) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	return errors.New(errors.ErrorTypeConfig, "upsert not supported by S3 (append-only)")
}

// DropSchema drops the schema (not applicable for S3)
func (d *S3Destination) DropSchema(ctx context.Context, schema *core.Schema) error {
	// For S3, we just remove schema metadata if it exists
	// Build S3 key using URLBuilder for optimized string handling
	ub := stringpool.NewURLBuilder(d.prefix)
	defer ub.Close()
	key := ub.AddPath("_schema", "schema.json").String()

	_, err := d.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		d.GetLogger().Warn("failed to delete schema metadata", zap.Error(err))
	}

	d.GetLogger().Info("schema dropped for S3 destination")
	return nil
}

// Private methods

func (d *S3Destination) initializeAWSClients(ctx context.Context) error {
	// Load AWS configuration
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(d.region),
	)
	if err != nil {
		return err
	}

	// Create S3 client
	d.s3Client = s3.NewFromConfig(cfg)

	// Create uploader with custom configuration
	d.uploader = manager.NewUploader(d.s3Client, func(u *manager.Uploader) {
		u.PartSize = d.uploadPartSize
		u.Concurrency = d.maxConcurrency
	})

	return nil
}

func (d *S3Destination) initializeColumnarWriter() error {
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
		return errors.New(errors.ErrorTypeConfig, stringpool.Sprintf("unsupported file format: %s", d.fileFormat))
	}

	return nil
}

func (d *S3Destination) testBucketAccess(ctx context.Context) error {
	// Test bucket access with HeadBucket
	_, err := d.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(d.bucket),
	})

	if err != nil {
		// Try to create bucket if it doesn't exist
		if strings.Contains(err.Error(), "NotFound") {
			_, createErr := d.s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(d.bucket),
				CreateBucketConfiguration: &types.CreateBucketConfiguration{
					LocationConstraint: types.BucketLocationConstraint(d.region),
				},
			})
			if createErr != nil {
				return createErr
			}
			d.GetLogger().Info("created S3 bucket", zap.String("bucket", d.bucket))
			return nil
		}
		return err
	}

	return nil
}

func (d *S3Destination) addToBatch(ctx context.Context, record *models.Record) error {
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

func (d *S3Destination) flushBatch(ctx context.Context) error {
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

func (d *S3Destination) uploadBatch(ctx context.Context, batch []*models.Record) error {
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
			return errors.Wrap(err, errors.ErrorTypeData, "failed to convert to columnar format")
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
			return errors.New(errors.ErrorTypeConfig, stringpool.Sprintf("unsupported format: %s", d.fileFormat))
		}

		if err != nil {
			return errors.Wrap(err, errors.ErrorTypeData, "failed to convert batch")
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
					return errors.Wrap(err, errors.ErrorTypeData, "failed to create compressor")
				}
				content, err = compressor.Compress(content)
				if err != nil {
					return errors.Wrap(err, errors.ErrorTypeData, "failed to compress data")
				}
			}
		}

		data = bytes.NewReader(content)
		size = int64(len(content))
	}

	// Upload to S3
	result, err := d.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(d.bucket),
		Key:         aws.String(filePath),
		Body:        data,
		ContentType: aws.String(d.getContentType()),
		Metadata: map[string]string{
			"records":     stringpool.Sprintf("%d", len(batch)),
			"format":      d.fileFormat,
			"compression": d.compressionType,
			"created":     time.Now().UTC().Format(time.RFC3339),
		},
	})

	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to upload to S3")
	}

	// Update metrics
	d.recordsWritten += int64(len(batch))
	d.bytesWritten += size
	d.filesCreated++

	d.GetLogger().Info("batch uploaded to S3",
		zap.String("location", result.Location),
		zap.Int("records", len(batch)),
		zap.Int64("bytes", size),
		zap.Duration("duration", time.Since(start)))

	return nil
}

func (d *S3Destination) generateFilePath() string {
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

	// Build S3 key using URLBuilder for optimized string handling
	ub := stringpool.NewURLBuilder(d.prefix)
	defer ub.Close()

	if partitionPath != "" {
		return ub.AddPath(partitionPath, filename).String()
	}

	return ub.AddPath(filename).String()
}

func (d *S3Destination) getFileExtension() string {
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

func (d *S3Destination) getContentType() string {
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

func (d *S3Destination) batchToCSV(batch []*models.Record) ([]byte, error) {
	if len(batch) == 0 {
		return []byte{}, nil
	}

	// Estimate size: assume average 20 chars per field
	estimatedCols := len(batch[0].Data)
	csvBuilder := stringpool.NewCSVBuilder(len(batch), estimatedCols)
	defer csvBuilder.Close()

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

func (d *S3Destination) batchToJSON(batch []*models.Record) ([]byte, error) {
	// Convert batch to JSON array
	data := make([]map[string]interface{}, len(batch))
	for i, record := range batch {
		data[i] = record.Data
	}

	return jsonpool.Marshal(data)
}

func (d *S3Destination) batchToJSONL(batch []*models.Record) ([]byte, error) {
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

func (d *S3Destination) recordUploadError(err error) {
	d.errorMutex.Lock()
	defer d.errorMutex.Unlock()
	d.uploadErrors = append(d.uploadErrors, err)
}

func (d *S3Destination) checkUploadErrors() error {
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

	return errors.New(errors.ErrorTypeData,
		stringpool.Sprintf("upload errors: %s", stringpool.JoinPooled(errMsgs, "; ")))
}
