// Package performance provides bulk loading optimizations
package performance

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/compression"
	"github.com/ajitpratap0/nebula/pkg/formats/columnar"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
)

// BulkLoader provides optimized bulk loading capabilities
type BulkLoader struct {
	config       *BulkLoadConfig
	optimizer    *Optimizer
	memOptimizer *MemoryOptimizer
	compressor   *compression.CompressorPool
	metrics      *BulkLoadMetrics
	stages       []LoadStage
	mu           sync.RWMutex
}

// BulkLoadConfig configures bulk loading
type BulkLoadConfig struct {
	TargetWarehouse string
	MaxBatchSize    int
	MaxFileSize     int64
	ParallelLoaders int
	StagingLocation string

	// Format options
	FileFormat     string // csv, json, parquet, avro
	Compression    string // none, gzip, snappy, zstd
	EnableColumnar bool

	// Optimization options
	EnableSorting      bool
	SortKeys           []string
	EnablePartitioning bool
	PartitionKeys      []string
	EnableClustering   bool
	ClusterKeys        []string

	// Performance options
	MemoryLimit      int64
	NetworkBandwidth int64 // bytes/sec
	RetryAttempts    int
	RetryDelay       time.Duration
}

// DefaultBulkLoadConfig returns default configuration
func DefaultBulkLoadConfig(warehouse string) *BulkLoadConfig {
	config := &BulkLoadConfig{
		TargetWarehouse:    warehouse,
		MaxBatchSize:       100000,
		MaxFileSize:        100 * 1024 * 1024, // 100MB
		ParallelLoaders:    4,
		FileFormat:         "parquet",
		Compression:        "snappy",
		EnableColumnar:     true,
		EnableSorting:      true,
		EnablePartitioning: false,
		EnableClustering:   false,
		MemoryLimit:        1024 * 1024 * 1024, // 1GB
		NetworkBandwidth:   100 * 1024 * 1024,  // 100MB/s
		RetryAttempts:      3,
		RetryDelay:         5 * time.Second,
	}

	// Warehouse-specific defaults
	switch warehouse {
	case "snowflake":
		config.FileFormat = "parquet"
		config.Compression = "zstd"
		config.EnableClustering = true
	case "bigquery":
		config.FileFormat = "avro"
		config.Compression = "snappy"
		config.EnablePartitioning = true
	case "redshift":
		config.FileFormat = "parquet"
		config.Compression = "gzip"
		config.EnableSorting = true
	}

	return config
}

// BulkLoadMetrics tracks bulk load metrics
type BulkLoadMetrics struct {
	RecordsLoaded    int64
	BytesLoaded      int64
	FilesCreated     int64
	LoadDuration     time.Duration
	CompressionRatio float64
	Throughput       float64
	Errors           int64
	Retries          int64
}

// LoadStage represents a loading stage
type LoadStage interface {
	Name() string
	Process(ctx context.Context, records []*models.Record) ([]*models.Record, error)
}

// NewBulkLoader creates a new bulk loader
func NewBulkLoader(config *BulkLoadConfig) *BulkLoader {
	if config == nil {
		config = DefaultBulkLoadConfig("generic")
	}

	bl := &BulkLoader{
		config:       config,
		optimizer:    NewOptimizer(DefaultOptimizerConfig()),
		memOptimizer: NewMemoryOptimizer(DefaultMemoryConfig()),
		metrics:      &BulkLoadMetrics{},
		stages:       make([]LoadStage, 0),
	}

	// Initialize compressor
	if config.Compression != "none" {
		algo := compression.Snappy
		switch config.Compression {
		case "gzip":
			algo = compression.Gzip
		case "zstd":
			algo = compression.Zstd
		case "lz4":
			algo = compression.LZ4
		}

		bl.compressor = compression.NewCompressorPool(&compression.Config{
			Algorithm: algo,
			Level:     compression.Default,
		})
	}

	// Initialize stages
	bl.initializeStages()

	return bl
}

// initializeStages initializes processing stages
func (bl *BulkLoader) initializeStages() {
	// Memory optimization stage
	bl.stages = append(bl.stages, &MemoryOptimizationStage{
		optimizer: bl.memOptimizer,
	})

	// Sorting stage
	if bl.config.EnableSorting && len(bl.config.SortKeys) > 0 {
		bl.stages = append(bl.stages, &SortingStage{
			keys: bl.config.SortKeys,
		})
	}

	// Partitioning stage
	if bl.config.EnablePartitioning && len(bl.config.PartitionKeys) > 0 {
		bl.stages = append(bl.stages, &PartitioningStage{
			keys: bl.config.PartitionKeys,
		})
	}

	// Clustering stage
	if bl.config.EnableClustering && len(bl.config.ClusterKeys) > 0 {
		bl.stages = append(bl.stages, &ClusteringStage{
			keys: bl.config.ClusterKeys,
		})
	}

	// Format conversion stage
	bl.stages = append(bl.stages, &FormatConversionStage{
		format:      bl.config.FileFormat,
		compression: bl.config.Compression,
	})
}

// Load performs optimized bulk loading
func (bl *BulkLoader) Load(ctx context.Context, records []*models.Record) error {
	start := time.Now()
	defer func() {
		bl.metrics.LoadDuration = time.Since(start)
		bl.metrics.Throughput = float64(bl.metrics.RecordsLoaded) / bl.metrics.LoadDuration.Seconds()
	}()

	// Split into optimal batches
	batches := bl.splitIntoBatches(records)

	// Process batches in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, len(batches))
	semaphore := make(chan struct{}, bl.config.ParallelLoaders)

	for _, batch := range batches {
		wg.Add(1)
		go func(batch []*models.Record) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := bl.loadBatch(ctx, batch); err != nil {
				errChan <- err
			}
		}(batch)
	}

	wg.Wait()
	close(errChan)

	// Collect errors
	var loadErrors []error
	for err := range errChan {
		if err != nil {
			loadErrors = append(loadErrors, err)
			atomic.AddInt64(&bl.metrics.Errors, 1)
		}
	}

	if len(loadErrors) > 0 {
		return nebulaerrors.New(nebulaerrors.ErrorTypeData, stringpool.Sprintf("bulk load errors: %v", loadErrors))
	}

	return nil
}

// loadBatch loads a single batch
func (bl *BulkLoader) loadBatch(ctx context.Context, records []*models.Record) error {
	// Apply stages
	processed := records
	for _, stage := range bl.stages {
		var err error
		processed, err = stage.Process(ctx, processed)
		if err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, stringpool.Sprintf("stage %s failed", stage.Name()))
		}
	}

	// Create files
	files, err := bl.createFiles(processed)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeFile, "file creation failed")
	}

	// Upload files with retries
	for _, file := range files {
		if err := bl.uploadFileWithRetry(ctx, file); err != nil {
			return err
		}
	}

	// Update metrics
	atomic.AddInt64(&bl.metrics.RecordsLoaded, int64(len(records)))
	atomic.AddInt64(&bl.metrics.FilesCreated, int64(len(files)))

	return nil
}

// splitIntoBatches splits records into optimal batches
func (bl *BulkLoader) splitIntoBatches(records []*models.Record) [][]*models.Record {
	var batches [][]*models.Record

	// Calculate batch size based on memory limit
	recordSize := bl.estimateRecordSize(records[0])
	maxBatchSize := int(bl.config.MemoryLimit / int64(recordSize))
	if maxBatchSize > bl.config.MaxBatchSize {
		maxBatchSize = bl.config.MaxBatchSize
	}

	for i := 0; i < len(records); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(records) {
			end = len(records)
		}
		batches = append(batches, records[i:end])
	}

	return batches
}

// estimateRecordSize estimates average record size
func (bl *BulkLoader) estimateRecordSize(record *models.Record) int64 {
	// Simple estimation
	size := int64(0)
	for k, v := range record.Data {
		size += int64(len(k))
		switch val := v.(type) {
		case string:
			size += int64(len(val))
		case []byte:
			size += int64(len(val))
		default:
			size += 8 // Assume 8 bytes for other types
		}
	}
	return size
}

// createFiles creates optimized files from records
func (bl *BulkLoader) createFiles(records []*models.Record) ([]*LoadFile, error) {
	var files []*LoadFile

	switch bl.config.FileFormat {
	case "parquet":
		files = append(files, bl.createParquetFile(records))
	case "avro":
		files = append(files, bl.createAvroFile(records))
	case "csv":
		files = append(files, bl.createCSVFile(records))
	case "json":
		files = append(files, bl.createJSONFile(records))
	default:
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeConfig, stringpool.Sprintf("unsupported format: %s", bl.config.FileFormat))
	}

	return files, nil
}

// createParquetFile creates Parquet file
func (bl *BulkLoader) createParquetFile(records []*models.Record) *LoadFile {
	// Use pooled builder for large file operations
	builder := stringpool.GetBuilder(stringpool.Large)
	defer stringpool.PutBuilder(builder, stringpool.Large)

	// Create Parquet writer
	config := &columnar.WriterConfig{
		Format:      columnar.Parquet,
		Compression: bl.config.Compression,
		BatchSize:   10000,
		EnableStats: true,
	}

	writer, _ := columnar.NewWriter(builder, config)
	_ = writer.WriteRecords(records) // Ignore write records error
	_ = writer.Close()               // Ignore close error

	// Get data from pooled builder
	originalSize := builder.Len()
	data := stringpool.StringToBytes(builder.String())

	// Compress if needed
	if bl.compressor != nil {
		compressed, _ := bl.compressor.Compress(data)
		data = compressed
		bl.metrics.CompressionRatio = float64(originalSize) / float64(len(compressed))
	}

	return &LoadFile{
		Name:   stringpool.Sprintf("bulk_%d.parquet", time.Now().Unix()),
		Format: "parquet",
		Data:   data,
		Size:   int64(len(data)),
	}
}

// createAvroFile creates Avro file
func (bl *BulkLoader) createAvroFile(records []*models.Record) *LoadFile {
	// Use pooled builder for large file operations
	builder := stringpool.GetBuilder(stringpool.Large)
	defer stringpool.PutBuilder(builder, stringpool.Large)

	// In practice, would use Avro writer
	for _, record := range records {
		fmt.Fprintf(builder, "%v\n", record.Data)
	}

	data := stringpool.StringToBytes(builder.String())
	if bl.compressor != nil {
		compressed, _ := bl.compressor.Compress(data)
		data = compressed
	}

	return &LoadFile{
		Name:   stringpool.Sprintf("bulk_%d.avro", time.Now().Unix()),
		Format: "avro",
		Data:   data,
		Size:   int64(len(data)),
	}
}

// createCSVFile creates CSV file
func (bl *BulkLoader) createCSVFile(records []*models.Record) *LoadFile {
	// Use pooled builder for large file operations
	builder := stringpool.GetBuilder(stringpool.Large)
	defer stringpool.PutBuilder(builder, stringpool.Large)

	// Write headers
	if len(records) > 0 {
		headers := make([]string, 0)
		for k := range records[0].Data {
			headers = append(headers, k)
		}
		for i, h := range headers {
			if i > 0 {
				builder.WriteString(",")
			}
			builder.WriteString(h)
		}
		builder.WriteString("\n")

		// Write data
		for _, record := range records {
			for i, h := range headers {
				if i > 0 {
					builder.WriteString(",")
				}
				fmt.Fprintf(builder, "%v", record.Data[h])
			}
			builder.WriteString("\n")
		}
	}

	data := stringpool.StringToBytes(builder.String())
	if bl.compressor != nil {
		compressed, _ := bl.compressor.Compress(data)
		data = compressed
	}

	return &LoadFile{
		Name:   stringpool.Sprintf("bulk_%d.csv", time.Now().Unix()),
		Format: "csv",
		Data:   data,
		Size:   int64(len(data)),
	}
}

// createJSONFile creates JSON file
func (bl *BulkLoader) createJSONFile(records []*models.Record) *LoadFile {
	// Use pooled builder for large file operations
	builder := stringpool.GetBuilder(stringpool.Large)
	defer stringpool.PutBuilder(builder, stringpool.Large)

	// Write JSON lines
	for _, record := range records {
		fmt.Fprintf(builder, "%v\n", record.Data)
	}

	data := stringpool.StringToBytes(builder.String())
	if bl.compressor != nil {
		compressed, _ := bl.compressor.Compress(data)
		data = compressed
	}

	return &LoadFile{
		Name:   stringpool.Sprintf("bulk_%d.json", time.Now().Unix()),
		Format: "json",
		Data:   data,
		Size:   int64(len(data)),
	}
}

// uploadFileWithRetry uploads file with retries
func (bl *BulkLoader) uploadFileWithRetry(ctx context.Context, file *LoadFile) error {
	var lastErr error

	for i := 0; i < bl.config.RetryAttempts; i++ {
		if i > 0 {
			atomic.AddInt64(&bl.metrics.Retries, 1)
			time.Sleep(bl.config.RetryDelay)
		}

		err := bl.uploadFile(ctx, file)
		if err == nil {
			atomic.AddInt64(&bl.metrics.BytesLoaded, file.Size)
			return nil
		}

		lastErr = err
	}

	return nebulaerrors.Wrap(lastErr, nebulaerrors.ErrorTypeConnection, stringpool.Sprintf("upload failed after %d attempts", bl.config.RetryAttempts))
}

// uploadFile uploads file to warehouse
func (bl *BulkLoader) uploadFile(ctx context.Context, file *LoadFile) error {
	// In practice, this would upload to specific warehouse
	// For now, simulate upload with bandwidth throttling

	uploadTime := time.Duration(float64(file.Size) / float64(bl.config.NetworkBandwidth) * float64(time.Second))

	select {
	case <-time.After(uploadTime):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetMetrics returns bulk load metrics
func (bl *BulkLoader) GetMetrics() *BulkLoadMetrics {
	return bl.metrics
}

// LoadFile represents a file to load
type LoadFile struct {
	Name   string
	Format string
	Data   []byte
	Size   int64
}

// MemoryOptimizationStage optimizes memory usage
type MemoryOptimizationStage struct {
	optimizer *MemoryOptimizer
}

func (mos *MemoryOptimizationStage) Name() string {
	return "memory_optimization"
}

func (mos *MemoryOptimizationStage) Process(ctx context.Context, records []*models.Record) ([]*models.Record, error) {
	return mos.optimizer.OptimizeRecords(records), nil
}

// SortingStage sorts records by keys
type SortingStage struct {
	keys []string
}

func (ss *SortingStage) Name() string {
	return "sorting"
}

func (ss *SortingStage) Process(ctx context.Context, records []*models.Record) ([]*models.Record, error) {
	// Simple bubble sort for demonstration
	// In practice, use more efficient sorting
	for i := 0; i < len(records)-1; i++ {
		for j := 0; j < len(records)-i-1; j++ {
			if ss.compare(records[j], records[j+1]) > 0 {
				records[j], records[j+1] = records[j+1], records[j]
			}
		}
	}
	return records, nil
}

func (ss *SortingStage) compare(a, b *models.Record) int {
	for _, key := range ss.keys {
		aVal := stringpool.Sprintf("%v", a.Data[key])
		bVal := stringpool.Sprintf("%v", b.Data[key])
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
	}
	return 0
}

// PartitioningStage partitions records
type PartitioningStage struct {
	keys []string
}

func (ps *PartitioningStage) Name() string {
	return "partitioning"
}

func (ps *PartitioningStage) Process(ctx context.Context, records []*models.Record) ([]*models.Record, error) {
	// Add partition metadata
	for _, record := range records {
		if record.Metadata.Custom == nil {
			record.Metadata.Custom = pool.GetMap()
		}

		partition := ""
		for _, key := range ps.keys {
			if val, ok := record.Data[key]; ok {
				partition += stringpool.Sprintf("%v_", val)
			}
		}
		record.Metadata.Custom["partition"] = partition
	}
	return records, nil
}

// ClusteringStage clusters records
type ClusteringStage struct {
	keys []string
}

func (cs *ClusteringStage) Name() string {
	return "clustering"
}

func (cs *ClusteringStage) Process(ctx context.Context, records []*models.Record) ([]*models.Record, error) {
	// Group records by cluster keys
	clusters := make(map[string][]*models.Record)

	for _, record := range records {
		clusterKey := ""
		for _, key := range cs.keys {
			if val, ok := record.Data[key]; ok {
				clusterKey += stringpool.Sprintf("%v_", val)
			}
		}
		clusters[clusterKey] = append(clusters[clusterKey], record)
	}

	// Flatten clusters back
	result := pool.GetBatchSlice(len(records))
	defer pool.PutBatchSlice(result)
	for _, cluster := range clusters {
		result = append(result, cluster...)
	}

	return result, nil
}

// FormatConversionStage converts to target format
type FormatConversionStage struct {
	format      string
	compression string
}

func (fcs *FormatConversionStage) Name() string {
	return "format_conversion"
}

func (fcs *FormatConversionStage) Process(ctx context.Context, records []*models.Record) ([]*models.Record, error) {
	// Mark records as ready for format conversion
	for _, record := range records {
		if record.Metadata.Custom == nil {
			record.Metadata.Custom = pool.GetMap()
		}
		record.Metadata.Custom["target_format"] = fcs.format
		record.Metadata.Custom["compression"] = fcs.compression
	}
	return records, nil
}

// BulkLoadOptimizer provides warehouse-specific optimizations
type BulkLoadOptimizer struct {
	warehouse string
	config    *BulkLoadConfig
	loader    *BulkLoader
}

// NewBulkLoadOptimizer creates warehouse-specific optimizer
func NewBulkLoadOptimizer(warehouse string) *BulkLoadOptimizer {
	config := DefaultBulkLoadConfig(warehouse)
	return &BulkLoadOptimizer{
		warehouse: warehouse,
		config:    config,
		loader:    NewBulkLoader(config),
	}
}

// OptimizeForSnowflake optimizes for Snowflake
func (blo *BulkLoadOptimizer) OptimizeForSnowflake() {
	blo.config.FileFormat = "parquet"
	blo.config.Compression = "zstd"
	blo.config.EnableClustering = true
	blo.config.MaxFileSize = 100 * 1024 * 1024 // 100MB optimal for Snowflake
}

// OptimizeForBigQuery optimizes for BigQuery
func (blo *BulkLoadOptimizer) OptimizeForBigQuery() {
	blo.config.FileFormat = "avro"
	blo.config.Compression = "snappy"
	blo.config.EnablePartitioning = true
	blo.config.MaxBatchSize = 100000 // BigQuery batch limit
}

// OptimizeForRedshift optimizes for Redshift
func (blo *BulkLoadOptimizer) OptimizeForRedshift() {
	blo.config.FileFormat = "parquet"
	blo.config.Compression = "gzip"
	blo.config.EnableSorting = true
	blo.config.ParallelLoaders = 8 // Redshift parallel copy
}

// Load performs optimized bulk load
func (blo *BulkLoadOptimizer) Load(ctx context.Context, records []*models.Record) error {
	return blo.loader.Load(ctx, records)
}
