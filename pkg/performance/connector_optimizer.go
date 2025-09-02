// Package performance provides connector-specific optimizations
package performance

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/compression"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/formats/columnar"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// ConnectorOptimizer provides connector-specific optimizations
type ConnectorOptimizer struct {
	config       *ConnectorOptConfig
	optimizer    *Optimizer
	memOptimizer *MemoryOptimizer
	compressor   *compression.CompressorPool
	profiler     *Profiler
	strategies   map[string]OptimizationStrategy
	mu           sync.RWMutex
}

// ConnectorOptConfig configures connector optimization
type ConnectorOptConfig struct {
	ConnectorType    string
	TargetThroughput int // records/sec
	MaxMemoryMB      int
	MaxCPUPercent    float64
	EnableProfiling  bool

	// Optimization strategies
	EnableBatching    bool
	EnableCompression bool
	EnableColumnar    bool
	EnableParallelism bool
	EnableCaching     bool
	EnableZeroCopy    bool
}

// DefaultConnectorOptConfig returns default configuration
func DefaultConnectorOptConfig(connectorType string) *ConnectorOptConfig {
	return &ConnectorOptConfig{
		ConnectorType:     connectorType,
		TargetThroughput:  100000,
		MaxMemoryMB:       1024,
		MaxCPUPercent:     80.0,
		EnableProfiling:   true,
		EnableBatching:    true,
		EnableCompression: true,
		EnableColumnar:    true,
		EnableParallelism: true,
		EnableCaching:     true,
		EnableZeroCopy:    true,
	}
}

// OptimizationStrategy defines an optimization strategy
type OptimizationStrategy interface {
	Name() string
	Apply(ctx context.Context, data interface{}) (interface{}, error)
	ShouldApply(metrics *Metrics) bool
}

// NewConnectorOptimizer creates a connector optimizer
func NewConnectorOptimizer(config *ConnectorOptConfig) *ConnectorOptimizer {
	co := &ConnectorOptimizer{
		config:       config,
		optimizer:    NewOptimizer(DefaultOptimizerConfig()),
		memOptimizer: NewMemoryOptimizer(DefaultMemoryConfig()),
		strategies:   make(map[string]OptimizationStrategy),
	}

	// Initialize compression pool
	if config.EnableCompression {
		co.compressor = compression.NewCompressorPool(&compression.Config{
			Algorithm: compression.Snappy,
			Level:     compression.Default,
		})
	}

	// Initialize profiler
	if config.EnableProfiling {
		co.profiler = NewProfiler(DefaultProfilerConfig(config.ConnectorType))
	}

	// Register optimization strategies
	co.registerStrategies()

	return co
}

// registerStrategies registers optimization strategies
func (co *ConnectorOptimizer) registerStrategies() {
	// Batching strategy
	if co.config.EnableBatching {
		co.strategies["batching"] = &BatchingStrategy{
			optimizer: co.optimizer,
		}
	}

	// Compression strategy
	if co.config.EnableCompression {
		co.strategies["compression"] = &CompressionStrategy{
			compressor: co.compressor,
		}
	}

	// Columnar strategy
	if co.config.EnableColumnar {
		co.strategies["columnar"] = &ColumnarStrategy{}
	}

	// Parallelism strategy
	if co.config.EnableParallelism {
		co.strategies["parallelism"] = &ParallelismStrategy{
			optimizer: co.optimizer,
		}
	}

	// Caching strategy
	if co.config.EnableCaching {
		co.strategies["caching"] = &CachingStrategy{
			cache: NewCacheOptimizer(10000),
		}
	}

	// Zero-copy strategy
	if co.config.EnableZeroCopy {
		co.strategies["zerocopy"] = &ZeroCopyStrategy{
			optimizer: NewZeroCopyOptimizer(true),
		}
	}
}

// OptimizeSource optimizes a source connector
func (co *ConnectorOptimizer) OptimizeSource(source core.Source) core.Source {
	switch co.config.ConnectorType {
	case "postgresql_cdc", "mysql_cdc":
		return co.optimizeCDCSource(source)
	case "google_ads":
		return co.optimizeAPISource(source)
	default:
		return co.optimizeGenericSource(source)
	}
}

// OptimizeDestination optimizes a destination connector
func (co *ConnectorOptimizer) OptimizeDestination(dest core.Destination) core.Destination {
	switch co.config.ConnectorType {
	case "snowflake":
		return co.optimizeSnowflakeDestination(dest)
	case "bigquery", "redshift":
		return co.optimizeCloudWarehouse(dest)
	default:
		return co.optimizeGenericDestination(dest)
	}
}

// optimizeCDCSource optimizes CDC sources
func (co *ConnectorOptimizer) optimizeCDCSource(source core.Source) core.Source {
	return &OptimizedCDCSource{
		source:    source,
		optimizer: co,
		config: CDCOptConfig{
			SnapshotBatchSize:  50000,
			StreamingBatchSize: 10000,
			ParallelSnapshots:  4,
			CompressionEnabled: true,
			MemoryOptimization: true,
		},
	}
}

// optimizeAPISource optimizes API sources
func (co *ConnectorOptimizer) optimizeAPISource(source core.Source) core.Source {
	return &OptimizedAPISource{
		source:    source,
		optimizer: co,
		config: APIOptConfig{
			RequestBatching:    true,
			ResponseCaching:    true,
			CompressionEnabled: true,
			ParallelRequests:   10,
			CacheTTL:           5 * time.Minute,
		},
	}
}

// optimizeSnowflakeDestination optimizes Snowflake destination
func (co *ConnectorOptimizer) optimizeSnowflakeDestination(dest core.Destination) core.Destination {
	return &OptimizedSnowflakeDestination{
		destination: dest,
		optimizer:   co,
		config: SnowflakeOptConfig{
			UseStagingArea:  true,
			FileFormat:      "parquet",
			CompressionType: "zstd",
			ParallelUploads: 8,
			MicroBatchSize:  100000,
			ColumnarEnabled: true,
		},
	}
}

// optimizeCloudWarehouse optimizes cloud warehouse destinations
func (co *ConnectorOptimizer) optimizeCloudWarehouse(dest core.Destination) core.Destination {
	return &OptimizedCloudWarehouse{
		destination: dest,
		optimizer:   co,
		config: CloudWarehouseOptConfig{
			BatchSize:          50000,
			ParallelLoaders:    4,
			CompressionEnabled: true,
			ColumnarFormat:     true,
			StreamingInserts:   false,
		},
	}
}

// optimizeGenericSource optimizes generic sources
func (co *ConnectorOptimizer) optimizeGenericSource(source core.Source) core.Source {
	return &OptimizedGenericSource{
		source:    source,
		optimizer: co,
	}
}

// optimizeGenericDestination optimizes generic destinations
func (co *ConnectorOptimizer) optimizeGenericDestination(dest core.Destination) core.Destination {
	return &OptimizedGenericDestination{
		destination: dest,
		optimizer:   co,
	}
}

// ApplyOptimizations applies all applicable optimizations
func (co *ConnectorOptimizer) ApplyOptimizations(ctx context.Context, records []*models.Record) ([]*models.Record, error) {
	if co.profiler != nil {
		co.profiler.Start()
		defer co.profiler.Stop()
	}

	// Apply memory optimization first
	records = co.memOptimizer.OptimizeRecords(records)

	// Apply strategies based on current metrics
	metrics := co.profiler.GetMetrics()
	for name, strategy := range co.strategies {
		if strategy.ShouldApply(metrics) {
			optimized, err := strategy.Apply(ctx, records)
			if err != nil {
				return nil, fmt.Errorf("strategy %s failed: %w", name, err)
			}
			records = optimized.([]*models.Record)
		}
	}

	return records, nil
}

// GetOptimizationReport generates optimization report
func (co *ConnectorOptimizer) GetOptimizationReport() string {
	if co.profiler == nil {
		return "Profiling not enabled"
	}

	result := co.profiler.GenerateReport()
	return result.Report
}

// OptimizedCDCSource wraps CDC source with optimizations
type OptimizedCDCSource struct {
	source    core.Source
	optimizer *ConnectorOptimizer
	config    CDCOptConfig
}

// CDCOptConfig configures CDC optimizations
type CDCOptConfig struct {
	SnapshotBatchSize  int
	StreamingBatchSize int
	ParallelSnapshots  int
	CompressionEnabled bool
	MemoryOptimization bool
}

// Initialize delegates to underlying source
func (ocs *OptimizedCDCSource) Initialize(ctx context.Context, config *config.BaseConfig) error {
	return ocs.source.Initialize(ctx, config)
}

// Discover delegates to underlying source
func (ocs *OptimizedCDCSource) Discover(ctx context.Context) (*core.Schema, error) {
	return ocs.source.Discover(ctx)
}

// Read implements optimized read
func (ocs *OptimizedCDCSource) Read(ctx context.Context) (*core.RecordStream, error) {
	// Read from underlying source
	stream, err := ocs.source.Read(ctx)
	if err != nil {
		return nil, err
	}

	// Create optimized stream wrapper
	optimizedRecords := make(chan *models.Record, 10000)
	optimizedErrors := make(chan error, 1)

	go func() {
		defer close(optimizedRecords)
		defer close(optimizedErrors)

		// Buffer records for optimization
		buffer := pool.GetBatchSlice(ocs.config.SnapshotBatchSize)

		defer pool.PutBatchSlice(buffer)

		for record := range stream.Records {
			buffer = append(buffer, record)

			if len(buffer) >= ocs.config.SnapshotBatchSize {
				// Apply optimizations to batch
				optimized, err := ocs.optimizer.ApplyOptimizations(ctx, buffer)
				if err != nil {
					optimizedErrors <- err
					return
				}

				// Send optimized records
				for _, r := range optimized {
					select {
					case optimizedRecords <- r:
					case <-ctx.Done():
						return
					}
				}

				buffer = buffer[:0]
			}
		}

		// Process remaining records
		if len(buffer) > 0 {
			optimized, err := ocs.optimizer.ApplyOptimizations(ctx, buffer)
			if err != nil {
				optimizedErrors <- err
				return
			}

			for _, r := range optimized {
				optimizedRecords <- r
			}
		}

		// Forward any errors
		for err := range stream.Errors {
			optimizedErrors <- err
		}
	}()

	return &core.RecordStream{
		Records: optimizedRecords,
		Errors:  optimizedErrors,
	}, nil
}

// ReadBatch delegates to underlying source
func (ocs *OptimizedCDCSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	return ocs.source.ReadBatch(ctx, batchSize)
}

// Close implements source close
func (ocs *OptimizedCDCSource) Close(ctx context.Context) error {
	return ocs.source.Close(ctx)
}

// GetPosition delegates to underlying source
func (ocs *OptimizedCDCSource) GetPosition() core.Position {
	return ocs.source.GetPosition()
}

// SetPosition delegates to underlying source
func (ocs *OptimizedCDCSource) SetPosition(position core.Position) error {
	return ocs.source.SetPosition(position)
}

// GetState delegates to underlying source
func (ocs *OptimizedCDCSource) GetState() core.State {
	return ocs.source.GetState()
}

// SetState delegates to underlying source
func (ocs *OptimizedCDCSource) SetState(state core.State) error {
	return ocs.source.SetState(state)
}

// SupportsIncremental delegates to underlying source
func (ocs *OptimizedCDCSource) SupportsIncremental() bool {
	return ocs.source.SupportsIncremental()
}

// SupportsRealtime delegates to underlying source
func (ocs *OptimizedCDCSource) SupportsRealtime() bool {
	return ocs.source.SupportsRealtime()
}

// SupportsBatch delegates to underlying source
func (ocs *OptimizedCDCSource) SupportsBatch() bool {
	return ocs.source.SupportsBatch()
}

// Subscribe delegates to underlying source
func (ocs *OptimizedCDCSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	return ocs.source.Subscribe(ctx, tables)
}

// Health delegates to underlying source
func (ocs *OptimizedCDCSource) Health(ctx context.Context) error {
	return ocs.source.Health(ctx)
}

// Metrics returns combined metrics
func (ocs *OptimizedCDCSource) Metrics() map[string]interface{} {
	metrics := ocs.source.Metrics()
	// Add optimization metrics
	metrics["optimization_enabled"] = true
	metrics["snapshot_batch_size"] = ocs.config.SnapshotBatchSize
	metrics["streaming_batch_size"] = ocs.config.StreamingBatchSize
	return metrics
}

// OptimizedAPISource wraps API source with optimizations
type OptimizedAPISource struct {
	source    core.Source
	optimizer *ConnectorOptimizer
	config    APIOptConfig
	cache     *CacheOptimizer
}

// APIOptConfig configures API optimizations
type APIOptConfig struct {
	RequestBatching    bool
	ResponseCaching    bool
	CompressionEnabled bool
	ParallelRequests   int
	CacheTTL           time.Duration
}

// Initialize delegates to underlying source
func (oas *OptimizedAPISource) Initialize(ctx context.Context, config *config.BaseConfig) error {
	return oas.source.Initialize(ctx, config)
}

// Discover delegates to underlying source
func (oas *OptimizedAPISource) Discover(ctx context.Context) (*core.Schema, error) {
	return oas.source.Discover(ctx)
}

// Read implements optimized read with caching
func (oas *OptimizedAPISource) Read(ctx context.Context) (*core.RecordStream, error) {
	// Check cache first
	if oas.config.ResponseCaching && oas.cache != nil {
		if cached, found := oas.cache.Get("api_stream"); found {
			return cached.(*core.RecordStream), nil
		}
	}

	// Read from underlying source
	stream, err := oas.source.Read(ctx)
	if err != nil {
		return nil, err
	}

	// Cache stream if enabled
	if oas.config.ResponseCaching && oas.cache != nil {
		oas.cache.Set("api_stream", stream)
	}

	return stream, nil
}

// ReadBatch delegates to underlying source
func (oas *OptimizedAPISource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	return oas.source.ReadBatch(ctx, batchSize)
}

// Close implements source close
func (oas *OptimizedAPISource) Close(ctx context.Context) error {
	return oas.source.Close(ctx)
}

// GetPosition delegates to underlying source
func (oas *OptimizedAPISource) GetPosition() core.Position {
	return oas.source.GetPosition()
}

// SetPosition delegates to underlying source
func (oas *OptimizedAPISource) SetPosition(position core.Position) error {
	return oas.source.SetPosition(position)
}

// GetState delegates to underlying source
func (oas *OptimizedAPISource) GetState() core.State {
	return oas.source.GetState()
}

// SetState delegates to underlying source
func (oas *OptimizedAPISource) SetState(state core.State) error {
	return oas.source.SetState(state)
}

// SupportsIncremental delegates to underlying source
func (oas *OptimizedAPISource) SupportsIncremental() bool {
	return oas.source.SupportsIncremental()
}

// SupportsRealtime delegates to underlying source
func (oas *OptimizedAPISource) SupportsRealtime() bool {
	return oas.source.SupportsRealtime()
}

// SupportsBatch delegates to underlying source
func (oas *OptimizedAPISource) SupportsBatch() bool {
	return oas.source.SupportsBatch()
}

// Subscribe delegates to underlying source
func (oas *OptimizedAPISource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	return oas.source.Subscribe(ctx, tables)
}

// Health delegates to underlying source
func (oas *OptimizedAPISource) Health(ctx context.Context) error {
	return oas.source.Health(ctx)
}

// Metrics returns combined metrics
func (oas *OptimizedAPISource) Metrics() map[string]interface{} {
	metrics := oas.source.Metrics()
	// Add optimization metrics
	metrics["optimization_enabled"] = true
	metrics["caching_enabled"] = oas.config.ResponseCaching
	return metrics
}

// OptimizedSnowflakeDestination wraps Snowflake with optimizations
type OptimizedSnowflakeDestination struct {
	destination core.Destination
	optimizer   *ConnectorOptimizer
	config      SnowflakeOptConfig
	writer      columnar.Writer
}

// SnowflakeOptConfig configures Snowflake optimizations
type SnowflakeOptConfig struct {
	UseStagingArea  bool
	FileFormat      string
	CompressionType string
	ParallelUploads int
	MicroBatchSize  int
	ColumnarEnabled bool
}

// Initialize delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	return osd.destination.Initialize(ctx, config)
}

// CreateSchema delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	return osd.destination.CreateSchema(ctx, schema)
}

// Write implements optimized write
func (osd *OptimizedSnowflakeDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	// If columnar format is enabled, buffer and convert
	if osd.config.ColumnarEnabled && osd.writer != nil {
		// Buffer records for columnar conversion
		buffer := pool.GetBatchSlice(osd.config.MicroBatchSize)

		defer pool.PutBatchSlice(buffer)

		for record := range stream.Records {
			buffer = append(buffer, record)

			if len(buffer) >= osd.config.MicroBatchSize {
				// Apply optimizations
				optimized, err := osd.optimizer.ApplyOptimizations(ctx, buffer)
				if err != nil {
					return err
				}

				// Write to columnar format
				err = osd.writer.WriteRecords(optimized)
				if err != nil {
					return err
				}

				buffer = buffer[:0]
			}
		}

		// Write remaining records
		if len(buffer) > 0 {
			optimized, err := osd.optimizer.ApplyOptimizations(ctx, buffer)
			if err != nil {
				return err
			}

			err = osd.writer.WriteRecords(optimized)
			if err != nil {
				return err
			}
		}

		// Flush columnar writer
		return osd.writer.Flush()
	}

	// Otherwise, pass through to underlying destination
	return osd.destination.Write(ctx, stream)
}

// WriteBatch delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	return osd.destination.WriteBatch(ctx, stream)
}

// Close implements destination close
func (osd *OptimizedSnowflakeDestination) Close(ctx context.Context) error {
	if osd.writer != nil {
		_ = osd.writer.Close()
	}
	return osd.destination.Close(ctx)
}

// SupportsBulkLoad delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) SupportsBulkLoad() bool {
	return osd.destination.SupportsBulkLoad()
}

// SupportsTransactions delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) SupportsTransactions() bool {
	return osd.destination.SupportsTransactions()
}

// SupportsUpsert delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) SupportsUpsert() bool {
	return osd.destination.SupportsUpsert()
}

// SupportsBatch delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) SupportsBatch() bool {
	return osd.destination.SupportsBatch()
}

// SupportsStreaming delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) SupportsStreaming() bool {
	return osd.destination.SupportsStreaming()
}

// BulkLoad delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	return osd.destination.BulkLoad(ctx, reader, format)
}

// BeginTransaction delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return osd.destination.BeginTransaction(ctx)
}

// Upsert delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	return osd.destination.Upsert(ctx, records, keys)
}

// AlterSchema delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	return osd.destination.AlterSchema(ctx, oldSchema, newSchema)
}

// DropSchema delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) DropSchema(ctx context.Context, schema *core.Schema) error {
	return osd.destination.DropSchema(ctx, schema)
}

// Health delegates to underlying destination
func (osd *OptimizedSnowflakeDestination) Health(ctx context.Context) error {
	return osd.destination.Health(ctx)
}

// Metrics returns combined metrics
func (osd *OptimizedSnowflakeDestination) Metrics() map[string]interface{} {
	metrics := osd.destination.Metrics()
	// Add optimization metrics
	metrics["optimization_enabled"] = true
	metrics["columnar_enabled"] = osd.config.ColumnarEnabled
	metrics["staging_enabled"] = osd.config.UseStagingArea
	return metrics
}

// OptimizedCloudWarehouse wraps cloud warehouse with optimizations
type OptimizedCloudWarehouse struct {
	destination core.Destination
	optimizer   *ConnectorOptimizer
	config      CloudWarehouseOptConfig
}

// CloudWarehouseOptConfig configures cloud warehouse optimizations
type CloudWarehouseOptConfig struct {
	BatchSize          int
	ParallelLoaders    int
	CompressionEnabled bool
	ColumnarFormat     bool
	StreamingInserts   bool
}

// Initialize delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) Initialize(ctx context.Context, config *config.BaseConfig) error {
	return ocw.destination.Initialize(ctx, config)
}

// CreateSchema delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) CreateSchema(ctx context.Context, schema *core.Schema) error {
	return ocw.destination.CreateSchema(ctx, schema)
}

// Write implements optimized write
func (ocw *OptimizedCloudWarehouse) Write(ctx context.Context, stream *core.RecordStream) error {
	// For now, pass through to underlying destination
	// In a real implementation, we would apply parallel loading optimizations
	return ocw.destination.Write(ctx, stream)
}

// WriteBatch delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	return ocw.destination.WriteBatch(ctx, stream)
}

// Close implements destination close
func (ocw *OptimizedCloudWarehouse) Close(ctx context.Context) error {
	return ocw.destination.Close(ctx)
}

// SupportsBulkLoad delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) SupportsBulkLoad() bool {
	return ocw.destination.SupportsBulkLoad()
}

// SupportsTransactions delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) SupportsTransactions() bool {
	return ocw.destination.SupportsTransactions()
}

// SupportsUpsert delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) SupportsUpsert() bool {
	return ocw.destination.SupportsUpsert()
}

// SupportsBatch delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) SupportsBatch() bool {
	return ocw.destination.SupportsBatch()
}

// SupportsStreaming delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) SupportsStreaming() bool {
	return ocw.destination.SupportsStreaming()
}

// BulkLoad delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	return ocw.destination.BulkLoad(ctx, reader, format)
}

// BeginTransaction delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return ocw.destination.BeginTransaction(ctx)
}

// Upsert delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	return ocw.destination.Upsert(ctx, records, keys)
}

// AlterSchema delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	return ocw.destination.AlterSchema(ctx, oldSchema, newSchema)
}

// DropSchema delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) DropSchema(ctx context.Context, schema *core.Schema) error {
	return ocw.destination.DropSchema(ctx, schema)
}

// Health delegates to underlying destination
func (ocw *OptimizedCloudWarehouse) Health(ctx context.Context) error {
	return ocw.destination.Health(ctx)
}

// Metrics returns combined metrics
func (ocw *OptimizedCloudWarehouse) Metrics() map[string]interface{} {
	metrics := ocw.destination.Metrics()
	// Add optimization metrics
	metrics["optimization_enabled"] = true
	metrics["batch_size"] = ocw.config.BatchSize
	metrics["parallel_loaders"] = ocw.config.ParallelLoaders
	return metrics
}

// OptimizedGenericSource wraps generic source with basic optimizations
type OptimizedGenericSource struct {
	source    core.Source
	optimizer *ConnectorOptimizer
}

// Initialize delegates to underlying source
func (ogs *OptimizedGenericSource) Initialize(ctx context.Context, config *config.BaseConfig) error {
	return ogs.source.Initialize(ctx, config)
}

// Discover delegates to underlying source
func (ogs *OptimizedGenericSource) Discover(ctx context.Context) (*core.Schema, error) {
	return ogs.source.Discover(ctx)
}

// Read implements optimized read
func (ogs *OptimizedGenericSource) Read(ctx context.Context) (*core.RecordStream, error) {
	return ogs.source.Read(ctx)
}

// ReadBatch delegates to underlying source
func (ogs *OptimizedGenericSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	return ogs.source.ReadBatch(ctx, batchSize)
}

// Close implements source close
func (ogs *OptimizedGenericSource) Close(ctx context.Context) error {
	return ogs.source.Close(ctx)
}

// GetPosition delegates to underlying source
func (ogs *OptimizedGenericSource) GetPosition() core.Position {
	return ogs.source.GetPosition()
}

// SetPosition delegates to underlying source
func (ogs *OptimizedGenericSource) SetPosition(position core.Position) error {
	return ogs.source.SetPosition(position)
}

// GetState delegates to underlying source
func (ogs *OptimizedGenericSource) GetState() core.State {
	return ogs.source.GetState()
}

// SetState delegates to underlying source
func (ogs *OptimizedGenericSource) SetState(state core.State) error {
	return ogs.source.SetState(state)
}

// SupportsIncremental delegates to underlying source
func (ogs *OptimizedGenericSource) SupportsIncremental() bool {
	return ogs.source.SupportsIncremental()
}

// SupportsRealtime delegates to underlying source
func (ogs *OptimizedGenericSource) SupportsRealtime() bool {
	return ogs.source.SupportsRealtime()
}

// SupportsBatch delegates to underlying source
func (ogs *OptimizedGenericSource) SupportsBatch() bool {
	return ogs.source.SupportsBatch()
}

// Subscribe delegates to underlying source
func (ogs *OptimizedGenericSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	return ogs.source.Subscribe(ctx, tables)
}

// Health delegates to underlying source
func (ogs *OptimizedGenericSource) Health(ctx context.Context) error {
	return ogs.source.Health(ctx)
}

// Metrics returns combined metrics
func (ogs *OptimizedGenericSource) Metrics() map[string]interface{} {
	return ogs.source.Metrics()
}

// OptimizedGenericDestination wraps generic destination with basic optimizations
type OptimizedGenericDestination struct {
	destination core.Destination
	optimizer   *ConnectorOptimizer
}

// Initialize delegates to underlying destination
func (ogd *OptimizedGenericDestination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	return ogd.destination.Initialize(ctx, config)
}

// CreateSchema delegates to underlying destination
func (ogd *OptimizedGenericDestination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	return ogd.destination.CreateSchema(ctx, schema)
}

// Write implements optimized write
func (ogd *OptimizedGenericDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	return ogd.destination.Write(ctx, stream)
}

// WriteBatch delegates to underlying destination
func (ogd *OptimizedGenericDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	return ogd.destination.WriteBatch(ctx, stream)
}

// Close implements destination close
func (ogd *OptimizedGenericDestination) Close(ctx context.Context) error {
	return ogd.destination.Close(ctx)
}

// SupportsBulkLoad delegates to underlying destination
func (ogd *OptimizedGenericDestination) SupportsBulkLoad() bool {
	return ogd.destination.SupportsBulkLoad()
}

// SupportsTransactions delegates to underlying destination
func (ogd *OptimizedGenericDestination) SupportsTransactions() bool {
	return ogd.destination.SupportsTransactions()
}

// SupportsUpsert delegates to underlying destination
func (ogd *OptimizedGenericDestination) SupportsUpsert() bool {
	return ogd.destination.SupportsUpsert()
}

// SupportsBatch delegates to underlying destination
func (ogd *OptimizedGenericDestination) SupportsBatch() bool {
	return ogd.destination.SupportsBatch()
}

// SupportsStreaming delegates to underlying destination
func (ogd *OptimizedGenericDestination) SupportsStreaming() bool {
	return ogd.destination.SupportsStreaming()
}

// BulkLoad delegates to underlying destination
func (ogd *OptimizedGenericDestination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	return ogd.destination.BulkLoad(ctx, reader, format)
}

// BeginTransaction delegates to underlying destination
func (ogd *OptimizedGenericDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return ogd.destination.BeginTransaction(ctx)
}

// Upsert delegates to underlying destination
func (ogd *OptimizedGenericDestination) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	return ogd.destination.Upsert(ctx, records, keys)
}

// AlterSchema delegates to underlying destination
func (ogd *OptimizedGenericDestination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	return ogd.destination.AlterSchema(ctx, oldSchema, newSchema)
}

// DropSchema delegates to underlying destination
func (ogd *OptimizedGenericDestination) DropSchema(ctx context.Context, schema *core.Schema) error {
	return ogd.destination.DropSchema(ctx, schema)
}

// Health delegates to underlying destination
func (ogd *OptimizedGenericDestination) Health(ctx context.Context) error {
	return ogd.destination.Health(ctx)
}

// Metrics returns combined metrics
func (ogd *OptimizedGenericDestination) Metrics() map[string]interface{} {
	return ogd.destination.Metrics()
}

// Helper function to split records into batches
func splitIntoBatches(records []*models.Record, batchSize int) []interface{} {
	var batches []interface{}
	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		batches = append(batches, records[i:end])
	}
	return batches
}
