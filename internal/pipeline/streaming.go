// Package pipeline provides advanced streaming pipeline architecture with backpressure-aware data flow
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

// StreamingPipeline provides advanced streaming capabilities with backpressure and error recovery
type StreamingPipeline struct {
	name        string
	source      core.Source
	destination core.Destination
	config      *StreamingConfig
	logger      *zap.Logger

	// Streaming components
	flow         *DataFlow
	buffer       *AdaptiveBuffer
	errorMgr     *ErrorManager
	backpressure *BackpressureController
	metrics      *StreamingMetrics

	// State management
	isRunning int32
	startTime time.Time
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// StreamingConfig configures the streaming pipeline
type StreamingConfig struct {
	// Buffer configuration
	InitialBufferSize  int           `json:"initial_buffer_size"`
	MaxBufferSize      int           `json:"max_buffer_size"`
	BufferGrowthFactor float64       `json:"buffer_growth_factor"`
	BufferShrinkDelay  time.Duration `json:"buffer_shrink_delay"`

	// Parallelism
	MaxConcurrency   int            `json:"max_concurrency"`
	StageParallelism map[string]int `json:"stage_parallelism"`

	// Backpressure
	BackpressureThreshold float64 `json:"backpressure_threshold"`
	BackpressureStrategy  string  `json:"backpressure_strategy"`
	FlowControlEnabled    bool    `json:"flow_control_enabled"`

	// Error recovery
	DeadLetterQueueSize   int           `json:"dead_letter_queue_size"`
	MaxRetryAttempts      int           `json:"max_retry_attempts"`
	RetryBackoffBase      time.Duration `json:"retry_backoff_base"`
	RetryBackoffMax       time.Duration `json:"retry_backoff_max"`
	CircuitBreakerEnabled bool          `json:"circuit_breaker_enabled"`

	// Performance
	BatchSize       int           `json:"batch_size"`
	MaxBatchDelay   time.Duration `json:"max_batch_delay"`
	PipelineFusion  bool          `json:"pipeline_fusion"`
	ZeroCopyEnabled bool          `json:"zero_copy_enabled"`

	// Monitoring
	MetricsInterval     time.Duration `json:"metrics_interval"`
	HealthCheckInterval time.Duration `json:"health_check_interval"`
}

// DefaultStreamingConfig returns optimized default configuration
func DefaultStreamingConfig() *StreamingConfig {
	return &StreamingConfig{
		InitialBufferSize:  10000,
		MaxBufferSize:      100000,
		BufferGrowthFactor: 1.5,
		BufferShrinkDelay:  30 * time.Second,
		MaxConcurrency:     8,
		StageParallelism: map[string]int{
			"extract":   4,
			"transform": 8,
			"load":      2,
		},
		BackpressureThreshold: 0.8,
		BackpressureStrategy:  "adaptive",
		FlowControlEnabled:    true,
		DeadLetterQueueSize:   1000,
		MaxRetryAttempts:      3,
		RetryBackoffBase:      100 * time.Millisecond,
		RetryBackoffMax:       30 * time.Second,
		CircuitBreakerEnabled: true,
		BatchSize:             1000,
		MaxBatchDelay:         5 * time.Second,
		PipelineFusion:        true,
		ZeroCopyEnabled:       true,
		MetricsInterval:       15 * time.Second,
		HealthCheckInterval:   30 * time.Second,
	}
}

// NewStreamingPipeline creates a new high-performance streaming pipeline
func NewStreamingPipeline(name string, source core.Source, destination core.Destination, config *StreamingConfig) *StreamingPipeline {
	if config == nil {
		config = DefaultStreamingConfig()
	}

	logger, _ := zap.NewProduction()
	ctx, cancel := context.WithCancel(context.Background())

	pipeline := &StreamingPipeline{
		name:        name,
		source:      source,
		destination: destination,
		config:      config,
		logger:      logger.With(zap.String("streaming_pipeline", name)),
		ctx:         ctx,
		cancel:      cancel,
	}

	// Initialize streaming components
	pipeline.flow = NewDataFlow(config, logger)
	pipeline.buffer = NewAdaptiveBuffer(config, logger)
	pipeline.errorMgr = NewErrorManager(config, logger)

	// Create backpressure config from streaming config
	bpConfig := &BackpressureConfig{
		ActivationThreshold:   config.BackpressureThreshold,
		DeactivationThreshold: config.BackpressureThreshold * 0.75,
		CriticalThreshold:     0.95,
		Strategy:              BackpressureStrategy(config.BackpressureStrategy),
		EmergencyShutdown:     true,
		EmergencyThreshold:    0.99,
	}
	pipeline.backpressure = NewBackpressureController(name, bpConfig, logger)
	pipeline.metrics = NewStreamingMetrics(name, logger)

	return pipeline
}

// Run executes the streaming pipeline with advanced features
func (sp *StreamingPipeline) Run(ctx context.Context) error {
	// Check if already running
	if !atomic.CompareAndSwapInt32(&sp.isRunning, 0, 1) {
		return fmt.Errorf("streaming pipeline already running")
	}
	defer atomic.StoreInt32(&sp.isRunning, 0)

	sp.startTime = time.Now()
	sp.logger.Info("starting streaming pipeline",
		zap.String("source", "streaming-source"),
		zap.String("destination", "streaming-destination"),
		zap.Bool("fusion_enabled", sp.config.PipelineFusion),
		zap.Bool("zero_copy", sp.config.ZeroCopyEnabled))

	// Initialize connectors
	if err := sp.source.Initialize(ctx, nil); err != nil {
		return fmt.Errorf("failed to initialize source: %w", err)
	}
	defer sp.source.Close(ctx)

	if err := sp.destination.Initialize(ctx, nil); err != nil {
		return fmt.Errorf("failed to initialize destination: %w", err)
	}
	defer sp.destination.Close(ctx)

	// Start monitoring
	sp.wg.Add(2)
	go sp.monitorHealth(ctx)
	go sp.reportMetrics(ctx)

	// Start streaming data flow
	if sp.config.PipelineFusion {
		return sp.runFusedPipeline(ctx)
	}
	return sp.runStandardPipeline(ctx)
}

// runFusedPipeline executes optimized fused pipeline stages
func (sp *StreamingPipeline) runFusedPipeline(ctx context.Context) error {
	sp.logger.Info("running fused pipeline", zap.Int("concurrency", sp.config.MaxConcurrency))

	// Create streaming channels with adaptive buffering
	recordStream := pool.GetRecordChannel(sp.config.InitialBufferSize)
	defer pool.PutRecordChannel(recordStream)
	errorStream := make(chan *StreamingError, 100)

	// Start parallel processing stages
	for i := 0; i < sp.config.MaxConcurrency; i++ {
		sp.wg.Add(1)
		go sp.fusedWorker(ctx, i, recordStream, errorStream)
	}

	// Start error recovery
	sp.wg.Add(1)
	go sp.errorMgr.ProcessErrors(ctx, errorStream, &sp.wg)

	// Read from source and distribute work
	return sp.distributeWork(ctx, recordStream, errorStream)
}

// runStandardPipeline executes traditional multi-stage pipeline
func (sp *StreamingPipeline) runStandardPipeline(ctx context.Context) error {
	sp.logger.Info("running standard pipeline")

	// Create stage channels
	extractCh := pool.GetRecordChannel(sp.config.InitialBufferSize)
	defer pool.PutRecordChannel(extractCh)
	transformCh := pool.GetRecordChannel(sp.config.InitialBufferSize)
	defer pool.PutRecordChannel(transformCh)
	loadCh := pool.GetRecordChannel(sp.config.InitialBufferSize)
	defer pool.PutRecordChannel(loadCh)
	errorCh := make(chan *StreamingError, 100)

	// Start pipeline stages
	sp.wg.Add(4)
	go sp.extractStage(ctx, extractCh, errorCh)
	go sp.transformStage(ctx, extractCh, transformCh, errorCh)
	go sp.loadStage(ctx, transformCh, loadCh, errorCh)
	go sp.errorMgr.ProcessErrors(ctx, errorCh, &sp.wg)

	// Wait for completion
	sp.wg.Wait()
	return nil
}

// fusedWorker processes records in a fused extract-transform-load operation
func (sp *StreamingPipeline) fusedWorker(ctx context.Context, workerID int, records <-chan *StreamingRecord, errors chan<- *StreamingError) {
	defer sp.wg.Done()

	workerLogger := sp.logger.With(zap.Int("worker_id", workerID))
	batch := pool.GetBatchSlice(sp.config.BatchSize)
	defer pool.PutBatchSlice(batch)
	batchTimeout := time.NewTimer(sp.config.MaxBatchDelay)
	defer batchTimeout.Stop()

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		start := time.Now()

		// Create batch stream channels
		batchChan := pool.GetBatchChannel()
		defer pool.PutBatchChannel(batchChan)
		errorChan := pool.GetErrorChannel()
		defer pool.PutErrorChannel(errorChan)
		batchStream := &core.BatchStream{
			Batches: batchChan,
			Errors:  errorChan,
		}

		// Send batch to channel
		go func() {
			batchChan <- batch
			close(batchChan)
		}()

		// Write batch to destination with retry
		if err := sp.destination.WriteBatch(ctx, batchStream); err != nil {
			// Send to error recovery
			select {
			case errors <- &StreamingError{
				Type:      "load_error",
				Error:     err,
				Records:   batch,
				WorkerID:  workerID,
				Timestamp: time.Now(),
			}:
			default:
				workerLogger.Error("error channel full, dropping error", zap.Error(err))
			}
		} else {
			// Record successful batch
			sp.metrics.RecordBatch(len(batch), time.Since(start))
			workerLogger.Debug("batch processed",
				zap.Int("size", len(batch)),
				zap.Duration("duration", time.Since(start)))
		}

		// Reset batch
		batch = batch[:0]
		batchTimeout.Reset(sp.config.MaxBatchDelay)
	}

	for {
		select {
		case record, ok := <-records:
			if !ok {
				flushBatch()
				return
			}

			// Apply backpressure control
			allowed, err := sp.backpressure.Apply(ctx, record)
			if err != nil || !allowed {
				sp.metrics.RecordBackpressure()
				continue
			}

			// Fused processing: extract -> transform -> prepare for load
			processed := sp.processRecord(ctx, record, workerID)
			if processed != nil {
				batch = append(batch, processed)
			}

			// Check if batch is full
			if len(batch) >= sp.config.BatchSize {
				flushBatch()
			}

		case <-batchTimeout.C:
			flushBatch()

		case <-ctx.Done():
			flushBatch()
			return
		}
	}
}

// processRecord performs fused record processing
func (sp *StreamingPipeline) processRecord(ctx context.Context, record *StreamingRecord, workerID int) *models.Record {
	// Zero-copy processing when possible
	if sp.config.ZeroCopyEnabled {
		return sp.processRecordZeroCopy(record)
	}

	// Standard processing - use the unified Record constructor
	result := models.NewRecord(record.Metadata.Source, record.Data)
	result.ID = record.ID
	result.SetTimestamp(record.GetTimestamp())

	// Copy custom metadata if it exists
	if record.Metadata.Custom != nil {
		for k, v := range record.Metadata.Custom {
			result.SetMetadata(k, v)
		}
	}

	return result
}

// processRecordZeroCopy performs zero-allocation record processing
func (sp *StreamingPipeline) processRecordZeroCopy(record *StreamingRecord) *models.Record {
	// Since StreamingRecord is now just a type alias for models.Record,
	// we can return it directly (zero-copy)
	return record
}

// distributeWork reads from source and distributes to workers
func (sp *StreamingPipeline) distributeWork(ctx context.Context, records chan<- *StreamingRecord, errors chan<- *StreamingError) error {
	defer close(records)

	// Check if source supports batch mode
	if sp.source.SupportsBatch() {
		// Use batch reading for better performance
		recordStream, err := sp.source.ReadBatch(ctx, sp.config.BatchSize)
		if err != nil {
			return fmt.Errorf("failed to start batch stream: %w", err)
		}
		return sp.processBatchStream(ctx, recordStream, records, errors)
	} else {
		// Fall back to regular streaming
		recordStream, err := sp.source.Read(ctx)
		if err != nil {
			return fmt.Errorf("failed to start record stream: %w", err)
		}
		return sp.processRecordStream(ctx, recordStream, records, errors)
	}
}

// processBatchStream handles batch-based source streams
func (sp *StreamingPipeline) processBatchStream(ctx context.Context, recordStream *core.BatchStream, records chan<- *StreamingRecord, errors chan<- *StreamingError) error {
	for {
		select {
		case batch, ok := <-recordStream.Batches:
			if !ok {
				return nil
			}

			// Distribute records to workers
			for _, record := range batch {
				// StreamingRecord is just a type alias for models.Record now
				streamingRecord := record

				select {
				case records <- streamingRecord:
					sp.metrics.RecordExtracted()
				case <-ctx.Done():
					return ctx.Err()
				}
			}

		case err := <-recordStream.Errors:
			if err != nil {
				select {
				case errors <- &StreamingError{
					Type:      "extract_error",
					Error:     err,
					Timestamp: time.Now(),
				}:
				default:
					sp.logger.Error("error channel full", zap.Error(err))
				}
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// processRecordStream handles record-based source streams (fallback for non-batch sources)
func (sp *StreamingPipeline) processRecordStream(ctx context.Context, recordStream *core.RecordStream, records chan<- *StreamingRecord, errors chan<- *StreamingError) error {
	for {
		select {
		case record, ok := <-recordStream.Records:
			if !ok {
				return nil
			}

			// StreamingRecord is just a type alias for models.Record now
			streamingRecord := record

			select {
			case records <- streamingRecord:
				sp.metrics.RecordExtracted()
			case <-ctx.Done():
				return ctx.Err()
			}

		case err := <-recordStream.Errors:
			if err != nil {
				select {
				case errors <- &StreamingError{
					Type:      "extract_error",
					Error:     err,
					Timestamp: time.Now(),
				}:
				default:
					sp.logger.Error("error channel full", zap.Error(err))
				}
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Traditional stage implementations for non-fused pipeline

func (sp *StreamingPipeline) extractStage(ctx context.Context, out chan<- *StreamingRecord, errors chan<- *StreamingError) {
	defer sp.wg.Done()
	defer close(out)

	// Implementation similar to distributeWork but for stage-based pipeline
	sp.logger.Info("extract stage started", zap.Int("parallelism", sp.config.StageParallelism["extract"]))

	// Get source stream
	recordStream, err := sp.source.ReadBatch(ctx, sp.config.BatchSize)
	if err != nil {
		sp.logger.Error("failed to start extract stage", zap.Error(err))
		return
	}

	for {
		select {
		case batch, ok := <-recordStream.Batches:
			if !ok {
				return
			}

			for _, record := range batch {
				// StreamingRecord is just a type alias for models.Record now
				streamingRecord := record

				select {
				case out <- streamingRecord:
					sp.metrics.RecordExtracted()
				case <-ctx.Done():
					return
				}
			}

		case err := <-recordStream.Errors:
			if err != nil {
				select {
				case errors <- &StreamingError{
					Type:      "extract_error",
					Error:     err,
					Timestamp: time.Now(),
				}:
				default:
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

func (sp *StreamingPipeline) transformStage(ctx context.Context, in <-chan *StreamingRecord, out chan<- *StreamingRecord, errors chan<- *StreamingError) {
	defer sp.wg.Done()
	defer close(out)

	// Start transform workers
	workerWg := sync.WaitGroup{}
	for i := 0; i < sp.config.StageParallelism["transform"]; i++ {
		workerWg.Add(1)
		go sp.transformWorker(ctx, i, in, out, errors, &workerWg)
	}

	workerWg.Wait()
}

func (sp *StreamingPipeline) transformWorker(ctx context.Context, workerID int, in <-chan *StreamingRecord, out chan<- *StreamingRecord, errors chan<- *StreamingError, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case record, ok := <-in:
			if !ok {
				return
			}

			// Apply transformations (placeholder for now)
			transformed := record // No transforms implemented yet

			select {
			case out <- transformed:
				sp.metrics.RecordTransformed()
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func (sp *StreamingPipeline) loadStage(ctx context.Context, in <-chan *StreamingRecord, out chan<- *StreamingRecord, errors chan<- *StreamingError) {
	defer sp.wg.Done()
	defer close(out)

	// Start load workers
	workerWg := sync.WaitGroup{}
	for i := 0; i < sp.config.StageParallelism["load"]; i++ {
		workerWg.Add(1)
		go sp.loadWorker(ctx, i, in, errors, &workerWg)
	}

	workerWg.Wait()
}

func (sp *StreamingPipeline) loadWorker(
	ctx context.Context, workerID int,
	in <-chan *StreamingRecord, errors chan<- *StreamingError,
	wg *sync.WaitGroup) {
	defer wg.Done()

	batch := pool.GetBatchSlice(sp.config.BatchSize)
	defer pool.PutBatchSlice(batch)
	batchTimeout := time.NewTimer(sp.config.MaxBatchDelay)
	defer batchTimeout.Stop()

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		batchChan := pool.GetBatchChannel()
		defer pool.PutBatchChannel(batchChan)
		errorChan := pool.GetErrorChannel()
		defer pool.PutErrorChannel(errorChan)
		batchStream := &core.BatchStream{
			Batches: batchChan,
			Errors:  errorChan,
		}
		go func() {
			batchChan <- batch
			close(batchChan)
		}()

		if err := sp.destination.WriteBatch(ctx, batchStream); err != nil {
			select {
			case errors <- &StreamingError{
				Type:      "load_error",
				Error:     err,
				Records:   batch,
				WorkerID:  workerID,
				Timestamp: time.Now(),
			}:
			default:
			}
		} else {
			sp.metrics.RecordLoaded(len(batch))
		}

		batch = batch[:0]
		batchTimeout.Reset(sp.config.MaxBatchDelay)
	}

	for {
		select {
		case record, ok := <-in:
			if !ok {
				flushBatch()
				return
			}

			// Since StreamingRecord is now just models.Record, no conversion needed
			coreRecord := record
			batch = append(batch, coreRecord)

			if len(batch) >= sp.config.BatchSize {
				flushBatch()
			}

		case <-batchTimeout.C:
			flushBatch()

		case <-ctx.Done():
			flushBatch()
			return
		}
	}
}

// Monitoring functions

func (sp *StreamingPipeline) monitorHealth(ctx context.Context) {
	defer sp.wg.Done()

	ticker := time.NewTicker(sp.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check connector health
			sourceHealthErr := sp.source.Health(ctx)
			destHealthErr := sp.destination.Health(ctx)

			sourceHealthy := sourceHealthErr == nil
			destHealthy := destHealthErr == nil

			sp.logger.Debug("health check",
				zap.Bool("source_healthy", sourceHealthy),
				zap.Bool("dest_healthy", destHealthy),
				zap.Error(sourceHealthErr),
				zap.Error(destHealthErr))

			// Update metrics
			sp.metrics.RecordHealth(sourceHealthy && destHealthy)

		case <-ctx.Done():
			return
		}
	}
}

func (sp *StreamingPipeline) reportMetrics(ctx context.Context) {
	defer sp.wg.Done()

	ticker := time.NewTicker(sp.config.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			stats := sp.metrics.GetStats()
			uptime := time.Since(sp.startTime)

			sp.logger.Info("streaming pipeline metrics",
				zap.Duration("uptime", uptime),
				zap.Int64("records_processed", stats.RecordsProcessed),
				zap.Int64("batches_processed", stats.BatchesProcessed),
				zap.Float64("throughput_rps", stats.ThroughputRPS),
				zap.Float64("avg_batch_duration", stats.AvgBatchDuration.Seconds()),
				zap.Int64("backpressure_events", stats.BackpressureEvents),
				zap.Int64("errors_recovered", stats.ErrorsRecovered))

		case <-ctx.Done():
			return
		}
	}
}

// Stop gracefully stops the streaming pipeline
func (sp *StreamingPipeline) Stop() {
	sp.logger.Info("stopping streaming pipeline")
	sp.cancel()
	sp.wg.Wait()
}

// GetMetrics returns current pipeline metrics
func (sp *StreamingPipeline) GetMetrics() *StreamingStats {
	return sp.metrics.GetStats()
}

// IsRunning returns true if pipeline is currently running
func (sp *StreamingPipeline) IsRunning() bool {
	return atomic.LoadInt32(&sp.isRunning) == 1
}
