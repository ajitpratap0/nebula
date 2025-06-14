package pipeline

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

// ParallelPipeline extends SimplePipeline with high-performance parallel processing
type ParallelPipeline struct {
	source      core.Source
	destination core.Destination
	transforms  []Transform

	// Configuration
	batchSize       int
	workerCount     int
	parallelGroups  int
	useParallel     bool
	vectorBatchSize int

	// Parallel processing
	parallelProcessor *ParallelProcessor

	// Metrics
	recordsProcessed int64
	recordsFailed    int64
	startTime        time.Time

	// State
	logger *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
}

// ParallelPipelineConfig contains configuration for parallel pipeline
type ParallelPipelineConfig struct {
	BatchSize       int
	WorkerCount     int  // 0 = auto (NumCPU * 2)
	ParallelGroups  int  // 0 = auto (NumCPU)
	UseParallel     bool // Enable parallel processing
	VectorBatchSize int  // Batch size for vectorized operations
	EnableAffinity  bool // Enable CPU affinity
}

// DefaultParallelPipelineConfig returns default configuration
func DefaultParallelPipelineConfig() *ParallelPipelineConfig {
	return &ParallelPipelineConfig{
		BatchSize:       1000,
		WorkerCount:     runtime.NumCPU() * 2,
		ParallelGroups:  runtime.NumCPU(),
		UseParallel:     true,
		VectorBatchSize: 100,
		EnableAffinity:  true,
	}
}

// NewParallelPipeline creates a new parallel pipeline
func NewParallelPipeline(source core.Source, destination core.Destination, config *ParallelPipelineConfig, logger *zap.Logger) *ParallelPipeline {
	if config == nil {
		config = DefaultParallelPipelineConfig()
	}

	// Adjust worker count based on CPU cores
	if config.WorkerCount == 0 {
		config.WorkerCount = runtime.NumCPU() * 2
	}
	if config.ParallelGroups == 0 {
		config.ParallelGroups = runtime.NumCPU()
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &ParallelPipeline{
		source:          source,
		destination:     destination,
		transforms:      make([]Transform, 0),
		batchSize:       config.BatchSize,
		workerCount:     config.WorkerCount,
		parallelGroups:  config.ParallelGroups,
		useParallel:     config.UseParallel,
		vectorBatchSize: config.VectorBatchSize,
		logger:          logger.With(zap.String("component", "parallel-pipeline")),
		ctx:             ctx,
		cancel:          cancel,
	}

	// Create parallel processor if enabled
	if config.UseParallel {
		parallelConfig := ParallelConfig{
			Name:           "pipeline-processor",
			NumWorkers:     config.WorkerCount,
			QueueSize:      config.BatchSize * 10,
			EnableAffinity: config.EnableAffinity,
			BatchSize:      config.VectorBatchSize,
			MaxBatchWait:   10 * time.Millisecond,
		}
		p.parallelProcessor = NewParallelProcessor(parallelConfig, p.transforms, logger)
	}

	return p
}

// AddTransform adds a transformation to the pipeline
func (p *ParallelPipeline) AddTransform(transform Transform) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	p.transforms = append(p.transforms, transform)
	
	// Update parallel processor transforms if enabled
	if p.parallelProcessor != nil {
		p.parallelProcessor.transforms = p.transforms
	}
}

// Run executes the parallel pipeline
func (p *ParallelPipeline) Run(ctx context.Context) error {
	p.startTime = time.Now()
	p.logger.Info("starting parallel pipeline",
		zap.Int("batch_size", p.batchSize),
		zap.Int("worker_count", p.workerCount),
		zap.Int("parallel_groups", p.parallelGroups),
		zap.Bool("use_parallel", p.useParallel),
		zap.Int("transforms", len(p.transforms)))

	// Start parallel processor if enabled
	if p.parallelProcessor != nil {
		p.parallelProcessor.Start()
		defer p.parallelProcessor.Stop()
	}

	// Create channels for data flow
	recordChan := make(chan *models.Record, p.batchSize*2)
	transformedChan := make(chan *models.Record, p.batchSize*2)
	batchChan := make(chan []*models.Record, 10)
	errorChan := make(chan error, 100)

	// Start source reader
	p.wg.Add(1)
	go p.readSource(ctx, recordChan, errorChan)

	// Start processing based on mode
	if p.useParallel && p.parallelProcessor != nil {
		// Use parallel processor
		p.wg.Add(1)
		go p.parallelProcessing(ctx, recordChan, transformedChan, errorChan)
	} else {
		// Use traditional transform workers
		transformWg := &sync.WaitGroup{}
		for i := 0; i < p.workerCount; i++ {
			transformWg.Add(1)
			go func(id int) {
				defer transformWg.Done()
				p.transformWorker(ctx, id, recordChan, transformedChan, errorChan)
			}(i)
		}

		// Close transformed channel when all workers are done
		go func() {
			transformWg.Wait()
			close(transformedChan)
		}()
	}

	// Start batch collector with vectorized processing
	p.wg.Add(1)
	go p.vectorizedBatchCollector(ctx, transformedChan, batchChan)

	// Start destination writer with parallel batching
	p.wg.Add(1)
	go p.parallelDestinationWriter(ctx, batchChan, errorChan)

	// Start error handler
	errorHandlerDone := make(chan struct{})
	go func() {
		p.errorHandler(ctx, errorChan)
		close(errorHandlerDone)
	}()

	// Wait for completion
	p.wg.Wait()
	close(errorChan)
	<-errorHandlerDone

	duration := time.Since(p.startTime)
	throughput := float64(p.recordsProcessed) / duration.Seconds()

	p.logger.Info("parallel pipeline completed",
		zap.Int64("records_processed", p.recordsProcessed),
		zap.Int64("records_failed", p.recordsFailed),
		zap.Duration("duration", duration),
		zap.Float64("throughput_rps", throughput))

	return nil
}

// parallelProcessing uses the parallel processor for transforms
func (p *ParallelPipeline) parallelProcessing(ctx context.Context, input <-chan *models.Record, output chan<- *models.Record, errors chan<- error) {
	defer p.wg.Done()
	defer close(output)

	// Get output from parallel processor
	processorOutput := p.parallelProcessor.GetOutput()

	// Feed records to parallel processor
	go func() {
		for record := range input {
			if err := p.parallelProcessor.Process(record); err != nil {
				select {
				case errors <- err:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Collect processed records
	for {
		select {
		case record, ok := <-processorOutput:
			if !ok {
				return
			}
			select {
			case output <- record:
				atomic.AddInt64(&p.recordsProcessed, 1)
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// vectorizedBatchCollector collects records into batches with vectorized optimization
func (p *ParallelPipeline) vectorizedBatchCollector(ctx context.Context, input <-chan *models.Record, output chan<- []*models.Record) {
	defer p.wg.Done()
	defer close(output)

	// Use pooled batch slice
	batch := pool.GetBatchSlice(p.batchSize)
	defer pool.PutBatchSlice(batch)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case record, ok := <-input:
			if !ok {
				// Send final batch
				if len(batch) > 0 {
					// Create a copy for sending
					finalBatch := make([]*models.Record, len(batch))
					copy(finalBatch, batch)
					select {
					case output <- finalBatch:
					case <-ctx.Done():
					}
				}
				return
			}

			batch = append(batch, record)
			if len(batch) >= p.batchSize {
				// Send full batch
				fullBatch := make([]*models.Record, len(batch))
				copy(fullBatch, batch)
				select {
				case output <- fullBatch:
					// Get new batch from pool
					pool.PutBatchSlice(batch)
					batch = pool.GetBatchSlice(p.batchSize)
				case <-ctx.Done():
					return
				}
			}

		case <-ticker.C:
			// Send partial batch on timeout
			if len(batch) > 0 {
				partialBatch := make([]*models.Record, len(batch))
				copy(partialBatch, batch)
				select {
				case output <- partialBatch:
					batch = batch[:0] // Reset slice
				case <-ctx.Done():
					return
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

// parallelDestinationWriter writes batches to destination with parallel processing
func (p *ParallelPipeline) parallelDestinationWriter(ctx context.Context, batches <-chan []*models.Record, errors chan<- error) {
	defer p.wg.Done()

	// Create parallel writers
	numWriters := 4
	if p.parallelGroups > 4 {
		numWriters = p.parallelGroups / 2
	}

	writerWg := &sync.WaitGroup{}
	for i := 0; i < numWriters; i++ {
		writerWg.Add(1)
		go func(id int) {
			defer writerWg.Done()
			p.destinationWorker(ctx, id, batches, errors)
		}(i)
	}

	writerWg.Wait()
}

// destinationWorker writes batches to the destination
func (p *ParallelPipeline) destinationWorker(ctx context.Context, id int, batches <-chan []*models.Record, errors chan<- error) {
	// Create a batch stream for this worker
	batchChan := make(chan []*models.Record, 1)
	errorChan := make(chan error, 1)
	
	stream := &core.BatchStream{
		Batches: batchChan,
		Errors:  errorChan,
	}

	// Start destination writer in background
	go func() {
		if err := p.destination.WriteBatch(ctx, stream); err != nil {
			select {
			case errors <- err:
			case <-ctx.Done():
			}
		}
	}()

	// Process batches
	for batch := range batches {
		select {
		case batchChan <- batch:
			// Check for errors from destination
			select {
			case err := <-errorChan:
				if err != nil {
					select {
					case errors <- err:
					case <-ctx.Done():
						return
					}
				}
			default:
			}
		case <-ctx.Done():
			close(batchChan)
			return
		}
	}

	close(batchChan)
}

// Traditional methods (fallback to SimplePipeline behavior)

func (p *ParallelPipeline) readSource(ctx context.Context, output chan<- *models.Record, errors chan<- error) {
	defer p.wg.Done()
	defer close(output)

	stream, err := p.source.Read(ctx)
	if err != nil {
		select {
		case errors <- err:
		case <-ctx.Done():
		}
		return
	}

	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				return
			}
			select {
			case output <- record:
			case <-ctx.Done():
				return
			}

		case err := <-stream.Errors:
			if err != nil {
				select {
				case errors <- err:
				case <-ctx.Done():
					return
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

func (p *ParallelPipeline) transformWorker(ctx context.Context, id int, input <-chan *models.Record, output chan<- *models.Record, errors chan<- error) {
	for record := range input {
		// Apply transforms
		processedRecord := record
		for _, transform := range p.transforms {
			var err error
			processedRecord, err = transform(ctx, processedRecord)
			if err != nil {
				atomic.AddInt64(&p.recordsFailed, 1)
				select {
				case errors <- err:
				case <-ctx.Done():
					return
				}
				break
			}
		}

		if processedRecord != nil {
			select {
			case output <- processedRecord:
				atomic.AddInt64(&p.recordsProcessed, 1)
			case <-ctx.Done():
				return
			}
		}
	}
}

func (p *ParallelPipeline) errorHandler(ctx context.Context, errors <-chan error) {
	for err := range errors {
		if err != nil {
			p.logger.Error("pipeline error", zap.Error(err))
		}
	}
}

// Stop stops the parallel pipeline
func (p *ParallelPipeline) Stop() {
	p.cancel()
	p.wg.Wait()
}

// GetMetrics returns pipeline metrics
func (p *ParallelPipeline) GetMetrics() PipelineMetrics {
	return PipelineMetrics{
		RecordsProcessed: atomic.LoadInt64(&p.recordsProcessed),
		RecordsFailed:    atomic.LoadInt64(&p.recordsFailed),
		Duration:         time.Since(p.startTime),
		Throughput:       float64(atomic.LoadInt64(&p.recordsProcessed)) / time.Since(p.startTime).Seconds(),
	}
}

// PipelineMetrics contains pipeline performance metrics
type PipelineMetrics struct {
	RecordsProcessed int64
	RecordsFailed    int64
	Duration         time.Duration
	Throughput       float64 // records per second
}