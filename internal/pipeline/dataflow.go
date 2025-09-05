// Package pipeline implements advanced data flow management
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/models"
	"go.uber.org/zap"
)

// DataFlow manages the overall data flow through the pipeline
type DataFlow struct {
	topology    *PipelineTopology
	stages      map[string]*StageExecutor
	connections map[string][]chan *models.Record
	config      *StreamingConfig
	logger      *zap.Logger
	metrics     *DataFlowMetrics
	isRunning   int32
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewDataFlow creates a new data flow manager
func NewDataFlow(config *StreamingConfig, logger *zap.Logger) *DataFlow {
	ctx, cancel := context.WithCancel(context.Background())

	return &DataFlow{
		topology:    buildDefaultTopology(config),
		stages:      make(map[string]*StageExecutor),
		connections: make(map[string][]chan *models.Record),
		config:      config,
		logger:      logger.With(zap.String("component", "dataflow")),
		metrics:     NewDataFlowMetrics(),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// buildDefaultTopology creates a default pipeline topology
func buildDefaultTopology(config *StreamingConfig) *PipelineTopology {
	stages := []DataFlowStage{
		{
			Name:        "extract",
			Type:        "extract",
			Parallelism: config.StageParallelism["extract"],
			BufferSize:  config.InitialBufferSize,
			ErrorHandling: ErrorHandlingStrategy{
				Strategy:       "retry",
				MaxRetries:     config.MaxRetryAttempts,
				RetryDelay:     config.RetryBackoffBase,
				CircuitBreaker: config.CircuitBreakerEnabled,
			},
		},
		{
			Name:         "transform",
			Type:         "transform",
			Parallelism:  config.StageParallelism["transform"],
			BufferSize:   config.InitialBufferSize,
			Dependencies: []string{"extract"},
			ErrorHandling: ErrorHandlingStrategy{
				Strategy:       "retry",
				MaxRetries:     config.MaxRetryAttempts,
				RetryDelay:     config.RetryBackoffBase,
				CircuitBreaker: config.CircuitBreakerEnabled,
			},
		},
		{
			Name:         "load",
			Type:         "load",
			Parallelism:  config.StageParallelism["load"],
			BufferSize:   config.InitialBufferSize,
			Dependencies: []string{"transform"},
			ErrorHandling: ErrorHandlingStrategy{
				Strategy:       "dead_letter",
				MaxRetries:     config.MaxRetryAttempts,
				RetryDelay:     config.RetryBackoffBase,
				DeadLetterTTL:  24 * time.Hour,
				CircuitBreaker: config.CircuitBreakerEnabled,
			},
		},
	}

	connections := []StageConnection{
		{From: "extract", To: "transform", BufferSize: config.InitialBufferSize},
		{From: "transform", To: "load", BufferSize: config.InitialBufferSize},
	}

	fusionGroups := [][]string{}
	if config.PipelineFusion {
		fusionGroups = append(fusionGroups, []string{"extract", "transform", "load"})
	}

	return &PipelineTopology{
		Stages:       stages,
		Connections:  connections,
		FusionGroups: fusionGroups,
		Parallelism:  config.StageParallelism,
		BufferSizes: map[string]int{
			"extract":   config.InitialBufferSize,
			"transform": config.InitialBufferSize,
			"load":      config.InitialBufferSize,
		},
	}
}

// StageExecutor executes a single pipeline stage
type StageExecutor struct {
	stage     *DataFlowStage
	workers   []*StageWorker
	inputCh   chan *StreamingRecord
	outputCh  chan *StreamingRecord
	errorCh   chan *StreamingError
	metrics   *StageMetrics
	logger    *zap.Logger
	isRunning int32 //nolint:unused // Reserved for stage execution state tracking
	wg        sync.WaitGroup
}

// StageWorker represents a worker for a pipeline stage
type StageWorker struct {
	id        int
	stage     *DataFlowStage
	processor StageProcessor
	metrics   *WorkerMetrics
	logger    *zap.Logger
}

// StageProcessor defines the interface for stage processing logic
type StageProcessor interface {
	Process(ctx context.Context, record *StreamingRecord) (*StreamingRecord, error)
	Name() string
	Type() string
}

// StageMetrics tracks metrics for a pipeline stage
type StageMetrics struct {
	recordsIn      int64
	recordsOut     int64
	recordsErrored int64
	processingTime int64 // nanoseconds
	lastProcessed  time.Time
	mu             sync.RWMutex
}

// WorkerMetrics tracks metrics for individual workers
type WorkerMetrics struct {
	recordsProcessed  int64
	errorsEncountered int64
	processingTime    int64
	lastActive        time.Time
}

// DataFlowMetrics tracks overall data flow metrics
type DataFlowMetrics struct {
	totalRecords int64
	totalErrors  int64
	totalLatency int64
	stageMetrics map[string]*StageMetrics
	mu           sync.RWMutex
}

// NewDataFlowMetrics creates new data flow metrics
func NewDataFlowMetrics() *DataFlowMetrics {
	return &DataFlowMetrics{
		stageMetrics: make(map[string]*StageMetrics),
	}
}

// Initialize sets up the data flow with the configured topology
func (df *DataFlow) Initialize() error {
	df.logger.Info("initializing data flow",
		zap.Int("stages", len(df.topology.Stages)),
		zap.Int("connections", len(df.topology.Connections)))

	// Create stage executors
	for _, stage := range df.topology.Stages {
		executor := &StageExecutor{
			stage:    &stage,
			inputCh:  make(chan *StreamingRecord, stage.BufferSize),
			outputCh: make(chan *StreamingRecord, stage.BufferSize),
			errorCh:  make(chan *StreamingError, 100),
			metrics:  &StageMetrics{},
			logger:   df.logger.With(zap.String("stage", stage.Name)),
		}

		// Create workers for the stage
		for i := 0; i < stage.Parallelism; i++ {
			worker := &StageWorker{
				id:        i,
				stage:     &stage,
				processor: df.createProcessor(&stage),
				metrics:   &WorkerMetrics{},
				logger:    executor.logger.With(zap.Int("worker", i)),
			}
			executor.workers = append(executor.workers, worker)
		}

		df.stages[stage.Name] = executor
		df.metrics.stageMetrics[stage.Name] = executor.metrics
	}

	// Create connections between stages
	for _, conn := range df.topology.Connections {
		_, exists := df.stages[conn.From]
		if !exists {
			return fmt.Errorf("source stage %s not found", conn.From)
		}

		toStage, exists := df.stages[conn.To]
		if !exists {
			return fmt.Errorf("destination stage %s not found", conn.To)
		}

		// Connect output of source to input of destination
		df.connections[conn.From] = append(df.connections[conn.From], toStage.inputCh)

		df.logger.Debug("connected stages",
			zap.String("from", conn.From),
			zap.String("to", conn.To),
			zap.Int("buffer_size", conn.BufferSize))
	}

	return nil
}

// createProcessor creates a processor for the given stage
func (df *DataFlow) createProcessor(stage *DataFlowStage) StageProcessor {
	switch stage.Type {
	case "extract":
		return &ExtractProcessor{logger: df.logger}
	case "transform":
		return &TransformProcessor{logger: df.logger}
	case "load":
		return &LoadProcessor{logger: df.logger}
	default:
		return &PassthroughProcessor{logger: df.logger}
	}
}

// Start begins data flow execution
func (df *DataFlow) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&df.isRunning, 0, 1) {
		return fmt.Errorf("data flow already running")
	}

	df.logger.Info("starting data flow")

	// Start all stage executors
	for name, executor := range df.stages {
		df.wg.Add(1)
		go df.runStageExecutor(ctx, name, executor)
	}

	// Start metrics collection
	df.wg.Add(1)
	go df.collectMetrics(ctx)

	return nil
}

// runStageExecutor runs a single stage executor
func (df *DataFlow) runStageExecutor(ctx context.Context, name string, executor *StageExecutor) {
	defer df.wg.Done()

	executor.logger.Info("starting stage executor", zap.Int("workers", len(executor.workers)))

	// Start workers
	for _, worker := range executor.workers {
		executor.wg.Add(1)
		go df.runStageWorker(ctx, executor, worker)
	}

	// Start output distribution
	executor.wg.Add(1)
	go df.distributeOutput(ctx, executor)

	// Wait for all workers to complete
	executor.wg.Wait()

	executor.logger.Info("stage executor stopped")
}

// runStageWorker runs a single worker within a stage
func (df *DataFlow) runStageWorker(ctx context.Context, executor *StageExecutor, worker *StageWorker) {
	defer executor.wg.Done()

	worker.logger.Debug("worker started")

	for {
		select {
		case record, ok := <-executor.inputCh:
			if !ok {
				worker.logger.Debug("input channel closed")
				return
			}

			start := time.Now()
			processed, err := worker.processor.Process(ctx, record)
			duration := time.Since(start)

			// Update metrics
			atomic.AddInt64(&worker.metrics.recordsProcessed, 1)
			atomic.AddInt64(&worker.metrics.processingTime, duration.Nanoseconds())
			worker.metrics.lastActive = time.Now()

			atomic.AddInt64(&executor.metrics.recordsIn, 1)
			atomic.AddInt64(&executor.metrics.processingTime, duration.Nanoseconds())

			if err != nil {
				// Handle error
				atomic.AddInt64(&worker.metrics.errorsEncountered, 1)
				atomic.AddInt64(&executor.metrics.recordsErrored, 1)

				select {
				case executor.errorCh <- &StreamingError{
					Type:      fmt.Sprintf("%s_error", executor.stage.Type),
					Error:     err,
					WorkerID:  worker.id,
					Timestamp: time.Now(),
				}:
				default:
					worker.logger.Error("error channel full", zap.Error(err))
				}
				continue
			}

			if processed != nil {
				// Send to output
				select {
				case executor.outputCh <- processed:
					atomic.AddInt64(&executor.metrics.recordsOut, 1)
					executor.metrics.lastProcessed = time.Now()
				case <-ctx.Done():
					return
				}
			}

		case <-ctx.Done():
			worker.logger.Debug("worker context canceled")
			return
		}
	}
}

// distributeOutput distributes stage output to connected stages
func (df *DataFlow) distributeOutput(ctx context.Context, executor *StageExecutor) {
	defer executor.wg.Done()

	connections := df.connections[executor.stage.Name]
	if len(connections) == 0 {
		// Final stage, just drain the output
		for range executor.outputCh {
			// Record processed but don't forward
		}
		return
	}

	for {
		select {
		case record, ok := <-executor.outputCh:
			if !ok {
				// Close all connected input channels
				for _, ch := range connections {
					close(ch)
				}
				return
			}

			// Distribute to all connected stages
			for _, ch := range connections {
				select {
				case ch <- record:
				case <-ctx.Done():
					return
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

// collectMetrics periodically collects and aggregates metrics
func (df *DataFlow) collectMetrics(ctx context.Context) {
	defer df.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			df.aggregateMetrics()

		case <-ctx.Done():
			return
		}
	}
}

// aggregateMetrics aggregates metrics from all stages
func (df *DataFlow) aggregateMetrics() {
	df.metrics.mu.Lock()
	defer df.metrics.mu.Unlock()

	var totalRecords, totalErrors, totalLatency int64

	for _, stageMetrics := range df.metrics.stageMetrics {
		stageMetrics.mu.RLock()
		totalRecords += stageMetrics.recordsOut
		totalErrors += stageMetrics.recordsErrored
		totalLatency += stageMetrics.processingTime
		stageMetrics.mu.RUnlock()
	}

	df.metrics.totalRecords = totalRecords
	df.metrics.totalErrors = totalErrors
	df.metrics.totalLatency = totalLatency
}

// Stop gracefully stops the data flow
func (df *DataFlow) Stop() {
	if !atomic.CompareAndSwapInt32(&df.isRunning, 1, 0) {
		return
	}

	df.logger.Info("stopping data flow")
	df.cancel()
	df.wg.Wait()

	// Close all channels
	for _, executor := range df.stages {
		close(executor.inputCh)
		close(executor.outputCh)
		close(executor.errorCh)
	}
}

// GetMetrics returns current metrics
func (df *DataFlow) GetMetrics() *DataFlowMetrics {
	df.aggregateMetrics()
	return df.metrics
}

// Stage processor implementations

// ExtractProcessor handles extraction logic
type ExtractProcessor struct {
	logger *zap.Logger
}

func (p *ExtractProcessor) Process(ctx context.Context, record *StreamingRecord) (*StreamingRecord, error) {
	// Placeholder for extraction logic
	return record, nil
}

func (p *ExtractProcessor) Name() string { return "extract" }
func (p *ExtractProcessor) Type() string { return "extract" }

// TransformProcessor handles transformation logic
type TransformProcessor struct {
	logger *zap.Logger
}

func (p *TransformProcessor) Process(ctx context.Context, record *StreamingRecord) (*StreamingRecord, error) {
	// Placeholder for transformation logic
	return record, nil
}

func (p *TransformProcessor) Name() string { return "transform" }
func (p *TransformProcessor) Type() string { return "transform" }

// LoadProcessor handles loading logic
type LoadProcessor struct {
	logger *zap.Logger
}

func (p *LoadProcessor) Process(ctx context.Context, record *StreamingRecord) (*StreamingRecord, error) {
	// Placeholder for loading logic
	return record, nil
}

func (p *LoadProcessor) Name() string { return "load" }
func (p *LoadProcessor) Type() string { return "load" }

// PassthroughProcessor passes records through unchanged
type PassthroughProcessor struct {
	logger *zap.Logger
}

func (p *PassthroughProcessor) Process(ctx context.Context, record *StreamingRecord) (*StreamingRecord, error) {
	return record, nil
}

func (p *PassthroughProcessor) Name() string { return "passthrough" }
func (p *PassthroughProcessor) Type() string { return "passthrough" }
