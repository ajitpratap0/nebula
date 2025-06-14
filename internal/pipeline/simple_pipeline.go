// Package pipeline provides simplified pipeline execution for Nebula
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

// SimplePipeline represents a simplified data pipeline
type SimplePipeline struct {
	source      core.Source
	destination core.Destination
	transforms  []Transform

	// Configuration
	batchSize   int
	workerCount int

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

// Transform represents a data transformation function
type Transform func(ctx context.Context, record *models.Record) (*models.Record, error)

// PipelineConfig contains pipeline configuration
type PipelineConfig struct {
	BatchSize   int
	WorkerCount int
}

// DefaultPipelineConfig returns default configuration
func DefaultPipelineConfig() *PipelineConfig {
	return &PipelineConfig{
		BatchSize:   1000,
		WorkerCount: 4,
	}
}

// NewSimplePipeline creates a new simplified pipeline
func NewSimplePipeline(source core.Source, destination core.Destination, config *PipelineConfig, logger *zap.Logger) *SimplePipeline {
	if config == nil {
		config = DefaultPipelineConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &SimplePipeline{
		source:      source,
		destination: destination,
		transforms:  make([]Transform, 0),
		batchSize:   config.BatchSize,
		workerCount: config.WorkerCount,
		logger:      logger.With(zap.String("component", "pipeline")),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// AddTransform adds a transformation to the pipeline
func (p *SimplePipeline) AddTransform(transform Transform) {
	p.transforms = append(p.transforms, transform)
}

// Run executes the pipeline
func (p *SimplePipeline) Run(ctx context.Context) error {
	p.startTime = time.Now()
	p.logger.Info("starting pipeline",
		zap.Int("batch_size", p.batchSize),
		zap.Int("worker_count", p.workerCount),
		zap.Int("transforms", len(p.transforms)))

	// Create channels for data flow
	recordChan := make(chan *models.Record, p.batchSize*2)
	transformedChan := make(chan *models.Record, p.batchSize*2)
	batchChan := make(chan []*models.Record, 10)
	errorChan := make(chan error, 100)

	// Start source reader
	p.wg.Add(1)
	go p.readSource(ctx, recordChan, errorChan)

	// Start transform workers with separate wait group
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
		p.logger.Info("all transform workers completed, closed transformed channel")
	}()

	// Start batch collector
	p.wg.Add(1)
	go p.batchCollector(ctx, transformedChan, batchChan)

	// Start destination writer
	p.wg.Add(1)
	go p.writeDestination(ctx, batchChan, errorChan)

	// Start error handler (not part of wait group to avoid deadlock)
	errorHandlerDone := make(chan struct{})
	go func() {
		p.errorHandler(ctx, errorChan)
		close(errorHandlerDone)
	}()

	// Wait for completion of producers
	p.wg.Wait()

	// Close error channel after all producers are done
	close(errorChan)

	// Wait for error handler to finish
	<-errorHandlerDone

	duration := time.Since(p.startTime)
	throughput := float64(p.recordsProcessed) / duration.Seconds()

	p.logger.Info("pipeline completed",
		zap.Int64("records_processed", p.recordsProcessed),
		zap.Int64("records_failed", p.recordsFailed),
		zap.Duration("duration", duration),
		zap.Float64("throughput_rps", throughput))

	return nil
}

// readSource reads records from the source
func (p *SimplePipeline) readSource(ctx context.Context, recordChan chan<- *models.Record, errorChan chan<- error) {
	defer p.wg.Done()
	defer close(recordChan)

	p.logger.Info("starting source reader")

	// Get record stream from source
	stream, err := p.source.Read(ctx)
	if err != nil {
		errorChan <- fmt.Errorf("failed to start source read: %w", err)
		return
	}

	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				p.logger.Info("source stream closed")
				return
			}

			select {
			case recordChan <- record:
			case <-ctx.Done():
				return
			}

		case err := <-stream.Errors:
			if err != nil {
				errorChan <- fmt.Errorf("source error: %w", err)
			}

		case <-ctx.Done():
			p.logger.Info("source reader cancelled")
			return
		}
	}
}

// transformWorker applies transformations to records
func (p *SimplePipeline) transformWorker(ctx context.Context, id int, in <-chan *models.Record, out chan<- *models.Record, errorChan chan<- error) {
	logger := p.logger.With(zap.Int("worker", id))
	logger.Info("transform worker started")

	for {
		select {
		case record, ok := <-in:
			if !ok {
				logger.Info("input channel closed, worker exiting", zap.Int("worker", id))
				return
			}

			// Apply all transforms in sequence
			transformed := record
			for i, transform := range p.transforms {
				result, err := transform(ctx, transformed)
				if err != nil {
					errorChan <- fmt.Errorf("transform %d failed: %w", i, err)
					p.mu.Lock()
					p.recordsFailed++
					p.mu.Unlock()
					transformed = nil
					break
				}
				transformed = result
			}

			// Send transformed record (or pass through if no transforms)
			if transformed != nil || len(p.transforms) == 0 {
				if transformed == nil {
					transformed = record // Pass through if no transforms
				}
				select {
				case out <- transformed:
				case <-ctx.Done():
					return
				}
			}

		case <-ctx.Done():
			logger.Debug("transform worker cancelled")
			return
		}
	}
}

// batchCollector collects records into batches
func (p *SimplePipeline) batchCollector(ctx context.Context, in <-chan *models.Record, out chan<- []*models.Record) {
	defer p.wg.Done()
	defer close(out)

	p.logger.Info("starting batch collector")

	batch := pool.GetBatchSlice(p.batchSize)
	ticker := time.NewTicker(time.Second) // Flush every second
	defer ticker.Stop()

	flush := func() {
		if len(batch) > 0 {
			// Send the batch without returning to pool yet
			// The consumer is responsible for returning it
			select {
			case out <- batch:
				// Get a new batch from pool
				batch = pool.GetBatchSlice(p.batchSize)
			case <-ctx.Done():
			}
		}
	}

	for {
		select {
		case record, ok := <-in:
			if !ok {
				flush() // Final flush
				p.logger.Info("batch collector finished")
				return
			}

			batch = append(batch, record)

			// Flush when batch is full
			if len(batch) >= p.batchSize {
				flush()
			}

		case <-ticker.C:
			// Periodic flush to avoid records getting stuck
			flush()

		case <-ctx.Done():
			flush()
			p.logger.Info("batch collector cancelled")
			return
		}
	}
}

// writeDestination writes batches to the destination
func (p *SimplePipeline) writeDestination(ctx context.Context, batchChan <-chan []*models.Record, errorChan chan<- error) {
	defer p.wg.Done()

	p.logger.Info("starting destination writer")

	// Create batch stream for destination
	destBatchChan := make(chan []*models.Record, 10)
	destErrorChan := make(chan error, 10)

	batchStream := &core.BatchStream{
		Batches: destBatchChan,
		Errors:  destErrorChan,
	}

	// Start destination write in background
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		if err := p.destination.WriteBatch(ctx, batchStream); err != nil {
			errorChan <- fmt.Errorf("destination write failed: %w", err)
		}
	}()

	// Forward batches to destination
	for {
		select {
		case batch, ok := <-batchChan:
			if !ok {
				close(destBatchChan)
				// Wait for WriteBatch to complete
				<-writeDone
				p.logger.Info("destination writer finished")
				return
			}

			select {
			case destBatchChan <- batch:
				p.mu.Lock()
				p.recordsProcessed += int64(len(batch))
				p.mu.Unlock()

			case <-ctx.Done():
				close(destBatchChan)
				return
			}

		case err := <-destErrorChan:
			if err != nil {
				errorChan <- err
			}

		case <-ctx.Done():
			close(destBatchChan)
			p.logger.Info("destination writer cancelled")
			return
		}
	}
}

// errorHandler handles pipeline errors
func (p *SimplePipeline) errorHandler(ctx context.Context, errorChan <-chan error) {
	p.logger.Debug("error handler started")

	for {
		select {
		case err, ok := <-errorChan:
			if !ok {
				p.logger.Debug("error channel closed, error handler exiting")
				return
			}

			if err != nil {
				p.logger.Error("pipeline error", zap.Error(err))
			}

		case <-ctx.Done():
			p.logger.Debug("error handler context cancelled")
			return
		}
	}
}

// Stop gracefully stops the pipeline
func (p *SimplePipeline) Stop() {
	p.logger.Info("stopping pipeline")
	p.cancel()
	p.wg.Wait()
}

// Metrics returns pipeline metrics
func (p *SimplePipeline) Metrics() map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	duration := time.Since(p.startTime)
	throughput := float64(p.recordsProcessed) / duration.Seconds()

	return map[string]interface{}{
		"records_processed": p.recordsProcessed,
		"records_failed":    p.recordsFailed,
		"duration":          duration.String(),
		"throughput_rps":    throughput,
		"worker_count":      p.workerCount,
		"batch_size":        p.batchSize,
		"transform_count":   len(p.transforms),
	}
}

// Common Transforms

// FieldMapperTransform creates a transform that maps fields
func FieldMapperTransform(mapping map[string]string) Transform {
	return func(ctx context.Context, record *models.Record) (*models.Record, error) {
		if record.Data == nil {
			return record, nil
		}

		newData := pool.GetMap()

		for oldField, newField := range mapping {
			if value, ok := record.Data[oldField]; ok {
				newData[newField] = value
			}
		}

		// Copy unmapped fields
		for field, value := range record.Data {
			if _, mapped := mapping[field]; !mapped {
				newData[field] = value
			}
		}

		record.Data = newData
		return record, nil
	}
}

// FilterTransform creates a transform that filters records
func FilterTransform(predicate func(*models.Record) bool) Transform {
	return func(ctx context.Context, record *models.Record) (*models.Record, error) {
		if predicate(record) {
			return record, nil
		}
		return nil, nil // Filtered out
	}
}

// TypeConverterTransform creates a transform that converts field types
func TypeConverterTransform(field string, converter func(interface{}) (interface{}, error)) Transform {
	return func(ctx context.Context, record *models.Record) (*models.Record, error) {
		if record.Data == nil {
			return record, nil
		}

		if value, ok := record.Data[field]; ok {
			converted, err := converter(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert field %s: %w", field, err)
			}
			record.Data[field] = converted
		}

		return record, nil
	}
}
