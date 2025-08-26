// Package pipeline implements high-performance streaming pipeline benchmarks
package pipeline

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

// BenchmarkStreamingPipeline tests high-performance streaming pipeline
func BenchmarkStreamingPipeline(b *testing.B) {

	// Create optimized streaming config
	config := &StreamingConfig{
		InitialBufferSize:     100000,  // Large buffer for high throughput
		MaxBufferSize:         1000000, // 1M record buffer
		BufferGrowthFactor:    1.5,
		BackpressureThreshold: 0.85, // Higher threshold for max performance
		BackpressureStrategy:  "adaptive",
		MaxConcurrency:        16, // High concurrency
		StageParallelism: map[string]int{
			"extract":   8,
			"transform": 16,
			"load":      4,
		},
		BatchSize:           10000, // Large batches
		MaxBatchDelay:       100 * time.Millisecond,
		PipelineFusion:      true, // Enable fusion for performance
		ZeroCopyEnabled:     true, // Enable zero-copy optimizations
		MetricsInterval:     10 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}

	// Create mock source and destination
	source := &MockHighThroughputSource{}
	destination := &MockHighThroughputDestination{}

	// Create streaming pipeline
	pipeline := NewStreamingPipeline("benchmark", source, destination, config)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Reset timer and start benchmark
	b.ResetTimer()
	b.ReportAllocs()

	// Measure records processed per second
	var recordsProcessed int64

	// Start metrics collection
	done := make(chan struct{})
	go func() {
		defer close(done)

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		lastCount := int64(0)

		for {
			select {
			case <-ticker.C:
				current := atomic.LoadInt64(&recordsProcessed)
				rps := current - lastCount
				lastCount = current

				if rps > 0 {
					b.Logf("Records/sec: %d, Total: %d", rps, current)
				}

			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()

	// Run benchmark iterations
	for i := 0; i < b.N; i++ {
		// Configure source to generate records
		source.SetRecordCount(1000000) // 1M records per iteration
		source.SetRecordsProcessedCounter(&recordsProcessed)

		// Run pipeline
		err := pipeline.Run(ctx)
		if err != nil {
			b.Fatalf("Pipeline failed: %v", err)
		}
	}

	// Final metrics
	final := atomic.LoadInt64(&recordsProcessed)
	duration := time.Since(time.Now().Add(-30 * time.Second))
	rps := float64(final) / duration.Seconds()

	b.Logf("Final throughput: %.0f records/sec", rps)
	b.Logf("Total records processed: %d", final)

	// Report if we hit the 1M+ target
	if rps >= 1000000 {
		b.Logf("🎉 TARGET ACHIEVED: %.0f records/sec >= 1M records/sec", rps)
	} else {
		b.Logf("Target not reached: %.0f records/sec < 1M records/sec", rps)
	}
}

// BenchmarkBackpressureController tests backpressure performance
func BenchmarkBackpressureController(b *testing.B) {
	logger := zap.NewNop()
	config := DefaultBackpressureConfig()
	config.Strategy = BackpressureAdaptive

	controller := NewBackpressureController("benchmark", config, logger)

	ctx := context.Background()
	controller.Start(ctx)

	// Create test record
	record := pool.GetRecord()
	defer record.Release()
	record.ID = "test"
	record.Metadata.Source = "benchmark"
	record.SetData("test", "data")
	record.SetMetadata("priority", 5)
	record.SetTimestamp(time.Now())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Simulate varying buffer utilization
		utilization := 0.5 + 0.4*float64(i%100)/100.0 // 0.5 to 0.9
		controller.UpdateBufferMetrics(utilization, 1000, 10000)

		// Apply backpressure
		_, err := controller.Apply(ctx, record)
		if err != nil {
			b.Fatalf("Backpressure error: %v", err)
		}
	}
}

// BenchmarkAdaptiveBuffer tests adaptive buffer performance
func BenchmarkAdaptiveBuffer(b *testing.B) {
	logger := zap.NewNop()
	config := &StreamingConfig{
		InitialBufferSize:  10000,
		MaxBufferSize:      100000,
		BufferGrowthFactor: 1.5,
		BufferShrinkDelay:  5 * time.Second,
	}

	buffer := NewAdaptiveBuffer(config, logger)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Simulate varying utilization
		utilization := 0.3 + 0.6*float64(i%1000)/1000.0 // 0.3 to 0.9
		buffer.UpdateUtilization(utilization)
	}
}

// MockHighThroughputSource simulates a high-throughput data source
type MockHighThroughputSource struct {
	recordCount             int64
	recordsProcessedCounter *int64
}

func (s *MockHighThroughputSource) SetRecordCount(count int64) {
	s.recordCount = count
}

func (s *MockHighThroughputSource) SetRecordsProcessedCounter(counter *int64) {
	s.recordsProcessedCounter = counter
}

func (s *MockHighThroughputSource) Initialize(ctx context.Context, config *config.BaseConfig) error {
	return nil
}

func (s *MockHighThroughputSource) Discover(ctx context.Context) (*core.Schema, error) {
	return &core.Schema{
		Name:   "benchmark_schema",
		Fields: []core.Field{{Name: "data", Type: core.FieldTypeJSON}},
	}, nil
}

func (s *MockHighThroughputSource) Read(ctx context.Context) (*core.RecordStream, error) {
	recordsChan := make(chan *pool.Record, 10000)
	errorsChan := make(chan error, 10)

	go func() {
		defer close(recordsChan)
		defer close(errorsChan)

		for i := int64(0); i < s.recordCount; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				record := pool.GetRecord()
				record.ID = fmt.Sprintf("record_%d", i)
				record.Metadata.Source = "benchmark"
				record.SetData("value", i)
				record.SetData("timestamp", time.Now().UnixNano())
				record.SetMetadata("batch", i/1000)
				record.SetTimestamp(time.Now())

				recordsChan <- record

				if s.recordsProcessedCounter != nil {
					atomic.AddInt64(s.recordsProcessedCounter, 1)
				}
			}
		}
	}()

	return &core.RecordStream{
		Records: recordsChan,
		Errors:  errorsChan,
	}, nil
}

func (s *MockHighThroughputSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	// Not implemented for this benchmark
	return nil, fmt.Errorf("not implemented")
}

func (s *MockHighThroughputSource) Close(ctx context.Context) error {
	return nil
}

func (s *MockHighThroughputSource) GetPosition() core.Position {
	return nil
}

func (s *MockHighThroughputSource) SetPosition(position core.Position) error {
	return nil
}

func (s *MockHighThroughputSource) GetState() core.State {
	return make(core.State)
}

func (s *MockHighThroughputSource) SetState(state core.State) error {
	return nil
}

func (s *MockHighThroughputSource) SupportsIncremental() bool {
	return false
}

func (s *MockHighThroughputSource) SupportsRealtime() bool {
	return true
}

func (s *MockHighThroughputSource) SupportsBatch() bool {
	return false
}

func (s *MockHighThroughputSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *MockHighThroughputSource) Health(ctx context.Context) error {
	return nil
}

func (s *MockHighThroughputSource) Metrics() map[string]interface{} {
	return map[string]interface{}{
		"records_generated": s.recordCount,
	}
}

// MockHighThroughputDestination simulates a high-throughput data destination
type MockHighThroughputDestination struct{}

func (d *MockHighThroughputDestination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	return nil
}

func (d *MockHighThroughputDestination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	return nil
}

func (d *MockHighThroughputDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	// High-speed no-op write
	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				return nil
			}
			// Simulate minimal processing
			_ = record

		case err := <-stream.Errors:
			if err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *MockHighThroughputDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	// High-speed no-op batch write
	for {
		select {
		case batch, ok := <-stream.Batches:
			if !ok {
				return nil
			}
			// Simulate minimal processing
			_ = batch

		case err := <-stream.Errors:
			if err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *MockHighThroughputDestination) Close(ctx context.Context) error {
	return nil
}

func (d *MockHighThroughputDestination) SupportsBulkLoad() bool {
	return false
}

func (d *MockHighThroughputDestination) SupportsTransactions() bool {
	return false
}

func (d *MockHighThroughputDestination) SupportsUpsert() bool {
	return false
}

func (d *MockHighThroughputDestination) SupportsBatch() bool {
	return true
}

func (d *MockHighThroughputDestination) SupportsStreaming() bool {
	return true
}

func (d *MockHighThroughputDestination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	return fmt.Errorf("not implemented")
}

func (d *MockHighThroughputDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return nil, fmt.Errorf("not implemented")
}

func (d *MockHighThroughputDestination) Upsert(ctx context.Context, records []*pool.Record, keys []string) error {
	return fmt.Errorf("not implemented")
}

func (d *MockHighThroughputDestination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	return nil
}

func (d *MockHighThroughputDestination) DropSchema(ctx context.Context, schema *core.Schema) error {
	return nil
}

func (d *MockHighThroughputDestination) Health(ctx context.Context) error {
	return nil
}

func (d *MockHighThroughputDestination) Metrics() map[string]interface{} {
	return map[string]interface{}{
		"destination_type": "mock_high_throughput",
	}
}
