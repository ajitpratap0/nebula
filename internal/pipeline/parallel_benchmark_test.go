package pipeline

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

// MockSource generates test records
type MockSource struct {
	recordCount int
	generated   int32
}

func (m *MockSource) Initialize(ctx context.Context, config *config.BaseConfig) error { return nil }
func (m *MockSource) Discover(ctx context.Context) (*core.Schema, error)       { return nil, nil }
func (m *MockSource) GetState() core.State                                    { return nil }
func (m *MockSource) SetState(state core.State) error                         { return nil }
func (m *MockSource) Close(ctx context.Context) error                         { return nil }
func (m *MockSource) Health(ctx context.Context) error                        { return nil }
func (m *MockSource) Metrics() map[string]interface{}                          { return nil }
func (m *MockSource) GetPosition() core.Position                              { return nil }
func (m *MockSource) SetPosition(position core.Position) error                { return nil }
func (m *MockSource) SupportsIncremental() bool                               { return false }
func (m *MockSource) SupportsRealtime() bool                                  { return false }
func (m *MockSource) SupportsBatch() bool                                     { return true }
func (m *MockSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	return nil, nil
}
func (m *MockSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	return nil, nil
}

func (m *MockSource) Read(ctx context.Context) (*core.RecordStream, error) {
	recordChan := make(chan *models.Record, 1000)
	errorChan := make(chan error, 1)
	
	stream := &core.RecordStream{
		Records: recordChan,
		Errors:  errorChan,
	}

	go func() {
		defer close(recordChan)
		for i := 0; i < m.recordCount; i++ {
			record := pool.GetRecord()
			record.Data = map[string]interface{}{
				"id":        i,
				"name":      "test",
				"value":     float64(i) * 1.5,
				"timestamp": time.Now().Unix(),
				"category":  "benchmark",
				"status":    "active",
			}
			record.Metadata.Source = "mock"
			record.Metadata.Timestamp = time.Now()
			
			select {
			case recordChan <- record:
				atomic.AddInt32(&m.generated, 1)
			case <-ctx.Done():
				return
			}
		}
	}()

	return stream, nil
}

// MockDestination consumes records
type MockDestination struct {
	received int64
}

func (m *MockDestination) Initialize(ctx context.Context, config *config.BaseConfig) error     { return nil }
func (m *MockDestination) CreateSchema(ctx context.Context, schema *core.Schema) error  { return nil }
func (m *MockDestination) AlterSchema(ctx context.Context, old, new *core.Schema) error { return nil }
func (m *MockDestination) DropSchema(ctx context.Context, schema *core.Schema) error    { return nil }
func (m *MockDestination) Close(ctx context.Context) error                             { return nil }
func (m *MockDestination) Health(ctx context.Context) error                            { return nil }
func (m *MockDestination) Metrics() map[string]interface{}                              { return nil }
func (m *MockDestination) SupportsBulkLoad() bool                                      { return true }
func (m *MockDestination) SupportsTransactions() bool                                  { return false }
func (m *MockDestination) SupportsUpsert() bool                                        { return false }
func (m *MockDestination) SupportsBatch() bool                                         { return true }
func (m *MockDestination) SupportsStreaming() bool                                     { return true }
func (m *MockDestination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	return nil
}
func (m *MockDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return nil, nil
}
func (m *MockDestination) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	return nil
}

func (m *MockDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	for record := range stream.Records {
		if record != nil {
			atomic.AddInt64(&m.received, 1)
			record.Release()
		}
	}
	return nil
}

func (m *MockDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	for batch := range stream.Batches {
		for _, record := range batch {
			if record != nil {
				atomic.AddInt64(&m.received, 1)
				record.Release()
			}
		}
	}
	return nil
}

// CPU-intensive transform
func cpuIntensiveTransform(ctx context.Context, record *models.Record) (*models.Record, error) {
	// Simulate CPU-bound work
	sum := 0.0
	for i := 0; i < 100; i++ {
		sum += float64(i) * 1.234567
		if val, ok := record.Data["value"].(float64); ok {
			sum += val
		}
	}
	record.Data["computed"] = sum
	return record, nil
}

// Benchmark parallel vs sequential processing
func BenchmarkParallelProcessing(b *testing.B) {
	logger := zap.NewNop()
	recordCounts := []int{10000, 50000, 100000}

	for _, recordCount := range recordCounts {
		// Benchmark sequential pipeline
		b.Run(fmt.Sprintf("Sequential_%d", recordCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				source := &MockSource{recordCount: recordCount}
				dest := &MockDestination{}
				
				config := &PipelineConfig{
					BatchSize:   1000,
					WorkerCount: 1, // Sequential
				}
				
				pipeline := NewSimplePipeline(source, dest, config, logger)
				pipeline.AddTransform(cpuIntensiveTransform)
				
				ctx := context.Background()
				start := time.Now()
				if err := pipeline.Run(ctx); err != nil {
					b.Fatal(err)
				}
				duration := time.Since(start)
				
				b.ReportMetric(float64(recordCount)/duration.Seconds(), "records/sec")
			}
		})

		// Benchmark parallel pipeline with different worker counts
		workerCounts := []int{2, 4, 8, runtime.NumCPU(), runtime.NumCPU() * 2}
		for _, workers := range workerCounts {
			b.Run(fmt.Sprintf("Parallel_%d_workers_%d", recordCount, workers), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					source := &MockSource{recordCount: recordCount}
					dest := &MockDestination{}
					
					config := &ParallelPipelineConfig{
						BatchSize:       1000,
						WorkerCount:     workers,
						UseParallel:     true,
						VectorBatchSize: 100,
						EnableAffinity:  true,
					}
					
					pipeline := NewParallelPipeline(source, dest, config, logger)
					pipeline.AddTransform(cpuIntensiveTransform)
					
					ctx := context.Background()
					start := time.Now()
					if err := pipeline.Run(ctx); err != nil {
						b.Fatal(err)
					}
					duration := time.Since(start)
					
					b.ReportMetric(float64(recordCount)/duration.Seconds(), "records/sec")
				}
			})
		}
	}
}

// Benchmark vectorized transform operations
func BenchmarkVectorizedTransforms(b *testing.B) {
	logger := zap.NewNop()
	
	// Field mapping transform
	fieldMapper := func(ctx context.Context, record *models.Record) (*models.Record, error) {
		newData := pool.GetMap()
		mappings := map[string]string{
			"id":        "user_id",
			"name":      "user_name",
			"value":     "score",
			"timestamp": "created_at",
			"category":  "type",
			"status":    "state",
		}
		
		for k, v := range record.Data {
			if newKey, ok := mappings[k]; ok {
				newData[newKey] = v
			} else {
				newData[k] = v
			}
		}
		
		pool.PutMap(record.Data)
		record.Data = newData
		return record, nil
	}
	
	recordCount := 100000
	
	b.Run("Sequential_FieldMapping", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			source := &MockSource{recordCount: recordCount}
			dest := &MockDestination{}
			
			config := &PipelineConfig{
				BatchSize:   1000,
				WorkerCount: 1,
			}
			
			pipeline := NewSimplePipeline(source, dest, config, logger)
			pipeline.AddTransform(fieldMapper)
			
			ctx := context.Background()
			start := time.Now()
			if err := pipeline.Run(ctx); err != nil {
				b.Fatal(err)
			}
			duration := time.Since(start)
			
			b.ReportMetric(float64(recordCount)/duration.Seconds(), "records/sec")
		}
	})
	
	b.Run("Parallel_FieldMapping", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			source := &MockSource{recordCount: recordCount}
			dest := &MockDestination{}
			
			config := &ParallelPipelineConfig{
				BatchSize:       1000,
				WorkerCount:     runtime.NumCPU() * 2,
				UseParallel:     true,
				VectorBatchSize: 100,
				EnableAffinity:  true,
			}
			
			pipeline := NewParallelPipeline(source, dest, config, logger)
			pipeline.AddTransform(fieldMapper)
			
			ctx := context.Background()
			start := time.Now()
			if err := pipeline.Run(ctx); err != nil {
				b.Fatal(err)
			}
			duration := time.Since(start)
			
			b.ReportMetric(float64(recordCount)/duration.Seconds(), "records/sec")
		}
	})
}

// Benchmark different batch sizes
func BenchmarkBatchSizes(b *testing.B) {
	logger := zap.NewNop()
	recordCount := 50000
	batchSizes := []int{100, 500, 1000, 5000, 10000}
	
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				source := &MockSource{recordCount: recordCount}
				dest := &MockDestination{}
				
				config := &ParallelPipelineConfig{
					BatchSize:       batchSize,
					WorkerCount:     runtime.NumCPU() * 2,
					UseParallel:     true,
					VectorBatchSize: batchSize / 10,
					EnableAffinity:  true,
				}
				
				pipeline := NewParallelPipeline(source, dest, config, logger)
				pipeline.AddTransform(cpuIntensiveTransform)
				
				ctx := context.Background()
				start := time.Now()
				if err := pipeline.Run(ctx); err != nil {
					b.Fatal(err)
				}
				duration := time.Since(start)
				
				b.ReportMetric(float64(recordCount)/duration.Seconds(), "records/sec")
			}
		})
	}
}