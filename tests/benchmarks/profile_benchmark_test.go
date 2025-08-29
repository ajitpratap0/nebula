package benchmarks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/internal/pipeline"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	csvdest "github.com/ajitpratap0/nebula/pkg/connector/destinations/csv"
	csvsrc "github.com/ajitpratap0/nebula/pkg/connector/sources/csv"
	"github.com/ajitpratap0/nebula/pkg/logger"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/performance/profiling"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// BenchmarkPipelineWithProfiling benchmarks pipeline with profiling enabled
func BenchmarkPipelineWithProfiling(b *testing.B) {
	ctx := context.Background()
	logger := logger.NewLogger("benchmark")

	// Create test data
	testFile := createTestCSV(b, 100000, 10) // 100K records, 10 columns
	defer removeTestFile(testFile)

	outputFile := fmt.Sprintf("benchmark_output_%d.csv", time.Now().Unix())
	defer removeTestFile(outputFile)

	// Profile configuration
	profileConfig := &profiling.ProfileConfig{
		Types:                   []profiling.ProfileType{profiling.CPUProfile, profiling.MemoryProfile},
		OutputDir:               "./benchmark_profiles",
		CPUDuration:             30 * time.Second,
		CollectRuntimeMetrics:   true,
		MetricsSamplingInterval: 100 * time.Millisecond,
	}

	// Create pipeline profiler
	pipelineProfiler := profiling.NewPipelineProfiler(profileConfig)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Start profiling
		err := pipelineProfiler.Start(ctx)
		require.NoError(b, err)

		// Run pipeline
		runBenchmarkPipeline(b, ctx, pipelineProfiler, testFile, outputFile, logger)

		// Stop profiling and get results
		result, err := pipelineProfiler.Stop()
		require.NoError(b, err)

		// Report metrics
		b.ReportMetric(result.Throughput, "records/sec")
		b.ReportMetric(result.ByteThroughput/(1024*1024), "MB/sec")
		b.ReportMetric(float64(result.RuntimeMetrics.AllocBytes)/(1024*1024), "MB_allocated")
		b.ReportMetric(float64(result.RuntimeMetrics.NumGC), "gc_runs")

		// Log bottlenecks if any
		if result.BottleneckAnalysis != nil && len(result.BottleneckAnalysis.Bottlenecks) > 0 {
			b.Logf("Bottlenecks detected:")
			for _, bottleneck := range result.BottleneckAnalysis.Bottlenecks {
				b.Logf("  [%s] %s (Impact: %.1f%%)", bottleneck.Severity, bottleneck.Description, bottleneck.Impact)
			}
		}
	}
}

// TestProfileBottleneckDetection tests bottleneck detection
func TestProfileBottleneckDetection(t *testing.T) {
	ctx := context.Background()
	logger := logger.NewLogger("test")

	tests := []struct {
		name               string
		recordCount        int
		expectedBottleneck profiling.BottleneckType
		simulateIssue      func(*testing.T, *pipeline.SimplePipeline)
	}{
		{
			name:               "Memory Leak Detection",
			recordCount:        50000,
			expectedBottleneck: profiling.MemoryBottleneck,
			simulateIssue: func(t *testing.T, p *pipeline.SimplePipeline) {
				// Simulate memory leak by holding references
				var leakedRecords []*models.Record
				p.AddTransform(func(record *models.Record) (*models.Record, error) {
					// Leak memory by keeping references
					leakedRecords = append(leakedRecords, record)
					return record, nil
				})
			},
		},
		{
			name:               "CPU Bottleneck Detection",
			recordCount:        10000,
			expectedBottleneck: profiling.CPUBottleneck,
			simulateIssue: func(t *testing.T, p *pipeline.SimplePipeline) {
				// Simulate CPU-intensive operation
				p.AddTransform(func(record *models.Record) (*models.Record, error) {
					// Expensive computation
					sum := 0
					for i := 0; i < 10000; i++ {
						sum += i * i
					}
					record.Data["computed"] = sum
					return record, nil
				})
			},
		},
		{
			name:               "GC Pressure Detection",
			recordCount:        50000,
			expectedBottleneck: profiling.GCBottleneck,
			simulateIssue: func(t *testing.T, p *pipeline.SimplePipeline) {
				// Create lots of garbage
				p.AddTransform(func(record *models.Record) (*models.Record, error) {
					// Create temporary allocations
					for i := 0; i < 100; i++ {
						temp := make([]byte, 1024)
						_ = temp
					}
					return record, nil
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test data
			testFile := createTestCSV(t, tt.recordCount, 5)
			defer removeTestFile(testFile)

			outputFile := fmt.Sprintf("test_output_%s.csv", tt.name)
			defer removeTestFile(outputFile)

			// Profile configuration
			profileConfig := &profiling.ProfileConfig{
				Types:                   []profiling.ProfileType{profiling.AllProfiles},
				OutputDir:               "./test_profiles",
				CPUDuration:             10 * time.Second,
				CollectRuntimeMetrics:   true,
				MetricsSamplingInterval: 50 * time.Millisecond,
			}

			// Create pipeline profiler
			pipelineProfiler := profiling.NewPipelineProfiler(profileConfig)

			// Start profiling
			err := pipelineProfiler.Start(ctx)
			require.NoError(t, err)

			// Create pipeline with simulated issue
			p := createTestPipeline(t, testFile, outputFile, logger)
			if tt.simulateIssue != nil {
				tt.simulateIssue(t, p)
			}

			// Run pipeline
			runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()

			err = p.Start(runCtx)
			require.NoError(t, err)

			<-runCtx.Done()
			p.Stop(context.Background())

			// Stop profiling and analyze
			result, err := pipelineProfiler.Stop()
			require.NoError(t, err)

			// Check if expected bottleneck was detected
			found := false
			if result.BottleneckAnalysis != nil {
				for _, bottleneck := range result.BottleneckAnalysis.Bottlenecks {
					if bottleneck.Type == tt.expectedBottleneck {
						found = true
						t.Logf("Detected %s bottleneck: %s", bottleneck.Type, bottleneck.Description)
						break
					}
				}
			}

			require.True(t, found, "Expected %s bottleneck not detected", tt.expectedBottleneck)
		})
	}
}

// TestProfileAccuracy tests profiling accuracy
func TestProfileAccuracy(t *testing.T) {
	ctx := context.Background()
	logger := logger.NewLogger("test")

	// Create test data with known size
	recordCount := 10000
	testFile := createTestCSV(t, recordCount, 5)
	defer removeTestFile(testFile)

	outputFile := "accuracy_test_output.csv"
	defer removeTestFile(outputFile)

	// Profile configuration
	profileConfig := profiling.DefaultProfileConfig()
	profileConfig.OutputDir = "./accuracy_test_profiles"

	// Create pipeline profiler
	pipelineProfiler := profiling.NewPipelineProfiler(profileConfig)

	// Start profiling
	err := pipelineProfiler.Start(ctx)
	require.NoError(t, err)

	// Run pipeline
	runBenchmarkPipeline(t, ctx, pipelineProfiler, testFile, outputFile, logger)

	// Stop profiling
	result, err := pipelineProfiler.Stop()
	require.NoError(t, err)

	// Verify metrics accuracy
	require.NotNil(t, result)

	// Check record count accuracy (allowing 1% margin)
	expectedRecords := float64(recordCount)
	actualRecords := result.Throughput * result.Duration.Seconds()
	marginOfError := expectedRecords * 0.01

	require.InDelta(t, expectedRecords, actualRecords, marginOfError,
		"Record count mismatch: expected ~%d, got %.0f", recordCount, actualRecords)

	// Verify runtime metrics
	require.NotNil(t, result.RuntimeMetrics)
	require.Greater(t, result.RuntimeMetrics.AllocBytes, uint64(0), "No memory allocated")
	require.Greater(t, result.RuntimeMetrics.NumGoroutines, 0, "No goroutines detected")

	// Verify stage metrics if available
	if len(result.StageMetrics) > 0 {
		totalIn := int64(0)
		totalOut := int64(0)
		for _, stage := range result.StageMetrics {
			totalIn += stage.RecordsIn
			totalOut += stage.RecordsOut
		}

		t.Logf("Stage metrics: %d records in, %d records out", totalIn, totalOut)
	}
}

// Helper functions

func runBenchmarkPipeline(b testing.TB, ctx context.Context, profiler *profiling.PipelineProfiler, inputFile, outputFile string, logger *zap.Logger) {
	// Create source
	sourceConfig := &core.Config{
		Name: "csv-source",
		Type: core.ConnectorTypeSource,
		Properties: map[string]interface{}{
			"file_path": inputFile,
		},
	}

	source, err := csvsrc.NewCSVSource("benchmark-source", sourceConfig)
	require.NoError(b, err)

	// Wrap with profiling
	source = profiler.ProfileSource(source)

	err = source.Initialize(ctx, sourceConfig)
	require.NoError(b, err)
	defer source.Close(ctx)

	// Create destination
	destConfig := &core.Config{
		Name: "csv-destination",
		Type: core.ConnectorTypeDestination,
		Properties: map[string]interface{}{
			"file_path": outputFile,
		},
	}

	dest, err := csvdest.NewCSVDestination("benchmark-dest", destConfig)
	require.NoError(b, err)

	// Wrap with profiling
	dest = profiler.ProfileDestination(dest)

	err = dest.Initialize(ctx, destConfig)
	require.NoError(b, err)
	defer dest.Close(ctx)

	// Create pipeline
	pipelineConfig := &pipeline.Config{
		Name:        "benchmark-pipeline",
		BufferSize:  10000,
		WorkerCount: 4,
		BatchSize:   1000,
	}

	p, err := pipeline.NewSimplePipeline(pipelineConfig, source, dest, logger)
	require.NoError(b, err)

	// Run pipeline
	err = p.Start(ctx)
	require.NoError(b, err)

	// Wait for completion
	p.Wait()
}

func createTestPipeline(t *testing.T, inputFile, outputFile string, logger *zap.Logger) *pipeline.SimplePipeline {
	ctx := context.Background()

	// Create source
	sourceConfig := &core.Config{
		Name: "test-source",
		Type: core.ConnectorTypeSource,
		Properties: map[string]interface{}{
			"file_path": inputFile,
		},
	}

	source, err := csvsrc.NewCSVSource("test-source", sourceConfig)
	require.NoError(t, err)

	err = source.Initialize(ctx, sourceConfig)
	require.NoError(t, err)

	// Create destination
	destConfig := &core.Config{
		Name: "test-destination",
		Type: core.ConnectorTypeDestination,
		Properties: map[string]interface{}{
			"file_path": outputFile,
		},
	}

	dest, err := csvdest.NewCSVDestination("test-dest", destConfig)
	require.NoError(t, err)

	err = dest.Initialize(ctx, destConfig)
	require.NoError(t, err)

	// Create pipeline
	pipelineConfig := &pipeline.Config{
		Name:        "test-pipeline",
		BufferSize:  1000,
		WorkerCount: 2,
		BatchSize:   100,
	}

	p, err := pipeline.NewSimplePipeline(pipelineConfig, source, dest, logger)
	require.NoError(t, err)

	return p
}
