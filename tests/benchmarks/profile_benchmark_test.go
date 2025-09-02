package benchmarks

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/internal/pipeline"
	"github.com/ajitpratap0/nebula/pkg/config"
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
	logger := logger.Get()

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
		if err != nil {
			b.Skipf("Skipping benchmark: %v", err)
		}

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
	logger := logger.Get()

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
				p.AddTransform(func(ctx context.Context, record *models.Record) (*models.Record, error) {
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
				p.AddTransform(func(ctx context.Context, record *models.Record) (*models.Record, error) {
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
				p.AddTransform(func(ctx context.Context, record *models.Record) (*models.Record, error) {
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
			if err != nil {
				t.Skipf("Skipping test: %v", err)
			}

			// Create pipeline with simulated issue
			p := createTestPipeline(t, testFile, outputFile, logger)
			if tt.simulateIssue != nil {
				tt.simulateIssue(t, p)
			}

			// Run pipeline
			runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()

			err = p.Run(runCtx)
			require.NoError(t, err)

			p.Stop()

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
	logger := logger.Get()

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
	if err != nil {
		t.Skipf("Skipping test: %v", err)
	}

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

func runBenchmarkPipeline(
	b testing.TB, ctx context.Context,
	profiler *profiling.PipelineProfiler,
	inputFile, outputFile string, logger *zap.Logger) {
	// Create source
	sourceConfig := config.NewBaseConfig("csv-source", "source")
	sourceConfig.Security.Credentials = map[string]string{
		"file_path": inputFile,
	}

	source, err := csvsrc.NewCSVSource(sourceConfig)
	require.NoError(b, err)

	// Wrap with profiling
	source = profiler.ProfileSource(source)

	err = source.Initialize(ctx, sourceConfig)
	require.NoError(b, err)
	defer source.Close(ctx)

	// Create destination
	destConfig := config.NewBaseConfig("csv-destination", "destination")
	destConfig.Security.Credentials = map[string]string{
		"file_path": outputFile,
	}
	dest, err := csvdest.NewCSVDestination(destConfig)
	require.NoError(b, err)

	// Wrap with profiling
	dest = profiler.ProfileDestination(dest)

	err = dest.Initialize(ctx, destConfig)
	require.NoError(b, err)
	defer dest.Close(ctx)

	// Create pipeline
	pipelineConfig := &pipeline.PipelineConfig{
		BatchSize:   1000,
		WorkerCount: 4,
	}

	p := pipeline.NewSimplePipeline(source, dest, pipelineConfig, logger)

	// Run pipeline
	err = p.Run(ctx)
	require.NoError(b, err)


	// Pipeline run is complete
}

func createTestPipeline(t *testing.T, inputFile, outputFile string, logger *zap.Logger) *pipeline.SimplePipeline {
	ctx := context.Background()

	// Create source
	sourceConfig := config.NewBaseConfig("test-source", "source")
	sourceConfig.Security.Credentials = map[string]string{
		"path": inputFile,
	}

	require.NoError(t, err)

	err = source.Initialize(ctx, sourceConfig)
	require.NoError(t, err)

	// Create destination
	destConfig := config.NewBaseConfig("test-destination", "destination")
	destConfig.Security.Credentials = map[string]string{
		"path": outputFile,
	}


	dest, err := csvdest.NewCSVDestination(destConfig)

	require.NoError(t, err)

	err = dest.Initialize(ctx, destConfig)
	require.NoError(t, err)

	// Create pipeline
	pipelineConfig := &pipeline.PipelineConfig{
		BatchSize:   100,
		WorkerCount: 2,
	}

	p := pipeline.NewSimplePipeline(source, dest, pipelineConfig, logger)

	return p
}

// createTestCSV creates a CSV file with test data for benchmarking
func createTestCSV(tb testing.TB, recordCount, columnCount int) string {
	tempFile := fmt.Sprintf("/tmp/test_%d_%d.csv", recordCount, columnCount)
	file, err := os.Create(tempFile)
	if err != nil {
		tb.Fatal(err)
	}
	defer file.Close() // Ignore close error

	writer := csv.NewWriter(file)
	defer writer.Flush() // Ignore flush error

	// Write header
	headers := make([]string, columnCount)
	for i := 0; i < columnCount; i++ {
		headers[i] = fmt.Sprintf("col_%d", i)
	}
	_ = writer.Write(headers) // Ignore write error

	// Write records
	for i := 0; i < recordCount; i++ {
		record := make([]string, columnCount)
		for j := 0; j < columnCount; j++ {
			record[j] = fmt.Sprintf("value_%d_%d", i, j)
		}
		_ = writer.Write(record) // Ignore write error
	}

	return tempFile
}

// removeTestFile removes a test file, ignoring errors
func removeTestFile(filename string) {
	_ = os.Remove(filename) // Best effort cleanup
}
