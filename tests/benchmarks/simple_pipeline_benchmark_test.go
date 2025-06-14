package benchmarks

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/internal/pipeline"
	"github.com/ajitpratap0/nebula/pkg/config"
	csvdest "github.com/ajitpratap0/nebula/pkg/connector/destinations/csv"
	csvsrc "github.com/ajitpratap0/nebula/pkg/connector/sources/csv"
	"github.com/ajitpratap0/nebula/pkg/logger"
	"github.com/stretchr/testify/require"
)

// BenchmarkSimplePipeline benchmarks a simple CSV to CSV pipeline
func BenchmarkSimplePipeline(b *testing.B) {
	// Create test CSV file
	testFile := "/tmp/benchmark_test.csv"
	err := createBenchmarkCSV(testFile, 100000) // 100K records
	require.NoError(b, err)
	defer os.Remove(testFile)

	// Create output file
	outputFile := "/tmp/benchmark_output.csv"
	defer os.Remove(outputFile)

	// Initialize logger
	log := logger.Get()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Clean up output file between runs
		os.Remove(outputFile)

		// Create source config
		sourceConfig := config.NewBaseConfig("csv-source", "source")
		sourceConfig.Version = "1.0.0"
		sourceConfig.Security.Credentials = map[string]string{
			"file_path": testFile,
		}
		sourceConfig.Performance.BatchSize = 5000
		sourceConfig.Performance.BufferSize = 10000

		// Create destination config
		destConfig := config.NewBaseConfig("csv-dest", "destination")
		destConfig.Version = "1.0.0"
		destConfig.Security.Credentials = map[string]string{
			"file_path": outputFile,
		}
		destConfig.Performance.BatchSize = 5000

		// Create source
		source, err := csvsrc.NewCSVSource(sourceConfig)
		require.NoError(b, err)

		err = source.Initialize(ctx, sourceConfig)
		require.NoError(b, err)

		// Create destination
		dest, err := csvdest.NewCSVDestination(destConfig)
		require.NoError(b, err)

		err = dest.Initialize(ctx, destConfig)
		require.NoError(b, err)

		// Create pipeline config
		pipelineConfig := &pipeline.PipelineConfig{
			BatchSize:   5000,
			WorkerCount: 8,
		}

		// Create and run pipeline
		p := pipeline.NewSimplePipeline(source, dest, pipelineConfig, log)

		start := time.Now()
		err = p.Run(ctx)
		duration := time.Since(start)
		require.NoError(b, err)

		// Report metrics
		metrics := p.Metrics()
		processedRecords := metrics["records_processed"].(int64)
		recordsPerSec := float64(processedRecords) / duration.Seconds()
		b.ReportMetric(recordsPerSec, "records/sec")
		b.ReportMetric(float64(processedRecords), "total_records")
		b.ReportMetric(duration.Seconds(), "duration_sec")

		// Clean up
		p.Stop()
		source.Close(ctx)
		dest.Close(ctx)
	}
}

// createBenchmarkCSV creates a test CSV file with the specified number of records
func createBenchmarkCSV(filename string, records int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header
	fmt.Fprintln(file, "id,name,email,age,department,salary,created_at")

	// Write records
	departments := []string{"Engineering", "Sales", "Marketing", "HR", "Finance"}
	for i := 0; i < records; i++ {
		fmt.Fprintf(file, "%d,User_%d,user%d@example.com,%d,%s,%.2f,%s\n",
			i,
			i,
			i,
			20+(i%40),
			departments[i%len(departments)],
			50000+float64(i%50000),
			time.Now().Format(time.RFC3339))
	}

	return nil
}