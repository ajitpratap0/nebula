package benchmarks

import (
	"context"
	"os"
	"testing"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/sources/iceberg"
)

// BenchmarkIcebergSource_Initialization benchmarks the initialization process
func BenchmarkIcebergSource_Initialization(b *testing.B) {
	if os.Getenv("BENCHMARK_TESTS") != "true" {
		b.Skip("Skipping benchmark tests. Set BENCHMARK_TESTS=true to run")
	}

	cfg := getTestConfig()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source, err := iceberg.NewIcebergSource(cfg)
		if err != nil {
			b.Fatalf("Failed to create source: %v", err)
		}

		err = source.Initialize(ctx, cfg)
		if err != nil {
			b.Skipf("Failed to initialize source: %v", err)
		}

		source.Close(ctx)
	}
}

// BenchmarkIcebergSource_SchemaDiscovery benchmarks schema discovery
func BenchmarkIcebergSource_SchemaDiscovery(b *testing.B) {
	if os.Getenv("BENCHMARK_TESTS") != "true" {
		b.Skip("Skipping benchmark tests. Set BENCHMARK_TESTS=true to run")
	}

	cfg := getTestConfig()
	ctx := context.Background()

	source, err := iceberg.NewIcebergSource(cfg)
	if err != nil {
		b.Fatalf("Failed to create source: %v", err)
	}

	err = source.Initialize(ctx, cfg)
	if err != nil {
		b.Skipf("Failed to initialize source: %v", err)
	}
	defer source.Close(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := source.Discover(ctx)
		if err != nil {
			b.Fatalf("Failed to discover schema: %v", err)
		}
	}
}

// BenchmarkIcebergSource_ReadRecords benchmarks record reading performance
func BenchmarkIcebergSource_ReadRecords(b *testing.B) {
	if os.Getenv("BENCHMARK_TESTS") != "true" {
		b.Skip("Skipping benchmark tests. Set BENCHMARK_TESTS=true to run")
	}

	cfg := getTestConfig()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source, err := iceberg.NewIcebergSource(cfg)
		if err != nil {
			b.Fatalf("Failed to create source: %v", err)
		}

		err = source.Initialize(ctx, cfg)
		if err != nil {
			b.Skipf("Failed to initialize source: %v", err)
		}

		stream, err := source.Read(ctx)
		if err != nil {
			b.Fatalf("Failed to start reading: %v", err)
		}

		// Read a fixed number of records
		recordCount := 0
		maxRecords := 1000
		for recordCount < maxRecords {
			select {
			case record, ok := <-stream.Records:
				if !ok {
					goto cleanup
				}
				if record != nil {
					recordCount++
					record.Release()
				}
			case err := <-stream.Errors:
				if err != nil {
					b.Logf("Error during read: %v", err)
				}
			}
		}

	cleanup:
		source.Close(ctx)
	}

	b.ReportMetric(float64(1000), "records/op")
}

// BenchmarkIcebergSource_ReadBatches benchmarks batch reading performance
func BenchmarkIcebergSource_ReadBatches(b *testing.B) {
	if os.Getenv("BENCHMARK_TESTS") != "true" {
		b.Skip("Skipping benchmark tests. Set BENCHMARK_TESTS=true to run")
	}

	batchSizes := []int{100, 1000, 5000, 10000}

	for _, batchSize := range batchSizes {
		b.Run(formatBatchSize(batchSize), func(b *testing.B) {
			cfg := getTestConfig()
			cfg.Performance.BatchSize = batchSize
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				source, err := iceberg.NewIcebergSource(cfg)
				if err != nil {
					b.Fatalf("Failed to create source: %v", err)
				}

				err = source.Initialize(ctx, cfg)
				if err != nil {
					b.Skipf("Failed to initialize source: %v", err)
				}

				stream, err := source.ReadBatch(ctx, batchSize)
				if err != nil {
					b.Fatalf("Failed to start batch reading: %v", err)
				}

				// Read a fixed number of batches
				batchCount := 0
				maxBatches := 10
				totalRecords := 0
				for batchCount < maxBatches {
					select {
					case batch, ok := <-stream.Batches:
						if !ok {
							goto cleanup
						}
						if batch != nil {
							batchCount++
							totalRecords += len(batch)
							for _, record := range batch {
								record.Release()
							}
						}
					case err := <-stream.Errors:
						if err != nil {
							b.Logf("Error during read: %v", err)
						}
					}
				}

			cleanup:
				source.Close(ctx)
				b.ReportMetric(float64(totalRecords), "records/op")
			}
		})
	}
}

// BenchmarkIcebergSource_StateManagement benchmarks state management operations
func BenchmarkIcebergSource_StateManagement(b *testing.B) {
	if os.Getenv("BENCHMARK_TESTS") != "true" {
		b.Skip("Skipping benchmark tests. Set BENCHMARK_TESTS=true to run")
	}

	cfg := getTestConfig()
	ctx := context.Background()

	source, err := iceberg.NewIcebergSource(cfg)
	if err != nil {
		b.Fatalf("Failed to create source: %v", err)
	}

	err = source.Initialize(ctx, cfg)
	if err != nil {
		b.Skipf("Failed to initialize source: %v", err)
	}
	defer source.Close(ctx)

	state := source.GetState()

	b.ResetTimer()
	b.Run("GetState", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = source.GetState()
		}
	})

	b.Run("SetState", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = source.SetState(state)
		}
	})

	b.Run("GetPosition", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = source.GetPosition()
		}
	})
}

// BenchmarkIcebergSource_Metrics benchmarks metrics retrieval
func BenchmarkIcebergSource_Metrics(b *testing.B) {
	if os.Getenv("BENCHMARK_TESTS") != "true" {
		b.Skip("Skipping benchmark tests. Set BENCHMARK_TESTS=true to run")
	}

	cfg := getTestConfig()
	ctx := context.Background()

	source, err := iceberg.NewIcebergSource(cfg)
	if err != nil {
		b.Fatalf("Failed to create source: %v", err)
	}

	err = source.Initialize(ctx, cfg)
	if err != nil {
		b.Skipf("Failed to initialize source: %v", err)
	}
	defer source.Close(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = source.Metrics()
	}
}

// BenchmarkIcebergSource_HealthCheck benchmarks health check operations
func BenchmarkIcebergSource_HealthCheck(b *testing.B) {
	if os.Getenv("BENCHMARK_TESTS") != "true" {
		b.Skip("Skipping benchmark tests. Set BENCHMARK_TESTS=true to run")
	}

	cfg := getTestConfig()
	ctx := context.Background()

	source, err := iceberg.NewIcebergSource(cfg)
	if err != nil {
		b.Fatalf("Failed to create source: %v", err)
	}

	err = source.Initialize(ctx, cfg)
	if err != nil {
		b.Skipf("Failed to initialize source: %v", err)
	}
	defer source.Close(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = source.Health(ctx)
	}
}

// BenchmarkIcebergSource_ConcurrentReads benchmarks concurrent read operations
func BenchmarkIcebergSource_ConcurrentReads(b *testing.B) {
	if os.Getenv("BENCHMARK_TESTS") != "true" {
		b.Skip("Skipping benchmark tests. Set BENCHMARK_TESTS=true to run")
	}

	cfg := getTestConfig()
	ctx := context.Background()

	source, err := iceberg.NewIcebergSource(cfg)
	if err != nil {
		b.Fatalf("Failed to create source: %v", err)
	}

	err = source.Initialize(ctx, cfg)
	if err != nil {
		b.Skipf("Failed to initialize source: %v", err)
	}
	defer source.Close(ctx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			stream, err := source.ReadBatch(ctx, 1000)
			if err != nil {
				b.Logf("Failed to start batch reading: %v", err)
				continue
			}

			// Read one batch
			select {
			case batch, ok := <-stream.Batches:
				if ok && batch != nil {
					for _, record := range batch {
						record.Release()
					}
				}
			case <-stream.Errors:
			}
		}
	})
}

// BenchmarkIcebergSource_MemoryAllocation benchmarks memory allocation patterns
func BenchmarkIcebergSource_MemoryAllocation(b *testing.B) {
	if os.Getenv("BENCHMARK_TESTS") != "true" {
		b.Skip("Skipping benchmark tests. Set BENCHMARK_TESTS=true to run")
	}

	cfg := getTestConfig()
	ctx := context.Background()

	source, err := iceberg.NewIcebergSource(cfg)
	if err != nil {
		b.Fatalf("Failed to create source: %v", err)
	}

	err = source.Initialize(ctx, cfg)
	if err != nil {
		b.Skipf("Failed to initialize source: %v", err)
	}
	defer source.Close(ctx)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		stream, err := source.ReadBatch(ctx, 100)
		if err != nil {
			b.Fatalf("Failed to start batch reading: %v", err)
		}

		// Read one batch
		select {
		case batch, ok := <-stream.Batches:
			if ok && batch != nil {
				for _, record := range batch {
					record.Release()
				}
			}
		case <-stream.Errors:
		}
	}
}

// Helper function to get test configuration
func getTestConfig() *config.BaseConfig {
	return &config.BaseConfig{
		Name: "bench-iceberg-source",
		Type: "iceberg",
		Security: config.SecurityConfig{
			Credentials: map[string]string{
				"catalog_type":              "nessie",
				"catalog_uri":               getEnv("NESSIE_URI", "http://localhost:19120/api/v1"),
				"catalog_name":              getEnv("CATALOG_NAME", "test_catalog"),
				"warehouse":                 getEnv("WAREHOUSE", "s3://warehouse"),
				"database":                  getEnv("DATABASE", "test_db"),
				"table":                     getEnv("TABLE", "test_table"),
				"branch":                    "main",
				"prop_s3.region":            "us-west-2",
				"prop_s3.endpoint":          getEnv("S3_ENDPOINT", "http://localhost:9000"),
				"prop_s3.access-key-id":     getEnv("S3_ACCESS_KEY", "minioadmin"),
				"prop_s3.secret-access-key": getEnv("S3_SECRET_KEY", "minioadmin"),
			},
		},
		Performance: config.PerformanceConfig{
			BatchSize:  1000,
			BufferSize: 100,
		},
	}
}

// Helper function to get environment variables with defaults
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// Helper function to format batch size for benchmark names
func formatBatchSize(size int) string {
	if size >= 1000 {
		return formatInt(size/1000) + "k"
	}
	return formatInt(size)
}

// Helper function to format integers
func formatInt(n int) string {
	return string(rune('0' + n))
}
