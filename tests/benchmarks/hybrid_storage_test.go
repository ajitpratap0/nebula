package benchmarks

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/sources"
	"github.com/ajitpratap0/nebula/pkg/pipeline"
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// BenchmarkHybridStorageComparison compares row vs columnar vs hybrid storage
func BenchmarkHybridStorageComparison(b *testing.B) {
	// Create test data
	testFile := "/tmp/hybrid_storage_test.csv"
	recordCount := 100000
	createBenchmarkTestData(b, testFile, recordCount)
	defer os.Remove(testFile)

	modes := []struct {
		name string
		mode string
	}{
		{"Row", "row"},
		{"Columnar", "columnar"},
		{"Hybrid", "hybrid"},
	}

	for _, mode := range modes {
		b.Run(mode.name, func(b *testing.B) {
			cfg := config.NewBaseConfig("test", "csv_source")
			cfg.Advanced.StorageMode = mode.mode
			cfg.Performance.BatchSize = 10000
			cfg.Performance.StreamingMode = false

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)

				source, err := sources.NewCSVHybridSource(cfg, testFile)
				if err != nil {
					b.Fatal(err)
				}

				ctx := context.Background()
				if err := source.Connect(ctx); err != nil {
					b.Fatal(err)
				}

				recordCh, errCh := source.Read(ctx)
				
				count := 0
				start := time.Now()
				
				for {
					select {
					case record, ok := <-recordCh:
						if !ok {
							goto done
						}
						count++
						record.Release()
					case err := <-errCh:
						if err != nil {
							b.Fatal(err)
						}
					}
				}

			done:
				duration := time.Since(start)
				
				runtime.GC()
				runtime.ReadMemStats(&m2)
				
				memUsed := m2.TotalAlloc - m1.TotalAlloc
				bytesPerRecord := float64(memUsed) / float64(count)
				recordsPerSec := float64(count) / duration.Seconds()

				b.ReportMetric(bytesPerRecord, "bytes/record")
				b.ReportMetric(recordsPerSec, "records/sec")
				b.ReportMetric(float64(memUsed), "total_bytes")

				source.Close(ctx)
			}
		})
	}
}

// BenchmarkColumnarMemoryEfficiency focuses on columnar memory optimization
func BenchmarkColumnarMemoryEfficiency(b *testing.B) {
	testFile := "/tmp/columnar_memory_test.csv"
	recordCounts := []int{10000, 50000, 100000, 200000}

	for _, count := range recordCounts {
		b.Run(fmt.Sprintf("Records_%d", count), func(b *testing.B) {
			createBenchmarkTestData(b, testFile, count)
			defer os.Remove(testFile)

			cfg := config.NewBaseConfig("test", "csv_source")
			cfg.Advanced.StorageMode = "columnar"
			cfg.Performance.BatchSize = count / 10

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)

				source, err := sources.NewCSVHybridSource(cfg, testFile)
				if err != nil {
					b.Fatal(err)
				}

				ctx := context.Background()
				if err := source.Connect(ctx); err != nil {
					b.Fatal(err)
				}

				recordCh, errCh := source.Read(ctx)
				
				processedCount := 0
				start := time.Now()
				
				for {
					select {
					case record, ok := <-recordCh:
						if !ok {
							goto done
						}
						processedCount++
						record.Release()
					case err := <-errCh:
						if err != nil {
							b.Fatal(err)
						}
					}
				}

			done:
				duration := time.Since(start)
				
				runtime.GC()
				runtime.ReadMemStats(&m2)
				
				memUsed := m2.TotalAlloc - m1.TotalAlloc
				bytesPerRecord := float64(memUsed) / float64(processedCount)
				recordsPerSec := float64(processedCount) / duration.Seconds()

				b.ReportMetric(bytesPerRecord, "bytes/record")
				b.ReportMetric(recordsPerSec, "records/sec")

				source.Close(ctx)
			}
		})
	}
}

// BenchmarkStorageModeAutoSelection tests hybrid mode intelligence
func BenchmarkStorageModeAutoSelection(b *testing.B) {
	testFile := "/tmp/auto_selection_test.csv"
	
	scenarios := []struct {
		name        string
		recordCount int
		expectMode  string
	}{
		{"Small_Dataset", 1000, "row"},      // Should use row-based
		{"Large_Dataset", 100000, "columnar"}, // Should use columnar
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			createBenchmarkTestData(b, testFile, scenario.recordCount)
			defer os.Remove(testFile)

			cfg := config.NewBaseConfig("test", "csv_source")
			cfg.Advanced.StorageMode = "hybrid"
			cfg.Performance.BatchSize = 10000

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				source, err := sources.NewCSVHybridSource(cfg, testFile)
				if err != nil {
					b.Fatal(err)
				}

				adapter := source.GetStorageAdapter()
				
				ctx := context.Background()
				source.Connect(ctx)
				
				recordCh, errCh := source.Read(ctx)
				
				count := 0
				for {
					select {
					case record, ok := <-recordCh:
						if !ok {
							goto done
						}
						count++
						record.Release()
					case err := <-errCh:
						if err != nil {
							b.Fatal(err)
						}
					}
				}

			done:
				memPerRecord := adapter.GetMemoryPerRecord()
				
				b.ReportMetric(memPerRecord, "bytes/record")
				b.ReportMetric(float64(count), "total_records")

				source.Close(ctx)
			}
		})
	}
}

// createBenchmarkTestData creates realistic CSV test data
func createBenchmarkTestData(b *testing.B, filename string, records int) {
	file, err := os.Create(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()

	// Write headers
	headers := "order_id,customer_id,product_sku,quantity,price,status,category,region,timestamp\n"
	file.WriteString(headers)

	// Generate realistic data patterns
	statuses := []string{"pending", "processing", "shipped", "delivered", "cancelled"}
	categories := []string{"Electronics", "Clothing", "Home", "Books", "Food", "Sports"}
	regions := []string{"North", "South", "East", "West", "Central"}

	for i := 0; i < records; i++ {
		line := fmt.Sprintf("%d,%d,SKU-%05d,%d,%.2f,%s,%s,%s,%s\n",
			1000000+i,                           // order_id
			10000+i/5,                          // customer_id (creates repeats)
			i%5000,                             // product_sku (high cardinality)
			1+i%10,                             // quantity (low cardinality integers)
			10.99+float64(i%100),               // price (floats)
			statuses[i%len(statuses)],          // status (categorical)
			categories[i%len(categories)],      // category (categorical)
			regions[i%len(regions)],            // region (categorical)
			time.Now().Format(time.RFC3339),    // timestamp
		)
		file.WriteString(line)
	}
}

// BenchmarkDirectStorageAdapter tests storage adapter performance directly
func BenchmarkDirectStorageAdapter(b *testing.B) {
	modes := []pipeline.StorageMode{
		pipeline.StorageModeRow,
		pipeline.StorageModeColumnar,
		pipeline.StorageModeHybrid,
	}

	for _, mode := range modes {
		b.Run(string(mode), func(b *testing.B) {
			cfg := config.NewBaseConfig("test", "benchmark")
			cfg.Performance.BatchSize = 10000
			
			adapter := pipeline.NewStorageAdapter(mode, cfg)
			defer adapter.Close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)

				// Add 50K records
				for j := 0; j < 50000; j++ {
					record := pool.GetRecord()
					record.SetData("id", fmt.Sprintf("%d", j))
					record.SetData("value", fmt.Sprintf("value_%d", j))
					record.SetData("category", fmt.Sprintf("cat_%d", j%10))
					record.SetData("amount", j*1.5)
					
					adapter.AddRecord(record)
					record.Release()
				}

				adapter.Flush()
				
				runtime.GC()
				runtime.ReadMemStats(&m2)
				
				memUsed := m2.TotalAlloc - m1.TotalAlloc
				bytesPerRecord := float64(memUsed) / 50000.0
				
				b.ReportMetric(bytesPerRecord, "bytes/record")
				b.ReportMetric(float64(adapter.GetRecordCount()), "total_records")
			}
		})
	}
}