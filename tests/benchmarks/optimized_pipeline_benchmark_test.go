package benchmarks

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/pkg/pool"
)

// BenchmarkOptimizedPipelinePerformance tests the performance with all pooling optimizations
func BenchmarkOptimizedPipelinePerformance(b *testing.B) {
	recordCounts := []int{10000, 100000, 500000}

	for _, recordCount := range recordCounts {
		b.Run(fmt.Sprintf("Records_%d", recordCount), func(b *testing.B) {
			// Create test data
			testFile := fmt.Sprintf("/tmp/optimized_bench_%d.csv", recordCount)
			createTestData(b, testFile, recordCount)
			defer os.Remove(testFile)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				start := time.Now()
				processed := processRecordsOptimized(b, testFile)
				duration := time.Since(start)

				recordsPerSec := float64(processed) / duration.Seconds()
				b.ReportMetric(recordsPerSec, "records/sec")
				b.ReportMetric(float64(processed), "total_records")
				b.ReportMetric(duration.Seconds()*1000, "duration_ms")

				if processed != recordCount {
					b.Fatalf("Expected %d records, got %d", recordCount, processed)
				}

				b.Logf("Processed %d records in %v (%d records/sec)", processed, duration, int(recordsPerSec))
			}
		})
	}
}

func processRecordsOptimized(b *testing.B, filename string) int {
	file, err := os.Open(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Skip header
	_, err = reader.Read()
	if err != nil {
		b.Fatal(err)
	}

	// var processedCount int64 // Not needed
	workers := 8
	batchSize := 5000

	// Use pooled channels
	recordChan := pool.GetBatchChannel()
	defer pool.PutBatchChannel(recordChan)

	resultChan := pool.GetErrorChannel() // Reuse error channel for results
	defer pool.PutErrorChannel(resultChan)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			count := 0
			for batch := range recordChan {
				// Process batch
				for _, record := range batch {
					// Record is already from pool, just use it
					// Simulate some processing
					_ = len(record.Data)

					// Release record back to pool
					record.Release()

					count++
				}
				// Return batch slice to pool
				pool.PutBatchSlice(batch)
			}
			// Send count safely
			select {
			case resultChan <- fmt.Errorf("%d", count):
			default:
				// Channel closed, ignore
			}
		}()
	}

	// Read and batch records
	go func() {
		// Use pooled batch slice
		batch := pool.GetBatchSlice(batchSize)

		for {
			row, err := reader.Read()
			if err != nil {
				if len(batch) > 0 {
					recordChan <- batch
					// Get new batch from pool
					batch = pool.GetBatchSlice(batchSize)
				}
				break
			}

			// Create record using pool
			record := pool.GetRecord()

			// Use pooled map for data
			data := record.Data // Already has pooled map
			for j, value := range row {
				data[fmt.Sprintf("col_%d", j)] = value
			}

			batch = append(batch, record)

			if len(batch) >= batchSize {
				recordChan <- batch
				// Get new batch from pool
				batch = pool.GetBatchSlice(batchSize)
			}
		}

		// Clean up final batch if not sent
		if len(batch) > 0 {
			pool.PutBatchSlice(batch)
		}

		close(recordChan)
	}()

	// Wait for completion
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	totalProcessed := 0
	for err := range resultChan {
		// Extract count from error message (hacky but works)
		var count int
		fmt.Sscanf(err.Error(), "%d", &count)
		totalProcessed += count
	}

	return totalProcessed
}

// BenchmarkMemoryComparison compares memory usage between optimized and non-optimized
func BenchmarkMemoryComparison(b *testing.B) {
	const recordCount = 10000

	b.Run("NonOptimized", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Simulate non-optimized allocations
			for j := 0; j < recordCount; j++ {
				// Allocate like the original code
				data := make(map[string]interface{})
				for k := 0; k < 5; k++ {
					data[fmt.Sprintf("col_%d", k)] = "value"
				}
				record := &pool.Record{
					ID:   fmt.Sprintf("record_%d", j),
					Data: data,
				}
				_ = record
			}
		}
		b.ReportMetric(float64(recordCount), "records")
	})

	b.Run("Optimized", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Use pooling for all allocations
			batch := pool.GetBatchSlice(1000)
			defer pool.PutBatchSlice(batch)

			for j := 0; j < recordCount; j++ {
				record := pool.GetRecord()
				// Data map is already allocated in record
				for k := 0; k < 5; k++ {
					record.Data[fmt.Sprintf("col_%d", k)] = "value"
				}
				batch = append(batch, record)

				// Release records periodically
				if len(batch) >= 1000 {
					for _, r := range batch {
						r.Release()
					}
					batch = batch[:0]
				}
			}

			// Release remaining
			for _, r := range batch {
				r.Release()
			}
		}
		b.ReportMetric(float64(recordCount), "records")
	})
}

// createTestData creates a CSV file with test data
func createTestData(b *testing.B, filename string, records int) {
	file, err := os.Create(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	headers := []string{"id", "name", "email", "age", "department", "salary", "created_at"}
	writer.Write(headers)

	// Write records
	departments := []string{"Engineering", "Sales", "Marketing", "HR", "Finance"}
	for i := 0; i < records; i++ {
		record := []string{
			fmt.Sprintf("%d", i),
			fmt.Sprintf("User_%d", i),
			fmt.Sprintf("user%d@example.com", i),
			fmt.Sprintf("%d", 20+(i%40)),
			departments[i%len(departments)],
			fmt.Sprintf("%.2f", 50000+float64(i%50000)),
			time.Now().Add(time.Duration(i) * time.Second).Format(time.RFC3339),
		}
		writer.Write(record)
	}
}
