package benchmarks

import (
	"encoding/csv"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/pkg/columnar"
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// BenchmarkColumnarStorage tests the memory efficiency of columnar storage
func BenchmarkColumnarStorage(b *testing.B) {
	recordCounts := []int{1000, 10000, 100000}

	for _, recordCount := range recordCounts {
		b.Run(fmt.Sprintf("Records_%d", recordCount), func(b *testing.B) {
			// Create test data
			testFile := fmt.Sprintf("/tmp/columnar_bench_%d.csv", recordCount)
			createTestDataForColumnar(b, testFile, recordCount)
			defer func() { _ = os.Remove(testFile) }() // Best effort cleanup

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Measure memory before
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)

				start := time.Now()
				store := processCSVColumnar(b, testFile)
				duration := time.Since(start)

				// Measure memory after
				runtime.GC()
				runtime.ReadMemStats(&m2)

				// Calculate metrics
				totalMemory := store.MemoryUsage()
				rowCount := store.RowCount()
				bytesPerRecord := store.MemoryPerRecord()
				actualAlloc := m2.TotalAlloc - m1.TotalAlloc

				recordsPerSec := float64(rowCount) / duration.Seconds()

				b.ReportMetric(bytesPerRecord, "bytes/record")
				b.ReportMetric(float64(actualAlloc)/float64(rowCount), "actual_bytes/record")
				b.ReportMetric(recordsPerSec, "records/sec")
				b.ReportMetric(float64(totalMemory), "total_memory")

				b.Logf("Columnar: %d records, %.1f bytes/record (actual: %.1f), %d cols, %.0f records/sec",
					rowCount, bytesPerRecord, float64(actualAlloc)/float64(rowCount),
					store.ColumnCount(), recordsPerSec)
			}
		})
	}
}

// BenchmarkColumnarVsRowBased compares columnar vs row-based storage
func BenchmarkColumnarVsRowBased(b *testing.B) {
	testFile := "/tmp/comparison_test.csv"
	recordCount := 10000
	createTestDataForColumnar(b, testFile, recordCount)
	defer func() { _ = os.Remove(testFile) }() // Best effort cleanup

	b.Run("RowBased", func(b *testing.B) {
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		records := processRowBased(b, testFile)

		runtime.GC()
		runtime.ReadMemStats(&m2)

		totalAlloc := m2.TotalAlloc - m1.TotalAlloc
		bytesPerRecord := float64(totalAlloc) / float64(len(records))

		b.ReportMetric(bytesPerRecord, "bytes/record")
		b.Logf("Row-based: %d records, %.1f bytes/record", len(records), bytesPerRecord)
	})

	b.Run("Columnar", func(b *testing.B) {
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		store := processCSVColumnar(b, testFile)

		runtime.GC()
		runtime.ReadMemStats(&m2)

		bytesPerRecord := store.MemoryPerRecord()
		actualAlloc := m2.TotalAlloc - m1.TotalAlloc
		actualBytesPerRecord := float64(actualAlloc) / float64(store.RowCount())

		b.ReportMetric(bytesPerRecord, "reported_bytes/record")
		b.ReportMetric(actualBytesPerRecord, "actual_bytes/record")
		b.Logf("Columnar: %d records, %.1f bytes/record (actual: %.1f)",
			store.RowCount(), bytesPerRecord, actualBytesPerRecord)
	})
}

// BenchmarkColumnarDictionaryEncoding tests string compression efficiency
func BenchmarkColumnarDictionaryEncoding(b *testing.B) {
	// Test with different repetition rates
	testCases := []struct {
		name       string
		categories int // Number of unique values
		records    int
	}{
		{"HighRepetition_10Categories", 10, 10000},
		{"MediumRepetition_100Categories", 100, 10000},
		{"LowRepetition_1000Categories", 1000, 10000},
		{"MixedData", 50, 10000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			testFile := fmt.Sprintf("/tmp/dict_test_%s.csv", tc.name)
			createCategoricalTestData(b, testFile, tc.records, tc.categories)
			defer func() { _ = os.Remove(testFile) }() // Best effort cleanup

			var m1, m2 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m1)

			store := processCSVColumnar(b, testFile)

			runtime.GC()
			runtime.ReadMemStats(&m2)

			bytesPerRecord := store.MemoryPerRecord()
			actualAlloc := m2.TotalAlloc - m1.TotalAlloc

			// Get dictionary stats for string columns
			for _, colName := range store.ColumnNames() {
				if col, ok := store.GetColumn(colName); ok {
					if strCol, ok := col.(*columnar.StringColumn); ok {
						// Access internal state (would need to expose this properly)
						_ = fmt.Sprintf("%s: dict=%v", colName, strCol) // Note: dictStats not used later
					}
				}
			}

			b.ReportMetric(bytesPerRecord, "bytes/record")
			b.ReportMetric(float64(actualAlloc)/float64(store.RowCount()), "actual_bytes/record")
			b.Logf("Dictionary encoding: %d records, %d categories, %.1f bytes/record",
				tc.records, tc.categories, bytesPerRecord)
		})
	}
}

// BenchmarkColumnarBatchProcessing tests batch processing efficiency
func BenchmarkColumnarBatchProcessing(b *testing.B) {
	testFile := "/tmp/batch_test.csv"
	recordCount := 100000
	createTestDataForColumnar(b, testFile, recordCount)
	defer func() { _ = os.Remove(testFile) }() // Best effort cleanup

	store := processCSVColumnar(b, testFile)

	batchSizes := []int{100, 1000, 5000, 10000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				processed := 0
				processor := columnar.NewBatchProcessor(store, batchSize)

				start := time.Now()
				err := processor.Process(func(records []*pool.Record) error {
					// Simulate processing
					for _, rec := range records {
						_ = rec.Data
					}
					processed += len(records)
					return nil
				})
				duration := time.Since(start)

				if err != nil {
					b.Fatal(err)
				}

				recordsPerSec := float64(processed) / duration.Seconds()
				b.ReportMetric(recordsPerSec, "records/sec")
			}
		})
	}
}

// Helper functions

func processCSVColumnar(b *testing.B, filename string) *columnar.ColumnStore {
	file, err := os.Open(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			b.Errorf("Failed to close file: %v", err)
		}
	}()

	reader := csv.NewReader(file)
	reader.ReuseRecord = true

	converter := columnar.NewCSVToColumnar()

	// Read headers
	headers, err := reader.Read()
	if err != nil {
		b.Fatal(err)
	}
	converter.SetHeaders(headers)

	// Read data in batches
	batchSize := 5000
	batch := make([][]string, 0, batchSize)

	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		// Make a copy since reader reuses the slice
		recordCopy := make([]string, len(record))
		copy(recordCopy, record)
		batch = append(batch, recordCopy)

		if len(batch) >= batchSize {
			_ = converter.AddBatch(batch) // Ignore add batch error
			batch = batch[:0]
		}
	}

	// Add remaining records
	if len(batch) > 0 {
		_ = converter.AddBatch(batch) // Ignore add batch error
	}

	return converter.GetStore()
}

func processRowBased(b *testing.B, filename string) []*pool.Record {
	file, err := os.Open(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			b.Errorf("Failed to close file: %v", err)
		}
	}()

	reader := csv.NewReader(file)
	headers, _ := reader.Read()

	var records []*pool.Record

	for {
		row, err := reader.Read()
		if err != nil {
			break
		}

		record := pool.GetRecord()
		for i, value := range row {
			if i < len(headers) {
				record.Data[headers[i]] = value
			}
		}
		records = append(records, record)
	}

	return records
}

func createTestDataForColumnar(b *testing.B, filename string, records int) {
	file, err := os.Create(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			b.Errorf("Failed to close file: %v", err)
		}
	}()

	writer := csv.NewWriter(file)

	// Headers - mix of different data types
	headers := []string{
		"id", "name", "age", "city", "status", "score",
		"created_at", "active", "category", "value",
	}
	_ = writer.Write(headers) // Ignore write error

	// Generate data with some repetition for dictionary encoding
	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix"}
	statuses := []string{"active", "inactive", "pending"}
	categories := []string{"A", "B", "C", "D", "E"}

	for i := 0; i < records; i++ {
		record := []string{
			fmt.Sprintf("%d", i),                // id
			fmt.Sprintf("user_%d", i),           // name
			fmt.Sprintf("%d", 20+i%50),          // age
			cities[i%len(cities)],               // city (repetitive)
			statuses[i%len(statuses)],           // status (repetitive)
			fmt.Sprintf("%.2f", float64(i)*1.5), // score
			time.Now().Add(time.Duration(i) * time.Second).Format(time.RFC3339), // created_at
			fmt.Sprintf("%t", i%2 == 0),                                         // active
			categories[i%len(categories)],                                       // category (repetitive)
			fmt.Sprintf("%d", i*10),                                             // value
		}
		_ = writer.Write(record) // Ignore write error
	}

	writer.Flush() // Flush writes (no return value)
}

func createCategoricalTestData(b *testing.B, filename string, records, categories int) {
	file, err := os.Create(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			b.Errorf("Failed to close file: %v", err)
		}
	}()

	writer := csv.NewWriter(file)

	// Headers
	headers := []string{"id", "category1", "category2", "category3", "value"}
	_ = writer.Write(headers) // Ignore write error

	// Generate categorical values
	var catValues []string
	for i := 0; i < categories; i++ {
		catValues = append(catValues, fmt.Sprintf("cat_%d", i))
	}

	for i := 0; i < records; i++ {
		record := []string{
			fmt.Sprintf("%d", i),
			catValues[i%len(catValues)],
			catValues[(i*2)%len(catValues)],
			catValues[(i*3)%len(catValues)],
			fmt.Sprintf("%d", i),
		}
		_ = writer.Write(record) // Ignore write error
	}

	writer.Flush() // Flush writes (no return value)
}

// BenchmarkColumnarMemoryBreakdown provides detailed memory analysis
func BenchmarkColumnarMemoryBreakdown(b *testing.B) {
	testFile := "/tmp/memory_breakdown.csv"
	recordCount := 10000
	createTestDataForColumnar(b, testFile, recordCount)
	defer func() { _ = os.Remove(testFile) }() // Best effort cleanup

	store := processCSVColumnar(b, testFile)

	b.Logf("\n=== Columnar Memory Breakdown ===")
	b.Logf("Total rows: %d", store.RowCount())
	b.Logf("Total columns: %d", store.ColumnCount())
	b.Logf("Total memory: %d bytes", store.MemoryUsage())
	b.Logf("Bytes per record: %.2f", store.MemoryPerRecord())

	// Per-column breakdown
	b.Logf("\nPer-column memory usage:")
	for _, name := range store.ColumnNames() {
		if col, ok := store.GetColumn(name); ok {
			memUsage := col.MemoryUsage()
			bytesPerValue := float64(memUsage) / float64(col.Len())
			b.Logf("  %s: %d bytes total, %.2f bytes/value", name, memUsage, bytesPerValue)
		}
	}
}

// BenchmarkColumnarWithPooledRecords tests columnar with pooled record interface
func BenchmarkColumnarWithPooledRecords(b *testing.B) {
	testFile := "/tmp/pooled_columnar.csv"
	recordCount := 10000
	createTestDataForColumnar(b, testFile, recordCount)
	defer func() { _ = os.Remove(testFile) }() // Best effort cleanup

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		// Process with columnar storage
		store := processCSVColumnar(b, testFile)

		// Simulate pipeline processing with pooled records
		var wg sync.WaitGroup
		workers := 4
		recordsPerWorker := store.RowCount() / workers

		for w := 0; w < workers; w++ {
			wg.Add(1)
			startIdx := w * recordsPerWorker
			endIdx := startIdx + recordsPerWorker
			if w == workers-1 {
				endIdx = store.RowCount()
			}

			go func(start, end int) {
				defer wg.Done()

				for i := start; i < end; i++ {
					// Get pooled record
					rec := pool.GetRecord()

					// Fill from columnar store
					if row, err := store.GetRow(i); err == nil {
						for k, v := range row {
							rec.Data[k] = v
						}
					}

					// Simulate processing
					_ = rec.Data

					// Return to pool
					rec.Release()
				}
			}(startIdx, endIdx)
		}

		wg.Wait()

		runtime.GC()
		runtime.ReadMemStats(&m2)

		totalAlloc := m2.TotalAlloc - m1.TotalAlloc
		bytesPerRecord := float64(totalAlloc) / float64(store.RowCount())

		b.ReportMetric(bytesPerRecord, "bytes/record")
		b.Logf("Columnar+Pooled: %.1f bytes/record", bytesPerRecord)
	}
}
