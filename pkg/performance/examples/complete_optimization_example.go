// Package examples demonstrates complete optimization workflow
package examples

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"github.com/ajitpratap0/nebula/pkg/compression"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/performance"
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// CompleteOptimizationWorkflow demonstrates full optimization pipeline
func CompleteOptimizationWorkflow() {
	fmt.Println("=== Complete Performance Optimization Workflow ===")

	// Step 1: Profile baseline performance
	fmt.Println("Step 1: Profiling Baseline Performance")
	baselineMetrics := profileBaseline()

	// Step 2: Apply memory optimizations
	fmt.Println("\nStep 2: Applying Memory Optimizations")
	memOptimizedMetrics := applyMemoryOptimizations()

	// Step 3: Apply CPU optimizations
	fmt.Println("\nStep 3: Applying CPU Optimizations")
	cpuOptimizedMetrics := applyCPUOptimizations()

	// Step 4: Apply connector-specific optimizations
	fmt.Println("\nStep 4: Applying Connector Optimizations")
	connectorMetrics := applyConnectorOptimizations()

	// Step 5: Apply bulk loading optimizations
	fmt.Println("\nStep 5: Applying Bulk Loading Optimizations")
	bulkLoadMetrics := applyBulkLoadOptimizations()

	// Step 6: Generate comprehensive report
	fmt.Println("\nStep 6: Performance Optimization Report")
	generateOptimizationReport(baselineMetrics, memOptimizedMetrics,
		cpuOptimizedMetrics, connectorMetrics, bulkLoadMetrics)
}

// profileBaseline establishes baseline performance
func profileBaseline() *performance.Metrics {
	profiler := performance.NewProfiler(&performance.ProfilerConfig{
		Name:             "baseline",
		EnableCPUProfile: true,
		EnableMemProfile: true,
		ResourceMonitor:  true,
	})

	profiler.Start()

	// Generate test data
	records := generateTestData(100000)

	// Process without optimizations
	start := time.Now()
	for _, record := range records {
		// Simulate processing
		processRecordBaseline(record)
		profiler.IncrementRecords(1)
		profiler.IncrementBytes(estimateRecordSize(record))
	}
	elapsed := time.Since(start)

	metrics := profiler.Stop()

	fmt.Printf("  Records: %d\n", metrics.RecordsProcessed)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Throughput: %.2f records/sec\n", metrics.RecordsPerSecond)
	fmt.Printf("  Memory: %d MB\n", metrics.MemoryUsageMB)
	fmt.Printf("  CPU: %.2f%%\n", metrics.CPUUsagePercent)

	return metrics
}

// applyMemoryOptimizations demonstrates memory optimization
func applyMemoryOptimizations() *performance.Metrics {
	// Create memory optimizer
	memOptimizer := performance.NewMemoryOptimizer(&performance.MemoryConfig{
		EnableObjectPooling:   true,
		ObjectPoolSize:        50000,
		EnableStringInterning: true,
		StringInternSize:      10000,
		EnableColumnStore:     true,
		EnableCompaction:      true,
	})

	profiler := performance.NewProfiler(&performance.ProfilerConfig{
		Name: "memory_optimized",
	})

	profiler.Start()

	// Generate and optimize test data
	records := generateTestData(100000)
	optimizedRecords := memOptimizer.OptimizeRecords(records)

	// Process with memory optimizations
	start := time.Now()
	for _, record := range optimizedRecords {
		processRecordOptimized(record)
		profiler.IncrementRecords(1)
	}
	elapsed := time.Since(start)

	// Release records back to pool
	for _, record := range optimizedRecords {
		memOptimizer.ReleaseRecord(record)
	}

	// Force compaction
	memOptimizer.Compact()

	metrics := profiler.Stop()
	memMetrics := memOptimizer.GetMetrics()

	fmt.Printf("  Records: %d\n", metrics.RecordsProcessed)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Throughput: %.2f records/sec\n", metrics.RecordsPerSecond)
	fmt.Printf("  Memory: %d MB (saved: %d MB)\n",
		metrics.MemoryUsageMB, memMetrics.BytesSaved/1024/1024)
	fmt.Printf("  Objects Pooled: %d\n", memMetrics.ObjectsPooled)
	fmt.Printf("  Strings Interned: %d\n", memMetrics.StringsInterned)

	return metrics
}

// applyCPUOptimizations demonstrates CPU optimization
func applyCPUOptimizations() *performance.Metrics {
	// Create CPU optimizer
	optimizer := performance.NewOptimizer(&performance.OptimizerConfig{
		WorkerCount:      runtime.NumCPU(),
		MaxGoroutines:    runtime.NumCPU() * 10,
		EnablePipelining: true,
		PipelineDepth:    4,
		AdaptiveBatching: true,
		OptimalBatchSize: 10000,
	})

	profiler := performance.NewProfiler(&performance.ProfilerConfig{
		Name: "cpu_optimized",
	})

	profiler.Start()

	// Generate test data
	records := generateTestData(100000)

	// Convert to interface slice for parallel processing
	items := make([]interface{}, len(records))
	for i, r := range records {
		items[i] = r
	}

	// Process with CPU optimizations
	ctx := context.Background()
	start := time.Now()

	err := optimizer.ExecuteParallel(ctx, items, func(item interface{}) error {
		record := item.(*models.Record)
		processRecordOptimized(record)
		profiler.IncrementRecords(1)
		return nil
	})
	if err != nil {
		log.Printf("CPU optimization error: %v", err)
	}

	elapsed := time.Since(start)
	metrics := profiler.Stop()

	fmt.Printf("  Records: %d\n", metrics.RecordsProcessed)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Throughput: %.2f records/sec\n", metrics.RecordsPerSecond)
	fmt.Printf("  CPU: %.2f%% (Workers: %d)\n",
		metrics.CPUUsagePercent, runtime.NumCPU())
	fmt.Printf("  Goroutines: %d\n", metrics.GoroutineCount)

	return metrics
}

// applyConnectorOptimizations demonstrates connector-specific optimization
func applyConnectorOptimizations() *performance.Metrics {
	// Create connector optimizer for Snowflake
	connOptimizer := performance.NewConnectorOptimizer(&performance.ConnectorOptConfig{
		ConnectorType:     "snowflake",
		TargetThroughput:  100000,
		EnableBatching:    true,
		EnableCompression: true,
		EnableColumnar:    true,
		EnableParallelism: true,
		EnableCaching:     true,
		EnableZeroCopy:    true,
	})

	profiler := performance.NewProfiler(&performance.ProfilerConfig{
		Name: "connector_optimized",
	})

	profiler.Start()

	// Generate test data
	records := generateTestData(100000)

	// Apply connector optimizations
	ctx := context.Background()
	start := time.Now()

	optimizedRecords, err := connOptimizer.ApplyOptimizations(ctx, records)
	if err != nil {
		log.Printf("Connector optimization error: %v", err)
	}

	// Simulate optimized write to Snowflake
	for _, record := range optimizedRecords {
		simulateSnowflakeWrite(record)
		profiler.IncrementRecords(1)
	}

	elapsed := time.Since(start)
	metrics := profiler.Stop()

	fmt.Printf("  Records: %d\n", metrics.RecordsProcessed)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Throughput: %.2f records/sec\n", metrics.RecordsPerSecond)
	fmt.Printf("  Optimization Report:\n%s\n",
		connOptimizer.GetOptimizationReport())

	return metrics
}

// applyBulkLoadOptimizations demonstrates bulk loading optimization
func applyBulkLoadOptimizations() *performance.Metrics {
	// Create bulk loader for Snowflake
	bulkLoader := performance.NewBulkLoader(&performance.BulkLoadConfig{
		TargetWarehouse:  "snowflake",
		MaxBatchSize:     50000,
		ParallelLoaders:  4,
		FileFormat:       "parquet",
		Compression:      "zstd",
		EnableColumnar:   true,
		EnableClustering: true,
	})

	profiler := performance.NewProfiler(&performance.ProfilerConfig{
		Name: "bulk_load_optimized",
	})

	profiler.Start()

	// Generate test data
	records := generateTestData(100000)

	// Perform bulk load
	ctx := context.Background()
	start := time.Now()

	err := bulkLoader.Load(ctx, records)
	if err != nil {
		log.Printf("Bulk load error: %v", err)
	}

	elapsed := time.Since(start)
	metrics := profiler.Stop()
	bulkMetrics := bulkLoader.GetMetrics()

	fmt.Printf("  Records: %d\n", bulkMetrics.RecordsLoaded)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Throughput: %.2f records/sec\n", bulkMetrics.Throughput)
	fmt.Printf("  Files Created: %d\n", bulkMetrics.FilesCreated)
	fmt.Printf("  Compression Ratio: %.2fx\n", bulkMetrics.CompressionRatio)
	fmt.Printf("  Bytes Loaded: %.2f MB\n",
		float64(bulkMetrics.BytesLoaded)/1024/1024)

	return metrics
}

// generateOptimizationReport creates comprehensive report
func generateOptimizationReport(baseline, memOpt, cpuOpt, connOpt, bulkOpt *performance.Metrics) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("PERFORMANCE OPTIMIZATION SUMMARY")
	fmt.Println(strings.Repeat("=", 60))

	fmt.Printf("\n%-20s %15s %15s %10s\n", "Stage", "Throughput", "Memory (MB)", "Improvement")
	fmt.Println(strings.Repeat("-", 60))

	// Baseline
	fmt.Printf("%-20s %15.0f %15d %10s\n",
		"Baseline",
		baseline.RecordsPerSecond,
		baseline.MemoryUsageMB,
		"-")

	// Memory Optimized
	memImprovement := (memOpt.RecordsPerSecond / baseline.RecordsPerSecond) - 1
	fmt.Printf("%-20s %15.0f %15d %10.1fx\n",
		"Memory Optimized",
		memOpt.RecordsPerSecond,
		memOpt.MemoryUsageMB,
		memImprovement+1)

	// CPU Optimized
	cpuImprovement := (cpuOpt.RecordsPerSecond / baseline.RecordsPerSecond) - 1
	fmt.Printf("%-20s %15.0f %15d %10.1fx\n",
		"CPU Optimized",
		cpuOpt.RecordsPerSecond,
		cpuOpt.MemoryUsageMB,
		cpuImprovement+1)

	// Connector Optimized
	connImprovement := (connOpt.RecordsPerSecond / baseline.RecordsPerSecond) - 1
	fmt.Printf("%-20s %15.0f %15d %10.1fx\n",
		"Connector Optimized",
		connOpt.RecordsPerSecond,
		connOpt.MemoryUsageMB,
		connImprovement+1)

	// Bulk Load (special case - already optimized)
	bulkImprovement := (bulkOpt.RecordsPerSecond / baseline.RecordsPerSecond) - 1
	fmt.Printf("%-20s %15.0f %15d %10.1fx\n",
		"Bulk Load",
		bulkOpt.RecordsPerSecond,
		bulkOpt.MemoryUsageMB,
		bulkImprovement+1)

	fmt.Println(strings.Repeat("-", 60))

	// Total improvement
	totalImprovement := (bulkOpt.RecordsPerSecond / baseline.RecordsPerSecond)
	fmt.Printf("\nTOTAL IMPROVEMENT: %.1fx throughput, %.0f%% memory reduction\n",
		totalImprovement,
		float64(baseline.MemoryUsageMB-bulkOpt.MemoryUsageMB)/float64(baseline.MemoryUsageMB)*100)

	// Key achievements
	fmt.Println("\nKEY ACHIEVEMENTS:")
	fmt.Printf("✅ Throughput: %.0f → %.0f records/sec\n",
		baseline.RecordsPerSecond, bulkOpt.RecordsPerSecond)
	fmt.Printf("✅ Memory: %d → %d MB\n",
		baseline.MemoryUsageMB, bulkOpt.MemoryUsageMB)
	fmt.Printf("✅ Latency: %v → %v\n",
		baseline.AvgLatency, bulkOpt.AvgLatency)
	fmt.Printf("✅ CPU Efficiency: %.0f%% → %.0f%%\n",
		baseline.CPUUsagePercent, bulkOpt.CPUUsagePercent)

	// Recommendations
	fmt.Println("\nRECOMMENDATIONS:")
	fmt.Println("1. Use memory pooling for all high-frequency allocations")
	fmt.Println("2. Enable parallel processing for CPU-bound operations")
	fmt.Println("3. Apply connector-specific optimizations")
	fmt.Println("4. Use bulk loading for large datasets")
	fmt.Println("5. Monitor and profile continuously")
}

// Helper functions

func generateTestData(count int) []*models.Record {
	records := make([]*models.Record, count)
	for i := 0; i < count; i++ {
		records[i] = &models.Record{
			Data: map[string]interface{}{
				"id":          i,
				"name":        fmt.Sprintf("Record_%d", i),
				"value":       float64(i) * 1.5,
				"timestamp":   time.Now(),
				"description": fmt.Sprintf("This is a test record %d with some data", i),
				"metadata":    `{"source": "test", "version": 1}`,
			},
		}
	}
	return records
}

func processRecordBaseline(record *models.Record) {
	// Simulate baseline processing
	time.Sleep(time.Microsecond)

	// Inefficient string operations
	_ = fmt.Sprintf("%v", record.Data)

	// Allocate unnecessary memory
	temp := pool.GetByteSlice()

	if cap(temp) < 1024 {
		temp = make([]byte, 1024)
	}

	defer pool.PutByteSlice(temp)
	_ = temp
}

func processRecordOptimized(record *models.Record) {
	// Optimized processing
	// No sleep, no allocations
	_ = record.Data["id"]
}

func simulateSnowflakeWrite(record *models.Record) {
	// Simulate optimized Snowflake write
	_ = record
}

func estimateRecordSize(record *models.Record) int64 {
	size := int64(0)
	for k, v := range record.Data {
		size += int64(len(k))
		if str, ok := v.(string); ok {
			size += int64(len(str))
		} else {
			size += 8
		}
	}
	return size
}

// FullPipelineExample demonstrates complete data pipeline with all optimizations
func FullPipelineExample() {
	fmt.Println("\n=== Full Optimized Data Pipeline Example ===")

	// Initialize all optimizers
	memOptimizer := performance.NewMemoryOptimizer(nil)
	cpuOptimizer := performance.NewOptimizer(nil)
	connOptimizer := performance.NewConnectorOptimizer(
		performance.DefaultConnectorOptConfig("snowflake"))
	bulkLoader := performance.NewBulkLoader(
		performance.DefaultBulkLoadConfig("snowflake"))

	// Create compression pool
	compressor := compression.NewCompressorPool(&compression.Config{
		Algorithm: compression.Snappy,
		Level:     compression.Default,
	})

	// Create columnar writer config (commented out for now)
	// columnarConfig := &columnar.WriterConfig{
	// 	Format:      columnar.Parquet,
	// 	Compression: "zstd",
	// 	BatchSize:   10000,
	// 	EnableStats: true,
	// }

	// Start profiling
	profiler := performance.NewProfiler(&performance.ProfilerConfig{
		Name:             "full_pipeline",
		EnableCPUProfile: true,
		EnableMemProfile: true,
		ResourceMonitor:  true,
	})
	profiler.Start()

	// Generate large dataset
	fmt.Println("Generating 1M records...")
	records := generateTestData(1000000)

	// Step 1: Memory optimization
	fmt.Println("Step 1: Optimizing memory...")
	records = memOptimizer.OptimizeRecords(records)

	// Step 2: Batch processing with CPU optimization
	fmt.Println("Step 2: Processing in parallel batches...")
	batchSize := 50000
	batches := splitIntoBatches(records, batchSize)

	ctx := context.Background()
	for i, batch := range batches {
		items := make([]interface{}, len(batch))
		for j, r := range batch {
			items[j] = r
		}

		_ = cpuOptimizer.ExecuteParallel(ctx, items, func(item interface{}) error {
			record := item.(*models.Record)

			// Compress large fields
			for k, v := range record.Data {
				if str, ok := v.(string); ok && len(str) > 100 {
					compressed, _ := compressor.Compress([]byte(str))
					record.Data[k] = compressed
				}
			}

			return nil
		})

		fmt.Printf("  Processed batch %d/%d\n", i+1, len(batches))
	}

	// Step 3: Apply connector optimizations
	fmt.Println("Step 3: Applying connector optimizations...")
	optimized, _ := connOptimizer.ApplyOptimizations(ctx, records)

	// Step 4: Bulk load with columnar format
	fmt.Println("Step 4: Bulk loading to Snowflake...")
	err := bulkLoader.Load(ctx, optimized)
	if err != nil {
		log.Printf("Bulk load error: %v", err)
	}

	// Stop profiling and get results
	metrics := profiler.Stop()
	report := profiler.GenerateReport()

	fmt.Println("\n" + report.Report)

	// Clean up
	for _, record := range optimized {
		memOptimizer.ReleaseRecord(record)
	}
	runtime.GC()
	// Note: runtime.FreeOSMemory() doesn't exist
	// Use debug.FreeOSMemory() instead if needed

	fmt.Printf("\nPipeline completed successfully!\n")
	fmt.Printf("Total records processed: %d\n", metrics.RecordsProcessed)
	fmt.Printf("Average throughput: %.0f records/sec\n", metrics.RecordsPerSecond)
}

func splitIntoBatches(records []*models.Record, batchSize int) [][]*models.Record {
	var batches [][]*models.Record
	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		batches = append(batches, records[i:end])
	}
	return batches
}
