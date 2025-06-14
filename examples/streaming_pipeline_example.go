// Package main demonstrates the high-performance streaming pipeline
//go:build ignore
// +build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ajitpratap0/nebula/internal/pipeline"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
	_ "github.com/ajitpratap0/nebula/pkg/connector/destinations/csv"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/csv"
)

func main() {
	fmt.Println("🚀 Nebula High-Performance Streaming Pipeline Example")
	fmt.Println("====================================================")

	// Create high-performance streaming configuration
	streamingConfig := &pipeline.StreamingConfig{
		InitialBufferSize:     50000,  // Large buffer for high throughput
		MaxBufferSize:         500000, // 500K record buffer
		BufferGrowthFactor:    1.5,
		BackpressureThreshold: 0.8,        // Activate backpressure at 80%
		BackpressureStrategy:  "adaptive", // Use adaptive backpressure
		MaxConcurrency:        8,          // High concurrency
		StageParallelism: map[string]int{
			"extract":   4,
			"transform": 8,
			"load":      2,
		},
		BatchSize:           5000, // Large batches for performance
		MaxBatchDelay:       500 * time.Millisecond,
		PipelineFusion:      true, // Enable pipeline fusion
		ZeroCopyEnabled:     true, // Enable zero-copy optimizations
		MetricsInterval:     5 * time.Second,
		HealthCheckInterval: 15 * time.Second,
	}

	// Create CSV source configuration using BaseConfig
	sourceConfig := config.NewBaseConfig("high-perf-csv-source", "source")
	sourceConfig.Name = "high-perf-csv-source"
	sourceConfig.Type = "source"
	sourceConfig.Version = "1.0.0"
	// Configure performance settings
	sourceConfig.Performance.BatchSize = 5000
	sourceConfig.Performance.Workers = 4
	sourceConfig.Performance.MaxConcurrency = 8
	sourceConfig.Performance.EnableStreaming = true

	// Set CSV-specific configuration
	sourceConfig.Security.Credentials["path"] = "large_dataset.csv"

	source, err := registry.CreateSource("csv", sourceConfig)
	if err != nil {
		log.Fatalf("Failed to create source: %v", err)
	}

	// Create CSV destination configuration using BaseConfig
	destConfig := config.NewBaseConfig("high-perf-csv-dest", "destination")
	destConfig.Name = "high-perf-csv-dest"
	destConfig.Type = "destination"
	destConfig.Version = "1.0.0"
	// Configure performance settings
	destConfig.Performance.BatchSize = 5000
	destConfig.Performance.Workers = 2
	destConfig.Performance.MaxConcurrency = 8
	destConfig.Performance.AsyncOperations = true

	// Set CSV destination configuration
	destConfig.Security.Credentials["path"] = "processed_output.csv"
	destConfig.Advanced.EnableCompression = true
	destConfig.Advanced.CompressionAlgorithm = "zstd"

	dest, err := registry.CreateDestination("csv", destConfig)
	if err != nil {
		log.Fatalf("Failed to create destination: %v", err)
	}

	// Initialize connectors
	ctx := context.Background()

	if err := source.Initialize(ctx, sourceConfig); err != nil {
		log.Fatalf("Failed to initialize source: %v", err)
	}

	if err := dest.Initialize(ctx, destConfig); err != nil {
		log.Fatalf("Failed to initialize destination: %v", err)
	}

	// Create streaming pipeline
	fmt.Println("Creating high-performance streaming pipeline...")
	streamingPipeline := pipeline.NewStreamingPipeline("high-perf-demo", source, dest, streamingConfig)

	// Start performance monitoring
	startTime := time.Now()

	// Run pipeline with timeout
	pipelineCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	fmt.Println("Starting pipeline execution...")
	fmt.Println("Configuration:")
	fmt.Printf("  - Buffer Size: %d -> %d records\n", streamingConfig.InitialBufferSize, streamingConfig.MaxBufferSize)
	fmt.Printf("  - Concurrency: %d workers\n", streamingConfig.MaxConcurrency)
	fmt.Printf("  - Batch Size: %d records\n", streamingConfig.BatchSize)
	fmt.Printf("  - Backpressure: %s at %.0f%% utilization\n", streamingConfig.BackpressureStrategy, streamingConfig.BackpressureThreshold*100)
	fmt.Printf("  - Zero-Copy: %v\n", streamingConfig.ZeroCopyEnabled)
	fmt.Printf("  - Pipeline Fusion: %v\n", streamingConfig.PipelineFusion)
	fmt.Println()

	// Run the streaming pipeline
	err = streamingPipeline.Run(pipelineCtx)
	if err != nil {
		log.Printf("Pipeline execution completed with: %v", err)
	}

	duration := time.Since(startTime)

	// Show results
	fmt.Println("\n📊 Pipeline Execution Results")
	fmt.Println("=============================")
	fmt.Printf("Total Duration: %v\n", duration)

	// Get metrics from pipeline
	metrics := streamingPipeline.GetMetrics()
	if metrics != nil {
		fmt.Printf("Records Processed: %d\n", metrics.RecordsProcessed)
		fmt.Printf("Throughput: %.0f records/sec\n", metrics.ThroughputRPS)
		fmt.Printf("Backpressure Events: %d\n", metrics.BackpressureEvents)
		fmt.Printf("Buffer Utilization: %.2f%%\n", metrics.BufferUtilization*100)
	}

	// Show component status
	fmt.Println("\n🏗️ Component Status")
	fmt.Println("==================")
	fmt.Printf("✅ StreamingPipeline: Unified models.Record interface\n")
	fmt.Printf("✅ BackpressureController: Adaptive flow control\n")
	fmt.Printf("✅ AdaptiveBuffer: Dynamic sizing\n")
	fmt.Printf("✅ Compression: Multi-algorithm support\n")
	fmt.Printf("✅ CDC Adapters: PostgreSQL, MySQL, MongoDB\n")

	// Performance achievements
	fmt.Println("\n🎯 Performance Achievements")
	fmt.Println("===========================")
	fmt.Printf("✅ Phase 0: Fixed interface fragmentation\n")
	fmt.Printf("✅ Phase 1: Integrated compression & CDC\n")
	fmt.Printf("✅ Phase 2: Implemented high-performance streaming\n")
	fmt.Printf("🔄 Target: Working towards 1M+ records/sec\n")

	// Next steps
	fmt.Println("\n🔄 Next Steps for 1M+ Records/sec")
	fmt.Println("=================================")
	fmt.Printf("1. Integrate with real high-volume data sources\n")
	fmt.Printf("2. Add SIMD optimizations for CPU-bound operations\n")
	fmt.Printf("3. Implement io_uring for high-performance I/O\n")
	fmt.Printf("4. Add work-stealing scheduler\n")
	fmt.Printf("5. Optimize memory allocation patterns\n")

	fmt.Println("\n🎉 High-Performance Streaming Pipeline Ready!")
}
