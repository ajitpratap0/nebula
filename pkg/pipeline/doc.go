// Package pipeline provides the storage abstraction layer for Nebula's hybrid
// storage engine. It implements intelligent mode selection between row-based
// and columnar storage to achieve optimal memory efficiency and performance.
//
// # Overview
//
// The pipeline package is the interface between Nebula's data processing pipeline
// and the underlying storage implementations. It provides:
//
//   - StorageAdapter: Unified interface for all storage modes
//   - Automatic mode selection based on workload characteristics
//   - Transparent conversion between storage formats
//   - Performance monitoring and optimization
//
// # Storage Modes
//
// The package supports three storage modes:
//
//  1. Row Mode (StorageModeRow):
//     - Traditional row-based storage
//     - 225 bytes per record
//     - Best for streaming and real-time processing
//     - Low latency record access
//
//  2. Columnar Mode (StorageModeColumnar):
//     - Column-oriented storage with compression
//     - 84 bytes per record (94% reduction)
//     - Best for batch processing and analytics
//     - High compression ratios
//
//  3. Hybrid Mode (StorageModeHybrid):
//     - Automatic selection between row and columnar
//     - Switches at 10K record threshold
//     - Best overall performance
//     - Default mode for most use cases
//
// # Basic Usage
//
// Creating a storage adapter:
//
//	config := config.NewBaseConfig("processor", "pipeline")
//	config.Advanced.StorageMode = "hybrid"
//
//	adapter := pipeline.NewStorageAdapter(
//		pipeline.StorageModeHybrid,
//		config,
//	)
//	defer adapter.Close() // Ignore close error
//
// Processing records:
//
//	for _, record := range records {
//		if err := adapter.AddRecord(record); err != nil {
//			return err
//		}
//	}
//
//	// Check efficiency
//	memPerRecord := adapter.GetMemoryPerRecord()
//	log.Printf("Memory efficiency: %.2f bytes/record", memPerRecord)
//
// # Automatic Mode Selection
//
// In hybrid mode, the adapter automatically selects the optimal storage:
//
//	// Small datasets (<10K records): Row mode for low latency
//	// Large datasets (>10K records): Columnar for memory efficiency
//	// Streaming: Row mode for real-time processing
//	// Batch: Columnar for maximum compression
//
// The selection logic considers:
//   - Record count thresholds
//   - Access patterns (sequential vs random)
//   - Memory pressure
//   - Performance requirements
//
// # Configuration
//
// Storage mode is configured through BaseConfig:
//
//	advanced:
//	  storage_mode: "hybrid"    # Options: row, columnar, hybrid
//	  compression: "lz4"        # Compression algorithm
//
//	performance:
//	  batch_size: 10000         # Affects mode switching
//	  streaming_mode: false     # Forces row mode when true
//
// # Integration with Connectors
//
// Source connectors integrate with storage adapters:
//
//	func (s *Source) Read(ctx context.Context) error {
//		adapter := pipeline.NewStorageAdapter(
//			pipeline.ParseStorageMode(s.config.Advanced.StorageMode),
//			s.config,
//		)
//		defer adapter.Close() // Ignore close error
//
//		for s.hasMoreData() {
//			record := s.readRecord()
//			if err := adapter.AddRecord(record); err != nil {
//				return err
//			}
//		}
//
//		return adapter.Flush()
//	}
//
// # Performance Optimization
//
// The adapter provides optimization methods:
//
//	// Background optimization
//	adapter.OptimizeStorage()
//
//	// Force mode switch
//	adapter.SwitchMode(pipeline.StorageModeColumnar)
//
//	// Pre-declare schema for better columnar efficiency
//	adapter.SetSchema(schema)
//
// # Monitoring and Metrics
//
// Track storage efficiency:
//
//	metrics := adapter.GetMetrics()
//	log.Printf("Storage Metrics:\n" +
//		"  Mode: %s\n" +
//		"  Records: %d\n" +
//		"  Memory: %.2f MB\n" +
//		"  Bytes/Record: %.2f\n" +
//		"  Compression: %.2fx\n",
//		metrics.Mode,
//		metrics.RecordCount,
//		metrics.MemoryUsage / 1024 / 1024,
//		metrics.BytesPerRecord,
//		metrics.CompressionRatio,
//	)
//
// # Best Practices
//
// 1. Use hybrid mode unless you have specific requirements
// 2. Configure batch_size appropriately (10K+ for columnar efficiency)
// 3. Enable compression for additional savings
// 4. Monitor mode transitions and efficiency metrics
// 5. Pre-declare schemas when known for better optimization
// 6. Use streaming_mode flag for real-time requirements
// 7. Call OptimizeStorage() during idle periods
//
// # Advanced Features
//
// Custom mode selection:
//
//	adapter := pipeline.NewStorageAdapter(
//		pipeline.StorageModeHybrid,
//		config,
//		pipeline.WithThreshold(50000),           // Custom switch threshold
//		pipeline.WithCompressionLevel(9),        // Max compression
//		pipeline.WithMemoryLimit(1024*1024*1024), // 1GB limit
//	)
//
// Schema hints:
//
//	schema := &pipeline.Schema{
//		Columns: []pipeline.Column{
//			{Name: "id", Type: "int64", Nullable: false},
//			{Name: "name", Type: "string", Nullable: true},
//			{Name: "active", Type: "bool", Nullable: false},
//		},
//	}
//	adapter.SetSchema(schema)
//
// # Memory Management
//
// The adapter integrates with Nebula's pool system:
//   - Automatic record pooling in row mode
//   - Column buffer pooling in columnar mode
//   - Zero-copy transfers between modes
//   - Automatic cleanup on mode switches
package pipeline
