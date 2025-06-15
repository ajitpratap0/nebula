// Package nebula provides a high-performance, cloud-native Extract & Load (EL)
// data integration platform designed to achieve extreme throughput (1M+ records/second)
// with minimal resource usage.
//
// Nebula represents a breakthrough in data integration technology, achieving:
//   - 1.7M-3.6M records/second throughput (170-360% of target)
//   - 94% memory reduction (1,365→84 bytes per record)
//   - <1ms P99 latency
//   - 12MB container size
//   - <80ms cold start time
//
// # Architecture
//
// Nebula is built on four revolutionary architectural principles:
//
// 1. Zero-Copy Architecture: Minimizes memory allocations through extensive
// object pooling and direct memory access patterns.
//
// 2. Unified Systems: Single implementations for records (pool.Record),
// memory pools (pool.Pool[T]), and configuration (config.BaseConfig).
//
// 3. Hybrid Storage Engine: Intelligent row/columnar storage that automatically
// selects the optimal format based on workload characteristics.
//
// 4. Production-First Design: Built-in circuit breakers, rate limiting,
// health monitoring, and comprehensive observability.
//
// # Quick Start
//
// Create a simple CSV to JSON pipeline:
//
//	import (
//	    "context"
//	    "github.com/ajitpratap0/nebula/pkg/config"
//	    "github.com/ajitpratap0/nebula/pkg/connector/registry"
//	    "github.com/ajitpratap0/nebula/internal/pipeline"
//	)
//	
//	// Create configuration
//	cfg := config.NewBaseConfig("my-pipeline", "pipeline")
//	cfg.Performance.BatchSize = 10000
//	cfg.Advanced.StorageMode = "hybrid"
//	
//	// Create source
//	sourceFactory, _ := registry.GetSourceFactory("csv")
//	source, _ := sourceFactory(cfg)
//	
//	// Create destination
//	destFactory, _ := registry.GetDestinationFactory("json")
//	dest, _ := destFactory(cfg)
//	
//	// Run pipeline
//	p := pipeline.New(source, dest, cfg)
//	err := p.Run(context.Background())
//
// # Key Packages
//
//	pkg/connector    - Connector framework for sources and destinations
//	pkg/pool         - High-performance object pooling system
//	pkg/columnar     - Hybrid storage engine with 94% memory reduction
//	pkg/config       - Unified configuration management
//	pkg/pipeline     - Storage abstraction with intelligent mode selection
//	pkg/errors       - Structured error handling
//	pkg/logger       - High-performance structured logging
//	pkg/metrics      - Comprehensive metrics collection
//
// # Performance
//
// Nebula achieves its extreme performance through:
//
// Memory Efficiency:
//   - Row mode: 225 bytes/record for streaming
//   - Columnar mode: 84 bytes/record for batch
//   - Hybrid mode: Automatic selection
//
// Zero-Allocation Patterns:
//   - Global object pools for all types
//   - Reusable buffers and slices
//   - Direct memory access
//
// Intelligent Optimization:
//   - Automatic type detection and conversion
//   - Dictionary encoding for strings
//   - Compression integration
//
// # Connectors
//
// Available source connectors:
//   - CSV, JSON files
//   - PostgreSQL (with CDC)
//   - MySQL (with CDC)
//   - Google Ads API
//   - Meta Ads API
//
// Available destination connectors:
//   - CSV, JSON files
//   - Snowflake (bulk loading)
//   - BigQuery (streaming/batch)
//   - S3, GCS (Parquet, Avro, ORC)
//   - PostgreSQL
//
// # Production Features
//
// Enterprise-ready capabilities:
//   - Circuit breakers for fault tolerance
//   - Adaptive rate limiting
//   - Health monitoring and checks
//   - Schema evolution support
//   - Exactly-once semantics (where supported)
//   - Comprehensive metrics and tracing
//
// # Configuration
//
// Nebula uses a unified configuration system:
//
//	type BaseConfig struct {
//	    Performance   PerformanceConfig   // Batch sizes, workers
//	    Timeouts      TimeoutConfig       // Connection, request timeouts
//	    Reliability   ReliabilityConfig   // Retries, circuit breakers
//	    Security      SecurityConfig      // Auth, TLS, encryption
//	    Observability ObservabilityConfig // Metrics, logging, tracing
//	    Memory        MemoryConfig        // Pools, limits, GC
//	    Advanced      AdvancedConfig      // Storage mode, compression
//	}
//
// Environment variables are supported with ${VAR_NAME} syntax.
//
// # Development
//
// Get started with development:
//
//	git clone https://github.com/ajitpratap0/nebula.git
//	cd nebula
//	make install-tools
//	make all
//	./bin/nebula help
//
// Run tests and benchmarks:
//
//	make test                            # Unit tests
//	make bench                           # Benchmarks
//	make coverage                        # Coverage report
//	./scripts/quick-perf-test.sh suite   # Performance suite
//
// # Documentation
//
// Comprehensive documentation is available in the docs/ directory:
//   - Architecture Overview: docs/architecture/
//   - API Reference: docs/api/
//   - Development Guide: docs/development/
//   - Connector Development: docs/guides/connector-development.md
//   - Performance Guide: docs/guides/performance.md
//   - Configuration Guide: docs/guides/configuration.md
//
// # License
//
// Nebula is released under the Apache 2.0 License.
// See LICENSE file for details.
package nebula