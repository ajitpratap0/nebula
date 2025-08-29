// Package connector provides a comprehensive framework for building high-performance
// data connectors in Nebula. It implements a production-ready architecture with
// enterprise features including circuit breakers, rate limiting, health monitoring,
// and automatic retries.
//
// # Architecture Overview
//
// The connector package is organized into several sub-packages:
//
//   - core: Defines the fundamental interfaces (Source, Destination) that all
//     connectors must implement. These interfaces provide a consistent API for
//     data extraction and loading operations.
//
//   - base: Provides BaseConnector, a foundation that implements common functionality
//     like circuit breakers, rate limiting, metrics collection, and health checks.
//     All production connectors should embed BaseConnector.
//
//   - sources: Contains source connector implementations for various data systems
//     including databases (PostgreSQL, MySQL), APIs (Google Ads, Meta Ads), and
//     file formats (CSV). Each source supports streaming, CDC, and batch operations.
//
//   - destinations: Contains destination connector implementations for data warehouses
//     (Snowflake, BigQuery), object storage (S3, GCS), and file formats (CSV, JSON).
//     Each destination supports bulk loading, streaming inserts, and schema evolution.
//
//   - registry: Implements a factory pattern for dynamic connector discovery and
//     instantiation. Connectors self-register during initialization.
//
//   - sdk: Provides utilities and helpers for building custom connectors, including
//     retry logic, rate limiting, and common patterns.
//
// # Core Concepts
//
// Unified Configuration: All connectors use config.BaseConfig which provides
// standardized configuration sections for performance, timeouts, reliability,
// security, observability, and memory management.
//
// Memory Efficiency: Connectors integrate with the pool package for zero-allocation
// patterns and the hybrid storage engine for optimal memory usage (84 bytes/record
// in columnar mode).
//
// Production Features:
//   - Circuit breakers for automatic failure detection and recovery
//   - Adaptive rate limiting with token bucket algorithm
//   - Continuous health monitoring with detailed metrics
//   - Automatic schema detection and evolution
//   - Exactly-once semantics for supported destinations
//   - Comprehensive error handling with structured errors
//
// # Example Usage
//
// Creating a source connector:
//
//	config := &config.BaseConfig{
//		Performance: config.PerformanceConfig{
//			BatchSize: 10000,
//			Workers:   4,
//		},
//	}
//
//	source, err := registry.GetSourceFactory("postgresql")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	conn, err := source(config)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Use the connector
//	records, err := conn.Read(ctx)
//
// Creating a destination connector:
//
//	dest, err := registry.GetDestinationFactory("snowflake")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	conn, err := dest(config)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Write records with automatic bulk loading
//	err = conn.Write(ctx, records)
//
// # Performance Considerations
//
// Connectors are designed for extreme performance:
//   - Zero-copy architecture throughout the pipeline
//   - Intelligent hybrid storage with automatic mode selection
//   - Connection pooling with configurable limits
//   - Parallel processing with work stealing
//   - Backpressure handling to prevent OOMs
//
// # Best Practices
//
// 1. Always use BaseConnector as the foundation for new connectors
// 2. Implement comprehensive health checks for production readiness
// 3. Use structured errors from the errors package
// 4. Leverage the pool package for memory efficiency
// 5. Implement proper cleanup in Close() methods
// 6. Add metrics for observability
// 7. Handle context cancellation properly
// 8. Test with large datasets to verify performance
//
// For more details on building custom connectors, see the SDK documentation.
package connector
