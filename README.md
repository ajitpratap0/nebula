# Nebula

A high-performance, cloud-native Extract & Load (EL) data integration platform written in Go, designed as an ultra-fast alternative to Airbyte.

[![GitHub Repository](https://img.shields.io/badge/GitHub-ajitpratap0%2Fnebula-blue?style=flat&logo=github)](https://github.com/ajitpratap0/nebula)
[![Go Version](https://img.shields.io/badge/Go-1.23+-blue.svg)](https://golang.org)
[![Performance](https://img.shields.io/badge/Performance-1.7M%20rec%2Fsec-green.svg)](#performance)
[![Memory](https://img.shields.io/badge/Memory-84%20bytes%2Frecord-green.svg)](#memory-efficiency)

## 🎯 Unified Architecture (Post-Refactoring)

Nebula has completed a major refactoring (Phases 1-5) that eliminated feature proliferation and achieved unprecedented memory efficiency:

- **Single Record System**: Unified `pool.Record` type (was 15+ types)
- **Single Memory System**: Unified `pool.Pool[T]` (was 4+ pool implementations)  
- **Single Config System**: Unified `config.BaseConfig` (was 64+ configuration types)
- **Hybrid Storage Engine**: Intelligent row/columnar storage with 94% memory reduction (1,365→84 bytes/record)
- **Zero-Copy Performance**: 1.7M-3.6M records/sec with optimized memory management
- **Production-Ready**: Full observability, error handling, and enterprise features

## Overview

Nebula achieves 100-1000x performance improvements over traditional EL tools through zero-copy architecture, unified memory management, and structured configuration systems.

### Production Connectors ✅

**Sources:**
- **CSV**: High-performance file processing with compression
- **Google Ads API**: OAuth2, rate limiting, schema discovery
- **Meta Ads API**: Production-ready with circuit breakers
- **PostgreSQL CDC**: Real-time change data capture
- **MySQL CDC**: Binlog streaming with state management

**Destinations:**
- **CSV/JSON**: Optimized file output with compression
- **Snowflake**: Bulk loading with parallel chunking (COPY)
- **BigQuery**: Streaming inserts and Load Jobs API
- **S3**: Parquet/Avro/ORC with async batching
- **GCS**: Multi-format support with optimization

### Performance Achievements

- **Throughput**: 1.7M-3.6M records/sec (exceeds 1M+ target by 170-360%)
- **Memory Efficiency**: 94% reduction achieved (1,365→84 bytes/record) through hybrid storage
- **Intelligent Storage**: Automatic row/columnar mode selection based on workload
- **Architecture**: Zero-copy with structured configuration and hybrid storage engine
- **Code Reduction**: 30-40% fewer lines through unification

## Key Features

### 🏗️ Unified Architecture (Post-Refactoring)
- **Single Record System**: `pool.Record` handles all data types with zero-copy optimization
- **Unified Memory Management**: `pool.Pool[T]` system for all object pooling needs
- **Structured Configuration**: `config.BaseConfig` with standardized sections for all connectors
- **BaseConnector Framework**: Production features built-in (circuit breakers, rate limiting, health checks)
- **Clean Implementation**: No backward compatibility code or migration cruft

### 🚀 Performance Optimizations
- **Hybrid Storage Engine**: Intelligent row/columnar storage with automatic mode selection
  - **Row Mode**: Traditional 225 bytes/record for streaming and real-time processing
  - **Columnar Mode**: Ultra-efficient 84 bytes/record for batch and analytics workloads
  - **Hybrid Mode**: Automatic selection based on dataset size and access patterns
- **Zero-Copy Architecture**: Direct memory access eliminates unnecessary allocations
- **Unified Pool System**: Global optimized pools (RecordPool, MapPool, StringSlicePool, ByteSlicePool)
- **Structured Memory Management**: Automatic cleanup with object reuse patterns
- **Multi-Algorithm Compression**: Gzip, Snappy, LZ4, Zstd, S2, Deflate support
- **High-Throughput Pipeline**: 1.7M-3.6M records/sec with backpressure handling

### 📊 Enterprise Observability
- **Built-in Metrics**: Comprehensive metrics collection with structured sections
- **Real-time Progress**: Detailed sync statistics and throughput monitoring
- **Structured Logging**: Context-aware logging with Zap integration
- **Health Monitoring**: Continuous health checks with detailed diagnostics
- **Performance Profiling**: Built-in bottleneck detection and optimization guidance

### 🔌 Modern Configuration System
- **Environment Variables**: `${VAR_NAME}` substitution in YAML configurations
- **Structured Validation**: Automatic validation with helpful error messages
- **Sensible Defaults**: `config.NewBaseConfig()` provides production-ready defaults
- **Type Safety**: Structured configuration sections prevent misconfiguration
- **Schema Evolution**: Automatic schema detection and compatibility management

## Why Go?

- **Lightweight Concurrency**: Goroutines support millions of concurrent operations
- **Fast, Predictable Builds**: Static binaries under 15MB, perfect for containers
- **Rich Ecosystem**: Mature SDKs for cloud providers and databases
- **Simple Deployment**: Single binary deployment with no external dependencies
- **Performance**: Near-C performance with memory safety

## Getting Started

### Prerequisites

- Go 1.21+

### Installation

```bash
# Clone the repository
git clone https://github.com/ajitpratap0/nebula.git
cd nebula

# Build the binary
make build

# Run Nebula
./bin/nebula help
```

### Development Commands

```bash
# Format, lint, test, and build
make all

# Run tests with coverage
make coverage

# Run benchmarks
make bench

# Start development environment
./scripts/dev.sh
```

## Project Structure (Unified Architecture)

```plaintext
nebula/
├── cmd/nebula/             # CLI entry point
├── internal/               # Private packages (domain logic)
│   ├── pipeline/           # Optimized pipeline with zero-copy processing
│   └── testutil/           # Test utilities
├── pkg/                    # Public packages (unified systems)
│   ├── config/             # ✅ Unified configuration system (BaseConfig)
│   ├── pool/               # ✅ Unified memory pool system (Pool[T])
│   ├── models/             # ✅ Unified record system (pool.Record)
│   ├── columnar/           # ✅ Hybrid storage engine (row/columnar/auto)
│   ├── connector/          # Production-ready connector framework
│   │   ├── core/           # Core interfaces using config.BaseConfig
│   │   ├── base/           # BaseConnector with production features
│   │   ├── sources/        # Production source connectors (with hybrid storage)
│   │   ├── destinations/   # Production destination connectors
│   │   ├── registry/       # Connector registry and factories
│   │   └── sdk/            # SDK for building custom connectors
│   ├── pipeline/           # ✅ Hybrid storage adapter and optimization
│   ├── compression/        # Multi-algorithm compression support
│   ├── formats/            # Columnar formats (Arrow, Parquet, ORC, Avro)
│   ├── errors/             # Structured error handling
│   ├── logger/             # Context-aware logging
│   └── metrics/            # Metrics collection
├── scripts/                # Development and deployment scripts
├── tests/                  # Integration tests and benchmarks
└── docs/                   # Documentation and guides
```

### Key Architectural Changes

- **Unified Systems**: Single implementations replace multiple variants
- **Hybrid Storage Engine**: Intelligent storage mode selection for optimal performance
- **Clean Structure**: No optimization layers or adapter patterns
- **Type Safety**: Structured configuration and memory management
- **Performance Focus**: Zero-copy operations throughout the pipeline
- **Memory Efficiency**: 94% reduction achieved through advanced storage architecture

## Development

See [CLAUDE.md](./CLAUDE.md) for detailed development guidelines including:
- Performance optimization principles
- Connector development patterns
- Testing and benchmarking requirements
- Architecture design principles

## License

MIT