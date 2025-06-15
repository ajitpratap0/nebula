# Package Overview

This document provides an overview of all packages in the Nebula project and their responsibilities.

## Core Packages (`pkg/`)

### pkg/connector
The connector framework provides interfaces and implementations for data sources and destinations.

**Sub-packages:**
- `core` - Core interfaces (Source, Destination)
- `base` - BaseConnector with production features
- `sources` - Source connector implementations
- `destinations` - Destination connector implementations
- `registry` - Dynamic connector registration
- `sdk` - Utilities for building custom connectors

**Key Types:**
- `core.Source` - Interface for data sources
- `core.Destination` - Interface for data destinations
- `base.BaseConnector` - Base implementation with circuit breakers, rate limiting

### pkg/pool
High-performance object pooling system for zero-allocation patterns.

**Key Types:**
- `Pool[T]` - Generic pool implementation
- `Record` - Unified record type
- Global pools: `RecordPool`, `MapPool`, `StringSlicePool`, `ByteSlicePool`

**Usage:**
```go
record := pool.GetRecord()
defer record.Release()
```

### pkg/columnar
Revolutionary hybrid storage engine achieving 94% memory reduction.

**Key Components:**
- `ColumnStore` - Columnar storage implementation
- `DirectAdapter` - Zero-copy CSV conversion
- `TypeOptimizer` - Automatic type detection
- `CompressionManager` - Multi-algorithm compression

**Storage Modes:**
- Row: 225 bytes/record (streaming)
- Columnar: 84 bytes/record (batch)
- Hybrid: Automatic selection

### pkg/config
Unified configuration system for all connectors.

**Key Types:**
- `BaseConfig` - Standard configuration structure
- `PerformanceConfig` - Performance settings
- `TimeoutConfig` - Timeout settings
- `ReliabilityConfig` - Retry and circuit breaker settings

**Features:**
- Environment variable substitution
- Automatic validation
- Sensible defaults

### pkg/pipeline
Storage abstraction layer with intelligent mode selection.

**Key Types:**
- `StorageAdapter` - Unified storage interface
- `StorageMode` - Row, columnar, or hybrid modes
- `Schema` - Schema definition for optimization

### pkg/errors
Structured error handling with context.

**Key Types:**
- `Error` - Rich error type with metadata
- `ErrorType` - Categorized error types
- Error wrapping and context functions

### pkg/logger
Context-aware logging with Zap.

**Features:**
- Structured logging
- Performance optimized
- Context propagation
- Multiple output formats

### pkg/metrics
Metrics collection and reporting.

**Features:**
- Prometheus integration
- Custom metrics
- Performance tracking
- Resource monitoring

### pkg/compression
Multi-algorithm compression support.

**Supported Algorithms:**
- Gzip - Standard compression
- LZ4 - Fast compression
- Zstd - Best compression ratio
- Snappy - Fastest compression
- S2 - Optimized Snappy

### pkg/formats
Columnar format support for data lakes.

**Supported Formats:**
- Apache Arrow
- Apache Parquet
- Apache ORC
- Apache Avro

## Internal Packages (`internal/`)

### internal/pipeline
Pipeline orchestration and execution engine.

**Key Components:**
- Pipeline coordinator
- Backpressure handling
- Error recovery
- Progress tracking

### internal/testutil
Testing utilities and helpers.

**Features:**
- Test fixtures
- Mock implementations
- Performance benchmarks
- Integration test helpers

## Command Package (`cmd/`)

### cmd/nebula
CLI entry point and command definitions.

**Commands:**
- `pipeline` - Run data pipelines
- `validate` - Validate configurations
- `list` - List available connectors
- `version` - Show version information

## Package Dependencies

```
cmd/nebula
    ├── pkg/connector
    │   ├── pkg/config
    │   ├── pkg/pool
    │   ├── pkg/errors
    │   └── pkg/metrics
    ├── pkg/pipeline
    │   ├── pkg/columnar
    │   └── pkg/pool
    └── internal/pipeline
        ├── pkg/logger
        └── pkg/errors
```

## Import Guidelines

### For Connector Development
```go
import (
    "github.com/ajitpratap0/nebula/pkg/connector/core"
    "github.com/ajitpratap0/nebula/pkg/connector/base"
    "github.com/ajitpratap0/nebula/pkg/config"
    "github.com/ajitpratap0/nebula/pkg/pool"
    "github.com/ajitpratap0/nebula/pkg/errors"
)
```

### For Pipeline Development
```go
import (
    "github.com/ajitpratap0/nebula/pkg/pipeline"
    "github.com/ajitpratap0/nebula/pkg/columnar"
    "github.com/ajitpratap0/nebula/internal/pipeline"
)
```

### For Testing
```go
import (
    "github.com/ajitpratap0/nebula/internal/testutil"
    "github.com/ajitpratap0/nebula/pkg/config"
)
```

## Package Design Principles

1. **Clear Separation**: Public APIs in `pkg/`, implementation details in `internal/`
2. **Zero Dependencies**: Minimize external dependencies
3. **Interface First**: Define interfaces before implementations
4. **Testability**: All packages must be easily testable
5. **Performance**: Every package must meet performance targets

## Version Compatibility

All packages follow semantic versioning:
- Major version changes indicate breaking API changes
- Minor version changes add functionality (backward compatible)
- Patch version changes are bug fixes only

Current version: v0.1.0 (pre-release)