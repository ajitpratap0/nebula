# Nebula API Reference

This directory contains the complete API documentation for the Nebula data integration platform.

## Package Documentation

### Core Packages

- **[pkg/connector](connectors.md)** - Connector framework for sources and destinations
- **[pkg/pool](../../../pkg/pool/doc.go)** - High-performance object pooling system  
- **[pkg/columnar](../../../pkg/columnar/doc.go)** - Hybrid storage engine
- **[pkg/config](../../../pkg/config/doc.go)** - Configuration management
- **[pkg/pipeline](../../../pkg/pipeline/doc.go)** - Storage abstraction layer

### Supporting Packages

- **[pkg/errors](errors.md)** - Structured error handling
- **[pkg/logger](logger.md)** - Structured logging with Zap
- **[pkg/metrics](metrics.md)** - Metrics collection and reporting
- **[pkg/compression](compression.md)** - Multi-algorithm compression
- **[pkg/formats](formats.md)** - Columnar format support

## Quick Links

- [Package Overview](packages.md) - High-level package descriptions
- [Core Interfaces](interfaces.md) - Key interfaces and contracts
- [Configuration Reference](configuration.md) - Complete configuration options

## Generating Documentation

To generate godoc locally:

```bash
# Install godoc if needed
go install golang.org/x/tools/cmd/godoc@latest

# Start godoc server
godoc -http=:6060

# View documentation at
# http://localhost:6060/pkg/github.com/ajitpratap0/nebula/
```

## API Stability

Nebula follows semantic versioning:

- **Stable APIs**: All types and functions in `pkg/` are stable
- **Internal APIs**: Types in `internal/` are not stable and may change
- **Experimental**: Features marked experimental may change

Current version: v0.1.0 (pre-release)

## Import Paths

All packages should be imported using the full module path:

```go
import (
    "github.com/ajitpratap0/nebula/pkg/connector/core"
    "github.com/ajitpratap0/nebula/pkg/config"
    "github.com/ajitpratap0/nebula/pkg/pool"
)
```

## Code Examples

Each package documentation includes runnable examples. You can run them with:

```bash
# Run all examples
go test -run Example

# Run specific example
go test -run ExampleNewConnector
```

## Contributing

When adding new APIs:

1. Add comprehensive godoc comments
2. Include at least one example
3. Document any breaking changes
4. Update interface documentation
5. Add to package overview

See the [Development Guide](../development/contributing.md) for more details.