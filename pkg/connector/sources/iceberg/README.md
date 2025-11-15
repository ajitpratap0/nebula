# Iceberg Source Connector

High-performance source connector for reading data from Apache Iceberg tables via Nessie catalog.

## Features

- ✅ Streaming and batch reading modes
- ✅ Incremental sync with snapshot management
- ✅ State persistence and position tracking
- ✅ Arrow-based data conversion
- ✅ S3/MinIO storage support
- ✅ Comprehensive error handling and health checks

## Prerequisites

### Required Services

```bash
# 1. Nessie Catalog
docker run -d -p 19120:19120 ghcr.io/projectnessie/nessie:latest

# 2. MinIO (S3-compatible storage)
docker run -d -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

### Go Dependencies

```bash
go get github.com/apache/iceberg-go
go get github.com/apache/arrow-go/v18
```

## Configuration

Create a configuration file (e.g., `iceberg-source.json`):

```json
{
  "name": "iceberg-source",
  "type": "iceberg",
  "security": {
    "credentials": {
      "catalog_type": "nessie",
      "catalog_uri": "http://localhost:19120/api/v1",
      "catalog_name": "demo",
      "warehouse": "s3://warehouse",
      "database": "your_database",
      "table": "your_table",
      "branch": "main",
      "prop_s3.region": "us-west-2",
      "prop_s3.endpoint": "http://localhost:9000",
      "prop_s3.access-key-id": "minioadmin",
      "prop_s3.secret-access-key": "minioadmin"
    }
  },
  "performance": {
    "batch_size": 10000,
    "buffer_size": 1000
  }
}
```

## Running the Connector

### Using Nebula CLI

```bash
# Run with Iceberg source and any destination
go run cmd/nebula/main.go run \
  --source examples/configs/iceberg-source.json \
  --destination examples/configs/iceberg-destination.json \
  --log-level info \
  --batch-size 10000 \
  --flush-interval 10s
```

### Programmatic Usage

```go
package main

import (
    "context"
    "github.com/ajitpratap0/nebula/pkg/config"
    "github.com/ajitpratap0/nebula/pkg/connector/sources/iceberg"
)

func main() {
    cfg := &config.BaseConfig{
        // ... configuration
    }

    source, _ := iceberg.NewIcebergSource(cfg)
    source.Initialize(context.Background(), cfg)
    defer source.Close(context.Background())

    // Read records
    stream, _ := source.Read(context.Background())
    for record := range stream.Records {
        // Process record
        record.Release()
    }
}
```

## Testing

### Unit Tests

Run all unit tests with coverage:

```bash
# Run tests
go test -v ./pkg/connector/sources/iceberg/...

# With coverage report
go test -v -coverprofile=coverage.out ./pkg/connector/sources/iceberg/...
go tool cover -html=coverage.out

# Specific test
go test -v -run TestIcebergSource_ParseConfig ./pkg/connector/sources/iceberg/...
```

**Current Coverage: 40.8%** (unit tests focus on business logic; integration points covered separately)

### Integration Tests

Requires running Nessie and MinIO services:

```bash
# Set environment variables
export INTEGRATION_TESTS=true
export NESSIE_URI=http://localhost:19120/api/v1
export S3_ENDPOINT=http://localhost:9000
export DATABASE=test_db
export TABLE=test_table

# Run integration tests
go test -v ./tests/integration/iceberg_source_integration_test.go
```

### Benchmark Tests

Performance benchmarking:

```bash
# Set environment variables
export BENCHMARK_TESTS=true
export NESSIE_URI=http://localhost:19120/api/v1
export DATABASE=bench_db
export TABLE=bench_table

# Run all benchmarks
go test -bench=. -benchmem ./tests/benchmarks/iceberg_source_bench_test.go

# Specific benchmark
go test -bench=BenchmarkIcebergSource_ReadBatches ./tests/benchmarks/
```

## Test Files

| File | Purpose | Tests |
|------|---------|-------|
| `iceberg_source_test.go` | Core source logic | Configuration, state, position, capabilities |
| `nessie_catalog_test.go` | Catalog operations | URI handling, S3 config, properties |
| `data_file_reader_test.go` | Data reading | Arrow types, conversion, extraction |
| `manifest_reader_test.go` | Manifest reading | Snapshot handling, state tracking |
| `iceberg_source_integration_test.go` | End-to-end flows | Full workflow with real services |
| `iceberg_source_bench_test.go` | Performance | Throughput, latency, memory usage |

## Performance Metrics

Typical throughput (varies by data size and network):
- **Single record streaming**: 50K-100K records/sec
- **Batch reading (10K batch)**: 200K-500K records/sec
- **Memory usage**: ~100-200 bytes per record

## Troubleshooting

### Common Issues

**1. Connection refused to Nessie**
```bash
# Verify Nessie is running
curl http://localhost:19120/api/v2/config
```

**2. S3 access errors**
```bash
# Check MinIO credentials
mc alias set myminio http://localhost:9000 minioadmin minioadmin
mc ls myminio/warehouse
```

**3. Table not found**
```bash
# Verify table exists in Nessie catalog
# Check catalog_name, database, and table in config
```

### Debug Logging

Enable detailed logging:
```bash
go run cmd/nebula/main.go run \
  --source iceberg-source.json \
  --destination iceberg-destination.json \
  --log-level debug
```

## Architecture

```
IcebergSource
├── NessieCatalog (REST API client)
├── SnapshotManager (version control)
├── ManifestReader (metadata processing)
└── DataFileReader (Parquet → Arrow → Nebula records)
```

## Limitations

- Currently supports Nessie catalog only (Hive/Glue coming soon)
- Read-only operations (write via destination connector)
- No real-time CDC (incremental snapshots only)

## Contributing

When adding tests:
1. Add unit tests for business logic
2. Add integration tests for I/O operations
3. Add benchmarks for performance-critical paths
4. Maintain >80% coverage for new code

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Nessie Catalog](https://projectnessie.org/)
- [Arrow Go Documentation](https://pkg.go.dev/github.com/apache/arrow-go)
