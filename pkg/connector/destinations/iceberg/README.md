# Iceberg Destination Connector

The Iceberg destination connector enables high-performance data ingestion into Apache Iceberg tables. It supports batch writing with optimized Arrow-based memory management and configurable buffer pools.

## Features

- **High Performance**: Arrow-based zero-copy architecture with builder pooling
- **Batch Processing**: Efficient batch writes with configurable sizes
- **Memory Optimization**: Configurable buffer multipliers for string and list data
- **Schema Validation**: Automatic schema validation and conversion
- **Multiple Catalogs**: Support for Nessie, REST, and other Iceberg catalogs
- **S3 Compatible**: Works with S3, MinIO, and other object storage systems

## Quick Start

### Prerequisites

- Apache Iceberg catalog (Nessie, REST catalog, etc.)
- Object storage (S3, MinIO, etc.)
- Target Iceberg table must exist

### Basic Usage

```bash
go run cmd/nebula/main.go run \
  --source examples/configs/csv-source.json \
  --destination examples/configs/iceberg-destination.json \
  --batch-size 30000 \
  --flush-interval 10s \
  --log-level info
```

## Configuration

### Complete Configuration Example

```json
{
  "name": "iceberg-writer",
  "type": "iceberg",
  "version": "1.0",
  "performance": {
    "batch_size": 30000,
    "workers": 4,
    "enable_streaming": true,
    "flush_interval": "10s"
  },
  "memory": {
    "enable_pools": true,
    "string_data_multiplier": 32,
    "list_element_multiplier": 5,
    "buffer_pool_size": 100
  },
  "timeouts": {
    "request": "30s",
    "connection": "10s"
  },
  "reliability": {
    "retry_attempts": 3,
    "retry_delay": "1s",
    "circuit_breaker": true
  },
  "security": {
    "credentials": {
      "catalog_uri": "http://localhost:19120/api/v1",
      "warehouse": "s3://warehouse/",
      "catalog_name": "nessie",
      "database": "analytics",
      "table": "events",
      "branch": "main",
      "prop_s3.region": "us-east-1",
      "prop_s3.endpoint": "http://localhost:9000",
      "prop_s3.access-key-id": "admin",
      "prop_s3.secret-access-key": "password",
      "prop_s3.path-style-access": "true"
    }
  }
}
```

### Required Configuration Fields

| Field | Description | Example |
|-------|-------------|---------|
| `catalog_uri` | Iceberg catalog endpoint | `http://localhost:19120/api/v1` |
| `warehouse` | Warehouse location (S3 path) | `s3://data-warehouse/` |
| `catalog_name` | Name of the catalog | `nessie` |
| `database` | Database/namespace name | `analytics` |
| `table` | Target table name | `events` |
| `branch` | Branch name (for versioned catalogs) | `main` |

### S3 Configuration

All S3 properties are prefixed with `prop_s3.`:

| Property | Description | Example |
|----------|-------------|---------|
| `prop_s3.region` | AWS region | `us-east-1` |
| `prop_s3.endpoint` | S3 endpoint URL | `http://localhost:9000` |
| `prop_s3.access-key-id` | Access key ID | `admin` |
| `prop_s3.secret-access-key` | Secret access key | `password` |
| `prop_s3.path-style-access` | Use path-style access | `true` |

### Memory Configuration

Configure buffer sizes for optimal performance:

| Field | Description | Default | Recommended Range |
|-------|-------------|---------|-------------------|
| `string_data_multiplier` | Chars per string field | 32 | 16-64 |
| `list_element_multiplier` | Elements per list field | 5 | 3-10 |
| `buffer_pool_size` | Number of pooled builders | 100 | 50-200 |

## Performance Tuning

### Batch Size Optimization

```bash
# Small batches (low memory, higher overhead)
--batch-size 1000

# Medium batches (balanced)
--batch-size 10000

# Large batches (high throughput, more memory)
--batch-size 50000
```

### Memory Buffer Tuning

For string-heavy data:
```json
{
  "memory": {
    "string_data_multiplier": 64
  }
}
```

For list/array-heavy data:
```json
{
  "memory": {
    "list_element_multiplier": 10
  }
}
```

### Concurrency Settings

```json
{
  "performance": {
    "workers": 8,
    "max_concurrency": 16
  }
}
```

## Development Setup

### Local Development with Docker Compose

1. **Start Nessie + MinIO stack**:
```bash
docker-compose up -d nessie minio
```

2. **Create MinIO bucket**:
```bash
docker exec nebula-minio-1 mc mb local/warehouse
```

3. **Create test table** (using Spark or other tool):
```sql
CREATE TABLE nessie.analytics.events (
  id BIGINT,
  event_name STRING,
  timestamp TIMESTAMP,
  properties MAP<STRING, STRING>
) USING ICEBERG
LOCATION 's3://warehouse/analytics/events'
```

### Running Tests

```bash
# Unit tests
go test ./pkg/connector/destinations/iceberg/...

# Integration tests with real Iceberg
go test -tags=integration ./tests/integration/iceberg/...

# Benchmarks
go test -bench=. ./pkg/connector/destinations/iceberg/...
```

## Common Commands

### CSV to Iceberg
```bash
go run cmd/nebula/main.go run \
  --source examples/configs/csv-source.json \
  --destination examples/configs/iceberg-destination.json \
  --batch-size 30000 \
  --workers 4 \
  --log-level info
```

### With Performance Monitoring
```bash
go run cmd/nebula/main.go run \
  --source examples/configs/csv-source.json \
  --destination examples/configs/iceberg-destination.json \
  --batch-size 30000 \
  --flush-interval 10s \
  --enable-metrics \
  --log-level info
```

### Memory-Optimized Run
```bash
go run cmd/nebula/main.go run \
  --source examples/configs/csv-source.json \
  --destination examples/configs/iceberg-destination.json \
  --batch-size 10000 \
  --memory-limit-mb 512 \
  --workers 2 \
  --log-level warn
```

## Architecture

### Component Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CSV Source    │───▶│  Nebula Pipeline  │───▶│ Iceberg Dest    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │                          │
                              ▼                          ▼
                       ┌─────────────┐           ┌──────────────┐
                       │ Batch Stream│           │ Arrow Builders│
                       └─────────────┘           └──────────────┘
                                                         │
                                                         ▼
                                                ┌─────────────────┐
                                                │ Iceberg Catalog │
                                                └─────────────────┘
```

### Key Components

- **IcebergDestination**: Main connector implementation
- **ArrowBuilderPool**: High-performance builder pooling
- **CatalogProvider**: Abstraction for different catalog types
- **SchemaValidator**: Schema conversion and validation
- **BufferConfig**: Configurable memory optimization

## Troubleshooting

### Common Issues

1. **Schema Mismatch**
```
Error: Schema validation failed
Solution: Ensure source data schema matches target Iceberg table
```

2. **S3 Connection Failed**
```
Error: Failed to connect to S3
Solution: Check S3 credentials and endpoint configuration
```

3. **Memory Issues**
```
Error: Out of memory
Solution: Reduce batch_size or adjust memory multipliers
```

### Debug Mode

Enable detailed logging:
```json
{
  "observability": {
    "log_level": "debug",
    "enable_logging": true
  },
  "advanced": {
    "debug": true
  }
}
```

### Performance Monitoring

```bash
# Enable metrics
--enable-metrics

# Memory profiling
go tool pprof http://localhost:6060/debug/pprof/heap

# CPU profiling
go tool pprof http://localhost:6060/debug/pprof/profile
```

## Supported Data Types

| Iceberg Type | Arrow Type | Go Type | Notes |
|--------------|------------|---------|-------|
| boolean | Boolean | bool | Direct mapping |
| int | Int32 | int32 | 32-bit integers |
| long | Int64 | int64 | 64-bit integers |
| float | Float32 | float32 | Single precision |
| double | Float64 | float64 | Double precision |
| string | String | string | UTF-8 strings |
| timestamp | Timestamp | time.Time | Microsecond precision |
| date | Date32 | time.Time | Date only |
| list<T> | List<T> | []T | Arrays/lists |
| struct | Struct | map[string]any | Nested objects |

## Contributing

1. Follow existing code patterns
2. Add tests for new features
3. Update documentation
4. Run linting: `make lint`
5. Run tests: `make test`

## License

This connector is part of the Nebula project and follows the same license terms.