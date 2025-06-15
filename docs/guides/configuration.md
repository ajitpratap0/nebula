# Configuration Guide

This guide covers Nebula's unified configuration system and how to configure connectors for optimal performance.

## Overview

Nebula uses a unified configuration system centered around `BaseConfig`. This structure provides:
- Consistent configuration across all connectors
- Environment variable substitution
- Automatic validation
- Sensible production defaults
- Structured organization

## Configuration Structure

### BaseConfig

All connectors inherit from `BaseConfig`:

```yaml
# Basic connector information
name: my-connector
type: source           # or destination
version: "1.0.0"

# Performance settings
performance:
  batch_size: 10000
  prefetch_size: 3
  workers: 4
  max_concurrency: 100
  buffer_size: 1048576
  queue_size: 1000
  limit: 0
  streaming_mode: false

# Timeout settings
timeouts:
  connection: 30s
  request: 60s
  read: 30s
  write: 30s
  idle: 5m
  keepalive: 30s
  shutdown_grace: 30s

# Reliability settings
reliability:
  max_retries: 3
  retry_delay: 1s
  retry_max_delay: 30s
  retry_multiplier: 2.0
  retry_jitter: true
  circuit_breaker:
    enabled: true
    threshold: 5
    timeout: 60s
    half_open_requests: 3
  rate_limit:
    enabled: false
    requests_per_second: 1000
    burst: 100

# Security settings
security:
  tls:
    enabled: false
    cert_file: ""
    key_file: ""
    ca_file: ""
    insecure_skip_verify: false
  auth:
    type: ""          # basic, bearer, oauth2, api_key
    username: ""
    password: ""
    token: ""
    api_key: ""
  encryption:
    enabled: false
    algorithm: "AES-256-GCM"

# Observability settings
observability:
  metrics:
    enabled: true
    port: 9090
    path: "/metrics"
    interval: 10s
  logging:
    level: "info"     # debug, info, warn, error
    format: "json"    # json, text
    output: "stdout"  # stdout, file
    file_path: ""
  tracing:
    enabled: false
    endpoint: ""
    sample_rate: 0.1

# Memory settings
memory:
  pool_size: 10000
  max_memory: 0       # 0 = unlimited
  gc_interval: 5m
  pre_warm_pools: true

# Advanced settings
advanced:
  storage_mode: "hybrid"    # row, columnar, hybrid
  compression: ""           # gzip, lz4, zstd, snappy
  compression_level: -1     # -1 = default
  enable_profiling: false
  profile_port: 6060
  debug_mode: false
  experimental:
    enable_jit: false
    enable_simd: false
```

## Environment Variables

### Variable Substitution

Use `${VAR_NAME}` syntax for environment variables:

```yaml
security:
  auth:
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}

# With defaults
performance:
  workers: ${WORKERS:-4}
  batch_size: ${BATCH_SIZE:-10000}
```

### Common Environment Variables

```bash
# Performance
export NEBULA_WORKERS=8
export NEBULA_BATCH_SIZE=50000
export NEBULA_STORAGE_MODE=columnar

# Security
export DB_USERNAME=myuser
export DB_PASSWORD=secretpass
export API_TOKEN=my-api-token

# Observability
export NEBULA_LOG_LEVEL=debug
export NEBULA_METRICS_ENABLED=true
```

## Configuration Examples

### High-Throughput Configuration

Optimized for maximum throughput:

```yaml
name: high-throughput-source
type: source

performance:
  batch_size: 50000        # Large batches
  prefetch_size: 5         # More prefetching
  workers: 16              # More workers
  max_concurrency: 200     # Higher concurrency
  buffer_size: 10485760    # 10MB buffers
  queue_size: 5000         # Larger queues

timeouts:
  connection: 60s          # Longer timeouts
  request: 300s            # 5 minutes for large batches

memory:
  pool_size: 50000         # Larger pools
  pre_warm_pools: true     # Pre-warm for performance

advanced:
  storage_mode: "columnar" # Force columnar for efficiency
  compression: "lz4"       # Fast compression
```

### Low-Latency Configuration

Optimized for minimal latency:

```yaml
name: low-latency-source
type: source

performance:
  batch_size: 100          # Small batches
  prefetch_size: 1         # Minimal prefetching
  workers: 4               # Moderate workers
  streaming_mode: true     # Force streaming

timeouts:
  connection: 5s           # Quick timeouts
  request: 10s             # Fast failures

advanced:
  storage_mode: "row"      # Row mode for low latency
  compression: ""          # No compression
```

### Reliable Configuration

Optimized for reliability:

```yaml
name: reliable-destination
type: destination

reliability:
  max_retries: 5
  retry_delay: 2s
  retry_max_delay: 60s
  retry_multiplier: 2.0
  circuit_breaker:
    enabled: true
    threshold: 3           # Strict threshold
    timeout: 120s          # Longer recovery
  rate_limit:
    enabled: true
    requests_per_second: 500  # Conservative rate
    burst: 50

timeouts:
  shutdown_grace: 60s      # Graceful shutdown

observability:
  logging:
    level: "debug"         # Detailed logging
  metrics:
    enabled: true
    interval: 5s           # Frequent metrics
```

## Connector-Specific Configuration

### Source Connectors

#### CSV Source

```yaml
name: csv-source
type: source

# CSV-specific settings
file_path: "/data/input.csv"
delimiter: ","
has_header: true
skip_lines: 0
encoding: "UTF-8"

# Inherit from BaseConfig
performance:
  batch_size: 10000
  streaming_mode: false
```

#### PostgreSQL CDC Source

```yaml
name: postgres-cdc
type: source

# PostgreSQL-specific settings
host: localhost
port: 5432
database: mydb
username: ${DB_USER}
password: ${DB_PASSWORD}
replication_slot: nebula_slot
publication: nebula_pub
tables:
  - users
  - orders

# Connection pooling
performance:
  max_concurrency: 20      # Max connections
  workers: 4               # Min connections = workers/4
```

#### API Source (Google Ads)

```yaml
name: google-ads-source
type: source

# API-specific settings
customer_id: "123-456-7890"
developer_token: ${GOOGLE_ADS_DEV_TOKEN}
refresh_token: ${GOOGLE_ADS_REFRESH_TOKEN}
client_id: ${GOOGLE_ADS_CLIENT_ID}
client_secret: ${GOOGLE_ADS_CLIENT_SECRET}

# API rate limiting
reliability:
  rate_limit:
    enabled: true
    requests_per_second: 10
    burst: 20
```

### Destination Connectors

#### Snowflake Destination

```yaml
name: snowflake-dest
type: destination

# Snowflake-specific settings
account: myaccount
warehouse: COMPUTE_WH
database: MYDB
schema: PUBLIC
table: DATA_LOAD
stage: "@~/nebula_stage"

# Bulk loading optimization
performance:
  batch_size: 100000       # Large batches for COPY
  workers: 8               # Parallel chunks

advanced:
  storage_mode: "columnar" # Columnar for efficiency
  compression: "gzip"      # Snowflake prefers gzip
```

#### BigQuery Destination

```yaml
name: bigquery-dest
type: destination

# BigQuery-specific settings
project_id: my-project
dataset_id: my_dataset
table_id: my_table
location: US
write_disposition: WRITE_APPEND
create_disposition: CREATE_IF_NEEDED

# Streaming vs batch
performance:
  batch_size: 50000        # For load jobs
  streaming_mode: false    # Use load jobs

memory:
  max_memory: 1073741824   # 1GB limit for payloads
```

## Loading Configuration

### From File

```go
// Load from YAML file
var config MyConnectorConfig
err := config.Load("config.yaml", &config)
if err != nil {
    log.Fatal(err)
}

// Validate
if err := config.Validate(); err != nil {
    log.Fatal(err)
}
```

### Programmatic Configuration

```go
// Create with defaults
config := config.NewBaseConfig("my-connector", "source")

// Override specific settings
config.Performance.BatchSize = 50000
config.Performance.Workers = 8
config.Advanced.StorageMode = "columnar"

// Create connector
connector := NewConnector(config)
```

### Configuration Validation

```go
// Custom validation
type MyConfig struct {
    config.BaseConfig `yaml:",inline"`
    DatabaseURL string `yaml:"database_url" validate:"required,url"`
}

func (c *MyConfig) Validate() error {
    // Base validation
    if err := c.BaseConfig.Validate(); err != nil {
        return err
    }
    
    // Custom validation
    if !strings.HasPrefix(c.DatabaseURL, "postgres://") {
        return errors.New("database_url must be a PostgreSQL URL")
    }
    
    return nil
}
```

## Best Practices

### 1. Use Environment Variables for Secrets

```yaml
# Good
security:
  auth:
    password: ${DB_PASSWORD}

# Bad
security:
  auth:
    password: "hardcoded-password"
```

### 2. Set Appropriate Batch Sizes

- **Streaming**: 100-1,000 records
- **Batch Processing**: 10,000-100,000 records
- **Bulk Loading**: 100,000+ records

### 3. Configure Timeouts Appropriately

```yaml
timeouts:
  connection: 30s      # Initial connection
  request: 60s         # Individual requests
  idle: 5m            # Keep-alive
  shutdown_grace: 30s  # Graceful shutdown
```

### 4. Enable Monitoring

```yaml
observability:
  metrics:
    enabled: true
  logging:
    level: "info"      # "debug" for troubleshooting
```

### 5. Use Storage Modes Wisely

```yaml
# Automatic optimization (recommended)
advanced:
  storage_mode: "hybrid"

# Force specific mode only when needed
advanced:
  storage_mode: "columnar"  # For known batch workloads
```

## Performance Tuning

### Memory Configuration

```yaml
# For high-throughput scenarios
memory:
  pool_size: 50000         # Larger pools
  max_memory: 8589934592   # 8GB limit
  pre_warm_pools: true     # Avoid cold start

# For memory-constrained environments
memory:
  pool_size: 1000          # Smaller pools
  max_memory: 1073741824   # 1GB limit
  gc_interval: 1m          # Aggressive GC
```

### Concurrency Configuration

```yaml
# CPU-bound workloads
performance:
  workers: 16              # 2x CPU cores
  max_concurrency: 200

# I/O-bound workloads
performance:
  workers: 32              # Higher multiplier
  max_concurrency: 500
```

### Storage Optimization

```yaml
# Analytics workloads
advanced:
  storage_mode: "columnar"
  compression: "zstd"
  compression_level: 3

# Real-time workloads
advanced:
  storage_mode: "row"
  compression: ""          # No compression
```

## Troubleshooting

### Debug Configuration

```yaml
# Enable debug mode
observability:
  logging:
    level: "debug"
advanced:
  debug_mode: true
  enable_profiling: true
```

### Common Issues

1. **Environment variable not substituted**
   - Check variable is exported
   - Verify syntax: `${VAR_NAME}`

2. **Validation errors**
   - Check required fields
   - Verify data types
   - Review constraints

3. **Performance issues**
   - Review batch sizes
   - Check worker counts
   - Monitor storage mode

4. **Memory issues**
   - Reduce pool sizes
   - Enable memory limits
   - Use columnar storage

## Configuration Reference

See the complete configuration reference in the [API documentation](../api/configuration.md).

## Next Steps

- Review [connector-specific examples](connector-development.md)
- Check [performance tuning guide](performance.md)
- See [troubleshooting guide](troubleshooting.md)