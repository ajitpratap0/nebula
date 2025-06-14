# Performance Tuning Guide

## Overview

Performance optimization strategies for Nebula connectors and pipelines, including hybrid storage configuration for optimal memory efficiency.

## Key Configuration Parameters

### Hybrid Storage Configuration

```yaml
# Storage mode selection
advanced:
  storage_mode: hybrid           # Options: row, columnar, hybrid
  streaming_mode: false          # Set to true for real-time processing

# Performance tuning
performance:
  batch_size: 10000             # Records per batch (affects mode selection)
  buffer_size: 100000           # Channel buffer size
  streaming_mode: false         # Batch vs streaming optimization
```

**Storage Mode Guidelines:**
- **Row Mode**: Use for streaming workloads, small datasets (<10K records), real-time processing
- **Columnar Mode**: Use for batch processing, large datasets (>10K records), analytics workloads  
- **Hybrid Mode**: Automatic selection based on dataset size and access patterns (recommended)

### Connector Settings

```yaml
# Batch and buffer sizes
batch_size: 5000        # Records per batch
buffer_size: 20000      # Channel buffer size
flush_interval: 1000    # Records between flushes

# Concurrency
max_concurrency: 10     # Parallel workers
request_timeout: 60s    # API request timeout

# Rate limiting
rate_limit: 1000        # Requests per second (0 = unlimited)

# Memory optimization
enable_compression: true
compression_threshold: 2048  # Bytes
```

### BaseConnector Optimization

All production connectors include automatic optimizations:
- **Memory pools** for object reuse
- **Circuit breakers** for fault tolerance  
- **Rate limiting** for API protection
- **Compression** for large payloads
- **Health monitoring** for reliability

## Performance Profiling

### Built-in Profiling

```bash
# Run with CPU profiling
go test -bench=. -cpuprofile=cpu.prof

# Analyze profile
go tool pprof cpu.prof

# Memory profiling  
go test -bench=. -memprofile=mem.prof
go tool pprof mem.prof
```

### Key Metrics to Monitor

- **Throughput**: Records processed per second
- **Latency**: P99 processing time per record
- **Memory**: Heap usage and GC frequency
- **CPU**: Utilization across cores
- **Errors**: Error rate and recovery time

## Memory Optimization

### Hybrid Storage Optimization

The hybrid storage engine provides unprecedented memory efficiency:

- **Row Mode**: 225 bytes/record - optimal for streaming
- **Columnar Mode**: 84 bytes/record - optimal for batch processing  
- **Hybrid Mode**: Automatic selection for best efficiency

### Storage Mode Selection Strategy

```yaml
# For streaming/real-time workloads
advanced:
  storage_mode: row
  streaming_mode: true

# For batch/analytics workloads
advanced:
  storage_mode: columnar
  streaming_mode: false

# For mixed workloads (recommended)
advanced:
  storage_mode: hybrid
```

### Pool Configuration

```go
// Configure memory pools with hybrid storage
optimizationConfig := &optimization.OptimizationConfig{
    EnableMemoryPools:    true,
    RecordPoolSize:       10000,
    BufferPoolSize:       1000,
    MaxPooledObjects:     100000,
    StorageMode:          "hybrid",
}
```

### Best Practices

- Use hybrid storage mode for automatic optimization
- Configure larger batch sizes for columnar efficiency
- Use provided memory pools for records and buffers
- Avoid string conversions in hot paths  
- Release pooled objects promptly
- Monitor pool hit rates (target >90%)
- Monitor storage mode selection accuracy

## Batch Processing

### Optimal Batch Sizes by Storage Mode

**Row Mode (Streaming/Real-time):**
- **CSV**: 1K-5K records for low latency
- **APIs**: 100-1K records due to rate limits
- **Databases**: 1K-5K records for transaction efficiency

**Columnar Mode (Batch/Analytics):**
- **CSV**: 10K-50K records for maximum efficiency
- **APIs**: 5K-10K records for memory optimization
- **Databases**: 10K-25K records for bulk operations

**Hybrid Mode (Automatic):**
- Automatically selects optimal batch size based on dataset characteristics
- Small datasets: Uses row mode with smaller batches
- Large datasets: Switches to columnar mode with larger batches

### Batch Configuration Examples

```yaml
# Streaming configuration (row mode)
performance:
  batch_size: 1000
  buffer_size: 10000
  streaming_mode: true
advanced:
  storage_mode: row

# Batch processing configuration (columnar mode)
performance:
  batch_size: 25000
  buffer_size: 100000
  streaming_mode: false
advanced:
  storage_mode: columnar

# Hybrid configuration (automatic)
performance:
  batch_size: 10000
  buffer_size: 50000
  streaming_mode: false
advanced:
  storage_mode: hybrid
```

## Compression

### Algorithm Selection

- **Snappy**: Fast compression for databases
- **LZ4**: Balanced speed/ratio for APIs
- **Zstd**: Best compression for cold storage

### Configuration

```yaml
properties:
  enable_compression: true
  compression_algorithm: "snappy"
  compression_level: "default"
  compression_threshold: 2048
```

## Concurrency

### Worker Pool Sizing

- **CPU-bound**: 1x CPU cores
- **I/O-bound**: 2-4x CPU cores  
- **API-bound**: Limited by rate limits

### Configuration

```yaml
properties:
  max_concurrency: 10    # Adjust based on workload
```

## Troubleshooting

### High Memory Usage
- **Check storage mode selection** - Verify hybrid mode is switching to columnar for large datasets
- **Optimize batch sizes** - Use larger batches for columnar mode efficiency
- Check pool hit rates and storage adapter efficiency
- Reduce batch sizes if using row mode unnecessarily
- Enable compression for additional savings
- Monitor GC frequency and storage mode transitions

### High CPU Usage
- **Profile storage mode overhead** - Check if frequent mode switching is occurring
- Profile hot paths in columnar conversion logic
- Optimize data transformations
- Reduce concurrency if thrashing
- Use more efficient algorithms
- Consider columnar mode for CPU-intensive transformations

### Low Throughput
- **Verify storage mode selection** - Ensure appropriate mode for workload
- Increase batch sizes for columnar efficiency
- Add concurrency (if not API-bound)  
- Enable compression
- Check for rate limiting
- Monitor storage adapter performance

### Suboptimal Storage Mode Selection
- **Monitor mode switching patterns** - Check if hybrid logic is working correctly
- **Adjust batch size thresholds** - Tune when columnar mode is selected
- **Validate dataset characteristics** - Ensure workload matches expected patterns
- **Check configuration** - Verify storage_mode and streaming_mode settings

### High Error Rates
- Check circuit breaker configuration
- Verify API quotas and limits
- Monitor health check status including storage adapter health
- Review retry settings
- Check storage mode compatibility with connectors

## Monitoring

### Key Metrics

```go
// Built-in metrics including hybrid storage
connector.RecordMetric("records_processed", count, core.MetricTypeCounter)
connector.RecordMetric("processing_time", duration, core.MetricTypeHistogram)
connector.RecordMetric("memory_usage", bytes, core.MetricTypeGauge)

// Hybrid storage specific metrics
storageAdapter.RecordMetric("storage_mode_selected", mode, core.MetricTypeGauge)
storageAdapter.RecordMetric("memory_per_record", bytesPerRecord, core.MetricTypeGauge)
storageAdapter.RecordMetric("mode_switches", switchCount, core.MetricTypeCounter)
storageAdapter.RecordMetric("columnar_efficiency", compressionRatio, core.MetricTypeGauge)
```

### Health Checks

All production connectors provide health status including storage adapter health:
```go
isHealthy := connector.IsHealthy()
healthDetails := connector.GetHealthDetails()

// Storage adapter specific health
storageHealth := storageAdapter.GetHealth()
memoryPerRecord := storageAdapter.GetMemoryPerRecord()
currentMode := storageAdapter.GetStorageMode()
```

## Best Practices

1. **Use hybrid storage mode** - Let the system automatically select optimal storage for each workload
2. **Start with defaults** - BaseConnector and StorageAdapter provide sensible defaults
3. **Measure first** - Always profile before optimizing, including storage mode selection patterns
4. **Optimize incrementally** - Change one parameter at a time, monitor storage mode impact
5. **Monitor continuously** - Track performance and storage efficiency over time
6. **Test across workloads** - Validate optimizations work for both streaming and batch scenarios
7. **Monitor memory per record** - Target <100 bytes/record for optimal efficiency

## Environment-Specific Notes

### Development
- **Use row mode** for faster iteration and debugging
- Lower batch sizes for faster feedback loops
- Disable compression for easier debugging
- Reduce concurrency for predictable behavior
- Monitor storage mode selection for testing

### Production  
- **Use hybrid mode** for automatic optimization
- Use larger batch sizes for throughput
- Enable compression for additional efficiency
- Monitor resource usage and storage mode performance continuously
- Set up alerting for performance regressions and storage inefficiencies
- Track memory per record metrics (target: <100 bytes/record)
- Monitor mode switching patterns for workload optimization