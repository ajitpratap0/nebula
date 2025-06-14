# Performance Profiling Guide

## Overview

Nebula includes a comprehensive performance profiling system designed to identify bottlenecks and optimize pipeline performance. The profiling framework provides detailed insights into CPU usage, memory allocation, goroutine behavior, and pipeline-specific metrics.

## Features

### System Profiling
- **CPU Profiling**: Identifies CPU hotspots and expensive functions
- **Memory Profiling**: Detects memory leaks and high allocation rates
- **Block Profiling**: Finds blocking operations and contention points
- **Mutex Profiling**: Identifies lock contention
- **Goroutine Profiling**: Detects goroutine leaks
- **Execution Tracing**: Provides detailed execution timeline

### Pipeline-Specific Profiling
- **Throughput Metrics**: Records/second and bytes/second
- **Stage Metrics**: Per-stage latency and record counts
- **Channel Utilization**: Buffer usage and blocking time
- **Connector Metrics**: Source and destination performance
- **Error Tracking**: Error rates and patterns

### Bottleneck Analysis
- **Automatic Detection**: Identifies performance bottlenecks
- **Severity Classification**: Critical, high, medium, low
- **Impact Assessment**: Quantifies performance impact
- **Recommendations**: Provides actionable optimization suggestions

## Usage

### Command-Line Tool

```bash
# Basic profiling
./bin/profile -source csv -destination json \
  -source-config "file_path=input.csv" \
  -dest-config "file_path=output.json"

# Profile with all metrics for 60 seconds
./bin/profile -types all -duration 60s \
  -source postgresql -destination snowflake

# CPU-focused profiling
./bin/profile -types cpu -cpu-duration 30s \
  -source google-ads -destination bigquery
```

### Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `-source` | Source connector type | csv |
| `-destination` | Destination connector type | json |
| `-source-config` | Source configuration (key=value,key=value) | - |
| `-dest-config` | Destination configuration | - |
| `-duration` | Total profiling duration | 30s |
| `-output` | Output directory for profiles | ./profiles |
| `-types` | Profile types (cpu,memory,block,mutex,goroutine,trace,all) | cpu,memory |
| `-batch` | Batch size for processing | 1000 |
| `-workers` | Number of worker threads | 4 |
| `-buffer` | Channel buffer size | 10000 |
| `-cpu-duration` | CPU profiling duration | 30s |

### Programmatic Usage

```go
import "github.com/ajitpratap0/nebula/pkg/performance/profiling"

// Create profile configuration
config := &profiling.ProfileConfig{
    Types:                   []profiling.ProfileType{profiling.CPUProfile, profiling.MemoryProfile},
    OutputDir:               "./profiles",
    CPUDuration:             30 * time.Second,
    CollectRuntimeMetrics:   true,
    MetricsSamplingInterval: 100 * time.Millisecond,
}

// Create pipeline profiler
profiler := profiling.NewPipelineProfiler(config)

// Start profiling
ctx := context.Background()
err := profiler.Start(ctx)

// Run your pipeline...

// Stop and analyze
result, err := profiler.Stop()

// Access results
fmt.Printf("Throughput: %.2f records/sec\n", result.Throughput)
fmt.Printf("Memory usage: %.2f MB\n", float64(result.RuntimeMetrics.AllocBytes)/(1024*1024))
```

## Bottleneck Types

### CPU Bottlenecks
**Symptoms**: High CPU usage, specific functions consuming >20% CPU time

**Common Causes**:
- Inefficient algorithms
- Unnecessary computations in loops
- Lack of parallelization
- Expensive serialization/deserialization

**Example Detection**:
```
[CRITICAL] Function 'json.Marshal' consuming 45.2% of CPU time
Suggestions:
- Consider using a faster JSON library (sonic, jsoniter)
- Use streaming JSON processing for large data
- Implement caching for expensive computations
```

### Memory Bottlenecks
**Symptoms**: Growing memory usage, high allocation rates

**Common Causes**:
- Memory leaks (holding references)
- Large in-memory buffers
- Inefficient data structures
- Missing object pooling

**Example Detection**:
```
[HIGH] Memory growing at 5.3 MB/s - potential memory leak
Suggestions:
- Profile heap allocations to identify leak source
- Check for unbounded data structures
- Ensure proper cleanup of resources
- Use object pools for frequently allocated objects
```

### GC Pressure
**Symptoms**: High GC overhead (>5% of runtime)

**Common Causes**:
- High allocation rate
- Many short-lived objects
- Large heap size
- Inappropriate GC settings

**Example Detection**:
```
[MEDIUM] GC overhead 8.5% - excessive garbage collection
Suggestions:
- Reduce allocation rate
- Use object pools
- Pre-allocate slices with appropriate capacity
- Consider using value types instead of pointers
```

### Concurrency Bottlenecks
**Symptoms**: Goroutine leaks, CPU underutilization

**Common Causes**:
- Missing goroutine cleanup
- Unbounded goroutine creation
- Lock contention
- Insufficient parallelism

**Example Detection**:
```
[HIGH] Goroutine leak detected: 500 new goroutines
Suggestions:
- Ensure all goroutines have proper exit conditions
- Use context for goroutine lifecycle management
- Implement worker pools instead of spawning unlimited goroutines
```

## Performance Optimization Workflow

### 1. Baseline Measurement
```bash
# Establish baseline performance
./bin/profile -duration 60s -types all -output ./baseline_profiles
```

### 2. Analyze Results
Review the generated reports:
- `bottleneck_analysis_*.txt` - Bottleneck summary
- `report_*.txt` - Performance metrics
- `*.prof` - Raw profile data

### 3. Identify Primary Bottleneck
Focus on the highest severity bottleneck first:
```
Primary bottleneck: Memory growing at 10.5 MB/s (MemoryBottleneck)
```

### 4. Apply Optimizations
Implement the suggested optimizations:
- Add object pooling
- Fix memory leaks
- Optimize algorithms
- Increase parallelism

### 5. Measure Impact
```bash
# Re-profile after optimization
./bin/profile -duration 60s -types all -output ./optimized_profiles
```

### 6. Compare Results
Compare baseline vs optimized metrics:
- Throughput improvement
- Memory usage reduction
- CPU efficiency gains
- Error rate changes

## Interpreting Profile Data

### CPU Profile Analysis
```bash
# Analyze CPU profile with pprof
go tool pprof cpu_*.prof

# Top CPU consumers
(pprof) top10

# Generate flame graph
(pprof) web
```

### Memory Profile Analysis
```bash
# Analyze memory profile
go tool pprof memory_*.prof

# Show allocation sites
(pprof) list main

# Check inuse vs alloc
(pprof) inuse_space
(pprof) alloc_space
```

### Trace Analysis
```bash
# View execution trace
go tool trace trace_*.out
```

## Pipeline-Specific Metrics

### Stage Metrics
Monitor individual pipeline stages:
```
Transform Stage:
  Records: 100000 in, 95000 out
  Processing Time: 5.2s
  Avg Latency: 52µs
  Max Latency: 120ms
```

### Channel Utilization
Check for backpressure:
```
Channel Metrics:
  Buffer Size: 10000
  Max Utilization: 9800 (98%)
  Avg Utilization: 7500 (75%)
```

### Connector Performance
Compare source vs destination:
```
Source (CSV): 50000 records/sec
Destination (BigQuery): 45000 records/sec
Bottleneck: Destination write speed
```

## Best Practices

### 1. Profile Representative Workloads
- Use production-like data volumes
- Include peak load scenarios
- Test with actual connectors

### 2. Profile Duration
- CPU: Minimum 30 seconds
- Memory: Include full GC cycles
- Trace: Keep under 10 seconds

### 3. Iterative Optimization
- Fix one bottleneck at a time
- Measure after each change
- Watch for new bottlenecks

### 4. Continuous Profiling
- Integrate into CI/CD pipeline
- Set performance regression alerts
- Track metrics over time

## Common Optimizations

### High CPU Usage
1. **Parallelize CPU-bound operations**
   ```go
   // Use worker pools
   pool := NewWorkerPool(runtime.NumCPU())
   ```

2. **Optimize hot functions**
   - Replace regex with simpler string operations
   - Cache compiled patterns
   - Use faster libraries (sonic for JSON)

3. **Reduce allocations in hot paths**
   ```go
   // Reuse buffers
   var bufferPool = sync.Pool{
       New: func() interface{} {
           return &bytes.Buffer{}
       },
   }
   ```

### High Memory Usage
1. **Implement streaming**
   ```go
   // Process records one at a time
   for record := range stream {
       process(record)
       record.Release() // Return to pool
   }
   ```

2. **Use object pools**
   ```go
   var recordPool = sync.Pool{
       New: func() interface{} {
           return &models.Record{}
       },
   }
   ```

3. **Optimize data structures**
   - Use arrays instead of slices where size is known
   - Consider compact representations
   - Release unused memory promptly

### GC Pressure
1. **Reduce allocation rate**
   ```go
   // Pre-allocate with capacity
   records := make([]*Record, 0, 10000)
   ```

2. **Use value types**
   ```go
   // Avoid pointers for small structs
   type Point struct {
       X, Y float64
   }
   ```

3. **Batch small allocations**
   ```go
   // Allocate in chunks
   chunk := make([]Record, 1000)
   ```

## Troubleshooting

### Profile Generation Issues
- **No CPU profile**: Increase CPU duration or workload
- **Empty memory profile**: Force GC with `runtime.GC()`
- **Missing trace**: Check disk space and permissions

### Inaccurate Metrics
- **Low throughput**: Check for blocking operations
- **High error rate**: Review error logs
- **Memory not freed**: Look for reference leaks

### Platform-Specific Issues
- **Linux**: Enable kernel symbols for better traces
- **macOS**: Grant permissions for profiling
- **Docker**: Use `--cap-add=SYS_ADMIN` for detailed profiles

## Advanced Topics

### Custom Metrics
```go
// Add custom stage metrics
profiler.RecordStageMetrics("custom_transform", 
    recordsIn, recordsOut, processingTime)
```

### Integration with APM
- Export metrics to Prometheus
- Send traces to Jaeger
- Dashboard in Grafana

### Production Profiling
- Use continuous profiling tools
- Sample profiles to reduce overhead
- Implement feature flags for profiling

## Conclusion

Nebula's profiling system provides comprehensive insights into pipeline performance. By following this guide and regularly profiling your pipelines, you can:

- Achieve optimal throughput
- Minimize resource usage
- Identify bottlenecks early
- Maintain consistent performance

Remember: **Profile first, optimize second, measure always!**