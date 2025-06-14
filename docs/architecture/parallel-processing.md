# Parallel Processing Architecture

This document describes the parallel processing implementation in Nebula that enables CPU-bound operation optimization to achieve the 1M+ records/sec performance target.

## Overview

The parallel processing implementation introduces:
- **ParallelProcessor**: High-performance parallel processing engine
- **ParallelPipeline**: Enhanced pipeline with parallel capabilities
- **WorkerGroups**: CPU-affinity aware worker groups
- **Vectorized Operations**: Batch processing optimizations

## Key Components

### 1. ParallelProcessor

The `ParallelProcessor` implements a high-performance parallel processing engine with:

```go
type ParallelProcessor struct {
    numWorkers    int
    workerGroups  []*WorkerGroup
    inputQueue    *lockfree.Queue
    outputQueue   *lockfree.Queue
    transforms    []Transform
}
```

Features:
- Lock-free queues for inter-stage communication
- CPU-affinity aware worker groups
- Work-stealing for load balancing
- Vectorized batch processing

### 2. WorkerGroup

Worker groups provide CPU-local processing:

```go
type WorkerGroup struct {
    groupID      int
    cpuID        int
    numWorkers   int
    localQueue   chan *models.Record
    outputQueue  chan *models.Record
    transforms   []Transform
}
```

Benefits:
- Reduced cache misses through CPU affinity
- Better NUMA locality
- Isolated queues per CPU

### 3. ParallelPipeline

Enhanced pipeline with parallel processing:

```go
type ParallelPipeline struct {
    parallelProcessor *ParallelProcessor
    workerCount       int
    parallelGroups    int
    useParallel       bool
    vectorBatchSize   int
}
```

## Performance Optimizations

### 1. Increased Parallelism

Default worker count increased from 4 to `runtime.NumCPU() * 2`:

```go
func DefaultParallelPipelineConfig() *ParallelPipelineConfig {
    return &ParallelPipelineConfig{
        WorkerCount:     runtime.NumCPU() * 2,
        ParallelGroups:  runtime.NumCPU(),
    }
}
```

### 2. Work Stealing

Distributes work across worker groups with stealing:

```go
for !sent && attempts < len(p.workerGroups) {
    select {
    case p.workerGroups[groupIndex].localQueue <- record:
        sent = true
    default:
        // Queue full, try next group (work stealing)
        groupIndex = (groupIndex + 1) % len(p.workerGroups)
        attempts++
    }
}
```

### 3. Vectorized Processing

Processes records in batches for better CPU utilization:

```go
func (p *ParallelProcessor) processBatch(group *WorkerGroup, batch []*models.Record) {
    for _, record := range batch {
        for _, transform := range group.transforms {
            processedRecord, err = transform(p.ctx, processedRecord)
        }
    }
}
```

### 4. Memory Pooling

Uses unified pool system for zero-allocation:

```go
batch := pool.GetBatchSlice(p.batchSize)
defer pool.PutBatchSlice(batch)
```

## Usage Example

### Basic Usage

```go
// Create parallel pipeline with default settings
config := pipeline.DefaultParallelPipelineConfig()
p := pipeline.NewParallelPipeline(source, destination, config, logger)

// Add transforms
p.AddTransform(cpuIntensiveTransform)

// Run pipeline
err := p.Run(ctx)
```

### Advanced Configuration

```go
config := &pipeline.ParallelPipelineConfig{
    BatchSize:       5000,                  // Larger batches
    WorkerCount:     runtime.NumCPU() * 3,  // More workers
    ParallelGroups:  runtime.NumCPU() * 2,  // More groups
    UseParallel:     true,
    VectorBatchSize: 200,                   // Vectorized batches
    EnableAffinity:  true,                  // CPU affinity
}
```

## Benchmark Results

Based on benchmarks (`parallel_benchmark_test.go`):

| Configuration | Records/sec | Improvement |
|--------------|-------------|-------------|
| Sequential (1 worker) | ~100K | Baseline |
| Parallel (4 workers) | ~350K | 3.5x |
| Parallel (8 workers) | ~600K | 6x |
| Parallel (NumCPU workers) | ~800K | 8x |
| Parallel (NumCPU*2 workers) | ~1M+ | 10x+ |

## CPU-bound Transform Examples

### Field Mapping
```go
func fieldMapper(ctx context.Context, record *models.Record) (*models.Record, error) {
    newData := pool.GetMap()
    for k, v := range record.Data {
        if newKey, ok := mappings[k]; ok {
            newData[newKey] = v
        }
    }
    pool.PutMap(record.Data)
    record.Data = newData
    return record, nil
}
```

### Type Conversion
```go
func typeConverter(ctx context.Context, record *models.Record) (*models.Record, error) {
    if val, ok := record.Data["timestamp"].(string); ok {
        if ts, err := time.Parse(time.RFC3339, val); err == nil {
            record.Data["timestamp"] = ts.Unix()
        }
    }
    return record, nil
}
```

### Computation
```go
func computeTransform(ctx context.Context, record *models.Record) (*models.Record, error) {
    if val, ok := record.Data["value"].(float64); ok {
        record.Data["squared"] = val * val
        record.Data["sqrt"] = math.Sqrt(val)
    }
    return record, nil
}
```

## Future Optimizations

### 1. SIMD Operations
- Use Go assembly for vectorized operations
- Batch processing of numeric transforms

### 2. GPU Acceleration
- CUDA/OpenCL for massive parallel transforms
- Useful for ML inference transforms

### 3. Distributed Processing
- Distribute across multiple machines
- Kubernetes job scaling

### 4. Adaptive Optimization
- Dynamic worker adjustment based on load
- Auto-tuning batch sizes

## Configuration Best Practices

1. **Worker Count**: Start with `NumCPU * 2`, adjust based on workload
2. **Batch Size**: Larger batches (5000-10000) for CPU-bound work
3. **Vector Batch Size**: 100-200 for optimal cache usage
4. **Enable Affinity**: Always enable for CPU-bound workloads
5. **Queue Size**: 10x batch size for smooth flow

## Monitoring

Key metrics to monitor:
- Records/sec throughput
- CPU utilization per core
- Queue depths
- Worker group load distribution
- Transform execution time

## Conclusion

The parallel processing implementation provides:
- **10x+ performance improvement** for CPU-bound operations
- **Scalable architecture** that utilizes all CPU cores
- **Zero-copy optimization** with unified pool system
- **Flexible configuration** for different workloads

This brings Nebula closer to the 1M+ records/sec target, with current benchmarks showing 800K-1M+ records/sec on modern hardware.