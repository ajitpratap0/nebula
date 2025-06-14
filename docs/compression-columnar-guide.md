# Compression & Columnar Format Guide

## Overview

Nebula provides comprehensive support for data compression and columnar storage formats, enabling efficient data storage, transfer, and processing. This guide covers the compression algorithms and columnar formats available in Nebula.

## Table of Contents

1. [Compression Support](#compression-support)
2. [Columnar Formats](#columnar-formats)
3. [Performance Characteristics](#performance-characteristics)
4. [Usage Examples](#usage-examples)
5. [Best Practices](#best-practices)
6. [Benchmarks](#benchmarks)

## Compression Support

### Available Algorithms

Nebula supports six compression algorithms, each with different trade-offs between compression ratio and speed:

| Algorithm | Speed | Ratio | Best For |
|-----------|-------|-------|----------|
| **None** | Fastest | 1x | No compression needed |
| **Snappy** | Very Fast | 2-4x | Real-time streaming |
| **LZ4** | Very Fast | 2-5x | High-throughput scenarios |
| **S2** | Fast | 3-5x | Snappy-compatible with better ratio |
| **Gzip** | Moderate | 4-8x | Balanced compression |
| **Zstd** | Moderate | 5-10x | Best compression with good speed |
| **Deflate** | Slow | 4-8x | Compatibility with older systems |

### Compression Levels

Most algorithms support multiple compression levels:

- **Fastest** (1): Maximum speed, lower compression
- **Default** (5): Balanced performance
- **Better** (7): Better compression, slower
- **Best** (9): Maximum compression

### Basic Usage

```go
import "github.com/ajitpratap0/nebula/pkg/compression"

// Create compressor
config := &compression.Config{
    Algorithm:  compression.Zstd,
    Level:      compression.Default,
    BufferSize: 64 * 1024, // 64KB
}

compressor, err := compression.NewCompressor(config)
if err != nil {
    log.Fatal(err)
}

// Compress data
compressed, err := compressor.Compress(originalData)

// Decompress data
decompressed, err := compressor.Decompress(compressed)
```

### Streaming Compression

For large datasets, use streaming compression:

```go
// Compress stream
err := compressor.CompressStream(destWriter, sourceReader)

// Decompress stream
err := compressor.DecompressStream(destWriter, sourceReader)
```

### Compression Pooling

For high-concurrency scenarios, use compression pools:

```go
pool := compression.NewCompressorPool(config)

// Use pooled compressor
compressed, err := pool.Compress(data)
decompressed, err := pool.Decompress(compressed)
```

## Columnar Formats

### Supported Formats

Nebula supports four major columnar storage formats:

| Format | Description | Best For |
|--------|-------------|----------|
| **Parquet** | Industry-standard columnar format | Analytics, long-term storage |
| **ORC** | Optimized Row Columnar | Hive/Hadoop ecosystems |
| **Arrow** | In-memory columnar format | Fast processing, zero-copy |
| **Avro** | Row-oriented with schema | Streaming, schema evolution |

### Format Features

| Feature | Parquet | ORC | Arrow | Avro |
|---------|---------|-----|-------|------|
| Compression | ✅ | ✅ | ✅ | ✅ |
| Schema Evolution | ✅ | ✅ | ✅ | ✅ |
| Column Pruning | ✅ | ✅ | ❌ | ❌ |
| Predicate Pushdown | ✅ | ✅ | ❌ | ❌ |
| Statistics | ✅ | ✅ | ❌ | ❌ |
| Bloom Filters | ✅ | ✅ | ❌ | ❌ |

### Basic Usage

```go
import "github.com/ajitpratap0/nebula/pkg/formats/columnar"

// Define schema
schema := &core.Schema{
    Name: "sales_data",
    Fields: []*core.Field{
        {Name: "id", Type: core.FieldTypeInteger, Required: true},
        {Name: "product", Type: core.FieldTypeString, Required: true},
        {Name: "price", Type: core.FieldTypeFloat, Required: true},
        {Name: "timestamp", Type: core.FieldTypeTimestamp, Required: true},
    },
}

// Create writer
config := &columnar.WriterConfig{
    Format:      columnar.Parquet,
    Schema:      schema,
    Compression: "snappy",
    BatchSize:   10000,
    EnableStats: true,
}

writer, err := columnar.NewWriter(outputFile, config)
if err != nil {
    log.Fatal(err)
}

// Write records
err = writer.WriteRecords(records)
writer.Close()
```

### Reading Columnar Data

```go
// Create reader
readerConfig := &columnar.ReaderConfig{
    Format:     columnar.Parquet,
    BatchSize:  1000,
    Projection: []string{"id", "price"}, // Read only specific columns
}

reader, err := columnar.NewReader(inputFile, readerConfig)
if err != nil {
    log.Fatal(err)
}

// Read all records
records, err := reader.ReadRecords()

// Or stream records
for reader.HasNext() {
    record, err := reader.Next()
    if err != nil {
        break
    }
    // Process record
}

reader.Close()
```

## Performance Characteristics

### Compression Algorithm Performance

Based on 1MB JSON data:

| Algorithm | Compress Speed | Decompress Speed | Compression Ratio |
|-----------|----------------|------------------|-------------------|
| Snappy | 450 MB/s | 1500 MB/s | 2.8x |
| LZ4 | 400 MB/s | 2000 MB/s | 3.2x |
| S2 | 350 MB/s | 1200 MB/s | 3.5x |
| Gzip | 50 MB/s | 150 MB/s | 5.2x |
| Zstd | 100 MB/s | 300 MB/s | 6.1x |

### Columnar Format Performance

Based on 100K records:

| Format | Write Speed | Read Speed | File Size |
|--------|-------------|------------|-----------|
| Parquet | 50K rec/s | 200K rec/s | Smallest |
| Arrow | 100K rec/s | 500K rec/s | Medium |
| ORC | 45K rec/s | 180K rec/s | Small |
| Avro | 80K rec/s | 150K rec/s | Large |

## Usage Examples

### Example 1: Compressed Parquet Pipeline

```go
// Create compressed Parquet writer
writer, err := columnar.NewWriter(output, &columnar.WriterConfig{
    Format:            columnar.Parquet,
    Schema:            schema,
    Compression:       "zstd",
    BatchSize:         50000,
    RowGroupSize:      128 * 1024 * 1024, // 128MB
    EnableStats:       true,
    EnableBloomFilter: true,
})

// Write data in batches
for batch := range dataSource {
    err := writer.WriteRecords(batch)
    if err != nil {
        log.Printf("Write error: %v", err)
    }
}

writer.Close()
```

### Example 2: Multi-Format Conversion

```go
// Read from one format
parquetReader, _ := columnar.NewReader(parquetFile, &columnar.ReaderConfig{
    Format: columnar.Parquet,
})

// Write to another format
arrowWriter, _ := columnar.NewWriter(arrowFile, &columnar.WriterConfig{
    Format:      columnar.Arrow,
    Schema:      schema,
    Compression: "lz4",
})

// Convert
records, _ := parquetReader.ReadRecords()
arrowWriter.WriteRecords(records)

parquetReader.Close()
arrowWriter.Close()
```

### Example 3: Streaming with Compression

```go
// Create compression + columnar pipeline
compressor, _ := compression.NewCompressor(&compression.Config{
    Algorithm: compression.Snappy,
})

// Compress columnar data on the fly
var compressedBuffer bytes.Buffer
compressWriter := compressor.CompressWriter(&compressedBuffer)

columnarWriter, _ := columnar.NewWriter(compressWriter, &columnar.WriterConfig{
    Format:    columnar.Arrow,
    Schema:    schema,
    BatchSize: 1000,
})

// Stream data through pipeline
for record := range recordStream {
    columnarWriter.WriteRecord(record)
}

columnarWriter.Close()
compressWriter.Close()
```

## Best Practices

### Choosing Compression

1. **For Streaming**: Use Snappy or LZ4 for low latency
2. **For Storage**: Use Zstd for best compression ratio
3. **For Compatibility**: Use Gzip for wide support
4. **For Throughput**: Use S2 for Snappy-compatible speed

### Choosing Columnar Format

1. **For Analytics**: Use Parquet with Zstd compression
2. **For Streaming**: Use Arrow with LZ4 compression
3. **For Hadoop**: Use ORC with Snappy compression
4. **For Schema Evolution**: Use Avro with compression

### Performance Tips

1. **Batch Size**: Use 10K-50K records per batch
2. **Row Group Size**: 64-128MB for Parquet/ORC
3. **Compression Level**: Default for real-time, Best for archival
4. **Column Projection**: Read only needed columns
5. **Statistics**: Enable for analytical workloads

### Memory Management

```go
// Use appropriate batch sizes
config := &columnar.WriterConfig{
    BatchSize:    10000,        // Records per batch
    PageSize:     8192,         // Page size in bytes
    RowGroupSize: 67108864,     // 64MB row groups
}

// Use compression pools for concurrency
pool := compression.NewCompressorPool(&compression.Config{
    Algorithm:   compression.Snappy,
    Concurrency: runtime.NumCPU(),
})
```

## Benchmarks

### Running Benchmarks

```bash
# Compression benchmarks
go test -bench=BenchmarkCompression ./pkg/compression/...

# Columnar format benchmarks
go test -bench=BenchmarkColumnar ./pkg/formats/columnar/...

# Compression ratio analysis
go test -bench=BenchmarkCompressionRatio -benchtime=1x ./pkg/compression/...
```

### Sample Results

```
BenchmarkCompression/Snappy/JSON/1.0MB          500   2.3ms   434.78 MB/s
BenchmarkCompression/Zstd/JSON/1.0MB            200   5.1ms   196.08 MB/s
BenchmarkColumnarWrite/Parquet/10000/snappy    100  11.2ms   892.86 rec/ms
BenchmarkColumnarRead/Parquet                   500   2.1ms  4761.90 rec/ms
```

## Integration with Nebula

### Snowflake Connector

```yaml
destination:
  type: snowflake
  properties:
    file_format: parquet
    compression: zstd
    batch_size: 50000
```

### CDC Sources

```yaml
source:
  type: postgresql_cdc
  properties:
    output_format: arrow
    compression: lz4
    streaming: true
```

## Troubleshooting

### Common Issues

1. **Out of Memory**: Reduce batch size or use streaming
2. **Slow Compression**: Switch to faster algorithm
3. **Large Files**: Enable compression and statistics
4. **Schema Mismatch**: Ensure consistent field types

### Debug Logging

```go
// Enable compression metrics
compressor.EnableMetrics()

// Log columnar statistics
writer.LogStatistics()
```

## Summary

Nebula's compression and columnar format support provides:

- **6 compression algorithms** with different trade-offs
- **4 columnar formats** for various use cases
- **Streaming support** for large datasets
- **High performance** with pooling and batching
- **Flexible configuration** for optimization

Choose the right combination based on your specific requirements for speed, compression ratio, and compatibility.