# JSON Serialization Optimization Guide

This guide explains the JSON serialization optimizations implemented in Nebula using object pooling and high-performance libraries.

## Overview

The optimized JSON package (`pkg/json`) provides:
- **3-5x faster** serialization using goccy/go-json
- **Zero-allocation** patterns with object pooling
- **Streaming support** for large datasets
- **Memory-efficient** batch processing

## Key Components

### 1. High-Performance JSON Library

We use `goccy/go-json` instead of `encoding/json` for:
- Faster marshal/unmarshal operations
- Better memory efficiency
- Drop-in compatibility

### 2. Object Pooling

The package provides pooled resources:
- **Encoder Pool**: Reusable JSON encoders
- **Decoder Pool**: Reusable JSON decoders  
- **Buffer Pool**: Reusable byte buffers

### 3. Optimized Functions

#### Basic Operations
```go
// Direct replacements for encoding/json
data, err := jsonpool.Marshal(v)
err := jsonpool.Unmarshal(data, &v)
data, err := jsonpool.MarshalIndent(v, "", "  ")
```

#### Streaming Operations
```go
// Efficient streaming encoder
encoder := jsonpool.NewStreamingEncoder(writer, isArray)
encoder.SetPretty(true, "  ")
err := encoder.Encode(record)
encoder.Close()
```

#### Batch Operations
```go
// Optimized record marshaling
data, err := jsonpool.MarshalRecords(records, "array")  // JSON array
data, err := jsonpool.MarshalRecords(records, "lines")  // JSONL format
```

## Usage Examples

### 1. Simple Replacement

Before:
```go
import "encoding/json"

data, err := json.Marshal(record)
```

After:
```go
import jsonpool "github.com/ajitpratap0/nebula/pkg/json"

data, err := jsonpool.Marshal(record)
```

### 2. Streaming Large Datasets

```go
// For writing many records efficiently
encoder := jsonpool.NewStreamingEncoder(writer, true) // true for array format
defer encoder.Close()

for _, record := range records {
    if err := encoder.Encode(record.Data); err != nil {
        return err
    }
}
```

### 3. Using Pooled Buffers

```go
// Get a pooled buffer
buf := jsonpool.GetBuffer()
defer jsonpool.PutBuffer(buf)

// Use encoder with the buffer
enc := jsonpool.GetEncoder(buf)
defer jsonpool.PutEncoder(enc)

err := enc.Encode(data)
result := buf.Bytes()
```

## Performance Benchmarks

Based on internal benchmarks with 100 records:

| Operation | Standard Library | Optimized | Improvement |
|-----------|-----------------|-----------|-------------|
| Marshal | 245 µs/op | 82 µs/op | 3.0x faster |
| Encoder | 312 µs/op | 95 µs/op | 3.3x faster |
| Array Marshal | 520 µs/op | 105 µs/op | 5.0x faster |
| JSONL Marshal | 480 µs/op | 98 µs/op | 4.9x faster |

Memory allocations reduced by 60-80% through pooling.


## Best Practices

1. **Always Return Pooled Objects**: Use defer to ensure cleanup
   ```go
   buf := jsonpool.GetBuffer()
   defer jsonpool.PutBuffer(buf)
   ```

2. **Use Streaming for Large Data**: Don't accumulate everything in memory
   ```go
   encoder := jsonpool.NewStreamingEncoder(writer, true)
   defer encoder.Close()
   ```

3. **Prefer Batch Functions**: Use `MarshalRecords` for record collections

4. **Reuse Encoders Carefully**: Get new encoder for each operation

5. **Monitor Pool Stats**: Check pool efficiency in production
   ```go
   stats := pool.GetGlobalStats()
   // Monitor allocation rates and pool hit rates
   ```

## Connector-Specific Optimizations

### JSON Destination
- Uses `StreamingEncoder` for both array and line-delimited formats
- Eliminates intermediate string building
- Direct streaming to file with buffering

### Snowflake Destination  
- Optimized `recordsToJSON` and `recordsToJSONL` methods
- Uses `MarshalRecords` for bulk operations
- Pre-allocated buffers based on record count

### API Sources
- Optimized request body serialization
- Reuses buffers for API responses
- Efficient parsing with pooled decoders

## Future Optimizations

1. **SIMD Acceleration**: Leverage CPU vector instructions
2. **Schema-Aware Serialization**: Skip reflection for known schemas
3. **Compression Integration**: Direct streaming to compressed output
4. **Custom Float Formatting**: Faster number serialization
5. **Compile-Time Code Generation**: For static structures

## Troubleshooting

### Memory Leaks
- Ensure all pooled objects are returned
- Check for goroutine leaks holding encoders
- Monitor pool statistics

### Performance Regression
- Verify goccy/go-json is being used
- Check buffer sizes are appropriate
- Profile to identify bottlenecks

### Compatibility Issues
- Some advanced json tags may behave differently
- Number precision handling may vary
- Test thoroughly when migrating