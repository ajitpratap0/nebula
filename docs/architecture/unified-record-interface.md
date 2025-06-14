# Unified Record Interface Architecture

## Overview

The Nebula platform uses a unified record interface (`IRecord`) to solve the fragmentation between different record types used across the system. Combined with the hybrid storage engine, this provides optimal performance across diverse workloads. This document explains the architecture and usage patterns.

## Problem Statement

The codebase had three different record types causing integration issues:

1. **`models.Record`** - The primary public type with performance optimizations (zero-allocation, pooling)
2. **`StreamingRecord`** - Internal pipeline type with streaming-specific fields (partition, offset)  
3. **`core.ChangeEvent`** - CDC-specific type with before/after states for database changes

Each component expected different types, making integration difficult.

## Solution: IRecord Interface

We implemented a unified `IRecord` interface that all record types can implement:

```go
type IRecord interface {
    // Core record properties
    GetID() string
    SetID(id string)
    GetSource() string
    SetSource(source string)
    GetTimestamp() time.Time
    SetTimestamp(ts time.Time)
    GetData() map[string]interface{}
    SetData(data map[string]interface{})
    GetMetadata() map[string]interface{}
    SetMetadata(key string, value interface{})
    GetMetadataValue(key string) (interface{}, bool)
    
    // Conversion methods
    ToModelsRecord() *Record  // Always convert to models.Record
    Clone() IRecord
    
    // Resource management
    Release() // For pool-based records
}
```

## Implementation Details

### 1. models.Record Adapter

`models.Record` implements `IRecord` through `RecordIRecordAdapter`:

```go
// Convert models.Record to IRecord
record := models.NewRecord("source")
irecord := models.AsIRecord(record)

// Use through interface
irecord.SetData(data)

// Convert back if needed
record = irecord.ToModelsRecord()
```

### 2. StreamingRecord Adapter

`StreamingRecord` implements `IRecord` through `StreamingRecordWrapper`:

- Streaming-specific fields (partition, offset) are stored in metadata
- Seamless conversion to/from `models.Record`

```go
// Convert StreamingRecord to IRecord
streamRec := &pipeline.StreamingRecord{
    ID: "stream-1",
    Partition: 1,
    Offset: 12345,
}
irecord := streamRec.AsIRecord()

// Convert to models.Record (preserves partition/offset in metadata)
record := irecord.ToModelsRecord()
```

### 3. ChangeEvent Adapter

`core.ChangeEvent` implements `IRecord` through `ChangeEventAdapter`:

- CDC operation type stored in metadata
- Before/After states mapped to data fields based on operation type

```go
// Convert ChangeEvent to IRecord
changeEvent := &core.ChangeEvent{
    Type: core.ChangeTypeUpdate,
    Table: "users",
    Before: map[string]interface{}{"name": "old"},
    After: map[string]interface{}{"name": "new"},
}
irecord := changeEvent.AsIRecord()

// Convert to models.Record
record := irecord.ToModelsRecord()
// Data will contain: {"_before": {...}, "_after": {...}}
```

## Usage Patterns

### 1. Component Interfaces

Components should accept `IRecord` when they need to handle multiple record types:

```go
func ProcessRecords(records []models.IRecord) error {
    for _, irecord := range records {
        // Always work with models.Record internally
        record := irecord.ToModelsRecord()
        
        // Process the record
        processData(record.Data)
        
        // Release when done
        irecord.Release()
    }
    return nil
}
```

### 2. Type-Specific Processing

When you need type-specific features, check metadata:

```go
func HandleRecord(irecord models.IRecord) {
    record := irecord.ToModelsRecord()
    
    // Check if it's a streaming record
    if partition, ok := record.GetMetadata("partition"); ok {
        // Handle streaming-specific logic
        processPartition(partition.(int))
    }
    
    // Check if it's a CDC event
    if changeType, ok := record.GetMetadata("change_type"); ok {
        // Handle CDC-specific logic
        switch changeType.(string) {
        case "insert":
            handleInsert(record.Data)
        case "update":
            handleUpdate(record.Data["_before"], record.Data["_after"])
        case "delete":
            handleDelete(record.Data)
        }
    }
}
```

### 3. Pipeline Integration

The pipeline can now accept any record type:

```go
// Source produces different record types
sourceRecords := source.Read() // Could be StreamingRecord, ChangeEvent, etc.

// Convert to IRecord for pipeline
var irecords []models.IRecord
for _, rec := range sourceRecords {
    irecords = append(irecords, rec.AsIRecord())
}

// Pipeline processes IRecord
pipeline.Process(irecords)

// Destination receives models.Record
for _, irecord := range irecords {
    destination.Write(irecord.ToModelsRecord())
}
```

## Hybrid Storage Integration

The unified record interface works seamlessly with the hybrid storage engine:

### Storage Mode Selection

```go
// Records are automatically processed through optimal storage
func ProcessRecords(records []models.IRecord, storageAdapter *pipeline.StorageAdapter) error {
    for _, irecord := range records {
        // Convert to unified format
        record := irecord.ToModelsRecord()
        
        // Storage adapter automatically selects row/columnar based on workload
        storageAdapter.AddRecord(record)
        
        irecord.Release()
    }
    return nil
}
```

### Type-Specific Optimizations

- **Streaming Records**: Automatically use row mode for low latency
- **CDC Events**: Optimal storage based on change frequency
- **Batch Records**: Automatic columnar selection for large datasets

## Benefits

1. **Unified Processing**: All components can work with any record type through `IRecord`
2. **Intelligent Storage**: Automatic row/columnar selection based on record characteristics
3. **Memory Efficiency**: 94% memory reduction (1,365→84 bytes/record) in optimal scenarios
4. **Type Safety**: Strong typing through interfaces rather than `interface{}`
5. **Performance**: Zero-allocation patterns preserved through `models.Record` as base
6. **Flexibility**: Type-specific features accessible through metadata
7. **Storage Transparency**: Applications don't need to manage storage mode selection
8. **Backward Compatible**: Existing code using specific types continues to work

## Migration Guide

To integrate new components with the unified record system and hybrid storage:

1. **Accept IRecord** in your interfaces:
   ```go
   type MyProcessor interface {
       Process(record models.IRecord, storageAdapter *pipeline.StorageAdapter) error
   }
   ```

2. **Convert to models.Record** and leverage storage adapter:
   ```go
   func (p *processor) Process(irecord models.IRecord, storageAdapter *pipeline.StorageAdapter) error {
       record := irecord.ToModelsRecord()
       
       // Add to storage adapter for optimal storage
       storageAdapter.AddRecord(record)
       
       // Process the record
       processData(record.Data)
       
       return nil
   }
   ```

3. **Use type-specific adapters** with storage considerations:
   ```go
   // For StreamingRecord (typically row mode)
   streamRec := &StreamingRecord{...}
   irecord := streamRec.AsIRecord()
   
   // For ChangeEvent (optimal storage based on frequency)
   changeEvent := &ChangeEvent{...}
   irecord := changeEvent.AsIRecord()
   
   // For models.Record (hybrid storage)
   record := models.NewRecord(...)
   irecord := models.AsIRecord(record)
   ```

4. **Configure storage adapter** for your use case:
   ```go
   // Create storage adapter with hybrid mode
   storageAdapter := pipeline.NewStorageAdapter(
       pipeline.StorageModeHybrid, 
       config,
   )
   
   // Process records through adapter
   for _, irecord := range records {
       processor.Process(irecord, storageAdapter)
   }
   ```

## Performance Considerations

### Record Processing
- `models.Record` remains the primary type for performance-critical paths
- Adapters add minimal overhead (method indirection only)
- Pool-based memory management preserved through `Release()` method
- Zero-allocation patterns maintained when using `models.Record` directly

### Storage Efficiency
- **Row Mode**: 225 bytes/record for streaming scenarios
- **Columnar Mode**: 84 bytes/record for batch processing (94% reduction)
- **Hybrid Mode**: Automatic selection provides optimal efficiency
- Storage adapter overhead is <5% of total processing time
- Memory pool integration reduces allocations by >90%

### Mode Selection Performance
- Automatic mode selection based on record count: O(1) operation
- Mode switching overhead: <100μs for typical workloads
- Intelligent caching reduces repeated mode evaluations
- Background optimization doesn't block record processing

## Future Enhancements

1. **Advanced Storage Modes**: Add specialized storage for time-series and graph data
2. **Distributed Storage**: Extend hybrid storage across multiple nodes
3. **Schema Evolution**: Dynamic schema adaptation within storage modes
4. **Serialization**: Add unified serialization methods with storage-aware optimization
5. **Metrics**: Enhanced tracking of record type usage, storage efficiency, and conversion patterns
6. **Type Registry**: Dynamic registration of new record types with storage hints
7. **ML-Driven Selection**: Machine learning for workload-based storage optimization
8. **Real-time Analytics**: In-memory analytics on columnar data for immediate insights