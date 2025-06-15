# Core Interfaces

This document describes the core interfaces that define Nebula's architecture.

## Connector Interfaces

### Source Interface

The `Source` interface defines the contract for all data sources.

```go
// Source represents a data source connector
type Source interface {
    // Read reads records from the source
    Read(ctx context.Context) (<-chan *pool.Record, error)
    
    // ReadBatch reads a batch of records
    ReadBatch(ctx context.Context, size int) ([]*pool.Record, error)
    
    // Schema returns the source schema
    Schema(ctx context.Context) (*Schema, error)
    
    // Initialize prepares the source for reading
    Initialize(ctx context.Context) error
    
    // Close cleans up resources
    Close(ctx context.Context) error
    
    // Metrics returns performance metrics
    Metrics() Metrics
    
    // Health checks if the source is healthy
    Health(ctx context.Context) error
}
```

### Destination Interface

The `Destination` interface defines the contract for all data destinations.

```go
// Destination represents a data destination connector
type Destination interface {
    // Write writes records to the destination
    Write(ctx context.Context, records <-chan *pool.Record) error
    
    // WriteBatch writes a batch of records
    WriteBatch(ctx context.Context, records []*pool.Record) error
    
    // Schema returns the destination schema
    Schema(ctx context.Context) (*Schema, error)
    
    // Initialize prepares the destination for writing
    Initialize(ctx context.Context) error
    
    // Close cleans up resources
    Close(ctx context.Context) error
    
    // Metrics returns performance metrics
    Metrics() Metrics
    
    // Health checks if the destination is healthy
    Health(ctx context.Context) error
    
    // BeginTransaction starts a transaction
    BeginTransaction(ctx context.Context) (Transaction, error)
}
```

### Transaction Interface

For destinations that support transactions.

```go
// Transaction represents a database transaction
type Transaction interface {
    // Commit commits the transaction
    Commit() error
    
    // Rollback rolls back the transaction
    Rollback() error
    
    // WriteBatch writes a batch within the transaction
    WriteBatch(ctx context.Context, records []*pool.Record) error
}
```

## Storage Interfaces

### StorageAdapter Interface

The unified interface for all storage modes.

```go
// StorageAdapter provides a unified interface for storage
type StorageAdapter interface {
    // AddRecord adds a record to storage
    AddRecord(record *pool.Record) error
    
    // GetRecords retrieves all records
    GetRecords() ([]*pool.Record, error)
    
    // GetIterator returns an iterator for records
    GetIterator() Iterator
    
    // Flush ensures all data is persisted
    Flush() error
    
    // Close releases resources
    Close() error
    
    // GetMemoryUsage returns current memory usage
    GetMemoryUsage() int64
    
    // GetRecordCount returns number of records
    GetRecordCount() int64
    
    // GetMemoryPerRecord returns bytes per record
    GetMemoryPerRecord() float64
    
    // OptimizeStorage runs background optimization
    OptimizeStorage()
    
    // GetStorageMode returns current storage mode
    GetStorageMode() StorageMode
}
```

### Iterator Interface

For efficient record iteration.

```go
// Iterator provides sequential access to records
type Iterator interface {
    // Next returns the next record
    Next() (*pool.Record, error)
    
    // HasNext checks if more records exist
    HasNext() bool
    
    // Reset resets the iterator
    Reset()
    
    // Close releases resources
    Close() error
}
```

## Pool Interfaces

### Poolable Interface

Objects that can be pooled must implement this interface.

```go
// Poolable represents an object that can be pooled
type Poolable interface {
    // Reset resets the object to initial state
    Reset()
}
```

### Pool Interface

The generic pool interface.

```go
// Pool provides object pooling
type Pool[T Poolable] interface {
    // Get retrieves an object from the pool
    Get() T
    
    // Put returns an object to the pool
    Put(obj T)
    
    // Len returns the current pool size
    Len() int
    
    // Cap returns the pool capacity
    Cap() int
}
```

## Record Interfaces

### IRecord Interface

The unified record interface implemented by all record types.

```go
// IRecord defines the interface for all record types
type IRecord interface {
    // GetSource returns the record source
    GetSource() string
    
    // GetData returns the record data
    GetData() map[string]any
    
    // SetData sets a field value
    SetData(key string, value any)
    
    // GetMetadata returns metadata
    GetMetadata() map[string]any
    
    // SetMetadata sets metadata
    SetMetadata(key string, value any)
    
    // Release returns the record to the pool
    Release()
    
    // Clone creates a copy of the record
    Clone() IRecord
    
    // Validate checks if the record is valid
    Validate() error
}
```

## Configuration Interfaces

### Validatable Interface

Configuration objects that can be validated.

```go
// Validatable represents a configuration that can be validated
type Validatable interface {
    // Validate checks if the configuration is valid
    Validate() error
}
```

### Defaultable Interface

Configuration objects that can apply defaults.

```go
// Defaultable represents a configuration with defaults
type Defaultable interface {
    // ApplyDefaults applies default values
    ApplyDefaults()
}
```

## Error Interfaces

### ErrorContext Interface

Errors with additional context.

```go
// ErrorContext provides additional error context
type ErrorContext interface {
    error
    
    // Type returns the error type
    Type() ErrorType
    
    // Details returns error details
    Details() map[string]any
    
    // Retryable indicates if the operation can be retried
    Retryable() bool
    
    // Wrap wraps another error
    Wrap(err error) ErrorContext
}
```

## Metrics Interfaces

### MetricsProvider Interface

Components that expose metrics.

```go
// MetricsProvider exposes metrics
type MetricsProvider interface {
    // Metrics returns current metrics
    Metrics() Metrics
    
    // ResetMetrics resets all metrics
    ResetMetrics()
}
```

### Metrics Interface

The metrics data structure.

```go
// Metrics represents performance metrics
type Metrics interface {
    // RecordsProcessed returns total records
    RecordsProcessed() int64
    
    // BytesProcessed returns total bytes
    BytesProcessed() int64
    
    // ProcessingTime returns total time
    ProcessingTime() time.Duration
    
    // ErrorCount returns error count
    ErrorCount() int64
    
    // Throughput returns records per second
    Throughput() float64
    
    // ToPrometheus exports to Prometheus format
    ToPrometheus() []prometheus.Metric
}
```

## Health Check Interfaces

### HealthCheckable Interface

Components that support health checks.

```go
// HealthCheckable supports health checks
type HealthCheckable interface {
    // Health performs a health check
    Health(ctx context.Context) error
    
    // HealthDetails returns detailed health information
    HealthDetails(ctx context.Context) HealthStatus
}
```

### HealthStatus Interface

Detailed health information.

```go
// HealthStatus represents component health
type HealthStatus interface {
    // Status returns the health status
    Status() string // "healthy", "degraded", "unhealthy"
    
    // Details returns status details
    Details() map[string]any
    
    // LastCheck returns last check time
    LastCheck() time.Time
}
```

## Circuit Breaker Interfaces

### CircuitBreaker Interface

For fault tolerance.

```go
// CircuitBreaker provides fault tolerance
type CircuitBreaker interface {
    // Execute runs a function with circuit breaker protection
    Execute(fn func() error) error
    
    // State returns current state
    State() CircuitState
    
    // Reset manually resets the circuit
    Reset()
    
    // Metrics returns circuit breaker metrics
    Metrics() CircuitMetrics
}
```

## Rate Limiter Interfaces

### RateLimiter Interface

For rate limiting.

```go
// RateLimiter controls request rates
type RateLimiter interface {
    // Allow checks if a request is allowed
    Allow() bool
    
    // AllowN checks if N requests are allowed
    AllowN(n int) bool
    
    // Wait blocks until allowed
    Wait(ctx context.Context) error
    
    // WaitN blocks until N allowed
    WaitN(ctx context.Context, n int) error
    
    // Limit returns current limit
    Limit() rate.Limit
    
    // SetLimit updates the limit
    SetLimit(newLimit rate.Limit)
}
```

## Schema Interfaces

### Schema Interface

For schema management.

```go
// Schema represents data schema
type Schema interface {
    // Columns returns column definitions
    Columns() []Column
    
    // GetColumn returns a specific column
    GetColumn(name string) (Column, bool)
    
    // AddColumn adds a new column
    AddColumn(col Column) error
    
    // RemoveColumn removes a column
    RemoveColumn(name string) error
    
    // Compatible checks compatibility with another schema
    Compatible(other Schema) bool
    
    // Evolve evolves to match another schema
    Evolve(target Schema) (Schema, error)
}
```

## Best Practices

1. **Interface Segregation**: Keep interfaces small and focused
2. **Explicit Context**: Always pass context.Context as first parameter
3. **Error Handling**: Return error as last return value
4. **Resource Management**: Include Close() method for cleanup
5. **Metrics**: Include Metrics() method for observability
6. **Health Checks**: Include Health() method for monitoring
7. **Validation**: Validate inputs at interface boundaries

## Implementation Guidelines

When implementing these interfaces:

1. Embed `base.BaseConnector` for common functionality
2. Use `config.BaseConfig` for configuration
3. Use `pool.Record` for data records
4. Use `errors` package for error handling
5. Implement comprehensive metrics
6. Add proper logging with context
7. Handle context cancellation
8. Test interface compliance

Example implementation:

```go
// Ensure interface compliance
var _ core.Source = (*MySource)(nil)

type MySource struct {
    base.BaseConnector
    config *MySourceConfig
}

func (s *MySource) Read(ctx context.Context) (<-chan *pool.Record, error) {
    // Implementation
}
```