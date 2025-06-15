# Connector Development Guide

This guide walks you through creating custom connectors for Nebula, following production-ready patterns and best practices.

## Overview

Nebula connectors are the bridge between external systems and the high-performance pipeline. All connectors must:
- Implement core interfaces (`Source` or `Destination`)
- Use `BaseConnector` for production features
- Follow the unified configuration pattern
- Integrate with the memory pool system
- Support the hybrid storage engine

## Connector Types

### Source Connectors
Extract data from external systems:
- Databases (PostgreSQL, MySQL, MongoDB)
- APIs (REST, GraphQL, gRPC)
- Files (CSV, JSON, Parquet)
- Streams (Kafka, Kinesis, Pub/Sub)

### Destination Connectors
Load data into target systems:
- Data Warehouses (Snowflake, BigQuery, Redshift)
- Databases (PostgreSQL, MySQL, MongoDB)
- Object Storage (S3, GCS, Azure Blob)
- Files (CSV, JSON, Parquet)

## Step-by-Step Guide

### 1. Create Connector Structure

```bash
# For a source connector
mkdir -p pkg/connector/sources/myconnector

# For a destination connector
mkdir -p pkg/connector/destinations/myconnector

# Create necessary files
touch pkg/connector/sources/myconnector/{connector.go,config.go,init.go,connector_test.go,README.md}
```

### 2. Define Configuration

Create `config.go`:

```go
package myconnector

import (
    "github.com/ajitpratap0/nebula/pkg/config"
    "github.com/ajitpratap0/nebula/pkg/errors"
)

// Config extends BaseConfig with connector-specific fields
type Config struct {
    config.BaseConfig `yaml:",inline" json:",inline"`
    
    // Connection settings
    Host     string `yaml:"host" json:"host" validate:"required"`
    Port     int    `yaml:"port" json:"port" validate:"required,min=1,max=65535"`
    Database string `yaml:"database" json:"database" validate:"required"`
    
    // Authentication
    Username string `yaml:"username" json:"username" validate:"required"`
    Password string `yaml:"password" json:"password" validate:"required"`
    
    // Connector-specific settings
    TableName    string `yaml:"table_name" json:"table_name"`
    QueryTimeout int    `yaml:"query_timeout" json:"query_timeout" default:"30"`
}

// Validate validates the configuration
func (c *Config) Validate() error {
    // First validate base configuration
    if err := c.BaseConfig.Validate(); err != nil {
        return errors.Wrap(err, errors.ErrorTypeConfig, "base config validation failed")
    }
    
    // Add custom validation
    if c.QueryTimeout < 1 {
        return errors.New(errors.ErrorTypeConfig, "query_timeout must be positive")
    }
    
    return nil
}

// ApplyDefaults applies default values
func (c *Config) ApplyDefaults() {
    c.BaseConfig.ApplyDefaults()
    
    if c.QueryTimeout == 0 {
        c.QueryTimeout = 30
    }
}
```

### 3. Implement the Connector

Create `connector.go`:

```go
package myconnector

import (
    "context"
    "database/sql"
    "fmt"
    "sync/atomic"
    "time"
    
    "github.com/ajitpratap0/nebula/pkg/connector/base"
    "github.com/ajitpratap0/nebula/pkg/connector/core"
    "github.com/ajitpratap0/nebula/pkg/errors"
    "github.com/ajitpratap0/nebula/pkg/logger"
    "github.com/ajitpratap0/nebula/pkg/pool"
    _ "github.com/lib/pq" // PostgreSQL driver
)

// Ensure interface compliance
var _ core.Source = (*Connector)(nil)

// Connector implements the Source interface
type Connector struct {
    base.BaseConnector
    
    config *Config
    db     *sql.DB
    
    // Metrics
    recordsRead atomic.Int64
    bytesRead   atomic.Int64
}

// NewConnector creates a new connector instance
func NewConnector(cfg *config.BaseConfig) (core.Source, error) {
    // Type assertion to our config
    myConfig, ok := cfg.(*Config)
    if !ok {
        return nil, errors.New(errors.ErrorTypeConfig, "invalid config type")
    }
    
    // Validate configuration
    if err := myConfig.Validate(); err != nil {
        return nil, err
    }
    
    connector := &Connector{
        config: myConfig,
    }
    
    // Initialize base connector with production features
    if err := connector.BaseConnector.Init(
        "myconnector",
        &myConfig.BaseConfig,
        connector,
    ); err != nil {
        return nil, errors.Wrap(err, errors.ErrorTypeInitialization, "failed to initialize base connector")
    }
    
    return connector, nil
}

// Initialize prepares the connector for reading
func (c *Connector) Initialize(ctx context.Context) error {
    return c.BaseConnector.Initialize(ctx, func() error {
        // Create connection string
        connStr := fmt.Sprintf(
            "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
            c.config.Host,
            c.config.Port,
            c.config.Username,
            c.config.Password,
            c.config.Database,
        )
        
        // Open database connection
        db, err := sql.Open("postgres", connStr)
        if err != nil {
            return errors.Wrap(err, errors.ErrorTypeConnection, "failed to open database")
        }
        
        // Configure connection pool
        db.SetMaxOpenConns(c.config.Performance.MaxConcurrency)
        db.SetMaxIdleConns(c.config.Performance.Workers / 4)
        db.SetConnMaxLifetime(c.config.Timeouts.Connection)
        db.SetConnMaxIdleTime(c.config.Timeouts.Idle)
        
        // Test connection
        ctx, cancel := context.WithTimeout(ctx, c.config.Timeouts.Connection)
        defer cancel()
        
        if err := db.PingContext(ctx); err != nil {
            db.Close()
            return errors.Wrap(err, errors.ErrorTypeConnection, "failed to ping database")
        }
        
        c.db = db
        logger.Info("connector initialized",
            logger.String("host", c.config.Host),
            logger.String("database", c.config.Database),
        )
        
        return nil
    })
}

// Read reads records from the source
func (c *Connector) Read(ctx context.Context) (<-chan *pool.Record, error) {
    recordChan := make(chan *pool.Record, c.config.Performance.BatchSize)
    
    go func() {
        defer close(recordChan)
        
        // Use circuit breaker for resilience
        err := c.CircuitBreaker.Execute(func() error {
            return c.readData(ctx, recordChan)
        })
        
        if err != nil {
            logger.Error("read failed",
                logger.Error(err),
                logger.String("connector", c.Name()),
            )
        }
    }()
    
    return recordChan, nil
}

// readData performs the actual data reading
func (c *Connector) readData(ctx context.Context, recordChan chan<- *pool.Record) error {
    // Prepare query
    query := fmt.Sprintf("SELECT * FROM %s", c.config.TableName)
    if c.config.Performance.Limit > 0 {
        query += fmt.Sprintf(" LIMIT %d", c.config.Performance.Limit)
    }
    
    // Execute query with timeout
    ctx, cancel := context.WithTimeout(ctx, time.Duration(c.config.QueryTimeout)*time.Second)
    defer cancel()
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return errors.Wrap(err, errors.ErrorTypeRead, "query failed")
    }
    defer rows.Close()
    
    // Get column information
    columns, err := rows.Columns()
    if err != nil {
        return errors.Wrap(err, errors.ErrorTypeRead, "failed to get columns")
    }
    
    // Read rows
    batchCount := 0
    for rows.Next() {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }
        
        // Apply rate limiting
        if !c.RateLimiter.Allow() {
            c.RateLimiter.Wait(ctx)
        }
        
        // Get record from pool
        record := pool.GetRecord()
        
        // Scan row into record
        values := make([]interface{}, len(columns))
        scanArgs := make([]interface{}, len(columns))
        for i := range values {
            scanArgs[i] = &values[i]
        }
        
        if err := rows.Scan(scanArgs...); err != nil {
            record.Release()
            return errors.Wrap(err, errors.ErrorTypeRead, "failed to scan row")
        }
        
        // Populate record data
        for i, col := range columns {
            record.SetData(col, values[i])
        }
        
        // Set metadata
        record.Metadata.Source = c.Name()
        record.Metadata.Timestamp = time.Now()
        record.SetMetadata("table", c.config.TableName)
        
        // Update metrics
        c.recordsRead.Add(1)
        c.bytesRead.Add(int64(record.Size()))
        
        // Send record
        select {
        case recordChan <- record:
            batchCount++
            
            // Check batch size for backpressure
            if batchCount >= c.config.Performance.BatchSize {
                time.Sleep(10 * time.Millisecond) // Simple backpressure
                batchCount = 0
            }
        case <-ctx.Done():
            record.Release()
            return ctx.Err()
        }
    }
    
    return rows.Err()
}

// ReadBatch reads a batch of records
func (c *Connector) ReadBatch(ctx context.Context, size int) ([]*pool.Record, error) {
    records := make([]*pool.Record, 0, size)
    
    recordChan, err := c.Read(ctx)
    if err != nil {
        return nil, err
    }
    
    for record := range recordChan {
        records = append(records, record)
        if len(records) >= size {
            break
        }
    }
    
    return records, nil
}

// Schema returns the source schema
func (c *Connector) Schema(ctx context.Context) (*core.Schema, error) {
    // Query table schema
    query := `
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = $1
        ORDER BY ordinal_position
    `
    
    rows, err := c.db.QueryContext(ctx, query, c.config.TableName)
    if err != nil {
        return nil, errors.Wrap(err, errors.ErrorTypeRead, "failed to query schema")
    }
    defer rows.Close()
    
    schema := &core.Schema{
        Name:    c.config.TableName,
        Columns: []core.Column{},
    }
    
    for rows.Next() {
        var colName, dataType, isNullable string
        if err := rows.Scan(&colName, &dataType, &isNullable); err != nil {
            return nil, errors.Wrap(err, errors.ErrorTypeRead, "failed to scan schema row")
        }
        
        schema.Columns = append(schema.Columns, core.Column{
            Name:     colName,
            Type:     mapPostgreSQLType(dataType),
            Nullable: isNullable == "YES",
        })
    }
    
    return schema, nil
}

// Close cleans up resources
func (c *Connector) Close(ctx context.Context) error {
    return c.BaseConnector.Close(ctx, func() error {
        if c.db != nil {
            return c.db.Close()
        }
        return nil
    })
}

// Metrics returns performance metrics
func (c *Connector) Metrics() core.Metrics {
    baseMetrics := c.BaseConnector.Metrics()
    
    // Add connector-specific metrics
    return core.Metrics{
        RecordsProcessed: c.recordsRead.Load(),
        BytesProcessed:   c.bytesRead.Load(),
        ErrorCount:       baseMetrics.ErrorCount,
        StartTime:        baseMetrics.StartTime,
        // ... other metrics
    }
}

// Health performs a health check
func (c *Connector) Health(ctx context.Context) error {
    return c.BaseConnector.Health(ctx, func() error {
        if c.db == nil {
            return errors.New(errors.ErrorTypeConnection, "database not connected")
        }
        
        ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
        defer cancel()
        
        return c.db.PingContext(ctx)
    })
}

// Helper function to map PostgreSQL types
func mapPostgreSQLType(pgType string) string {
    switch pgType {
    case "integer", "bigint", "smallint":
        return "int64"
    case "numeric", "decimal", "real", "double precision":
        return "float64"
    case "boolean":
        return "bool"
    case "timestamp", "timestamptz", "date":
        return "timestamp"
    default:
        return "string"
    }
}
```

### 4. Register the Connector

Create `init.go`:

```go
package myconnector

import (
    "github.com/ajitpratap0/nebula/pkg/config"
    "github.com/ajitpratap0/nebula/pkg/connector/core"
    "github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
    // Register the connector factory
    registry.RegisterSource("myconnector", func(cfg *config.BaseConfig) (core.Source, error) {
        // Convert to our config type
        myConfig := &Config{
            BaseConfig: *cfg,
        }
        
        return NewConnector(&myConfig.BaseConfig)
    })
}
```

### 5. Write Tests

Create `connector_test.go`:

```go
package myconnector

import (
    "context"
    "testing"
    "time"
    
    "github.com/ajitpratap0/nebula/pkg/config"
    "github.com/ajitpratap0/nebula/pkg/pool"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestConnectorImplementsInterface(t *testing.T) {
    // Ensure the connector implements the interface
    var _ core.Source = (*Connector)(nil)
}

func TestNewConnector(t *testing.T) {
    cfg := &Config{
        BaseConfig: *config.NewBaseConfig("test", "source"),
        Host:       "localhost",
        Port:       5432,
        Database:   "testdb",
        Username:   "user",
        Password:   "pass",
        TableName:  "test_table",
    }
    
    connector, err := NewConnector(&cfg.BaseConfig)
    require.NoError(t, err)
    require.NotNil(t, connector)
    
    // Type assert to access internal fields
    conn := connector.(*Connector)
    assert.Equal(t, "localhost", conn.config.Host)
    assert.Equal(t, 5432, conn.config.Port)
}

func TestConfigValidation(t *testing.T) {
    tests := []struct {
        name    string
        config  *Config
        wantErr bool
    }{
        {
            name: "valid config",
            config: &Config{
                BaseConfig: *config.NewBaseConfig("test", "source"),
                Host:       "localhost",
                Port:       5432,
                Database:   "testdb",
                Username:   "user",
                Password:   "pass",
            },
            wantErr: false,
        },
        {
            name: "missing host",
            config: &Config{
                BaseConfig: *config.NewBaseConfig("test", "source"),
                Port:       5432,
                Database:   "testdb",
                Username:   "user",
                Password:   "pass",
            },
            wantErr: true,
        },
        {
            name: "invalid port",
            config: &Config{
                BaseConfig: *config.NewBaseConfig("test", "source"),
                Host:       "localhost",
                Port:       99999,
                Database:   "testdb",
                Username:   "user",
                Password:   "pass",
            },
            wantErr: true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := tt.config.Validate()
            if tt.wantErr {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
            }
        })
    }
}

// Integration test with test container
func TestConnectorIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test")
    }
    
    // Use testcontainers or similar for database
    // This is a simplified example
    
    cfg := &Config{
        BaseConfig: *config.NewBaseConfig("test", "source"),
        Host:       "localhost",
        Port:       5432,
        Database:   "testdb",
        Username:   "postgres",
        Password:   "postgres",
        TableName:  "test_table",
    }
    
    connector, err := NewConnector(&cfg.BaseConfig)
    require.NoError(t, err)
    
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    // Initialize
    err = connector.Initialize(ctx)
    require.NoError(t, err)
    defer connector.Close(ctx)
    
    // Test health check
    err = connector.Health(ctx)
    assert.NoError(t, err)
    
    // Test reading
    recordChan, err := connector.Read(ctx)
    require.NoError(t, err)
    
    count := 0
    for record := range recordChan {
        assert.NotNil(t, record)
        assert.NotEmpty(t, record.GetSource())
        assert.NotNil(t, record.GetData())
        
        // Release record back to pool
        record.Release()
        
        count++
        if count >= 10 {
            break
        }
    }
    
    // Check metrics
    metrics := connector.Metrics()
    assert.Greater(t, metrics.RecordsProcessed, int64(0))
}

// Benchmark
func BenchmarkConnectorRead(b *testing.B) {
    cfg := &Config{
        BaseConfig: *config.NewBaseConfig("bench", "source"),
        Host:       "localhost",
        Port:       5432,
        Database:   "benchdb",
        Username:   "postgres",
        Password:   "postgres",
        TableName:  "bench_table",
    }
    
    connector, err := NewConnector(&cfg.BaseConfig)
    require.NoError(b, err)
    
    ctx := context.Background()
    err = connector.Initialize(ctx)
    require.NoError(b, err)
    defer connector.Close(ctx)
    
    b.ResetTimer()
    b.ReportAllocs()
    
    for i := 0; i < b.N; i++ {
        records, err := connector.ReadBatch(ctx, 1000)
        require.NoError(b, err)
        
        for _, record := range records {
            record.Release()
        }
    }
    
    b.StopTimer()
    
    metrics := connector.Metrics()
    b.ReportMetric(float64(metrics.RecordsProcessed)/b.Elapsed().Seconds(), "records/sec")
}
```

### 6. Add Documentation

Create `README.md`:

```markdown
# MyConnector Source

A high-performance source connector for PostgreSQL databases.

## Features

- Connection pooling with configurable limits
- Circuit breaker for fault tolerance
- Rate limiting to prevent overwhelming the source
- Schema discovery
- Streaming and batch read modes
- Comprehensive metrics and health checks

## Configuration

```yaml
name: my-postgres-source
type: source

# Connection settings
host: localhost
port: 5432
database: mydb
username: user
password: ${DB_PASSWORD}
table_name: users

# Performance settings (from BaseConfig)
performance:
  batch_size: 10000
  workers: 4
  max_concurrency: 100

# Timeouts (from BaseConfig)
timeouts:
  connection: 30s
  request: 60s
  idle: 5m

# Reliability (from BaseConfig)
reliability:
  max_retries: 3
  retry_delay: 1s
  circuit_breaker:
    enabled: true
    threshold: 5
    timeout: 60s
```

## Usage

```go
// Create connector
config := &myconnector.Config{
    BaseConfig: *config.NewBaseConfig("postgres", "source"),
    Host:       "localhost",
    Port:       5432,
    Database:   "mydb",
    Username:   "user",
    Password:   "pass",
    TableName:  "users",
}

connector, err := myconnector.NewConnector(&config.BaseConfig)
if err != nil {
    log.Fatal(err)
}

// Initialize
err = connector.Initialize(ctx)
if err != nil {
    log.Fatal(err)
}
defer connector.Close(ctx)

// Read records
recordChan, err := connector.Read(ctx)
if err != nil {
    log.Fatal(err)
}

for record := range recordChan {
    // Process record
    fmt.Printf("Record: %+v\n", record.GetData())
    
    // Always release back to pool
    record.Release()
}
```

## Performance

- Throughput: 100K+ records/second
- Memory: Integrated with Nebula's pool system
- Latency: <1ms per record in streaming mode
```

## Production Checklist

Before your connector is production-ready, ensure:

### ✅ Core Requirements
- [ ] Implements `core.Source` or `core.Destination` interface
- [ ] Uses `base.BaseConnector` for common functionality
- [ ] Extends `config.BaseConfig` for configuration
- [ ] Integrates with `pool.Record` system
- [ ] Supports hybrid storage modes

### ✅ Production Features
- [ ] Circuit breaker implementation
- [ ] Rate limiting support
- [ ] Health check endpoint
- [ ] Comprehensive metrics
- [ ] Graceful shutdown
- [ ] Context cancellation
- [ ] Backpressure handling

### ✅ Error Handling
- [ ] Uses structured errors from `pkg/errors`
- [ ] Distinguishes retryable vs fatal errors
- [ ] Provides meaningful error messages
- [ ] Logs errors with context

### ✅ Performance
- [ ] Zero allocations in hot paths
- [ ] Connection pooling configured
- [ ] Batch operations implemented
- [ ] Benchmarks included
- [ ] Memory usage optimized

### ✅ Testing
- [ ] Unit tests with >80% coverage
- [ ] Integration tests with testcontainers
- [ ] Benchmark tests
- [ ] Error scenario tests
- [ ] Performance regression tests

### ✅ Documentation
- [ ] README with examples
- [ ] Configuration reference
- [ ] Performance characteristics
- [ ] Troubleshooting guide
- [ ] API documentation

## Advanced Topics

### Implementing CDC Support

For Change Data Capture:

```go
func (c *Connector) ReadCDC(ctx context.Context, position string) (<-chan *pool.Record, error) {
    recordChan := make(chan *pool.Record, c.config.Performance.BatchSize)
    
    go func() {
        defer close(recordChan)
        
        // Set up CDC reader (e.g., PostgreSQL logical replication)
        // Read from position
        // Emit CDC records with operation type
    }()
    
    return recordChan, nil
}
```

### Schema Evolution

Handle schema changes:

```go
func (c *Connector) HandleSchemaChange(old, new *core.Schema) error {
    // Compare schemas
    // Apply evolution strategy
    // Update internal state
    return nil
}
```

### Custom Storage Integration

Optimize for specific storage modes:

```go
func (c *Connector) OptimizeForColumnar() {
    // Pre-sort data by columns
    // Enable compression
    // Adjust batch sizes
}
```

## Best Practices

1. **Always use BaseConnector** - Don't reinvent production features
2. **Pool everything** - Records, buffers, connections
3. **Handle backpressure** - Respect downstream capacity
4. **Test with large datasets** - Ensure scalability
5. **Profile regularly** - Monitor allocations and CPU usage
6. **Document thoroughly** - Include examples and benchmarks
7. **Version your schemas** - Plan for evolution
8. **Monitor in production** - Use metrics and health checks

## Next Steps

- Review existing connectors for examples
- Run the test suite for your connector
- Benchmark against performance targets
- Submit PR with complete documentation