# CDC Pipeline Example

This example demonstrates how to use the CDC (Change Data Capture) connectors with the Nebula pipeline framework.

## Prerequisites

### PostgreSQL CDC
1. PostgreSQL 10+ with logical replication enabled
2. Configuration requirements:
   ```
   wal_level = logical
   max_replication_slots = 4
   max_wal_senders = 4
   ```
3. Create publication:
   ```sql
   CREATE PUBLICATION nebula_pub FOR TABLE users, orders;
   ```

### MySQL CDC
1. MySQL 5.6+ with binary logging enabled
2. Configuration requirements:
   ```
   log_bin = ON
   binlog_format = ROW
   binlog_row_image = FULL
   ```
3. Grant replication privileges:
   ```sql
   GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
   ```

## Example 1: PostgreSQL CDC to CSV

```go
package main

import (
    "context"
    "log"
    
    "github.com/ajitpratap0/nebula/pkg/connector/core"
    "github.com/ajitpratap0/nebula/pkg/connector/registry"
    _ "github.com/ajitpratap0/nebula/pkg/connector/sources/postgresql_cdc"
    _ "github.com/ajitpratap0/nebula/pkg/connector/destinations/csv"
)

func main() {
    ctx := context.Background()
    
    // Create PostgreSQL CDC source
    sourceConfig := &core.Config{
        Name:    "pg-cdc",
        Type:    core.ConnectorTypeSource,
        Version: "1.0.0",
        Properties: map[string]interface{}{
            "connection_string": "postgres://user:pass@localhost/mydb?sslmode=disable",
            "database":          "mydb",
            "tables":            []string{"users", "orders"},
            "slot_name":         "nebula_slot",
            "publication":       "nebula_pub",
        },
    }
    
    source, err := registry.GetRegistry().CreateSource("postgresql-cdc", sourceConfig)
    if err != nil {
        log.Fatalf("Failed to create source: %v", err)
    }
    
    if err := source.Initialize(ctx, sourceConfig); err != nil {
        log.Fatalf("Failed to initialize source: %v", err)
    }
    
    // Create CSV destination
    destConfig := &core.Config{
        Name:    "csv-output",
        Type:    core.ConnectorTypeDestination,
        Version: "1.0.0",
        Properties: map[string]interface{}{
            "path":               "cdc_events.csv",
            "enable_compression": true,
            "compression_algorithm": "gzip",
        },
    }
    
    dest, err := registry.GetRegistry().CreateDestination("csv", destConfig)
    if err != nil {
        log.Fatalf("Failed to create destination: %v", err)
    }
    
    if err := dest.Initialize(ctx, destConfig); err != nil {
        log.Fatalf("Failed to initialize destination: %v", err)
    }
    
    // Start reading CDC events
    stream, err := source.Read(ctx)
    if err != nil {
        log.Fatalf("Failed to start reading: %v", err)
    }
    
    // Write to destination
    if err := dest.Write(ctx, stream); err != nil {
        log.Fatalf("Failed to write: %v", err)
    }
}
```

## Example 2: MySQL CDC with Real-time Subscription

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "github.com/ajitpratap0/nebula/pkg/connector/core"
    "github.com/ajitpratap0/nebula/pkg/connector/registry"
    _ "github.com/ajitpratap0/nebula/pkg/connector/sources/mysql_cdc"
)

func main() {
    ctx := context.Background()
    
    // Create MySQL CDC source
    config := &core.Config{
        Name:    "mysql-cdc",
        Type:    core.ConnectorTypeSource,
        Version: "1.0.0",
        Properties: map[string]interface{}{
            "connection_string": "user:pass@tcp(localhost:3306)/mydb",
            "database":          "mydb",
            "tables":            []string{"users", "products"},
            "server_id":         1001,
        },
    }
    
    source, err := registry.GetRegistry().CreateSource("mysql-cdc", config)
    if err != nil {
        log.Fatalf("Failed to create source: %v", err)
    }
    
    if err := source.Initialize(ctx, config); err != nil {
        log.Fatalf("Failed to initialize source: %v", err)
    }
    
    // Subscribe to real-time changes
    changeStream, err := source.Subscribe(ctx, []string{"users"})
    if err != nil {
        log.Fatalf("Failed to subscribe: %v", err)
    }
    
    // Process change events
    for {
        select {
        case change, ok := <-changeStream.Changes:
            if !ok {
                return
            }
            
            fmt.Printf("Change detected: %s on table %s\n", change.Type, change.Table)
            
            switch change.Type {
            case core.ChangeTypeInsert:
                fmt.Printf("  New record: %v\n", change.After)
            case core.ChangeTypeUpdate:
                fmt.Printf("  Before: %v\n", change.Before)
                fmt.Printf("  After: %v\n", change.After)
            case core.ChangeTypeDelete:
                fmt.Printf("  Deleted record: %v\n", change.Before)
            }
            
        case err := <-changeStream.Errors:
            log.Printf("Error: %v", err)
            
        case <-ctx.Done():
            return
        }
    }
}
```

## Example 3: CDC to Data Warehouse Pipeline

```go
package main

import (
    "context"
    "log"
    
    "github.com/ajitpratap0/nebula/pkg/connector/core"
    "github.com/ajitpratap0/nebula/pkg/connector/registry"
    _ "github.com/ajitpratap0/nebula/pkg/connector/sources/postgresql_cdc"
    _ "github.com/ajitpratap0/nebula/pkg/connector/destinations/snowflake"
)

func main() {
    ctx := context.Background()
    
    // PostgreSQL CDC source
    sourceConfig := &core.Config{
        Name: "pg-cdc",
        Properties: map[string]interface{}{
            "connection_string": "postgres://user:pass@localhost/sourcedb",
            "database":          "sourcedb",
            "tables":            []string{"customers", "orders", "products"},
            "slot_name":         "warehouse_sync",
        },
        BatchSize: 5000, // Batch CDC events for better warehouse performance
    }
    
    // Snowflake destination
    destConfig := &core.Config{
        Name: "snowflake-dw",
        Properties: map[string]interface{}{
            "account":   "myaccount",
            "user":      "etl_user",
            "password":  "secret",
            "database":  "ANALYTICS",
            "schema":    "RAW_DATA",
            "warehouse": "ETL_WH",
        },
    }
    
    // Create and initialize connectors
    source, _ := registry.GetRegistry().CreateSource("postgresql-cdc", sourceConfig)
    dest, _ := registry.GetRegistry().CreateDestination("snowflake", destConfig)
    
    source.Initialize(ctx, sourceConfig)
    dest.Initialize(ctx, destConfig)
    
    // Read in batches for better warehouse performance
    batchStream, err := source.ReadBatch(ctx, 5000)
    if err != nil {
        log.Fatalf("Failed to start batch reading: %v", err)
    }
    
    // Write batches to Snowflake
    if err := dest.WriteBatch(ctx, batchStream); err != nil {
        log.Fatalf("Failed to write batches: %v", err)
    }
}
```

## Configuration Options

### PostgreSQL CDC
- `connection_string`: PostgreSQL connection string
- `database`: Database name
- `tables`: Array of table names to replicate
- `slot_name`: Replication slot name (optional, auto-generated if not provided)
- `publication`: Publication name (optional, auto-generated if not provided)
- `start_lsn`: Starting LSN for replication (optional)

### MySQL CDC
- `connection_string`: MySQL connection string
- `database`: Database name
- `tables`: Array of table names to replicate
- `server_id`: Unique server ID for replication

## Performance Considerations

1. **Batching**: Use `ReadBatch()` for better performance when writing to analytical systems
2. **Compression**: Enable compression when writing to file-based destinations
3. **Filtering**: Subscribe only to specific tables you need
4. **Position Management**: Save and restore positions for resumable pipelines

## Error Handling

```go
// Example with proper error handling and position management
lastPosition := source.GetPosition()

stream, err := source.Read(ctx)
if err != nil {
    // Try to restore from last known position
    source.SetPosition(lastPosition)
    stream, err = source.Read(ctx)
    if err != nil {
        log.Fatalf("Failed to resume: %v", err)
    }
}
```

## Monitoring

```go
// Get CDC metrics
metrics := source.Metrics()
fmt.Printf("Events processed: %v\n", metrics["event_count"])
fmt.Printf("Current lag: %v\n", metrics["lag"])
fmt.Printf("Last event: %v\n", metrics["last_event"])

// Check health
if err := source.Health(ctx); err != nil {
    log.Printf("CDC source unhealthy: %v", err)
}
```