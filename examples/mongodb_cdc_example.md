# MongoDB CDC Pipeline Example

This example demonstrates how to use the MongoDB CDC (Change Data Capture) connector with the Nebula pipeline framework.

## Prerequisites

### MongoDB Configuration

1. **MongoDB 3.6+** with replica set or sharded cluster
2. Change streams require replica set or sharded cluster configuration
3. Example replica set setup:
   ```bash
   # Start MongoDB as a replica set
   mongod --replSet rs0 --port 27017 --dbpath /data/db
   
   # Initialize replica set
   mongosh
   > rs.initiate()
   ```

## Example 1: Basic MongoDB CDC to CSV

```go
package main

import (
    "context"
    "log"
    
    "github.com/ajitpratap0/nebula/pkg/connector/core"
    "github.com/ajitpratap0/nebula/pkg/connector/registry"
    _ "github.com/ajitpratap0/nebula/pkg/connector/sources/mongodb_cdc"
    _ "github.com/ajitpratap0/nebula/pkg/connector/destinations/csv"
)

func main() {
    ctx := context.Background()
    
    // Create MongoDB CDC source
    sourceConfig := &core.Config{
        Name:    "mongo-cdc",
        Type:    core.ConnectorTypeSource,
        Version: "1.0.0",
        Properties: map[string]interface{}{
            "connection_string": "mongodb://localhost:27017",
            "database":          "mydb",
            "collections":       []string{"users", "products", "orders"},
            "full_document":     "updateLookup",
            "full_document_before_change": "whenAvailable",
        },
    }
    
    source, err := registry.GetRegistry().CreateSource("mongodb-cdc", sourceConfig)
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
            "path":               "mongodb_changes.csv",
            "enable_compression": true,
            "compression_algorithm": "zstd",
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

## Example 2: MongoDB CDC with Resume Token

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "os"
    
    "github.com/ajitpratap0/nebula/pkg/connector/core"
    "github.com/ajitpratap0/nebula/pkg/connector/registry"
    _ "github.com/ajitpratap0/nebula/pkg/connector/sources/mongodb_cdc"
)

func main() {
    ctx := context.Background()
    
    // Load resume token if available
    var resumeToken string
    if data, err := os.ReadFile("resume_token.json"); err == nil {
        resumeToken = string(data)
    }
    
    // Create MongoDB CDC source with resume token
    config := &core.Config{
        Name:    "mongo-cdc",
        Type:    core.ConnectorTypeSource,
        Version: "1.0.0",
        Properties: map[string]interface{}{
            "connection_string": "mongodb://localhost:27017",
            "database":          "mydb",
            "collections":       []string{"users"},
            "resume_token":      resumeToken, // Resume from last position
            "batch_size":        100,
            "max_await_time":    "30s",
        },
    }
    
    source, err := registry.GetRegistry().CreateSource("mongodb-cdc", config)
    if err != nil {
        log.Fatalf("Failed to create source: %v", err)
    }
    
    if err := source.Initialize(ctx, config); err != nil {
        log.Fatalf("Failed to initialize source: %v", err)
    }
    
    // Read changes
    stream, err := source.Read(ctx)
    if err != nil {
        log.Fatalf("Failed to read: %v", err)
    }
    
    // Process changes and save position periodically
    count := 0
    for {
        select {
        case record, ok := <-stream.Records:
            if !ok {
                return
            }
            
            count++
            fmt.Printf("Change %d: %s on %s\n", 
                count,
                record.Metadata["cdc_operation"],
                record.Metadata["collection"])
            
            // Save position every 100 records
            if count%100 == 0 {
                position := source.GetPosition()
                if data, err := json.Marshal(position.String()); err == nil {
                    os.WriteFile("resume_token.json", data, 0644)
                }
            }
            
        case err := <-stream.Errors:
            log.Printf("Error: %v", err)
            
        case <-ctx.Done():
            return
        }
    }
}
```

## Example 3: Real-time Collection Subscription

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "github.com/ajitpratap0/nebula/pkg/connector/core"
    "github.com/ajitpratap0/nebula/pkg/connector/registry"
    _ "github.com/ajitpratap0/nebula/pkg/connector/sources/mongodb_cdc"
)

func main() {
    ctx := context.Background()
    
    // Create MongoDB CDC source
    config := &core.Config{
        Name:    "mongo-cdc",
        Type:    core.ConnectorTypeSource,
        Version: "1.0.0",
        Properties: map[string]interface{}{
            "connection_string": "mongodb://localhost:27017",
            "database":          "mydb",
            "include_operation_types": []interface{}{"insert", "update", "delete"},
        },
    }
    
    source, err := registry.GetRegistry().CreateSource("mongodb-cdc", config)
    if err != nil {
        log.Fatalf("Failed to create source: %v", err)
    }
    
    if err := source.Initialize(ctx, config); err != nil {
        log.Fatalf("Failed to initialize source: %v", err)
    }
    
    // Subscribe to specific collections
    changeStream, err := source.Subscribe(ctx, []string{"users", "products"})
    if err != nil {
        log.Fatalf("Failed to subscribe: %v", err)
    }
    
    // Process real-time changes
    for {
        select {
        case change, ok := <-changeStream.Changes:
            if !ok {
                return
            }
            
            fmt.Printf("Change detected: %s on collection %s\n", 
                change.Type, change.Table)
            
            switch change.Type {
            case core.ChangeTypeInsert:
                fmt.Printf("  New document: %v\n", change.After)
            case core.ChangeTypeUpdate:
                fmt.Printf("  Before: %v\n", change.Before)
                fmt.Printf("  After: %v\n", change.After)
            case core.ChangeTypeDelete:
                fmt.Printf("  Deleted document: %v\n", change.Before)
            }
            
        case err := <-changeStream.Errors:
            log.Printf("Error: %v", err)
            
        case <-ctx.Done():
            return
        }
    }
}
```

## Example 4: MongoDB CDC to Data Lake

```go
package main

import (
    "context"
    "log"
    
    "github.com/ajitpratap0/nebula/pkg/connector/core"
    "github.com/ajitpratap0/nebula/pkg/connector/registry"
    _ "github.com/ajitpratap0/nebula/pkg/connector/sources/mongodb_cdc"
    _ "github.com/ajitpratap0/nebula/pkg/connector/destinations/s3"
)

func main() {
    ctx := context.Background()
    
    // MongoDB CDC source
    sourceConfig := &core.Config{
        Name: "mongo-cdc",
        Properties: map[string]interface{}{
            "connection_string": "mongodb://localhost:27017",
            "database":          "production",
            "collections":       []string{"events", "transactions"},
            "full_document":     "updateLookup",
        },
        BatchSize: 10000, // Batch changes for better S3 performance
    }
    
    // S3 destination for data lake
    destConfig := &core.Config{
        Name: "s3-datalake",
        Properties: map[string]interface{}{
            "bucket":            "my-data-lake",
            "region":            "us-east-1",
            "prefix":            "mongodb-cdc/",
            "format":            "parquet",
            "compression":       "snappy",
            "partition_by":      []string{"year", "month", "day"},
        },
    }
    
    // Create and initialize connectors
    source, _ := registry.GetRegistry().CreateSource("mongodb-cdc", sourceConfig)
    dest, _ := registry.GetRegistry().CreateDestination("s3", destConfig)
    
    source.Initialize(ctx, sourceConfig)
    dest.Initialize(ctx, destConfig)
    
    // Read in batches for better S3 performance
    batchStream, err := source.ReadBatch(ctx, 10000)
    if err != nil {
        log.Fatalf("Failed to start batch reading: %v", err)
    }
    
    // Write batches to S3
    if err := dest.WriteBatch(ctx, batchStream); err != nil {
        log.Fatalf("Failed to write batches: %v", err)
    }
}
```

## Configuration Options

### MongoDB CDC Source

- `connection_string`: MongoDB connection string
- `database`: Database name
- `collections`: Array of collection names (optional, monitors all if not specified)
- `resume_token`: Resume token from previous run (optional)
- `full_document`: Options: "default", "updateLookup", "whenAvailable", "required"
- `full_document_before_change`: Options: "off", "whenAvailable", "required"
- `batch_size`: Number of changes to buffer (default: 1000)
- `max_await_time`: Maximum time to wait for new changes (default: "30s")
- `include_operation_types`: Filter specific operations (insert, update, delete, etc.)

## Performance Considerations

1. **Change Streams**: Require replica set or sharded cluster
2. **Resume Tokens**: Save and restore for resumable pipelines
3. **Batching**: Use `ReadBatch()` for better performance with analytical systems
4. **Full Document**: Use "updateLookup" to get complete documents on updates
5. **Network**: Place Nebula close to MongoDB for lower latency

## Error Handling

```go
// Example with proper error handling and position management
func processWithRecovery(ctx context.Context, source core.Source) {
    for {
        position := source.GetPosition()
        
        stream, err := source.Read(ctx)
        if err != nil {
            log.Printf("Failed to read, retrying from position: %v", position)
            source.SetPosition(position)
            time.Sleep(5 * time.Second)
            continue
        }
        
        // Process stream...
    }
}
```

## Monitoring

```go
// Get CDC metrics
metrics := source.Metrics()
fmt.Printf("Events processed: %v\n", metrics["event_count"])
fmt.Printf("Current lag: %v\n", metrics["lag"])
fmt.Printf("Status: %v\n", metrics["status"])
fmt.Printf("Collections: %v\n", metrics["collections"])

// Check health
if err := source.Health(ctx); err != nil {
    log.Printf("CDC source unhealthy: %v", err)
}
```

## Deployment Considerations

1. **Replica Set**: MongoDB must be configured as a replica set
2. **Permissions**: User needs read permissions and changeStream privilege
3. **OpLog Size**: Ensure sufficient oplog size for your change rate
4. **Network Security**: Use TLS for production deployments
5. **Resource Planning**: Change streams consume server resources

## Example MongoDB User Setup

```javascript
// Create a user with appropriate permissions
db.createUser({
  user: "nebula_cdc",
  pwd: "secure_password",
  roles: [
    { role: "read", db: "mydb" },
    { role: "changeStream", db: "mydb" }
  ]
})
```