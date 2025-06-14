# Schema Evolution Guide

This guide explains how to use Nebula's automatic schema evolution capabilities with cloud destinations.

## Overview

Schema evolution allows your data pipelines to automatically adapt to changes in data structure over time. When new fields appear in your source data, Nebula can automatically update the destination schema to accommodate these changes.

## Key Features

- **Automatic Schema Detection**: Nebula automatically detects new fields and schema changes
- **Compatibility Modes**: Support for backward, forward, full, and none compatibility modes
- **Evolution Strategies**: Choose between default, strict, and flexible evolution strategies
- **Schema Registry**: Built-in schema versioning and history tracking
- **Cloud Destination Support**: Works with Snowflake, BigQuery, S3, and GCS destinations

## Basic Usage

### 1. Create a Destination with Schema Evolution

```go
import (
    "github.com/ajitpratap0/nebula/pkg/connector/evolution"
    "github.com/ajitpratap0/nebula/pkg/connector/core"
    "github.com/ajitpratap0/nebula/pkg/schema"
)

// Configure evolution
evolutionConfig := &evolution.EvolutionConfig{
    Strategy:              "default",
    CompatibilityMode:     schema.CompatibilityBackward,
    EnableAutoEvolution:   true,
    SchemaCheckInterval:   5 * time.Minute,
    BatchSizeForInference: 100,
}

// Create Snowflake destination with evolution
dest, err := evolution.CreateSnowflakeWithEvolution(
    "my-snowflake", 
    snowflakeConfig, 
    evolutionConfig
)
```

### 2. Write Data with Automatic Evolution

When you write data containing new fields, the schema will automatically evolve:

```go
// Initial schema has 3 fields: user_id, event_type, timestamp
// New data includes additional fields
records := []*models.Record{
    {
        Data: map[string]interface{}{
            "user_id":    "user123",
            "event_type": "login",
            "timestamp":  "2024-01-10T10:00:00Z",
            "ip_address": "192.168.1.1",      // New field!
            "user_agent": "Mozilla/5.0...",   // New field!
        },
    },
}

// Schema will automatically evolve to include new fields
err := dest.Write(ctx, recordStream)
```

## Evolution Strategies

### Default Strategy
- Allows adding new nullable fields
- Prevents dropping existing fields
- Suitable for most use cases

### Strict Strategy
- Only allows backward-compatible changes
- New fields must be nullable
- Cannot change field types
- Best for production environments with strict requirements

### Flexible Strategy
- Allows all schema changes
- Can drop fields and change types
- Use with caution in production

## Compatibility Modes

### Backward Compatibility
- New schema can read data written with old schema
- Safe for adding optional fields
- Default mode

### Forward Compatibility
- Old schema can read data written with new schema
- Safe for removing optional fields

### Full Compatibility
- Both backward and forward compatible
- Most restrictive mode

### None
- No compatibility checks
- Allows any schema changes

## Configuration Options

```go
type EvolutionConfig struct {
    // Evolution strategy: "default", "strict", "flexible"
    Strategy string
    
    // Compatibility mode
    CompatibilityMode schema.CompatibilityMode
    
    // Enable automatic evolution
    EnableAutoEvolution bool
    
    // How often to check for schema changes
    SchemaCheckInterval time.Duration
    
    // Number of records to analyze for schema inference
    BatchSizeForInference int
    
    // Fail if schema change is incompatible
    FailOnIncompatible bool
    
    // Keep old fields even if not present in new data
    PreserveOldFields bool
    
    // Cache schema locally for performance
    CacheSchemaLocally bool
    
    // Maximum age of cached schema
    MaxSchemaAgeMinutes int
}
```

## Examples

### Example 1: Snowflake with Default Evolution

```go
config := &evolution.EvolutionConfig{
    Strategy:              "default",
    CompatibilityMode:     schema.CompatibilityBackward,
    EnableAutoEvolution:   true,
    SchemaCheckInterval:   5 * time.Minute,
}

dest, err := evolution.CreateSnowflakeWithEvolution(
    "snowflake-dest", 
    snowflakeConfig, 
    config
)
```

### Example 2: BigQuery with Strict Evolution

```go
config := &evolution.EvolutionConfig{
    Strategy:              "strict",
    CompatibilityMode:     schema.CompatibilityBackwardTransitive,
    EnableAutoEvolution:   true,
    FailOnIncompatible:    true,
    PreserveOldFields:     true,
}

dest, err := evolution.CreateBigQueryWithEvolution(
    "bigquery-dest", 
    bigqueryConfig, 
    config
)
```

### Example 3: Custom Evolution with Any Destination

```go
// Create any destination
baseDest := createYourDestination()

// Wrap with evolution
evolvedDest, err := evolution.WrapDestinationWithEvolution(
    baseDest, 
    evolutionConfig
)
```

## Best Practices

1. **Start with Default Strategy**: Use the default strategy for development and testing
2. **Use Strict in Production**: Switch to strict strategy with backward compatibility for production
3. **Monitor Schema Changes**: Use the metrics to track schema evolution:
   ```go
   metrics := dest.Metrics()
   fmt.Printf("Schema changes: %v\n", metrics["schema_changes"])
   fmt.Printf("Evolution failures: %v\n", metrics["evolution_failures"])
   ```
4. **Test Compatibility**: Always test schema changes in a staging environment first
5. **Set Appropriate Intervals**: Don't check for schema changes too frequently to avoid performance impact

## Troubleshooting

### Schema Evolution Not Working
- Ensure `EnableAutoEvolution` is set to `true`
- Check that `SchemaCheckInterval` is not too long
- Verify the destination supports schema alterations

### Performance Issues
- Increase `BatchSizeForInference` to reduce schema checks
- Enable `CacheSchemaLocally` to reduce registry lookups
- Increase `SchemaCheckInterval` for stable schemas

### Compatibility Errors
- Review the compatibility mode settings
- Check logs for specific field incompatibilities
- Consider using a less restrictive compatibility mode

## Advanced Usage

### Custom Evolution Strategy

You can implement custom evolution strategies by extending the base functionality:

```go
// Create custom strategy
customStrategy := &MyCustomStrategy{
    allowTypeChanges: false,
    maxNewFields: 10,
}

// Register with evolution manager
evolutionManager.RegisterStrategy("custom", customStrategy)
```

### Schema History

Access schema version history:

```go
history, err := schemaRegistry.GetSchemaHistory("my_table")
for _, version := range history {
    fmt.Printf("Version %d: %d fields\n", 
        version.Version, 
        len(version.Schema.Fields))
}
```

## Performance Considerations

- Schema evolution adds minimal overhead (< 1% in most cases)
- Evolution checks are performed asynchronously
- Batching reduces the frequency of schema checks
- Local caching minimizes registry lookups

## See Also

- [Connector Development Guide](./connector-development-guide.md)
- [Performance Tuning Guide](./performance-tuning-guide.md)
- [Examples](../examples/evolution/)