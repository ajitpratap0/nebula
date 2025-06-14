# Schema Evolution & Advanced Type Inference

The Nebula project includes a sophisticated schema evolution and type inference system that enables automatic schema detection, evolution, and data migration capabilities.

## Overview

The schema evolution system provides:
- **Automatic type inference** from data samples
- **Schema versioning** with full history tracking
- **Compatibility checking** (backward, forward, full, transitive)
- **Data migration** between schema versions
- **Format detection** (email, URL, UUID, timestamps, etc.)
- **Statistical analysis** of data types

## Architecture

### Core Components

1. **Schema Registry** (`pkg/schema/registry.go`)
   - Central repository for schema versions
   - Compatibility mode management
   - Schema history tracking
   - Change notification hooks

2. **Type Inference Engine** (`pkg/schema/type_inference.go`)
   - Advanced type detection with confidence scoring
   - Format detection (email, URL, UUID, dates, etc.)
   - Statistical analysis (numeric, string, temporal)
   - Complex type support (arrays, objects, JSON)

3. **Evolution Manager** (`pkg/schema/evolution.go`)
   - Schema change detection
   - Compatibility validation
   - Evolution strategies (default, strict, flexible)
   - Migration plan generation

4. **Migration Engine** (`pkg/schema/migration.go`)
   - Data transformation between schema versions
   - Type conversion support
   - Validation and error handling
   - Batch migration capabilities

5. **Schema-Aware Connector** (`pkg/connector/schema_aware.go`)
   - Automatic schema registration
   - Real-time schema evolution
   - Data validation and migration
   - Performance statistics

## Usage

### Basic Schema Registration

```go
// Create schema registry
logger, _ := zap.NewDevelopment()
registry := schema.NewRegistry(logger)

// Define a schema
userSchema := &models.Schema{
    Name:    "user",
    Version: "1.0",
    Fields: []models.Field{
        {Name: "id", Type: "integer", Required: true},
        {Name: "name", Type: "string", Required: true},
        {Name: "email", Type: "string", Required: false},
    },
}

// Register schema
version, err := registry.RegisterSchema(ctx, "user", userSchema)
```

### Automatic Type Inference

```go
// Sample data
samples := []map[string]interface{}{
    {"id": "123", "name": "Alice", "email": "alice@example.com", "age": "30"},
    {"id": "124", "name": "Bob", "email": "bob@example.com", "age": "25"},
}

// Infer schema from data
inferredSchema, err := registry.InferSchema(ctx, "user", samples)
// Result: id=integer, name=string, email=string(format:email), age=integer
```

### Schema Evolution

```go
// Automatic evolution based on new data
newData := []map[string]interface{}{
    {"id": "125", "name": "Charlie", "email": "charlie@example.com", 
     "age": "35", "department": "Engineering"}, // New field!
}

// Evolve schema
evolved, err := registry.EvolveSchema(ctx, "user", newData)
// Result: New version with "department" field added
```

### Using Schema-Aware Connectors

```go
// Create a regular CSV connector
csvSource, _ := connector.NewCSVSource("users", config)

// Wrap with schema awareness
schemaAware := connector.NewSchemaAwareConnector(csvSource, registry, logger)

// Enable automatic evolution
schemaAware.SetAutoEvolve(true)

// Connect and read - schema is automatically managed
err := schemaAware.Connect(ctx, config)
records, errors := schemaAware.Read(ctx)
```

## Compatibility Modes

### NONE
- Any schema change is allowed
- No validation performed

### BACKWARD (Default)
- New schema can read old data
- Cannot remove required fields
- Cannot change field types (unless compatible)
- Can add optional fields

### FORWARD
- Old schema can read new data
- Cannot add required fields
- Can remove optional fields

### FULL
- Both backward and forward compatible
- Most restrictive mode

### BACKWARD_TRANSITIVE
- Compatible with all previous versions
- Not just the immediate predecessor

## Type Inference Features

### Supported Types
- **Primitives**: string, integer, float, boolean
- **Temporal**: timestamp, date, datetime
- **Complex**: array, object, json
- **Formats**: email, url, uuid, json

### Confidence Scoring
- Each type inference includes a confidence score
- Configurable threshold (default: 95%)
- Mixed types default to string if below threshold

### Statistical Analysis
- **Numeric**: min, max, mean, median, std deviation
- **String**: min/max/avg length, common values
- **Temporal**: min/max dates, format detection

## Migration Capabilities

### Type Conversions
- string ↔ integer/float/boolean/timestamp
- integer ↔ float/boolean/string
- float ↔ integer/string
- boolean ↔ integer/string

### Migration Strategies
- **Default values** for new fields
- **Type transformation** for changed fields
- **Custom logic** via transformation functions
- **Validation** before and after migration

### Example Migration

```go
// Get migration plan between versions
plan, err := registry.GetEvolutionManager().GetMigrationPlan(v1, v2)

// Migrate data
migrationEngine := schema.NewMigrationEngine(logger, registry)
migratedRecord, err := migrationEngine.MigrateRecord(oldRecord, plan)
```

## Performance Considerations

The schema evolution system is designed for high performance:
- Efficient fingerprinting for schema comparison
- Caching of schema versions
- Parallel type inference for large datasets
- Zero-allocation design where possible
- Integration with Phase 1 optimization layer

## Best Practices

1. **Set appropriate compatibility mode** based on your use case
2. **Use type inference** for initial schema discovery
3. **Enable auto-evolution** for dynamic data sources
4. **Monitor schema changes** using registry hooks
5. **Test migrations** before applying to production data
6. **Use confidence thresholds** appropriate for your data quality

## Example: End-to-End Schema Evolution

```go
// 1. Start with CSV data
config := Config{
    "filepath": "users.csv",
    "type_inference": true,
}

// 2. Create schema-aware connector
source := connector.NewCSVSource("users", config)
schemaAware := connector.NewSchemaAwareConnector(source, registry, logger)

// 3. Set evolution callback
schemaAware.OnSchemaChange(func(old, new *schema.SchemaVersion) {
    log.Printf("Schema evolved from v%d to v%d", old.Version, new.Version)
})

// 4. Read data - schema is automatically managed
records, _ := schemaAware.Read(ctx)

// 5. Check schema history
history, _ := schemaAware.GetSchemaHistory()
for _, version := range history {
    log.Printf("Version %d: %d fields", version.Version, len(version.Schema.Fields))
}
```

## Integration with Nebula Pipeline

The schema evolution system integrates seamlessly with the Nebula pipeline:

1. **Sources** automatically register and evolve schemas
2. **Transformations** can adapt to schema changes
3. **Destinations** receive schema information for table creation
4. **Pipeline** validates data against schemas

This enables true schema-on-read capabilities with automatic adaptation to changing data structures.