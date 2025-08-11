# Nebula CLI Configuration Examples

This directory contains example configuration files for the simplified Nebula CLI.

## New CLI Format

The Nebula CLI has been simplified to use a single, generic command format:

```bash
go build -o bin/nebula cmd/nebula/main.go
```

```bash
./bin/nebula run --source src-cnf.json --destination dest-cnf.json [system-flags]
```

## Commands

### Run Pipeline
```bash
# Basic usage with JSON config files
./bin/nebula run --source csv-source.json --destination json-destination.json

# With system flags from command line
./bin/nebula run --source csv-source.json --destination json-destination.json \
  --batch-size 5000 --workers 8 --timeout 1h

# With system flags from JSON file
nebula run --source csv-source.json --destination json-destination.json \
  --system-config system-flags.json
```

### List Available Connectors
```bash
nebula list
```

### Show Version
```bash
nebula version
```

## Configuration Files

### Source Configuration (`csv-source.json`)
Defines the source connector configuration:
- **name**: Identifier for the connector instance
- **type**: Connector type (e.g., "csv", "postgresql", "s3")
- **performance**: Batch size, workers, streaming settings
- **timeouts**: Connection and operation timeouts
- **reliability**: Retry logic and error handling
- **observability**: Metrics and logging settings
- **connector_config**: Connector-specific settings

### Destination Configuration (`json-destination.json`)
Defines the destination connector configuration with the same structure as source.

### System Configuration (`system-flags.json`)
Optional system-level settings that override connector configurations:
- **batch_size**: Number of records per batch
- **workers**: Number of worker threads
- **max_concurrency**: Maximum concurrent operations
- **timeout**: Pipeline timeout duration
- **log_level**: Logging level (debug, info, warn, error)
- **enable_metrics**: Enable metrics collection

## Command Line System Flags

All system flags can be specified via command line:

- `--batch-size`: Number of records per batch (default: 1000)
- `--workers`: Number of worker threads (default: CPU count)
- `--max-concurrency`: Maximum concurrent operations (default: 2x CPU count)
- `--timeout`: Pipeline timeout (default: 30m)
- `--log-level`: Log level (default: info)
- `--enable-metrics`: Enable metrics collection (default: true)

## Examples

### CSV to JSON Conversion
```bash
nebula run --source csv-source.json --destination json-destination.json
```

### High-Performance Processing
```bash
nebula run --source large-csv-source.json --destination parquet-dest.json \
  --batch-size 10000 --workers 16 --timeout 2h
```

### With Custom System Configuration
```bash
nebula run --source db-source.json --destination s3-dest.json \
  --system-config high-performance-system.json
```

## Benefits of the New CLI

1. **Generic**: Works with any source/destination connector pair
2. **Flexible**: Supports both JSON config files and command-line flags
3. **Consistent**: Uses the unified BaseConfig structure
4. **Extensible**: Easy to add new connectors without CLI changes
5. **Performance-Tunable**: Rich system-level configuration options
6. **Production-Ready**: Comprehensive error handling and logging

## Migration from Old CLI

Old format:
```bash
nebula pipeline csv json --source-path input.csv --dest-path output.json --format lines
```

New format:
```bash
nebula run --source csv-source.json --destination json-destination.json
```

The new format requires creating JSON configuration files but provides much more flexibility and consistency across all connector types.