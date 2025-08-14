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

## Google Ads Source Connector

### Direct OAuth2 Authentication (Simplified)

The Google Ads source connector now uses direct OAuth2 token authentication instead of external Token Management Service (TMS). This provides a simpler, more direct authentication flow.

#### Configuration Structure

```json
{
  "name": "google_ads_direct",
  "type": "google_ads",
  "version": "1.0.0",
  "description": "Google Ads source connector with direct OAuth2 token authentication",
  "security": {
    "credentials": {
      "developer_token": "YOUR_DEVELOPER_TOKEN",
      "client_id": "YOUR_CLIENT_ID",
      "client_secret": "YOUR_CLIENT_SECRET",
      "refresh_token": "YOUR_REFRESH_TOKEN",
      "access_token": "",  // Optional - will be refreshed if empty
      "login_customer_id": "YOUR_LOGIN_CUSTOMER_ID",
      "customer_ids": "1234567890,0987654321",
      "query": "SELECT customer.id, customer.descriptive_name FROM customer LIMIT 10"
    }
  }
}
```

#### Required Fields

- **developer_token**: Your Google Ads API developer token
- **client_id**: OAuth2 client ID from Google Cloud Console
- **client_secret**: OAuth2 client secret from Google Cloud Console  
- **refresh_token**: OAuth2 refresh token for token renewal
- **customer_ids**: Comma-separated list of Google Ads customer IDs
- **query**: GAQL (Google Ads Query Language) query to execute

#### Optional Fields

- **access_token**: Pre-authenticated access token (will be refreshed automatically if not provided)
- **login_customer_id**: Manager account ID for MCC access

#### Usage Example

```bash
# Run Google Ads to JSON pipeline
nebula run --source google-ads-direct.json --destination json-destination.json

# With custom performance settings
nebula run --source google-ads-direct.json --destination json-destination.json \
  --batch-size 500 --workers 2 --timeout 10m
```

#### Authentication Flow

1. **Token Validation**: Validates all required OAuth2 credentials
2. **OAuth2 Setup**: Initializes OAuth2 client with provided credentials
3. **Token Refresh**: Automatically refreshes access token if needed
4. **API Connection**: Establishes connection to Google Ads API
5. **Data Extraction**: Executes GAQL query and streams results

#### Migration from TMS

If you were previously using TMS (Token Management Service), update your configuration:

**Old TMS Format:**
```json
{
  "credentials": {
    "account_id": "your_account_id",
    "platform": "GOOGLE"
  }
}
```

**New Direct Format:**
```json
{
  "credentials": {
    "developer_token": "YOUR_DEVELOPER_TOKEN",
    "client_id": "YOUR_CLIENT_ID",
    "client_secret": "YOUR_CLIENT_SECRET",
    "refresh_token": "YOUR_REFRESH_TOKEN"
  }
}
```