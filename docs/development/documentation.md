# Documentation Standards for Nebula

This guide outlines the documentation standards and best practices for the Nebula project. Following these guidelines ensures our documentation remains consistent, helpful, and maintainable.

## Table of Contents

- [Documentation Philosophy](#documentation-philosophy)
- [Types of Documentation](#types-of-documentation)
- [Go Documentation Standards](#go-documentation-standards)
- [Writing Effective Go Documentation](#writing-effective-go-documentation)
- [Documentation Structure](#documentation-structure)
- [Generating and Viewing Documentation](#generating-and-viewing-documentation)
- [Documentation Tools](#documentation-tools)
- [Review Checklist](#review-checklist)

## Documentation Philosophy

Our documentation philosophy is based on these principles:

1. **Documentation is Code**: Treat documentation with the same care as code
2. **Keep it Current**: Outdated documentation is worse than no documentation
3. **Show, Don't Just Tell**: Include examples wherever possible
4. **Write for Your Audience**: Consider who will read the documentation
5. **Be Concise but Complete**: Provide enough detail without being verbose

## Types of Documentation

### 1. Code Documentation (GoDoc)

- **Purpose**: API reference for developers using the package
- **Location**: In source code as comments
- **Format**: GoDoc comments above declarations
- **Audience**: Developers using the API

### 2. Project Documentation

- **Purpose**: High-level guides and explanations
- **Location**: `/docs` directory and root-level markdown files
- **Format**: Markdown files
- **Audience**: Users, contributors, and maintainers

### 3. Inline Comments

- **Purpose**: Explain complex logic within functions
- **Location**: Within function bodies
- **Format**: Standard Go comments
- **Audience**: Developers maintaining the code

### 4. Examples

- **Purpose**: Demonstrate usage patterns
- **Location**: `_example_test.go` files
- **Format**: Executable Go examples
- **Audience**: Developers learning the API

## Go Documentation Standards

### Package Documentation

Every package must have a package comment that describes its purpose:

```go
// Package connector provides the framework for building data connectors
// in Nebula. It includes base implementations, interfaces, and utilities
// for creating high-performance sources and destinations.
//
// The connector package is organized into several subpackages:
//
//   - core: Core interfaces and types
//   - base: Base implementations with common functionality
//   - sources: Source connector implementations
//   - destinations: Destination connector implementations
//   - registry: Connector registration and discovery
//
// Example usage:
//
//	source, err := registry.GetSource("csv", config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	
//	records, err := source.Stream(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
package connector
```

### Type Documentation

Document all exported types with their purpose and usage:

```go
// Record represents a single data record in the Nebula pipeline.
// It provides a unified interface for all data types and includes
// metadata for tracking and processing.
//
// Records are obtained from the global pool to minimize allocations:
//
//	record := pool.GetRecord()
//	defer record.Release()
//	
//	record.SetData("name", "John")
//	record.SetData("age", 30)
//
// Records support various data types and provide type-safe accessors:
//
//	name, _ := record.GetString("name")
//	age, _ := record.GetInt("age")
type Record struct {
    // Data holds the record's key-value pairs
    Data map[string]interface{}
    
    // Metadata contains processing information
    Metadata RecordMetadata
    
    // pool reference for recycling
    pool *sync.Pool
}
```

### Function Documentation

Document all exported functions with:
- Brief description
- Parameter explanations
- Return value descriptions
- Error conditions
- Examples when helpful

```go
// NewCSVSource creates a new CSV source connector with the provided configuration.
// The configuration must include a valid file path and optional parsing settings.
//
// Parameters:
//   - config: BaseConfig with CSV-specific settings
//
// Returns:
//   - *CSVSource: Configured CSV source connector
//   - error: Configuration validation error, if any
//
// Example:
//
//	config := &CSVConfig{
//	    BaseConfig: config.NewBaseConfig("csv", "source"),
//	    FilePath: "/data/users.csv",
//	    HasHeader: true,
//	}
//	
//	source, err := NewCSVSource(config)
//	if err != nil {
//	    log.Fatal(err)
//	}
func NewCSVSource(config *CSVConfig) (*CSVSource, error) {
    if err := config.Validate(); err != nil {
        return nil, errors.Wrap(err, errors.ErrorTypeConfig, "invalid CSV configuration")
    }
    
    return &CSVSource{
        BaseConnector: base.NewBaseConnector(config.BaseConfig),
        config:        config,
    }, nil
}
```

### Method Documentation

Document methods similarly to functions:

```go
// Stream reads records from the CSV file and sends them through the returned channel.
// The method handles file opening, parsing, and error recovery automatically.
// It respects context cancellation and implements backpressure.
//
// The returned channel is closed when:
//   - All records have been read
//   - An unrecoverable error occurs
//   - The context is cancelled
//
// Errors during streaming are logged but don't stop processing unless fatal.
// Use the Metrics() method to check for partial failures.
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
//	defer cancel()
//	
//	records, err := source.Stream(ctx)
//	if err != nil {
//	    return err
//	}
//	
//	for record := range records {
//	    process(record)
//	    record.Release()
//	}
func (s *CSVSource) Stream(ctx context.Context) (<-chan *pool.Record, error) {
    // Implementation
}
```

### Interface Documentation

Document interfaces with their contract and implementation requirements:

```go
// Source defines the interface for all data source connectors in Nebula.
// Implementations must be thread-safe and support context cancellation.
//
// Source connectors are responsible for:
//   - Establishing connections to data sources
//   - Reading data and converting to Records
//   - Managing state for incremental syncs
//   - Handling errors and retries
//   - Providing metrics and health status
//
// Example implementation:
//
//	type MySource struct {
//	    *base.BaseConnector
//	    client *MyClient
//	}
//	
//	func (s *MySource) Connect(ctx context.Context) error {
//	    client, err := NewMyClient(s.config)
//	    if err != nil {
//	        return err
//	    }
//	    s.client = client
//	    return nil
//	}
type Source interface {
    // Connect establishes a connection to the data source.
    // It should validate credentials and prepare for streaming.
    Connect(ctx context.Context) error
    
    // Stream returns a channel of Records from the source.
    // The channel is closed when streaming completes or an error occurs.
    Stream(ctx context.Context) (<-chan *pool.Record, error)
    
    // Close releases all resources associated with the source.
    Close() error
    
    // Metrics returns current performance and health metrics.
    Metrics() Metrics
}
```

## Writing Effective Go Documentation

### Good Example

```go
// ProcessBatch applies transformations to a batch of records concurrently.
// It uses a worker pool to maximize throughput while respecting memory limits.
//
// The function processes records in parallel using the configured number of workers.
// Each worker receives records through a channel and applies the transformation
// function. Results are collected in the same order as input.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - records: Slice of records to process
//   - transform: Function to apply to each record
//   - opts: Processing options (workers, timeout, etc.)
//
// Returns:
//   - []*Record: Transformed records in the same order as input
//   - error: First error encountered, if any
//
// Performance considerations:
//   - Memory usage scales with batch size and worker count
//   - Optimal worker count is typically GOMAXPROCS
//   - Large batches should be split to avoid memory pressure
//
// Example:
//
//	transformed, err := ProcessBatch(ctx, records, func(r *Record) (*Record, error) {
//	    // Transform logic here
//	    r.SetData("processed", true)
//	    return r, nil
//	}, DefaultOptions())
func ProcessBatch(ctx context.Context, records []*Record, transform TransformFunc, opts *Options) ([]*Record, error) {
    // Implementation
}
```

### Bad Example

```go
// ProcessBatch processes records.
func ProcessBatch(ctx context.Context, records []*Record, transform TransformFunc, opts *Options) ([]*Record, error) {
    // Implementation
}
```

### Documentation Best Practices

1. **Start with a brief summary** on the first line
2. **Use complete sentences** with proper punctuation
3. **Document all parameters** including their purpose and constraints
4. **Explain return values** and their meaning
5. **Describe error conditions** and when they occur
6. **Include examples** for complex APIs
7. **Add performance notes** for performance-critical code
8. **Cross-reference** related types and functions
9. **Use proper formatting** with code blocks and lists
10. **Keep it updated** when the code changes

## Documentation Structure

### Package Structure

```
pkg/connector/
├── doc.go                 # Package-level documentation
├── connector.go           # Main types and interfaces
├── examples_test.go       # Executable examples
└── README.md             # Package-specific guide
```

### Example doc.go

```go
// Package connector provides the framework for building data connectors
// in Nebula. It includes base implementations, interfaces, and utilities
// for creating high-performance sources and destinations.
//
// # Architecture
//
// The connector framework is built on these core concepts:
//
//   - Source: Reads data from external systems
//   - Destination: Writes data to external systems
//   - Record: Universal data representation
//   - Registry: Connector discovery and instantiation
//
// # Usage
//
// To use an existing connector:
//
//	source, err := registry.GetSource("postgresql", config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer source.Close()
//	
//	if err := source.Connect(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	
//	records, err := source.Stream(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Creating Custom Connectors
//
// To create a custom connector, implement the Source or Destination interface:
//
//	type MySource struct {
//	    *base.BaseConnector
//	    config *MyConfig
//	}
//	
//	func (s *MySource) Connect(ctx context.Context) error {
//	    // Connection logic
//	}
//	
//	func (s *MySource) Stream(ctx context.Context) (<-chan *pool.Record, error) {
//	    // Streaming logic
//	}
//
// # Performance
//
// The connector framework is designed for high performance:
//
//   - Zero-copy record handling
//   - Concurrent processing with backpressure
//   - Memory pooling for reduced GC pressure
//   - Automatic retry and circuit breaking
//
// See the performance guide for optimization techniques.
package connector
```

## Generating and Viewing Documentation

### Local Documentation Server

Use the provided script to run a local documentation server:

```bash
# Start the documentation server
./scripts/serve-docs.sh

# The server will be available at http://localhost:6060
```

### Generating Static Documentation

```bash
# Generate HTML documentation
godoc -http=:6060 &
wget -r -np -k -E -p -erobots=off http://localhost:6060/pkg/github.com/ajitpratap0/nebula/

# Generate markdown documentation (requires godocdown)
go get -u github.com/robertkrimen/godocdown/godocdown
godocdown ./pkg/... > API.md
```

### Online Documentation

The documentation is automatically published to:
- https://pkg.go.dev/github.com/ajitpratap0/nebula

### Checking Documentation Quality

```bash
# Check for missing documentation
go doc -all ./pkg/... | grep -E "^(func|type|var|const)" | grep -v "//"

# Lint documentation
golint ./...

# Check examples compile
go test ./... -run Example
```

## Documentation Tools

### Recommended Tools

1. **godoc**: Standard Go documentation tool
   ```bash
   go install golang.org/x/tools/cmd/godoc@latest
   ```

2. **golint**: Checks documentation style
   ```bash
   go install golang.org/x/lint/golint@latest
   ```

3. **godocdown**: Generates markdown from Go docs
   ```bash
   go install github.com/robertkrimen/godocdown/godocdown@latest
   ```

4. **gomarkdoc**: Advanced markdown generation
   ```bash
   go install github.com/princjef/gomarkdoc/cmd/gomarkdoc@latest
   ```

### IDE Integration

Most Go IDEs provide documentation features:

- **VSCode**: Hover for inline docs, Go to Definition
- **GoLand**: Quick Documentation (Ctrl+Q), External Documentation
- **Vim**: vim-go plugin with :GoDoc command

## Review Checklist

Before submitting code, ensure documentation meets these criteria:

### Code Documentation
- [ ] All exported types have documentation
- [ ] All exported functions have documentation
- [ ] All exported methods have documentation
- [ ] Package has a doc.go or package comment
- [ ] Examples provided for complex APIs
- [ ] Parameters and return values documented
- [ ] Error conditions explained

### Quality Checks
- [ ] Documentation is accurate and current
- [ ] Examples compile and run correctly
- [ ] No spelling or grammar errors
- [ ] Formatting is consistent
- [ ] Cross-references are valid
- [ ] Performance notes included where relevant

### Maintenance
- [ ] Documentation updated with code changes
- [ ] Deprecated features marked appropriately
- [ ] Migration guides provided for breaking changes
- [ ] Version information included where needed

## Examples of Good Documentation

### Complete Type Documentation

```go
// Config represents the configuration for a Nebula connector.
// It provides a unified structure for all connector settings including
// performance tuning, timeouts, security, and observability options.
//
// Configuration can be loaded from YAML files with environment variable
// substitution:
//
//	performance:
//	  workers: ${NEBULA_WORKERS:8}
//	  batch_size: ${NEBULA_BATCH_SIZE:1000}
//	
//	timeouts:
//	  connection: "30s"
//	  request: "60s"
//
// All durations use Go's time.ParseDuration format.
// Memory sizes accept human-readable formats (e.g., "100MB", "1GB").
//
// Example:
//
//	config := &Config{
//	    Performance: PerformanceConfig{
//	        Workers:    8,
//	        BatchSize:  1000,
//	        BufferSize: 10000,
//	    },
//	    Timeouts: TimeoutConfig{
//	        Connection: 30 * time.Second,
//	        Request:    60 * time.Second,
//	    },
//	}
//	
//	if err := config.Validate(); err != nil {
//	    log.Fatal(err)
//	}
type Config struct {
    // Performance tuning options
    Performance PerformanceConfig `yaml:"performance" json:"performance"`
    
    // Timeout configurations
    Timeouts TimeoutConfig `yaml:"timeouts" json:"timeouts"`
    
    // Security settings
    Security SecurityConfig `yaml:"security" json:"security"`
    
    // Observability options
    Observability ObservabilityConfig `yaml:"observability" json:"observability"`
}
```

### Complete Function Documentation

```go
// NewStorageAdapter creates a storage adapter with the specified mode and configuration.
// The adapter automatically manages storage mode transitions and memory optimization.
//
// Storage modes:
//   - Row: Optimized for streaming, ~225 bytes/record
//   - Columnar: Optimized for batch processing, ~84 bytes/record
//   - Hybrid: Automatic mode selection based on workload
//
// The adapter monitors data patterns and automatically switches between storage
// modes to optimize memory usage and performance. Mode transitions are transparent
// to the caller.
//
// Parameters:
//   - mode: Initial storage mode (can change in hybrid mode)
//   - config: Configuration including batch size, compression, etc.
//
// Returns:
//   - *StorageAdapter: Configured storage adapter
//   - error: Configuration validation error, if any
//
// Performance characteristics:
//   - Row mode: Low latency, suitable for streaming
//   - Columnar mode: High compression, suitable for analytics
//   - Hybrid mode: Balances latency and compression
//
// Memory usage:
//   - Row mode: O(n) with n = record count
//   - Columnar mode: O(u*c) with u = unique values, c = columns
//   - Overhead: ~1KB base + 100 bytes per column
//
// Example:
//
//	// Create hybrid storage adapter
//	adapter, err := NewStorageAdapter(StorageModeHybrid, config)
//	if err != nil {
//	    return err
//	}
//	defer adapter.Close()
//	
//	// Add records - adapter automatically selects optimal storage
//	for _, record := range records {
//	    if err := adapter.AddRecord(record); err != nil {
//	        return err
//	    }
//	}
//	
//	// Check current efficiency
//	fmt.Printf("Memory per record: %.2f bytes\n", adapter.GetMemoryPerRecord())
//	fmt.Printf("Current mode: %s\n", adapter.GetStorageMode())
//
// See the performance guide for detailed benchmarks and optimization tips.
func NewStorageAdapter(mode StorageMode, config *Config) (*StorageAdapter, error) {
    // Implementation
}
```

## Summary

Good documentation is essential for the success of the Nebula project. By following these standards, we ensure that:

- Users can quickly understand and use the APIs
- Contributors can effectively work with the codebase
- The project remains maintainable as it grows
- Performance characteristics are well understood
- Best practices are shared and followed

Remember: **Write documentation for the person who will read it, including future you!**