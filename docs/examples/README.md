# Nebula Code Examples

This directory contains comprehensive code examples demonstrating how to use Nebula's APIs and features.

## Example Test Files

The following example test files provide runnable code samples that appear in Go documentation:

### 1. **pkg/pool/example_test.go** - Memory Pool Examples
Demonstrates the unified memory pool system:
- Basic record pooling with `GetRecord()` and `Release()`
- Creating records with initial data using `NewRecord()`
- CDC record creation with `NewCDCRecord()`
- Generic pool creation with `pool.New()`
- Concurrent pool usage patterns
- Batch processing with pooled records
- Using global pools for maps, string slices, and byte slices

### 2. **pkg/errors/example_test.go** - Error Handling Examples
Shows structured error handling patterns:
- Creating errors with types and context
- Wrapping existing errors with additional information
- Different error types (connection, validation, permission, etc.)
- Checking if errors are retryable
- Error chains with multiple levels of context
- Custom error handling logic
- Type checking with `IsType()`

### 3. **pkg/connector/example_test.go** - Connector Framework Examples
Illustrates using the connector framework:
- Creating connectors via the registry
- Simple pipelines between source and destination
- Configuration with `BaseConfig`
- CDC connector setup and usage
- S3 destination with columnar formats
- Accessing connector metrics
- Custom record transformations
- Listing available connectors

## Running the Examples

To run all examples and see their output:

```bash
# Run pool examples
go test -v ./pkg/pool/example_test.go

# Run error handling examples  
go test -v ./pkg/errors/example_test.go

# Run connector examples (requires connector imports)
go test -v ./pkg/connector/example_test.go
```

## Viewing in Documentation

These examples will appear in:
- Local `go doc` output
- Online at pkg.go.dev when the module is published
- IDE documentation tooltips

For example:
```bash
go doc -all github.com/ajitpratap0/nebula/pkg/pool
```

## Best Practices

When writing example tests:
1. Name functions as `Example`, `ExampleType`, or `Example_method`
2. Include an `// Output:` comment with expected output
3. Keep examples focused on a single concept
4. Use realistic but simple scenarios
5. Include error handling where appropriate
6. Add explanatory comments for complex operations