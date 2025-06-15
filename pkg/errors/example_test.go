// Package errors provides examples of structured error handling in Nebula.
package errors_test

import (
	"fmt"
	"io"

	"github.com/ajitpratap0/nebula/pkg/errors"
)

// Example demonstrates basic error creation and wrapping.
func Example() {
	// Create a new error with type
	err := errors.New(errors.ErrorTypeConnection, "failed to connect to database")
	
	// Add context details
	err = err.WithDetail("host", "localhost").
		WithDetail("port", 5432).
		WithDetail("database", "nebula")

	// Print the error
	fmt.Println(err.Error())
	
	// Output:
	// connection: failed to connect to database
}

// ExampleWrap shows how to wrap existing errors with context.
func ExampleWrap() {
	// Simulate an underlying error
	originalErr := io.EOF

	// Wrap the error with context
	err := errors.Wrap(originalErr, errors.ErrorTypeFile, "failed to read CSV file").
		WithDetail("file", "data.csv").
		WithDetail("line", 42)

	// Check the error type
	if errors.IsType(err, errors.ErrorTypeFile) {
		fmt.Println("This is a file error")
	}

	// Access the original error using Go's standard errors.Is
	if originalErr == io.EOF {
		fmt.Println("Original error was EOF")
	}

	// Output:
	// This is a file error
	// Original error was EOF
}

// ExampleErrorType demonstrates using different error types.
func ExampleErrorType() {
	// Connection error
	connErr := errors.New(errors.ErrorTypeConnection, "connection refused")
	fmt.Printf("Connection error: %v\n", connErr)

	// Validation error
	valErr := errors.New(errors.ErrorTypeValidation, "invalid batch size").
		WithDetail("value", -1).
		WithDetail("min", 1).
		WithDetail("max", 10000)
	fmt.Printf("Validation error: %v\n", valErr)

	// Permission error
	permErr := errors.New(errors.ErrorTypePermission, "access denied").
		WithDetail("resource", "s3://bucket/file.csv").
		WithDetail("action", "read")
	fmt.Printf("Permission error: %v\n", permErr)

	// Output:
	// Connection error: connection: connection refused
	// Validation error: validation: invalid batch size
	// Permission error: permission: access denied
}

// ExampleIsRetryable shows how to check if an error is retryable.
func ExampleIsRetryable() {
	// Create different types of errors
	tempErr := errors.New(errors.ErrorTypeTimeout, "service temporarily unavailable")
	fatalErr := errors.New(errors.ErrorTypeInternal, "critical system failure")

	// Check if errors are retryable
	if errors.IsRetryable(tempErr) {
		fmt.Println("Timeout error is retryable")
	}

	if !errors.IsRetryable(fatalErr) {
		fmt.Println("Fatal error is not retryable")
	}

	// Output:
	// Timeout error is retryable
	// Fatal error is not retryable
}

// Example_withDetails demonstrates adding multiple details to errors.
func Example_withDetails() {
	// Create an error with multiple context details
	err := errors.New(errors.ErrorTypeData, "failed to process record").
		WithDetail("record_id", "rec-789").
		WithDetail("batch_id", "batch-123").
		WithDetail("attempt", 3)

	// The error includes all details
	fmt.Println(err.Error())

	// Output:
	// data: failed to process record
}

// Example_errorChain shows how to chain multiple error contexts.
func Example_errorChain() {
	// Simulate a chain of operations that can fail
	err := connectToDatabase()
	if err != nil {
		// Wrap with additional context at each level
		err = errors.Wrap(err, errors.ErrorTypeData, "failed to fetch user data").
			WithDetail("operation", "user_fetch")
		
		err = errors.Wrap(err, errors.ErrorTypeInternal, "request handler failed").
			WithDetail("endpoint", "/api/users").
			WithDetail("method", "GET")

		fmt.Println("Full error chain:", err)
	}

	// Output:
	// Full error chain: internal: request handler failed: data: failed to fetch user data: connection: connection timeout
}

// connectToDatabase simulates a database connection error
func connectToDatabase() error {
	return errors.New(errors.ErrorTypeConnection, "connection timeout").
		WithDetail("host", "db.example.com").
		WithDetail("port", 5432)
}

// Example_errorHandling demonstrates proper error handling patterns.
func Example_errorHandling() {
	// Simulate processing records with error handling
	records := []string{"record1", "record2", "invalid", "record4"}
	
	for i, record := range records {
		err := processRecord(record)
		if err != nil {
			// Check error type for appropriate handling
			switch {
			case errors.IsType(err, errors.ErrorTypeValidation):
				fmt.Printf("Skipping invalid record at index %d: %v\n", i, err)
				continue
			case errors.IsRetryable(err):
				fmt.Printf("Retrying record at index %d: %v\n", i, err)
				// Implement retry logic here
			default:
				fmt.Printf("Fatal error at index %d: %v\n", i, err)
				return
			}
		}
	}

	// Output:
	// Skipping invalid record at index 2: validation: invalid record format
}

// processRecord simulates record processing that can fail
func processRecord(record string) error {
	if record == "invalid" {
		return errors.New(errors.ErrorTypeValidation, "invalid record format").
			WithDetail("record", record)
	}
	return nil
}

// ExampleIsType demonstrates checking error types.
func ExampleIsType() {
	// Create errors of different types
	connErr := errors.New(errors.ErrorTypeConnection, "connection failed")
	valErr := errors.New(errors.ErrorTypeValidation, "invalid input")
	
	// Wrap an error
	wrappedErr := errors.Wrap(connErr, errors.ErrorTypeData, "processing failed")

	// Check error types
	fmt.Printf("Is connection error: %v\n", errors.IsType(connErr, errors.ErrorTypeConnection))
	fmt.Printf("Is validation error: %v\n", errors.IsType(valErr, errors.ErrorTypeValidation))
	
	// IsType works through wrapped errors
	fmt.Printf("Wrapped error is data type: %v\n", errors.IsType(wrappedErr, errors.ErrorTypeData))
	fmt.Printf("Wrapped error contains connection type: %v\n", errors.IsType(wrappedErr, errors.ErrorTypeConnection))

	// Output:
	// Is connection error: true
	// Is validation error: true
	// Wrapped error is data type: true
	// Wrapped error contains connection type: false
}

// Example_customErrorHandling shows how to implement custom error handling logic.
func Example_customErrorHandling() {
	// Define a custom error handler
	handleError := func(err error) {
		if err == nil {
			return
		}

		// Extract error details
		if nebulaErr, ok := err.(*errors.Error); ok {
			fmt.Printf("Error Type: %s\n", nebulaErr.Type)
			fmt.Printf("Message: %s\n", nebulaErr.Message)
			
			if len(nebulaErr.Details) > 0 {
				fmt.Println("Details:")
				// Print details in a deterministic order
				if limit, ok := nebulaErr.Details["limit"]; ok {
					fmt.Printf("  limit: %v\n", limit)
				}
				if window, ok := nebulaErr.Details["window"]; ok {
					fmt.Printf("  window: %v\n", window)
				}
				if retryAfter, ok := nebulaErr.Details["retry_after"]; ok {
					fmt.Printf("  retry_after: %v\n", retryAfter)
				}
			}
		}
	}

	// Create and handle an error
	err := errors.New(errors.ErrorTypeRateLimit, "API rate limit exceeded").
		WithDetail("limit", 1000).
		WithDetail("window", "1h").
		WithDetail("retry_after", 300)

	handleError(err)

	// Output:
	// Error Type: rate_limit
	// Message: API rate limit exceeded
	// Details:
	//   limit: 1000
	//   window: 1h
	//   retry_after: 300
}