// Package nebulaerrors provides structured error handling for Nebula with rich context,
// stack traces, and error categorization. It enables consistent error handling
// patterns across the entire codebase.
//
// # Overview
//
// The nebulaerrors package extends Go's standard error handling with:
//   - Error categorization through ErrorType
//   - Structured context with key-value details
//   - Automatic stack trace capture
//   - Error wrapping with cause preservation
//   - Retryability detection
//
// # Basic Usage
//
//	// Create a new error
//	err := nebulaerrors.New(nebulaerrors.ErrorTypeValidation, "invalid input")
//
//	// Add context
//	err = err.WithDetail("field", "email").
//	         WithDetail("value", "invalid@")
//
//	// Wrap existing errors
//	if err := db.Query(sql); err != nil {
//	    return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "database query failed").
//	        WithDetail("query", sql).
//	        WithDetail("table", "users")
//	}
//
// # Error Types
//
// Errors are categorized by type, which helps with:
//   - Error handling strategies (retry logic)
//   - Monitoring and alerting
//   - API response codes
//   - Debugging and troubleshooting
//
// # Stack Traces
//
// Stack traces are automatically captured at error creation points,
// providing valuable debugging information without manual intervention.
//
// # Thread Safety
//
// Error instances are not thread-safe for modification. Create new
// instances or use WithDetail before sharing across goroutines.
package nebulaerrors

import (
	"errors"
	"runtime"

	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
)

// ErrorType represents the category of error, used for error handling strategies,
// monitoring, and API response mapping.
type ErrorType string

const (
	// ErrorTypeInternal represents internal system errors
	ErrorTypeInternal ErrorType = "internal"
	// ErrorTypeValidation represents validation errors
	ErrorTypeValidation ErrorType = "validation"
	// ErrorTypeNotFound represents resource not found errors
	ErrorTypeNotFound ErrorType = "not_found"
	// ErrorTypeConflict represents conflict errors
	ErrorTypeConflict ErrorType = "conflict"
	// ErrorTypeRateLimit represents rate limit errors
	ErrorTypeRateLimit ErrorType = "rate_limit"
	// ErrorTypeTimeout represents timeout errors
	ErrorTypeTimeout ErrorType = "timeout"
	// ErrorTypeConnection represents connection errors
	ErrorTypeConnection ErrorType = "connection"
	// ErrorTypeAuthentication represents authentication errors
	ErrorTypeAuthentication ErrorType = "authentication"
	// ErrorTypePermission represents permission errors
	ErrorTypePermission ErrorType = "permission"
	// ErrorTypeConfig represents configuration errors
	ErrorTypeConfig ErrorType = "config"
	// ErrorTypeData represents data processing errors
	ErrorTypeData ErrorType = "data"
	// ErrorTypeCapability represents capability/feature not supported errors
	ErrorTypeCapability ErrorType = "capability"
	// ErrorTypeHealth represents health check errors
	ErrorTypeHealth ErrorType = "health"
	// ErrorTypeFile represents file operation errors
	ErrorTypeFile ErrorType = "file"
	// ErrorTypeQuery represents query execution errors
	ErrorTypeQuery ErrorType = "query"
)

// Error represents a structured error with context, providing rich debugging
// information and enabling sophisticated error handling strategies.
//
// Fields:
//   - Type: Categorizes the error for handling strategies
//   - Message: Human-readable error description
//   - Cause: The underlying error that caused this error
//   - Details: Key-value pairs providing additional context
//   - Stack: Call stack at the point of error creation
//
// Example:
//
//	err := &Error{
//	    Type:    ErrorTypeValidation,
//	    Message: "invalid user input",
//	    Details: map[string]interface{}{
//	        "field": "email",
//	        "value": "not-an-email",
//	    },
//	}
type Error struct {
	Type    ErrorType
	Message string
	Cause   error
	Details map[string]interface{}
	Stack   []StackFrame
}

// StackFrame represents a single frame in the call stack, capturing
// the function name, file path, and line number for debugging.
type StackFrame struct {
	Function string // Fully qualified function name
	File     string // Source file path
	Line     int    // Line number in source file
}

// Error implements the error interface, returning a formatted error message
// that includes the error type, message, and cause (if present).
func (e *Error) Error() string {
	if e.Cause != nil {
		return stringpool.Sprintf("%s: %s: %v", e.Type, e.Message, e.Cause)
	}
	return stringpool.Sprintf("%s: %s", e.Type, e.Message)
}

// Unwrap returns the underlying error, enabling compatibility with errors.Is
// and errors.As for error chain inspection.
func (e *Error) Unwrap() error {
	return e.Cause
}

// WithDetail adds a key-value detail to the error, providing additional context
// for debugging and monitoring. This method can be chained for adding multiple details.
//
// Example:
//
//	err := errors.New(ErrorTypeValidation, "invalid input").
//	    WithDetail("field", "email").
//	    WithDetail("value", userInput).
//	    WithDetail("expected", "valid email format")
func (e *Error) WithDetail(key string, value interface{}) *Error {
	if e.Details == nil {
		e.Details = make(map[string]interface{})
	}
	e.Details[key] = value
	return e
}

// New creates a new error with the given type and message, automatically
// capturing the call stack at the point of creation.
//
// Example:
//
//	if len(input) == 0 {
//	    return errors.New(errors.ErrorTypeValidation, "input cannot be empty")
//	}
func New(errType ErrorType, message string) *Error {
	return &Error{
		Type:    errType,
		Message: message,
		Stack:   captureStack(2),
	}
}

// Wrap wraps an existing error with additional context, preserving the original
// error as the cause. If the error is already a structured Error, its stack
// trace is preserved. Returns nil if the input error is nil.
//
// Example:
//
//	result, err := db.Query(sql)
//	if err != nil {
//	    return errors.Wrap(err, errors.ErrorTypeConnection, "failed to query database").
//	        WithDetail("query", sql).
//	        WithDetail("duration", time.Since(start))
//	}
func Wrap(err error, errType ErrorType, message string) *Error {
	if err == nil {
		return nil
	}

	// If already our error type, preserve the stack
	var existingErr *Error
	if errors.As(err, &existingErr) {
		return &Error{
			Type:    errType,
			Message: message,
			Cause:   err,
			Stack:   existingErr.Stack,
		}
	}

	return &Error{
		Type:    errType,
		Message: message,
		Cause:   err,
		Stack:   captureStack(2),
	}
}

// IsRetryable returns true if the error is retryable based on its type.
// Rate limit, timeout, and connection errors are considered retryable.
// This function is useful for implementing retry logic with exponential backoff.
//
// Example:
//
//	for attempt := 0; attempt < maxRetries; attempt++ {
//	    err := performOperation()
//	    if err == nil {
//	        return nil
//	    }
//	    if !errors.IsRetryable(err) {
//	        return err // Fatal error, don't retry
//	    }
//	    time.Sleep(backoff(attempt))
//	}
func IsRetryable(err error) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}

	switch e.Type {
	case ErrorTypeRateLimit, ErrorTypeTimeout, ErrorTypeConnection:
		return true
	case ErrorTypeInternal, ErrorTypeValidation, ErrorTypeNotFound, ErrorTypeConflict, 
		 ErrorTypeAuthentication, ErrorTypePermission, ErrorTypeConfig, ErrorTypeData, 
		 ErrorTypeCapability, ErrorTypeHealth, ErrorTypeFile, ErrorTypeQuery:
		return false
	default:
		return false
	}
}

// IsType checks if the error is of the given type, useful for error handling
// strategies and conditional logic based on error categories.
//
// Example:
//
//	if errors.IsType(err, errors.ErrorTypeNotFound) {
//	    // Handle not found case - maybe create the resource
//	    return createDefault()
//	}
//	if errors.IsType(err, errors.ErrorTypePermission) {
//	    // Handle permission denied - maybe request elevation
//	    return requestPermission()
//	}
func IsType(err error, errType ErrorType) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	return e.Type == errType
}

// captureStack captures the current call stack up to maxFrames deep,
// skipping the specified number of frames from the top. This is used
// internally to record the call stack at error creation points.
func captureStack(skip int) []StackFrame {
	const maxFrames = 32
	frames := make([]StackFrame, 0, maxFrames)

	for i := skip; i < maxFrames+skip; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}

		fn := runtime.FuncForPC(pc)
		if fn == nil {
			continue
		}

		frames = append(frames, StackFrame{
			Function: fn.Name(),
			File:     file,
			Line:     line,
		})
	}

	return frames
}
