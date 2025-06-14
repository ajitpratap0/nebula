// Package errors provides structured error handling for Nebula
package errors

import (
	"errors"
	"runtime"
	
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
)

// ErrorType represents the category of error
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

// Error represents a structured error with context
type Error struct {
	Type    ErrorType
	Message string
	Cause   error
	Details map[string]interface{}
	Stack   []StackFrame
}

// StackFrame represents a single frame in the call stack
type StackFrame struct {
	Function string
	File     string
	Line     int
}

// Error implements the error interface
func (e *Error) Error() string {
	if e.Cause != nil {
		return stringpool.Sprintf("%s: %s: %v", e.Type, e.Message, e.Cause)
	}
	return stringpool.Sprintf("%s: %s", e.Type, e.Message)
}

// Unwrap returns the underlying error
func (e *Error) Unwrap() error {
	return e.Cause
}

// WithDetail adds a key-value detail to the error
func (e *Error) WithDetail(key string, value interface{}) *Error {
	if e.Details == nil {
		e.Details = make(map[string]interface{})
	}
	e.Details[key] = value
	return e
}

// New creates a new error with the given type and message
func New(errType ErrorType, message string) *Error {
	return &Error{
		Type:    errType,
		Message: message,
		Stack:   captureStack(2),
	}
}

// Wrap wraps an existing error with additional context
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

// IsRetryable returns true if the error is retryable
func IsRetryable(err error) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}

	switch e.Type {
	case ErrorTypeRateLimit, ErrorTypeTimeout, ErrorTypeConnection:
		return true
	default:
		return false
	}
}

// IsType checks if the error is of the given type
func IsType(err error, errType ErrorType) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	return e.Type == errType
}

// captureStack captures the current call stack
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
