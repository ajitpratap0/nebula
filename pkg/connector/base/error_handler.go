package base

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/errors"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

// ErrorHandler handles errors with categorization and retry logic
type ErrorHandler struct {
	logger        *zap.Logger
	maxRetries    int
	baseDelay     time.Duration
	errorCounts   map[string]int64
	errorMutex    sync.RWMutex
	totalErrors   int64
	retriedErrors int64
	fatalErrors   int64
}

// NewErrorHandler creates a new error handler
func NewErrorHandler(logger *zap.Logger, maxRetries int, baseDelay time.Duration) *ErrorHandler {
	return &ErrorHandler{
		logger:      logger,
		maxRetries:  maxRetries,
		baseDelay:   baseDelay,
		errorCounts: make(map[string]int64),
	}
}

// HandleError processes an error with appropriate handling
func (eh *ErrorHandler) HandleError(ctx context.Context, err error, record *models.Record) error {
	if err == nil {
		return nil
	}

	atomic.AddInt64(&eh.totalErrors, 1)

	// Categorize error
	errorType := eh.categorizeError(err)
	eh.incrementErrorCount(errorType)

	// Log error with context
	fields := []zap.Field{
		zap.Error(err),
		zap.String("error_type", errorType),
	}

	if record != nil {
		fields = append(fields, zap.Any("record_id", record.ID))
	}

	if eh.ShouldRetry(err) {
		atomic.AddInt64(&eh.retriedErrors, 1)
		eh.logger.Warn("retryable error occurred", fields...)
		return errors.Wrap(err, errors.ErrorTypeTimeout, "error can be retried")
	}

	atomic.AddInt64(&eh.fatalErrors, 1)
	eh.logger.Error("fatal error occurred", fields...)
	return errors.Wrap(err, errors.ErrorTypeInternal, "error cannot be retried")
}

// ShouldRetry determines if an error should be retried
func (eh *ErrorHandler) ShouldRetry(err error) bool {
	if err == nil {
		return false
	}

	// Check if error is explicitly marked as non-retryable
	if errors.IsType(err, errors.ErrorTypeInternal) {
		return false
	}

	// Check if error is explicitly marked as retryable
	if errors.IsRetryable(err) {
		return true
	}

	errStr := strings.ToLower(err.Error())

	// Non-retryable errors
	nonRetryable := []string{
		"invalid credentials",
		"unauthorized",
		"forbidden",
		"not found",
		"bad request",
		"invalid configuration",
		"unsupported",
		"schema mismatch",
		"data corruption",
		"disk full",
		"out of memory",
	}

	for _, pattern := range nonRetryable {
		if strings.Contains(errStr, pattern) {
			return false
		}
	}

	// Retryable errors
	retryable := []string{
		"timeout",
		"connection refused",
		"connection reset",
		"broken pipe",
		"temporary failure",
		"service unavailable",
		"too many requests",
		"rate limit",
		"throttle",
		"deadlock",
		"lock timeout",
		"transaction",
		"network",
		"i/o error",
	}

	for _, pattern := range retryable {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	// Default: retry network and timeout errors
	return errors.IsType(err, errors.ErrorTypeConnection) ||
		errors.IsType(err, errors.ErrorTypeTimeout)
}

// GetRetryDelay calculates the retry delay for a given attempt
func (eh *ErrorHandler) GetRetryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return eh.baseDelay
	}

	// Exponential backoff with jitter
	delay := eh.baseDelay * time.Duration(1<<uint(attempt-1))

	// Cap at 5 minutes
	maxDelay := 5 * time.Minute
	if delay > maxDelay {
		delay = maxDelay
	}

	// Add jitter (±25%)
	jitter := time.Duration(float64(delay) * 0.25 * (2*randomFloat() - 1))
	delay += jitter

	return delay
}

// RecordError records error details for analysis
func (eh *ErrorHandler) RecordError(err error, details map[string]interface{}) {
	errorType := eh.categorizeError(err)

	fields := []zap.Field{
		zap.Error(err),
		zap.String("error_type", errorType),
		zap.Any("details", details),
	}

	eh.logger.Info("error recorded", fields...)
}

// GetErrorStats returns error statistics
func (eh *ErrorHandler) GetErrorStats() map[string]interface{} {
	eh.errorMutex.RLock()
	defer eh.errorMutex.RUnlock()

	stats := pool.GetMap()

	defer pool.PutMap(stats)
	stats["total_errors"] = atomic.LoadInt64(&eh.totalErrors)
	stats["retried_errors"] = atomic.LoadInt64(&eh.retriedErrors)
	stats["fatal_errors"] = atomic.LoadInt64(&eh.fatalErrors)

	// Copy error counts by type
	errorCounts := make(map[string]int64)
	for k, v := range eh.errorCounts {
		errorCounts[k] = v
	}
	stats["errors_by_type"] = errorCounts

	return stats
}

// ResetStats resets error statistics
func (eh *ErrorHandler) ResetStats() {
	eh.errorMutex.Lock()
	defer eh.errorMutex.Unlock()

	atomic.StoreInt64(&eh.totalErrors, 0)
	atomic.StoreInt64(&eh.retriedErrors, 0)
	atomic.StoreInt64(&eh.fatalErrors, 0)

	eh.errorCounts = make(map[string]int64)
}

// categorizeError determines the error category
func (eh *ErrorHandler) categorizeError(err error) string {
	if err == nil {
		return "none"
	}

	// Check for known error types
	if errors.IsType(err, errors.ErrorTypeConnection) {
		return "connection"
	}
	if errors.IsType(err, errors.ErrorTypeTimeout) {
		return "timeout"
	}
	if errors.IsType(err, errors.ErrorTypeAuthentication) {
		return "authentication"
	}
	if errors.IsType(err, errors.ErrorTypeRateLimit) {
		return "rate_limit"
	}
	if errors.IsType(err, errors.ErrorTypeConfig) {
		return "configuration"
	}
	if errors.IsType(err, errors.ErrorTypeData) {
		return "data_error"
	}
	if errors.IsType(err, errors.ErrorTypeInternal) {
		return "internal"
	}

	// Categorize by error message patterns
	errStr := strings.ToLower(err.Error())

	switch {
	case strings.Contains(errStr, "timeout"):
		return "timeout"
	case strings.Contains(errStr, "connection"):
		return "connection"
	case strings.Contains(errStr, "auth") || strings.Contains(errStr, "unauthorized"):
		return "authentication"
	case strings.Contains(errStr, "rate limit") || strings.Contains(errStr, "throttle"):
		return "rate_limit"
	case strings.Contains(errStr, "config"):
		return "configuration"
	case strings.Contains(errStr, "parse") || strings.Contains(errStr, "unmarshal"):
		return "parsing"
	case strings.Contains(errStr, "i/o"):
		return "io"
	default:
		return "unknown"
	}
}

// incrementErrorCount increments the count for a specific error type
func (eh *ErrorHandler) incrementErrorCount(errorType string) {
	eh.errorMutex.Lock()
	defer eh.errorMutex.Unlock()
	eh.errorCounts[errorType]++
}

// randomFloat returns a random float between 0 and 1
func randomFloat() float64 {
	return float64(time.Now().UnixNano()%1000) / 1000.0
}

// RetryFunc is a function that can be retried
type RetryFunc func() error

// ExecuteWithRetry executes a function with retry logic
func (eh *ErrorHandler) ExecuteWithRetry(ctx context.Context, fn RetryFunc) error {
	var lastErr error

	for attempt := 0; attempt <= eh.maxRetries; attempt++ {
		if attempt > 0 {
			delay := eh.GetRetryDelay(attempt)
			eh.logger.Info("retrying operation",
				zap.Int("attempt", attempt),
				zap.Duration("delay", delay),
				zap.Error(lastErr))

			select {
			case <-ctx.Done():
				return fmt.Errorf("context canceled during retry: %w", ctx.Err())
			case <-time.After(delay):
			}
		}

		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		if !eh.ShouldRetry(err) {
			return err
		}
	}

	return fmt.Errorf("max retries (%d) exceeded: %w", eh.maxRetries, lastErr)
}
