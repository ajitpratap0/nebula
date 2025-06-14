package observability

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// StructuredLogger provides enhanced structured logging with tracing integration
type StructuredLogger struct {
	logger        *zap.Logger
	connectorType string
	connectorName string
}

// NewStructuredLogger creates a new structured logger for a connector
func NewStructuredLogger(connectorType, connectorName string) *StructuredLogger {
	return &StructuredLogger{
		logger: GetLogger().With(
			zap.String("connector_type", connectorType),
			zap.String("connector_name", connectorName),
		),
		connectorType: connectorType,
		connectorName: connectorName,
	}
}

// WithContext adds tracing context to log fields
func (sl *StructuredLogger) WithContext(ctx context.Context) *ContextLogger {
	fields := make([]zap.Field, 0, 4)

	// Add trace information if available
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		fields = append(fields,
			zap.String("trace_id", span.SpanContext().TraceID().String()),
			zap.String("span_id", span.SpanContext().SpanID().String()),
		)
	}

	return &ContextLogger{
		logger: sl.logger.With(fields...),
		ctx:    ctx,
	}
}

// WithOperation adds operation context to the logger
func (sl *StructuredLogger) WithOperation(operation string) *OperationLogger {
	return &OperationLogger{
		logger:    sl.logger.With(zap.String("operation", operation)),
		operation: operation,
		startTime: time.Now(),
	}
}

// Debug logs a debug message
func (sl *StructuredLogger) Debug(msg string, fields ...zap.Field) {
	sl.logger.Debug(msg, fields...)
}

// Info logs an info message
func (sl *StructuredLogger) Info(msg string, fields ...zap.Field) {
	sl.logger.Info(msg, fields...)
}

// Warn logs a warning message
func (sl *StructuredLogger) Warn(msg string, fields ...zap.Field) {
	sl.logger.Warn(msg, fields...)
}

// Error logs an error message
func (sl *StructuredLogger) Error(msg string, fields ...zap.Field) {
	sl.logger.Error(msg, fields...)
}

// Fatal logs a fatal message and exits
func (sl *StructuredLogger) Fatal(msg string, fields ...zap.Field) {
	sl.logger.Fatal(msg, fields...)
}

// ContextLogger provides logging with tracing context
type ContextLogger struct {
	logger *zap.Logger
	ctx    context.Context
}

// Debug logs a debug message with context
func (cl *ContextLogger) Debug(msg string, fields ...zap.Field) {
	cl.logger.Debug(msg, fields...)
}

// Info logs an info message with context
func (cl *ContextLogger) Info(msg string, fields ...zap.Field) {
	cl.logger.Info(msg, fields...)
}

// Warn logs a warning message with context
func (cl *ContextLogger) Warn(msg string, fields ...zap.Field) {
	cl.logger.Warn(msg, fields...)
}

// Error logs an error message with context
func (cl *ContextLogger) Error(msg string, fields ...zap.Field) {
	cl.logger.Error(msg, fields...)
}

// Fatal logs a fatal message with context and exits
func (cl *ContextLogger) Fatal(msg string, fields ...zap.Field) {
	cl.logger.Fatal(msg, fields...)
}

// WithOperation adds operation context
func (cl *ContextLogger) WithOperation(operation string) *OperationLogger {
	return &OperationLogger{
		logger:    cl.logger.With(zap.String("operation", operation)),
		operation: operation,
		startTime: time.Now(),
	}
}

// OperationLogger provides operation-specific logging
type OperationLogger struct {
	logger    *zap.Logger
	operation string
	startTime time.Time
}

// Debug logs a debug message for the operation
func (ol *OperationLogger) Debug(msg string, fields ...zap.Field) {
	ol.logger.Debug(msg, fields...)
}

// Info logs an info message for the operation
func (ol *OperationLogger) Info(msg string, fields ...zap.Field) {
	ol.logger.Info(msg, fields...)
}

// Warn logs a warning message for the operation
func (ol *OperationLogger) Warn(msg string, fields ...zap.Field) {
	ol.logger.Warn(msg, fields...)
}

// Error logs an error message for the operation
func (ol *OperationLogger) Error(msg string, fields ...zap.Field) {
	ol.logger.Error(msg, fields...)
}

// Fatal logs a fatal message for the operation and exits
func (ol *OperationLogger) Fatal(msg string, fields ...zap.Field) {
	ol.logger.Fatal(msg, fields...)
}

// LogStart logs the start of an operation
func (ol *OperationLogger) LogStart(msg string, fields ...zap.Field) {
	allFields := append(fields, zap.String("phase", "start"))
	ol.logger.Info(msg, allFields...)
}

// LogProgress logs operation progress
func (ol *OperationLogger) LogProgress(msg string, progress float64, fields ...zap.Field) {
	allFields := append(fields,
		zap.String("phase", "progress"),
		zap.Float64("progress_percent", progress*100),
		zap.Duration("elapsed", time.Since(ol.startTime)),
	)
	ol.logger.Info(msg, allFields...)
}

// LogComplete logs the completion of an operation
func (ol *OperationLogger) LogComplete(msg string, fields ...zap.Field) {
	duration := time.Since(ol.startTime)
	allFields := append(fields,
		zap.String("phase", "complete"),
		zap.Duration("total_duration", duration),
	)
	ol.logger.Info(msg, allFields...)
}

// LogError logs an operation error
func (ol *OperationLogger) LogError(msg string, err error, fields ...zap.Field) {
	duration := time.Since(ol.startTime)
	allFields := append(fields,
		zap.String("phase", "error"),
		zap.Duration("duration_before_error", duration),
		zap.Error(err),
	)
	ol.logger.Error(msg, allFields...)
}

// RecordMetrics provides a convenient interface for common logging patterns
type RecordMetrics struct {
	logger       *OperationLogger
	recordsStart int64
	recordsTotal int64
	errorsTotal  int64
	bytesTotal   int64
	startTime    time.Time
	lastLogTime  time.Time
	logInterval  time.Duration
}

// NewRecordMetrics creates a new record metrics logger
func NewRecordMetrics(logger *OperationLogger) *RecordMetrics {
	now := time.Now()
	return &RecordMetrics{
		logger:      logger,
		startTime:   now,
		lastLogTime: now,
		logInterval: 30 * time.Second, // Log progress every 30 seconds
	}
}

// SetLogInterval sets the interval for progress logging
func (rm *RecordMetrics) SetLogInterval(interval time.Duration) {
	rm.logInterval = interval
}

// RecordProcessed records processed records and optionally logs progress
func (rm *RecordMetrics) RecordProcessed(count int, bytes int64) {
	rm.recordsTotal += int64(count)
	rm.bytesTotal += bytes

	// Log progress at intervals
	if time.Since(rm.lastLogTime) >= rm.logInterval {
		rm.LogProgress()
		rm.lastLogTime = time.Now()
	}
}

// RecordError records an error
func (rm *RecordMetrics) RecordError() {
	rm.errorsTotal++
}

// LogProgress logs current progress
func (rm *RecordMetrics) LogProgress() {
	elapsed := time.Since(rm.startTime)
	recordsPerSecond := float64(rm.recordsTotal) / elapsed.Seconds()
	bytesPerSecond := float64(rm.bytesTotal) / elapsed.Seconds()

	rm.logger.Info("processing progress",
		zap.Int64("records_processed", rm.recordsTotal),
		zap.Int64("errors", rm.errorsTotal),
		zap.Int64("bytes_processed", rm.bytesTotal),
		zap.Float64("records_per_second", recordsPerSecond),
		zap.Float64("bytes_per_second", bytesPerSecond),
		zap.Duration("elapsed", elapsed),
	)
}

// LogFinal logs final statistics
func (rm *RecordMetrics) LogFinal() {
	elapsed := time.Since(rm.startTime)
	recordsPerSecond := float64(rm.recordsTotal) / elapsed.Seconds()
	bytesPerSecond := float64(rm.bytesTotal) / elapsed.Seconds()
	errorRate := float64(rm.errorsTotal) / float64(rm.recordsTotal) * 100

	rm.logger.LogComplete("processing completed",
		zap.Int64("total_records", rm.recordsTotal),
		zap.Int64("total_errors", rm.errorsTotal),
		zap.Int64("total_bytes", rm.bytesTotal),
		zap.Float64("avg_records_per_second", recordsPerSecond),
		zap.Float64("avg_bytes_per_second", bytesPerSecond),
		zap.Float64("error_rate_percent", errorRate),
		zap.Duration("total_duration", elapsed),
	)
}

// PerformanceLogger provides specialized logging for performance monitoring
type PerformanceLogger struct {
	logger *zap.Logger
}

// NewPerformanceLogger creates a new performance logger
func NewPerformanceLogger() *PerformanceLogger {
	return &PerformanceLogger{
		logger: GetLogger().With(zap.String("component", "performance")),
	}
}

// LogThroughput logs throughput metrics
func (pl *PerformanceLogger) LogThroughput(operation string, recordsPerSecond float64, threshold float64) {
	level := zapcore.InfoLevel
	status := "normal"

	if recordsPerSecond < threshold*0.5 {
		level = zapcore.ErrorLevel
		status = "critical"
	} else if recordsPerSecond < threshold*0.8 {
		level = zapcore.WarnLevel
		status = "degraded"
	}

	pl.logger.Log(level, "throughput measurement",
		zap.String("operation", operation),
		zap.Float64("records_per_second", recordsPerSecond),
		zap.Float64("threshold", threshold),
		zap.String("status", status),
		zap.Float64("threshold_ratio", recordsPerSecond/threshold),
	)
}

// LogLatency logs latency metrics
func (pl *PerformanceLogger) LogLatency(operation string, latency time.Duration, threshold time.Duration) {
	level := zapcore.InfoLevel
	status := "normal"

	if latency > threshold*2 {
		level = zapcore.ErrorLevel
		status = "critical"
	} else if latency > threshold {
		level = zapcore.WarnLevel
		status = "degraded"
	}

	pl.logger.Log(level, "latency measurement",
		zap.String("operation", operation),
		zap.Duration("latency", latency),
		zap.Duration("threshold", threshold),
		zap.String("status", status),
		zap.Float64("threshold_ratio", float64(latency)/float64(threshold)),
	)
}

// LogMemoryUsage logs memory usage metrics
func (pl *PerformanceLogger) LogMemoryUsage(component string, allocated int64, threshold int64) {
	level := zapcore.InfoLevel
	status := "normal"

	if allocated > threshold*2 {
		level = zapcore.ErrorLevel
		status = "critical"
	} else if allocated > threshold {
		level = zapcore.WarnLevel
		status = "high"
	}

	pl.logger.Log(level, "memory usage",
		zap.String("component", component),
		zap.Int64("allocated_bytes", allocated),
		zap.Int64("threshold_bytes", threshold),
		zap.String("status", status),
		zap.Float64("threshold_ratio", float64(allocated)/float64(threshold)),
	)
}

// SecurityLogger provides specialized logging for security events
type SecurityLogger struct {
	logger *zap.Logger
}

// NewSecurityLogger creates a new security logger
func NewSecurityLogger() *SecurityLogger {
	return &SecurityLogger{
		logger: GetLogger().With(zap.String("component", "security")),
	}
}

// LogAuthEvent logs authentication events
func (sl *SecurityLogger) LogAuthEvent(event string, user string, success bool, details map[string]interface{}) {
	level := zapcore.InfoLevel
	if !success {
		level = zapcore.WarnLevel
	}

	fields := []zap.Field{
		zap.String("event", event),
		zap.String("user", user),
		zap.Bool("success", success),
	}

	for key, value := range details {
		fields = append(fields, zap.Any(key, value))
	}

	sl.logger.Log(level, "authentication event", fields...)
}

// LogSecurityViolation logs security violations
func (sl *SecurityLogger) LogSecurityViolation(violation string, severity string, details map[string]interface{}) {
	level := zapcore.WarnLevel
	if severity == "critical" {
		level = zapcore.ErrorLevel
	}

	fields := []zap.Field{
		zap.String("violation", violation),
		zap.String("severity", severity),
		zap.Time("timestamp", time.Now()),
	}

	for key, value := range details {
		fields = append(fields, zap.Any(key, value))
	}

	sl.logger.Log(level, "security violation", fields...)
}

// ErrorReporter provides centralized error reporting
type ErrorReporter struct {
	logger *zap.Logger
}

// NewErrorReporter creates a new error reporter
func NewErrorReporter() *ErrorReporter {
	return &ErrorReporter{
		logger: GetLogger().With(zap.String("component", "error_reporter")),
	}
}

// ReportError reports an error with context
func (er *ErrorReporter) ReportError(ctx context.Context, err error, component string, operation string, metadata map[string]interface{}) {
	fields := []zap.Field{
		zap.Error(err),
		zap.String("component", component),
		zap.String("operation", operation),
		zap.String("error_type", fmt.Sprintf("%T", err)),
		zap.Time("timestamp", time.Now()),
	}

	// Add trace information if available
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		fields = append(fields,
			zap.String("trace_id", span.SpanContext().TraceID().String()),
			zap.String("span_id", span.SpanContext().SpanID().String()),
		)
	}

	// Add metadata
	for key, value := range metadata {
		fields = append(fields, zap.Any(key, value))
	}

	er.logger.Error("error reported", fields...)
}
