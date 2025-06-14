package observability

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap/zapcore"
)

func TestObservabilityFramework(t *testing.T) {
	// Initialize observability with test config
	config := ObservabilityConfig{
		Tracing: TracingConfig{
			ServiceName:    "test-nebula",
			ServiceVersion: "1.0.0-test",
			Environment:    "test",
			SamplingRate:   1.0, // Sample everything for tests
			ExporterType:   "stdout",
			BatchTimeout:   1 * time.Second,
			MaxExportBatch: 100,
			MaxQueueSize:   1000,
		},
		Metrics: MetricsConfig{
			Namespace: "test_nebula",
			Subsystem: "test",
		},
		Logging: LoggingConfig{
			Level:       zapcore.DebugLevel,
			Format:      "json",
			Development: true,
		},
	}

	err := Initialize(config)
	if err != nil {
		t.Fatalf("Failed to initialize observability: %v", err)
	}

	// Test basic components are available
	if GetTracer() == nil {
		t.Error("Tracer should not be nil after initialization")
	}

	if GetMeter() == nil {
		t.Error("Meter should not be nil after initialization")
	}

	if GetLogger() == nil {
		t.Error("Logger should not be nil after initialization")
	}
}

func TestConnectorTracer(t *testing.T) {
	// Initialize with minimal config
	config := DefaultConfig()
	config.Logging.Level = zapcore.DebugLevel

	err := Initialize(config)
	if err != nil {
		t.Fatalf("Failed to initialize observability: %v", err)
	}

	// Create connector tracer
	tracer := NewConnectorTracer("file", "csv-reader")

	ctx := context.Background()

	// Test record tracing
	testError := errors.New("test error")

	err = tracer.TraceRecord(ctx, "record-123", "read", func() error {
		time.Sleep(10 * time.Millisecond) // Simulate work
		return nil
	})
	if err != nil {
		t.Errorf("TraceRecord should not return error for successful operation: %v", err)
	}

	err = tracer.TraceRecord(ctx, "record-456", "read", func() error {
		time.Sleep(5 * time.Millisecond) // Simulate work
		return testError
	})
	if err != testError {
		t.Errorf("TraceRecord should return the original error: got %v, want %v", err, testError)
	}

	// Test batch tracing
	err = tracer.TraceBatch(ctx, 100, "process", func() error {
		time.Sleep(20 * time.Millisecond) // Simulate batch work
		return nil
	})
	if err != nil {
		t.Errorf("TraceBatch should not return error for successful operation: %v", err)
	}
}

func TestMetricsCollector(t *testing.T) {
	// Initialize observability
	config := DefaultConfig()
	err := Initialize(config)
	if err != nil {
		t.Fatalf("Failed to initialize observability: %v", err)
	}

	// Create metrics collector
	collector := NewMetricsCollector("file", "csv-reader")

	// Test metrics recording
	collector.RecordDuration("read", 100*time.Millisecond, "success")
	collector.RecordThroughput("read", 1000.0)
	collector.RecordRecordsProcessed("read", 100, "success")
	collector.RecordBatchSize("read", 100)
	collector.RecordError("read", "io_error")
	collector.RecordRetry("read")
	collector.SetActiveConnections(5)

	// Verify the collector works without panicking
	// (Actual metric values would be tested with a metrics backend)
}

func TestStructuredLogger(t *testing.T) {
	// Initialize observability
	config := DefaultConfig()
	config.Logging.Level = zapcore.DebugLevel

	err := Initialize(config)
	if err != nil {
		t.Fatalf("Failed to initialize observability: %v", err)
	}

	// Create structured logger
	logger := NewStructuredLogger("file", "csv-reader")

	ctx := context.Background()

	// Test context logger
	ctxLogger := logger.WithContext(ctx)
	ctxLogger.Info("test message with context")

	// Test operation logger
	opLogger := logger.WithOperation("read")
	opLogger.LogStart("starting read operation")
	opLogger.LogProgress("reading records", 0.5)
	opLogger.LogComplete("read operation completed")

	// Test error logging
	testErr := errors.New("test error")
	opLogger.LogError("operation failed", testErr)
}

func TestPerformanceTracker(t *testing.T) {
	// Initialize observability
	config := DefaultConfig()
	err := Initialize(config)
	if err != nil {
		t.Fatalf("Failed to initialize observability: %v", err)
	}

	collector := NewMetricsCollector("file", "csv-reader")
	tracker := NewPerformanceTracker(collector, "read")

	// Simulate processing
	tracker.RecordProcessed(100)
	time.Sleep(10 * time.Millisecond)
	tracker.RecordProcessed(200)
	tracker.RecordError("io_error")
	tracker.RecordRetry()

	// Get current throughput
	throughput := tracker.GetCurrentThroughput()
	if throughput <= 0 {
		t.Error("Throughput should be greater than 0")
	}

	// Get final stats
	stats := tracker.GetStats()
	if stats.RecordsProcessed != 300 {
		t.Errorf("Expected 300 records processed, got %d", stats.RecordsProcessed)
	}
	if stats.Errors != 1 {
		t.Errorf("Expected 1 error, got %d", stats.Errors)
	}
	if stats.Retries != 1 {
		t.Errorf("Expected 1 retry, got %d", stats.Retries)
	}

	// Test stats logging
	stats.LogStats(GetLogger())
}

func TestConnectorMetrics(t *testing.T) {
	// Initialize observability
	config := DefaultConfig()
	config.Logging.Level = zapcore.DebugLevel

	err := Initialize(config)
	if err != nil {
		t.Fatalf("Failed to initialize observability: %v", err)
	}

	// Create connector metrics
	metrics := NewConnectorMetrics("file", "csv-reader")

	ctx := context.Background()

	// Test successful operation
	err = metrics.TrackOperation(ctx, "read", func() error {
		time.Sleep(5 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Errorf("TrackOperation should not return error for successful operation: %v", err)
	}

	// Test failed operation
	testError := errors.New("test error")
	err = metrics.TrackOperation(ctx, "read", func() error {
		time.Sleep(3 * time.Millisecond)
		return testError
	})
	if err != testError {
		t.Errorf("TrackOperation should return the original error: got %v, want %v", err, testError)
	}
}

func TestRecordMetrics(t *testing.T) {
	// Initialize observability
	config := DefaultConfig()
	config.Logging.Level = zapcore.DebugLevel

	err := Initialize(config)
	if err != nil {
		t.Fatalf("Failed to initialize observability: %v", err)
	}

	logger := NewStructuredLogger("file", "csv-reader")
	opLogger := logger.WithOperation("test")

	recordMetrics := NewRecordMetrics(opLogger)
	recordMetrics.SetLogInterval(1 * time.Millisecond) // Fast logging for tests

	// Simulate record processing
	recordMetrics.RecordProcessed(100, 1024)
	recordMetrics.RecordProcessed(200, 2048)
	recordMetrics.RecordError()

	// Force a progress log
	recordMetrics.LogProgress()

	// Log final stats
	recordMetrics.LogFinal()
}

func TestPerformanceLogger(t *testing.T) {
	// Initialize observability
	config := DefaultConfig()
	config.Logging.Level = zapcore.DebugLevel

	err := Initialize(config)
	if err != nil {
		t.Fatalf("Failed to initialize observability: %v", err)
	}

	perfLogger := NewPerformanceLogger()

	// Test throughput logging
	perfLogger.LogThroughput("read", 1000.0, 800.0) // Normal
	perfLogger.LogThroughput("read", 500.0, 800.0)  // Degraded
	perfLogger.LogThroughput("read", 200.0, 800.0)  // Critical

	// Test latency logging
	perfLogger.LogLatency("read", 1*time.Millisecond, 2*time.Millisecond) // Normal
	perfLogger.LogLatency("read", 3*time.Millisecond, 2*time.Millisecond) // Degraded
	perfLogger.LogLatency("read", 5*time.Millisecond, 2*time.Millisecond) // Critical

	// Test memory usage logging
	perfLogger.LogMemoryUsage("buffer", 1024*1024, 2*1024*1024)   // Normal
	perfLogger.LogMemoryUsage("buffer", 3*1024*1024, 2*1024*1024) // High
	perfLogger.LogMemoryUsage("buffer", 5*1024*1024, 2*1024*1024) // Critical
}

func TestSecurityAndErrorReporting(t *testing.T) {
	// Initialize observability
	config := DefaultConfig()
	config.Logging.Level = zapcore.DebugLevel

	err := Initialize(config)
	if err != nil {
		t.Fatalf("Failed to initialize observability: %v", err)
	}

	// Test security logger
	secLogger := NewSecurityLogger()
	secLogger.LogAuthEvent("login", "user123", true, map[string]interface{}{
		"ip":         "192.168.1.1",
		"user_agent": "test-client",
	})
	secLogger.LogAuthEvent("login", "user456", false, map[string]interface{}{
		"ip":     "192.168.1.2",
		"reason": "invalid_password",
	})
	secLogger.LogSecurityViolation("rate_limit_exceeded", "medium", map[string]interface{}{
		"ip":       "192.168.1.3",
		"attempts": 10,
	})

	// Test error reporter
	errorReporter := NewErrorReporter()
	ctx := context.Background()
	testErr := errors.New("test error")

	errorReporter.ReportError(ctx, testErr, "connector", "read", map[string]interface{}{
		"file": "test.csv",
		"line": 123,
	})
}

func TestShutdown(t *testing.T) {
	// Initialize observability
	config := DefaultConfig()
	err := Initialize(config)
	if err != nil {
		t.Fatalf("Failed to initialize observability: %v", err)
	}

	// Test graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = Shutdown(ctx)
	if err != nil {
		t.Errorf("Shutdown should not return error: %v", err)
	}
}
