package observability

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var (
	// Connector-specific metrics
	connectorDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "nebula",
			Subsystem: "connector",
			Name:      "operation_duration_seconds",
			Help:      "Duration of connector operations in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		},
		[]string{"connector_type", "connector_name", "operation", "status"},
	)

	connectorThroughput = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "nebula",
			Subsystem: "connector",
			Name:      "throughput_records_per_second",
			Help:      "Current throughput in records per second",
		},
		[]string{"connector_type", "connector_name", "operation"},
	)

	connectorRecordsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "nebula",
			Subsystem: "connector",
			Name:      "records_processed_total",
			Help:      "Total number of records processed",
		},
		[]string{"connector_type", "connector_name", "operation", "status"},
	)

	connectorBatchSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "nebula",
			Subsystem: "connector",
			Name:      "batch_size",
			Help:      "Size of batches processed",
			Buckets:   []float64{1, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000},
		},
		[]string{"connector_type", "connector_name", "operation"},
	)

	connectorErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "nebula",
			Subsystem: "connector",
			Name:      "errors_total",
			Help:      "Total number of connector errors",
		},
		[]string{"connector_type", "connector_name", "operation", "error_type"},
	)

	connectorRetries = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "nebula",
			Subsystem: "connector",
			Name:      "retries_total",
			Help:      "Total number of connector retries",
		},
		[]string{"connector_type", "connector_name", "operation"},
	)

	connectorConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "nebula",
			Subsystem: "connector",
			Name:      "active_connections",
			Help:      "Number of active connections",
		},
		[]string{"connector_type", "connector_name"},
	)

	// General metrics
	generalDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "nebula",
			Subsystem: "observability",
			Name:      "operation_duration_seconds",
			Help:      "Duration of operations in seconds",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
		[]string{"operation", "component", "status"},
	)

	generalGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "nebula",
			Subsystem: "observability",
			Name:      "gauge_value",
			Help:      "General gauge values",
		},
		[]string{"metric", "component"},
	)
)

// MetricsCollector provides high-performance metrics collection
type MetricsCollector struct {
	connectorType string
	connectorName string
	mutex         sync.RWMutex

	// Cached label values for performance
	labelCache map[string][]string
}

// NewMetricsCollector creates a new metrics collector for a connector
func NewMetricsCollector(connectorType, connectorName string) *MetricsCollector {
	return &MetricsCollector{
		connectorType: connectorType,
		connectorName: connectorName,
		labelCache:    make(map[string][]string),
	}
}

// RecordDuration records a duration metric with labels
func (mc *MetricsCollector) RecordDuration(operation string, duration time.Duration, status string) {
	labels := mc.getLabels(operation, status)
	connectorDuration.WithLabelValues(labels...).Observe(duration.Seconds())
}

// RecordThroughput records a throughput metric
func (mc *MetricsCollector) RecordThroughput(operation string, recordsPerSecond float64) {
	connectorThroughput.WithLabelValues(mc.connectorType, mc.connectorName, operation).Set(recordsPerSecond)
}

// RecordRecordsProcessed increments the records processed counter
func (mc *MetricsCollector) RecordRecordsProcessed(operation string, count int, status string) {
	connectorRecordsProcessed.WithLabelValues(mc.connectorType, mc.connectorName, operation, status).Add(float64(count))
}

// RecordBatchSize records the size of a processed batch
func (mc *MetricsCollector) RecordBatchSize(operation string, size int) {
	connectorBatchSize.WithLabelValues(mc.connectorType, mc.connectorName, operation).Observe(float64(size))
}

// RecordError increments the error counter
func (mc *MetricsCollector) RecordError(operation string, errorType string) {
	connectorErrors.WithLabelValues(mc.connectorType, mc.connectorName, operation, errorType).Inc()
}

// RecordRetry increments the retry counter
func (mc *MetricsCollector) RecordRetry(operation string) {
	connectorRetries.WithLabelValues(mc.connectorType, mc.connectorName, operation).Inc()
}

// SetActiveConnections sets the number of active connections
func (mc *MetricsCollector) SetActiveConnections(count int) {
	connectorConnections.WithLabelValues(mc.connectorType, mc.connectorName).Set(float64(count))
}

// getLabels returns cached label values for performance
func (mc *MetricsCollector) getLabels(operation, status string) []string {
	key := operation + ":" + status

	mc.mutex.RLock()
	if labels, exists := mc.labelCache[key]; exists {
		mc.mutex.RUnlock()
		return labels
	}
	mc.mutex.RUnlock()

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	// Double-check after acquiring write lock
	if labels, exists := mc.labelCache[key]; exists {
		return labels
	}

	labels := []string{mc.connectorType, mc.connectorName, operation, status}
	mc.labelCache[key] = labels
	return labels
}

// RecordDuration records a general duration metric (used by tracing.go)
func RecordDuration(metricName string, duration time.Duration, labels map[string]string) {
	// Convert labels map to slice
	labelValues := make([]string, 0, len(labels))

	// Fixed order: operation, component, status
	operation := labels["operation"]
	if operation == "" {
		operation = metricName
	}

	component := labels["component"]
	if component == "" {
		component = "unknown"
	}

	status := labels["status"]
	if status == "" {
		status = "unknown"
	}

	labelValues = append(labelValues, operation, component, status)

	generalDuration.WithLabelValues(labelValues...).Observe(duration.Seconds())
}

// RecordGauge records a general gauge metric (used by tracing.go)
func RecordGauge(metricName string, value float64, labels map[string]string) {
	// Convert labels map to slice
	labelValues := make([]string, 0, 2)

	// Fixed order: metric, component
	metric := metricName
	component := labels["component"]
	if component == "" {
		component = "unknown"
	}

	labelValues = append(labelValues, metric, component)

	generalGauge.WithLabelValues(labelValues...).Set(value)
}

// PerformanceTracker tracks performance metrics over time
type PerformanceTracker struct {
	collector      *MetricsCollector
	operation      string
	startTime      time.Time
	recordsStart   int64 //nolint:unused // Reserved for baseline performance tracking
	recordsCurrent int64
	errors         int64
	retries        int64
	mutex          sync.RWMutex
}

// NewPerformanceTracker creates a new performance tracker
func NewPerformanceTracker(collector *MetricsCollector, operation string) *PerformanceTracker {
	return &PerformanceTracker{
		collector: collector,
		operation: operation,
		startTime: time.Now(),
	}
}

// RecordProcessed increments the processed record count
func (pt *PerformanceTracker) RecordProcessed(count int) {
	pt.mutex.Lock()
	pt.recordsCurrent += int64(count)
	pt.mutex.Unlock()

	pt.collector.RecordRecordsProcessed(pt.operation, count, "success")
}

// RecordError increments the error count
func (pt *PerformanceTracker) RecordError(errorType string) {
	pt.mutex.Lock()
	pt.errors++
	pt.mutex.Unlock()

	pt.collector.RecordError(pt.operation, errorType)
}

// RecordRetry increments the retry count
func (pt *PerformanceTracker) RecordRetry() {
	pt.mutex.Lock()
	pt.retries++
	pt.mutex.Unlock()

	pt.collector.RecordRetry(pt.operation)
}

// GetCurrentThroughput calculates and returns current throughput
func (pt *PerformanceTracker) GetCurrentThroughput() float64 {
	pt.mutex.RLock()
	elapsed := time.Since(pt.startTime).Seconds()
	records := pt.recordsCurrent
	pt.mutex.RUnlock()

	if elapsed == 0 {
		return 0
	}

	throughput := float64(records) / elapsed
	pt.collector.RecordThroughput(pt.operation, throughput)

	return throughput
}

// GetStats returns current performance statistics
func (pt *PerformanceTracker) GetStats() PerformanceStats {
	pt.mutex.RLock()
	defer pt.mutex.RUnlock()

	elapsed := time.Since(pt.startTime)
	throughput := float64(pt.recordsCurrent) / elapsed.Seconds()

	return PerformanceStats{
		Operation:        pt.operation,
		Duration:         elapsed,
		RecordsProcessed: pt.recordsCurrent,
		Throughput:       throughput,
		Errors:           pt.errors,
		Retries:          pt.retries,
		ErrorRate:        float64(pt.errors) / float64(pt.recordsCurrent),
	}
}

// PerformanceStats contains performance statistics
type PerformanceStats struct {
	Operation        string
	Duration         time.Duration
	RecordsProcessed int64
	Throughput       float64
	Errors           int64
	Retries          int64
	ErrorRate        float64
}

// LogStats logs the performance statistics
func (ps PerformanceStats) LogStats(logger *zap.Logger) {
	logger.Info("performance stats",
		zap.String("operation", ps.Operation),
		zap.Duration("duration", ps.Duration),
		zap.Int64("records_processed", ps.RecordsProcessed),
		zap.Float64("throughput_rps", ps.Throughput),
		zap.Int64("errors", ps.Errors),
		zap.Int64("retries", ps.Retries),
		zap.Float64("error_rate", ps.ErrorRate),
	)
}

// ConnectorMetrics provides a unified interface for connector metrics
type ConnectorMetrics struct {
	Collector *MetricsCollector
	Tracer    *ConnectorTracer
	Logger    *zap.Logger
}

// NewConnectorMetrics creates a unified metrics interface for a connector
func NewConnectorMetrics(connectorType, connectorName string) *ConnectorMetrics {
	return &ConnectorMetrics{
		Collector: NewMetricsCollector(connectorType, connectorName),
		Tracer:    NewConnectorTracer(connectorType, connectorName),
		Logger: GetLogger().With(
			zap.String("connector_type", connectorType),
			zap.String("connector_name", connectorName),
		),
	}
}

// TrackOperation provides a convenient way to track an operation with metrics and tracing
func (cm *ConnectorMetrics) TrackOperation(ctx context.Context, operation string, fn func() error) error {
	start := time.Now()

	// Start tracing
	ctx, span := cm.Tracer.StartSpan(ctx, operation)
	defer span.End()

	// Execute operation
	err := fn()

	// Record metrics
	duration := time.Since(start)
	status := "success"
	if err != nil {
		status = "error"
		cm.Collector.RecordError(operation, "execution_error")
		span.SetAttribute("error", true)
		span.SetAttribute("error.message", err.Error())
	}

	cm.Collector.RecordDuration(operation, duration, status)

	// Log result
	if err != nil {
		cm.Logger.Error("operation failed",
			zap.String("operation", operation),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
		cm.Logger.Debug("operation completed",
			zap.String("operation", operation),
			zap.Duration("duration", duration),
		)
	}

	return err
}

// PipelineMetrics provides metrics for pipeline operations
type PipelineMetrics struct {
	Collector *MetricsCollector
	Logger    *zap.Logger

	// Counters
	recordsExtracted   int64
	recordsTransformed int64
	recordsLoaded      int64
	errors             int64

	// Timing
	startTime  time.Time
	lastUpdate time.Time

	// Mutex for thread safety
	mu sync.RWMutex
}

// NewPipelineMetrics creates a new pipeline metrics tracker
func NewPipelineMetrics(pipelineName string) *PipelineMetrics {
	return &PipelineMetrics{
		Collector:  NewMetricsCollector("pipeline", pipelineName),
		Logger:     GetLogger().With(zap.String("pipeline", pipelineName)),
		startTime:  time.Now(),
		lastUpdate: time.Now(),
	}
}

// RecordExtracted increments the extracted records counter
func (pm *PipelineMetrics) RecordExtracted() {
	pm.mu.Lock()
	pm.recordsExtracted++
	pm.lastUpdate = time.Now()
	pm.mu.Unlock()

	pm.Collector.RecordRecordsProcessed("extract", 1, "success")
}

// RecordTransformed increments the transformed records counter
func (pm *PipelineMetrics) RecordTransformed() {
	pm.mu.Lock()
	pm.recordsTransformed++
	pm.lastUpdate = time.Now()
	pm.mu.Unlock()

	pm.Collector.RecordRecordsProcessed("transform", 1, "success")
}

// RecordsLoaded increments the loaded records counter
func (pm *PipelineMetrics) RecordsLoaded(count int) {
	pm.mu.Lock()
	pm.recordsLoaded += int64(count)
	pm.lastUpdate = time.Now()
	pm.mu.Unlock()

	pm.Collector.RecordRecordsProcessed("load", count, "success")
	pm.Collector.RecordBatchSize("load", count)
}

// RecordError increments the error counter
func (pm *PipelineMetrics) RecordError(operation, errorType string) {
	pm.mu.Lock()
	pm.errors++
	pm.lastUpdate = time.Now()
	pm.mu.Unlock()

	pm.Collector.RecordError(operation, errorType)
}

// GetStats returns current pipeline statistics
func (pm *PipelineMetrics) GetStats() map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	elapsed := time.Since(pm.startTime)
	throughput := float64(pm.recordsLoaded) / elapsed.Seconds()

	return map[string]interface{}{
		"records_extracted":   pm.recordsExtracted,
		"records_transformed": pm.recordsTransformed,
		"records_loaded":      pm.recordsLoaded,
		"errors":              pm.errors,
		"elapsed_seconds":     elapsed.Seconds(),
		"throughput_rps":      throughput,
		"last_update":         pm.lastUpdate,
	}
}
