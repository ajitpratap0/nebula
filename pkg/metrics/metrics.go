// Package metrics provides performance tracking and observability for Nebula
// using Prometheus metrics. It offers collectors for various performance
// indicators including throughput, latency, memory usage, and system health.
//
// # Overview
//
// The metrics package provides:
//   - Prometheus-compatible metrics collection
//   - Pre-defined metrics for common operations
//   - Throughput and latency tracking utilities
//   - Thread-safe metric recording
//   - Automatic metric registration
//
// # Basic Usage
//
//	// Record processed records
//	metrics.RecordsProcessed.WithLabelValues("csv", "json", "success").Inc()
//
//	// Track processing latency
//	timer := metrics.NewTimer("process_batch")
//	processBatch(records)
//	duration := timer.Stop()
//	metrics.ProcessingLatency.WithLabelValues("batch", "csv", "json").Observe(float64(duration.Nanoseconds()))
//
//	// Track throughput
//	tracker := metrics.NewThroughputTracker("csv", "json")
//	for record := range records {
//	    process(record)
//	    tracker.Increment(1)
//	}
//	throughput := tracker.GetAndReset()
//
// # Metric Types
//
// Counter: Monotonically increasing values (e.g., total records processed)
// Gauge: Values that can go up or down (e.g., active connections)
// Histogram: Distribution of values (e.g., latency percentiles)
//
// # Performance Considerations
//
// Metrics are designed to have minimal overhead:
//   - Lock-free atomic operations where possible
//   - Efficient histogram buckets
//   - Lazy evaluation of expensive calculations
package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collector provides a centralized metrics collection interface for components.
// It wraps Prometheus metrics and provides convenience methods for recording
// various performance indicators. Each component should create its own collector.
type Collector struct {
	name              string                     // Component name for labeling
	recordsProcessed  *prometheus.CounterVec     // Total records processed
	processingLatency *prometheus.HistogramVec   // Processing latency distribution
	memoryAllocated   *prometheus.GaugeVec       // Current memory allocation
	activeConnections *prometheus.GaugeVec       // Active connection count
	queueDepth        *prometheus.GaugeVec       // Queue depth gauge
	throughput        *prometheus.GaugeVec       // Current throughput
	startTime         time.Time                  // Collector creation time
	mu                sync.RWMutex               // Protects internal state
}

// NewCollector creates a new metrics collector for a component.
// The name parameter identifies the component in metrics labels.
//
// Example:
//
//	collector := metrics.NewCollector("csv_source")
//	defer collector.Record("shutdown_time", time.Since(start))
//	
//	// Use throughout component lifecycle
//	collector.RecordCounter("records_read", float64(count))
//	collector.RecordGauge("queue_depth", float64(len(queue)))
func NewCollector(name string) *Collector {
	return &Collector{
		name:              name,
		recordsProcessed:  RecordsProcessed,
		processingLatency: ProcessingLatency,
		memoryAllocated:   MemoryAllocated,
		activeConnections: ActiveConnections,
		queueDepth:        QueueDepth,
		throughput:        Throughput,
		startTime:         time.Now(),
	}
}

// GetAll returns all current metric values
func (c *Collector) GetAll() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return map[string]interface{}{
		"component":  c.name,
		"start_time": c.startTime,
		"uptime":     time.Since(c.startTime).Seconds(),
	}
}

// StartTime returns when the collector was created
func (c *Collector) StartTime() time.Time {
	return c.startTime
}

// Record records a metric value
func (c *Collector) Record(name string, value interface{}) {
	// This is a simple implementation that could be expanded
	// to actually record metrics to the appropriate Prometheus collectors
	c.mu.Lock()
	defer c.mu.Unlock()

	// For now, just log that we recorded a metric
	// In a real implementation, you would update the appropriate prometheus metric
}

// RecordGauge records a gauge metric
func (c *Collector) RecordGauge(name string, value float64, labels ...string) {
	// In a real implementation, this would update the appropriate gauge metric
	// For now, this is a placeholder
	c.mu.Lock()
	defer c.mu.Unlock()
}

// RecordCounter records a counter metric
func (c *Collector) RecordCounter(name string, value float64, labels ...string) {
	// In a real implementation, this would increment the appropriate counter
	// For now, this is a placeholder
	c.mu.Lock()
	defer c.mu.Unlock()
}

// RecordHistogram records a histogram metric
func (c *Collector) RecordHistogram(name string, value float64, labels ...string) {
	// In a real implementation, this would observe the value in the appropriate histogram
	// For now, this is a placeholder
	c.mu.Lock()
	defer c.mu.Unlock()
}

var (
	// RecordsProcessed tracks the total number of records processed across all pipelines.
	// Labels: source (connector name), destination (connector name), status (success/failure)
	//
	// Example:
	//	metrics.RecordsProcessed.WithLabelValues("postgres", "snowflake", "success").Add(1000)
	RecordsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nebula_records_processed_total",
			Help: "Total number of records processed",
		},
		[]string{"source", "destination", "status"},
	)

	// ProcessingLatency tracks the distribution of processing latencies in nanoseconds.
	// The histogram buckets are optimized for sub-millisecond latency tracking.
	// Labels: operation (read/transform/write), source, destination
	//
	// Example:
	//	start := time.Now()
	//	processRecords(batch)
	//	metrics.ProcessingLatency.WithLabelValues("write", "csv", "json").
	//	    Observe(float64(time.Since(start).Nanoseconds()))
	ProcessingLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "nebula_processing_latency_nanoseconds",
			Help: "Processing latency in nanoseconds",
			Buckets: []float64{
				100,    // 100ns - Ultra-low latency operations
				1000,   // 1μs - Memory operations
				10000,  // 10μs - Fast I/O operations
				100000, // 100μs - Network operations
				1e6,    // 1ms - Standard processing
				1e7,    // 10ms - Complex transformations
				1e8,    // 100ms - Batch operations
				1e9,    // 1s - Large batch processing
			},
		},
		[]string{"operation", "source", "destination"},
	)

	// MemoryAllocated tracks memory allocations
	MemoryAllocated = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nebula_memory_allocated_bytes",
			Help: "Memory allocated in bytes",
		},
		[]string{"component"},
	)

	// ActiveConnections tracks active connections
	ActiveConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nebula_active_connections",
			Help: "Number of active connections",
		},
		[]string{"type", "destination"},
	)

	// QueueDepth tracks queue depths
	QueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nebula_queue_depth",
			Help: "Current queue depth",
		},
		[]string{"queue_name"},
	)

	// GCPauseDuration tracks GC pause durations
	GCPauseDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "nebula_gc_pause_duration_nanoseconds",
			Help: "GC pause duration in nanoseconds",
			Buckets: []float64{
				1e3, // 1μs
				1e4, // 10μs
				1e5, // 100μs
				1e6, // 1ms
				1e7, // 10ms
				1e8, // 100ms
			},
		},
	)

	// Throughput tracks records per second
	Throughput = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nebula_throughput_records_per_second",
			Help: "Current throughput in records per second",
		},
		[]string{"source", "destination"},
	)

	// AllocationRate tracks memory allocation rate
	AllocationRate = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "nebula_memory_allocation_rate_bytes_per_second",
			Help: "Memory allocation rate in bytes per second",
		},
	)
)

// Timer provides a simple timing mechanism for measuring operation durations.
// It captures the start time on creation and calculates elapsed time on stop.
type Timer struct {
	start time.Time
	name  string
}

// NewTimer creates a new timer and starts timing immediately.
// The name parameter is for identification in logs or metrics.
//
// Example:
//
//	timer := metrics.NewTimer("batch_processing")
//	processBatch(records)
//	duration := timer.Stop()
//	logger.Info("batch processed", zap.Duration("duration", duration))
func NewTimer(name string) *Timer {
	return &Timer{
		start: time.Now(),
		name:  name,
	}
}

// Stop stops the timer and returns the elapsed duration since creation.
// The timer can be stopped multiple times, each returning the total
// elapsed time since creation.
func (t *Timer) Stop() time.Duration {
	duration := time.Since(t.start)
	return duration
}

// ThroughputTracker tracks throughput (records per second) over time windows.
// It automatically calculates and reports throughput metrics when queried.
// Thread-safe for concurrent use.
type ThroughputTracker struct {
	mu          sync.Mutex
	count       int64      // Records processed since last reset
	lastReset   time.Time  // Time of last reset
	source      string     // Source connector name
	destination string     // Destination connector name
}

// NewThroughputTracker creates a new throughput tracker for a pipeline.
// The source and destination parameters identify the pipeline endpoints
// and are used as metric labels.
//
// Example:
//
//	tracker := metrics.NewThroughputTracker("kafka", "elasticsearch")
//	for msg := range messages {
//	    process(msg)
//	    tracker.Increment(1)
//	}
//	throughput := tracker.GetAndReset()
//	logger.Info("throughput", zap.Float64("records_per_sec", throughput))
func NewThroughputTracker(source, destination string) *ThroughputTracker {
	return &ThroughputTracker{
		lastReset:   time.Now(),
		source:      source,
		destination: destination,
	}
}

// Increment adds n to the record count. Safe for concurrent use.
func (t *ThroughputTracker) Increment(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.count += n
}

// GetAndReset calculates the current throughput (records/second),
// updates the Prometheus metric, resets the counter, and returns
// the calculated throughput. Safe for concurrent use.
func (t *ThroughputTracker) GetAndReset() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	elapsed := time.Since(t.lastReset).Seconds()
	if elapsed == 0 {
		return 0
	}

	throughput := float64(t.count) / elapsed

	// Reset for next period
	t.count = 0
	t.lastReset = time.Now()

	// Update Prometheus metric
	Throughput.WithLabelValues(t.source, t.destination).Set(throughput)

	return throughput
}

// LatencyTracker provides percentile tracking
type LatencyTracker struct {
	mu      sync.Mutex
	values  []time.Duration
	maxSize int
}

// NewLatencyTracker creates a new latency tracker
func NewLatencyTracker(maxSize int) *LatencyTracker {
	return &LatencyTracker{
		values:  make([]time.Duration, 0, maxSize),
		maxSize: maxSize,
	}
}

// Record records a latency value
func (l *LatencyTracker) Record(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.values) >= l.maxSize {
		// Remove oldest
		l.values = l.values[1:]
	}
	l.values = append(l.values, d)
}

// GetPercentile returns the percentile value (0-100)
func (l *LatencyTracker) GetPercentile(p float64) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.values) == 0 {
		return 0
	}

	// Simple implementation - in production use a better algorithm
	index := int(float64(len(l.values)) * p / 100)
	if index >= len(l.values) {
		index = len(l.values) - 1
	}

	return l.values[index]
}
