// Package metrics provides performance tracking for Nebula
package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collector is a metrics collector interface
type Collector struct {
	name              string
	recordsProcessed  *prometheus.CounterVec
	processingLatency *prometheus.HistogramVec
	memoryAllocated   *prometheus.GaugeVec
	activeConnections *prometheus.GaugeVec
	queueDepth        *prometheus.GaugeVec
	throughput        *prometheus.GaugeVec
	startTime         time.Time
	mu                sync.RWMutex
}

// NewCollector creates a new metrics collector for a component
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
	// RecordsProcessed tracks total records processed
	RecordsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nebula_records_processed_total",
			Help: "Total number of records processed",
		},
		[]string{"source", "destination", "status"},
	)

	// ProcessingLatency tracks processing latency in nanoseconds
	ProcessingLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "nebula_processing_latency_nanoseconds",
			Help: "Processing latency in nanoseconds",
			Buckets: []float64{
				100,    // 100ns
				1000,   // 1μs
				10000,  // 10μs
				100000, // 100μs
				1e6,    // 1ms
				1e7,    // 10ms
				1e8,    // 100ms
				1e9,    // 1s
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

// Timer provides a simple timing mechanism
type Timer struct {
	start time.Time
	name  string
}

// NewTimer creates a new timer
func NewTimer(name string) *Timer {
	return &Timer{
		start: time.Now(),
		name:  name,
	}
}

// Stop stops the timer and records the duration
func (t *Timer) Stop() time.Duration {
	duration := time.Since(t.start)
	return duration
}

// ThroughputTracker tracks throughput over time
type ThroughputTracker struct {
	mu          sync.Mutex
	count       int64
	lastReset   time.Time
	source      string
	destination string
}

// NewThroughputTracker creates a new throughput tracker
func NewThroughputTracker(source, destination string) *ThroughputTracker {
	return &ThroughputTracker{
		lastReset:   time.Now(),
		source:      source,
		destination: destination,
	}
}

// Increment increments the counter
func (t *ThroughputTracker) Increment(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.count += n
}

// GetAndReset returns the current throughput and resets the counter
func (t *ThroughputTracker) GetAndReset() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	elapsed := time.Since(t.lastReset).Seconds()
	if elapsed == 0 {
		return 0
	}

	throughput := float64(t.count) / elapsed

	// Reset
	t.count = 0
	t.lastReset = time.Now()

	// Update metric
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
