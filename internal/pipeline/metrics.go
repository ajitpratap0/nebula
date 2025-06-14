// Package pipeline implements streaming metrics collection and monitoring
package pipeline

import (
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// StreamingMetrics collects and aggregates pipeline metrics
type StreamingMetrics struct {
	name   string
	logger *zap.Logger

	// Core metrics
	recordsExtracted   int64
	recordsTransformed int64
	recordsLoaded      int64
	batchesProcessed   int64
	backpressureEvents int64

	// Timing metrics
	totalProcessingTime int64 // nanoseconds
	batchDurations      []time.Duration
	batchIndex          int

	// Health metrics
	healthySeconds   int64
	unhealthySeconds int64
	lastHealthCheck  time.Time

	// Error metrics
	errorsRecovered int64
	circuitBreaker  *CircuitBreakerMetrics

	// Performance metrics
	throughputSamples  []float64
	throughputIndex    int
	lastThroughputCalc time.Time

	// Resource metrics
	memoryUsage     int64
	cpuUtilization  float64
	networkBytesIn  int64
	networkBytesOut int64

	// Metadata
	startTime  time.Time
	lastUpdate time.Time

	mu sync.RWMutex
}

// CircuitBreakerMetrics tracks circuit breaker specific metrics
type CircuitBreakerMetrics struct {
	state              string
	totalRequests      int64
	successfulRequests int64
	failedRequests     int64
	timesClosed        int64
	timesOpened        int64
	timesHalfOpened    int64
	lastStateChange    time.Time
}

// NewStreamingMetrics creates a new streaming metrics collector
func NewStreamingMetrics(name string, logger *zap.Logger) *StreamingMetrics {
	return &StreamingMetrics{
		name:              name,
		logger:            logger.With(zap.String("component", "streaming_metrics")),
		batchDurations:    make([]time.Duration, 100), // Rolling window of 100 batches
		throughputSamples: make([]float64, 60),        // 60 samples for throughput
		startTime:         time.Now(),
		lastUpdate:        time.Now(),
		circuitBreaker: &CircuitBreakerMetrics{
			state: "closed",
		},
	}
}

// RecordExtracted increments extracted records counter
func (sm *StreamingMetrics) RecordExtracted() {
	atomic.AddInt64(&sm.recordsExtracted, 1)
	sm.updateLastUpdate()
}

// RecordTransformed increments transformed records counter
func (sm *StreamingMetrics) RecordTransformed() {
	atomic.AddInt64(&sm.recordsTransformed, 1)
	sm.updateLastUpdate()
}

// RecordLoaded increments loaded records counter and tracks batch processing
func (sm *StreamingMetrics) RecordLoaded(count int) {
	atomic.AddInt64(&sm.recordsLoaded, int64(count))
	atomic.AddInt64(&sm.batchesProcessed, 1)
	sm.updateLastUpdate()
}

// RecordBatch records batch processing metrics
func (sm *StreamingMetrics) RecordBatch(size int, duration time.Duration) {
	atomic.AddInt64(&sm.recordsLoaded, int64(size))
	atomic.AddInt64(&sm.batchesProcessed, 1)
	atomic.AddInt64(&sm.totalProcessingTime, duration.Nanoseconds())

	sm.mu.Lock()
	sm.batchDurations[sm.batchIndex] = duration
	sm.batchIndex = (sm.batchIndex + 1) % len(sm.batchDurations)
	sm.mu.Unlock()

	sm.updateThroughput(size, duration)
	sm.updateLastUpdate()
}

// RecordBackpressure increments backpressure events counter
func (sm *StreamingMetrics) RecordBackpressure() {
	atomic.AddInt64(&sm.backpressureEvents, 1)
	sm.updateLastUpdate()
}

// RecordHealth updates health status
func (sm *StreamingMetrics) RecordHealth(healthy bool) {
	now := time.Now()
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.lastHealthCheck.IsZero() {
		duration := now.Sub(sm.lastHealthCheck).Seconds()
		if healthy {
			atomic.AddInt64(&sm.healthySeconds, int64(duration))
		} else {
			atomic.AddInt64(&sm.unhealthySeconds, int64(duration))
		}
	}

	sm.lastHealthCheck = now
	sm.updateLastUpdate()
}

// updateThroughput calculates and stores throughput samples
func (sm *StreamingMetrics) updateThroughput(recordCount int, duration time.Duration) {
	if duration > 0 {
		throughput := float64(recordCount) / duration.Seconds()

		sm.mu.Lock()
		sm.throughputSamples[sm.throughputIndex] = throughput
		sm.throughputIndex = (sm.throughputIndex + 1) % len(sm.throughputSamples)
		sm.lastThroughputCalc = time.Now()
		sm.mu.Unlock()
	}
}

// updateLastUpdate updates the last update timestamp
func (sm *StreamingMetrics) updateLastUpdate() {
	sm.mu.Lock()
	sm.lastUpdate = time.Now()
	sm.mu.Unlock()
}

// GetStats returns current streaming statistics
func (sm *StreamingMetrics) GetStats() *StreamingStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	now := time.Now()
	uptime := now.Sub(sm.startTime)

	// Calculate throughput
	totalRecords := atomic.LoadInt64(&sm.recordsLoaded)
	throughputRPS := float64(totalRecords) / uptime.Seconds()

	// Calculate average batch duration
	var totalBatchDuration time.Duration
	validBatches := 0
	for _, duration := range sm.batchDurations {
		if duration > 0 {
			totalBatchDuration += duration
			validBatches++
		}
	}

	avgBatchDuration := time.Duration(0)
	if validBatches > 0 {
		avgBatchDuration = totalBatchDuration / time.Duration(validBatches)
	}

	// Calculate average batch size
	batchesProcessed := atomic.LoadInt64(&sm.batchesProcessed)
	avgBatchSize := float64(0)
	if batchesProcessed > 0 {
		avgBatchSize = float64(totalRecords) / float64(batchesProcessed)
	}

	// Calculate health percentage
	healthyTime := atomic.LoadInt64(&sm.healthySeconds)
	unhealthyTime := atomic.LoadInt64(&sm.unhealthySeconds)
	totalHealthTime := healthyTime + unhealthyTime

	isHealthy := true
	sourceHealthy := true
	destinationHealthy := true

	// In a real implementation, these would be based on actual health checks
	if totalHealthTime > 0 {
		healthPercentage := float64(healthyTime) / float64(totalHealthTime)
		isHealthy = healthPercentage > 0.95
	}

	return &StreamingStats{
		// Throughput metrics
		RecordsProcessed: totalRecords,
		BatchesProcessed: batchesProcessed,
		ThroughputRPS:    throughputRPS,
		AvgBatchSize:     avgBatchSize,
		AvgBatchDuration: avgBatchDuration,

		// Error and recovery metrics
		ErrorsTotal:     0, // Would be populated from error manager
		ErrorsRecovered: atomic.LoadInt64(&sm.errorsRecovered),
		DeadLetterCount: 0, // Would be populated from error manager
		RetryAttempts:   0, // Would be populated from error manager

		// Backpressure metrics
		BackpressureEvents: atomic.LoadInt64(&sm.backpressureEvents),
		BufferUtilization:  0.0,   // Would be populated from buffer manager
		FlowControlActive:  false, // Would be populated from backpressure controller

		// Performance metrics
		MemoryUsage:     atomic.LoadInt64(&sm.memoryUsage),
		CPUUtilization:  sm.cpuUtilization,
		NetworkBytesIn:  atomic.LoadInt64(&sm.networkBytesIn),
		NetworkBytesOut: atomic.LoadInt64(&sm.networkBytesOut),

		// Timing metrics
		StartTime:  sm.startTime,
		LastUpdate: sm.lastUpdate,
		Uptime:     uptime,

		// Health metrics
		IsHealthy:          isHealthy,
		SourceHealthy:      sourceHealthy,
		DestinationHealthy: destinationHealthy,
	}
}

// UpdateResourceMetrics updates resource usage metrics
func (sm *StreamingMetrics) UpdateResourceMetrics(memoryBytes int64, cpuPercent float64, networkIn, networkOut int64) {
	atomic.StoreInt64(&sm.memoryUsage, memoryBytes)
	atomic.StoreInt64(&sm.networkBytesIn, networkIn)
	atomic.StoreInt64(&sm.networkBytesOut, networkOut)

	sm.mu.Lock()
	sm.cpuUtilization = cpuPercent
	sm.mu.Unlock()

	sm.updateLastUpdate()
}

// RecordErrorRecovered increments error recovery counter
func (sm *StreamingMetrics) RecordErrorRecovered() {
	atomic.AddInt64(&sm.errorsRecovered, 1)
	sm.updateLastUpdate()
}

// UpdateCircuitBreakerState updates circuit breaker metrics
func (sm *StreamingMetrics) UpdateCircuitBreakerState(state string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.circuitBreaker.state != state {
		// State changed
		switch state {
		case "closed":
			atomic.AddInt64(&sm.circuitBreaker.timesClosed, 1)
		case "open":
			atomic.AddInt64(&sm.circuitBreaker.timesOpened, 1)
		case "half_open":
			atomic.AddInt64(&sm.circuitBreaker.timesHalfOpened, 1)
		}

		sm.circuitBreaker.state = state
		sm.circuitBreaker.lastStateChange = time.Now()

		sm.logger.Info("circuit breaker state changed",
			zap.String("new_state", state),
			zap.Time("timestamp", sm.circuitBreaker.lastStateChange))
	}
}

// RecordCircuitBreakerRequest records a circuit breaker request
func (sm *StreamingMetrics) RecordCircuitBreakerRequest(success bool) {
	atomic.AddInt64(&sm.circuitBreaker.totalRequests, 1)

	if success {
		atomic.AddInt64(&sm.circuitBreaker.successfulRequests, 1)
	} else {
		atomic.AddInt64(&sm.circuitBreaker.failedRequests, 1)
	}

	sm.updateLastUpdate()
}

// GetCircuitBreakerStats returns circuit breaker statistics
func (sm *StreamingMetrics) GetCircuitBreakerStats() *CircuitBreakerMetrics {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return &CircuitBreakerMetrics{
		state:              sm.circuitBreaker.state,
		totalRequests:      atomic.LoadInt64(&sm.circuitBreaker.totalRequests),
		successfulRequests: atomic.LoadInt64(&sm.circuitBreaker.successfulRequests),
		failedRequests:     atomic.LoadInt64(&sm.circuitBreaker.failedRequests),
		timesClosed:        atomic.LoadInt64(&sm.circuitBreaker.timesClosed),
		timesOpened:        atomic.LoadInt64(&sm.circuitBreaker.timesOpened),
		timesHalfOpened:    atomic.LoadInt64(&sm.circuitBreaker.timesHalfOpened),
		lastStateChange:    sm.circuitBreaker.lastStateChange,
	}
}

// GetThroughputTrend returns recent throughput trend
func (sm *StreamingMetrics) GetThroughputTrend() []float64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Return last 10 samples
	trend := make([]float64, 0, 10)
	for i := 0; i < 10 && i < len(sm.throughputSamples); i++ {
		idx := (sm.throughputIndex - 1 - i + len(sm.throughputSamples)) % len(sm.throughputSamples)
		if sm.throughputSamples[idx] > 0 {
			trend = append(trend, sm.throughputSamples[idx])
		}
	}

	return trend
}

// Reset resets all metrics (useful for testing)
func (sm *StreamingMetrics) Reset() {
	atomic.StoreInt64(&sm.recordsExtracted, 0)
	atomic.StoreInt64(&sm.recordsTransformed, 0)
	atomic.StoreInt64(&sm.recordsLoaded, 0)
	atomic.StoreInt64(&sm.batchesProcessed, 0)
	atomic.StoreInt64(&sm.backpressureEvents, 0)
	atomic.StoreInt64(&sm.totalProcessingTime, 0)
	atomic.StoreInt64(&sm.healthySeconds, 0)
	atomic.StoreInt64(&sm.unhealthySeconds, 0)
	atomic.StoreInt64(&sm.errorsRecovered, 0)
	atomic.StoreInt64(&sm.memoryUsage, 0)
	atomic.StoreInt64(&sm.networkBytesIn, 0)
	atomic.StoreInt64(&sm.networkBytesOut, 0)

	sm.mu.Lock()
	sm.batchDurations = make([]time.Duration, 100)
	sm.batchIndex = 0
	sm.throughputSamples = make([]float64, 60)
	sm.throughputIndex = 0
	sm.cpuUtilization = 0
	sm.startTime = time.Now()
	sm.lastUpdate = time.Now()
	sm.lastHealthCheck = time.Time{}
	sm.mu.Unlock()

	// Reset circuit breaker metrics
	atomic.StoreInt64(&sm.circuitBreaker.totalRequests, 0)
	atomic.StoreInt64(&sm.circuitBreaker.successfulRequests, 0)
	atomic.StoreInt64(&sm.circuitBreaker.failedRequests, 0)
	atomic.StoreInt64(&sm.circuitBreaker.timesClosed, 0)
	atomic.StoreInt64(&sm.circuitBreaker.timesOpened, 0)
	atomic.StoreInt64(&sm.circuitBreaker.timesHalfOpened, 0)

	sm.logger.Info("streaming metrics reset")
}
