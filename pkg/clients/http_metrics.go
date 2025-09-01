// Package clients provides HTTP metrics tracking
package clients

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// HTTPMetrics tracks HTTP client performance metrics including request counts,
// latencies, connection reuse, and error rates.
type HTTPMetrics struct {
	// Request counts
	totalRequests      int64
	successfulRequests int64
	failedRequests     int64

	// Connection metrics
	connectionsCreated int64
	connectionsReused  int64

	// Latency tracking
	latencyBuckets map[string]*LatencyBucket
	latencySamples []time.Duration
	sampleIndex    int
	maxSamples     int

	// Rate tracking
	requestsPerSecond float64
	lastRateCalc      time.Time
	rateWindow        []int64
	rateWindowIndex   int  //nolint:unused // Reserved for sliding window rate calculation

	// HTTP/2 metrics
	http2Connections int64
	http2Streams     int64

	// Error tracking
	errorsByType map[string]int64

	mu sync.RWMutex
}

// LatencyBucket tracks latency statistics for specific endpoints,
// maintaining samples for percentile calculations.
type LatencyBucket struct {
	host         string
	method       string
	count        int64
	totalLatency int64
	minLatency   time.Duration
	maxLatency   time.Duration
	samples      []time.Duration
}

// NewHTTPMetrics creates a new HTTP metrics tracker with pre-allocated buffers
// for efficient metric collection.
func NewHTTPMetrics() *HTTPMetrics {
	return &HTTPMetrics{
		latencyBuckets: make(map[string]*LatencyBucket),
		latencySamples: make([]time.Duration, 1000), // Keep last 1000 samples
		maxSamples:     1000,
		errorsByType:   make(map[string]int64),
		rateWindow:     make([]int64, 60), // 60-second window
		lastRateCalc:   time.Now(),
	}
}

// RecordRequest records metrics for an HTTP request including its method, host,
// latency, and whether it succeeded or failed.
func (hm *HTTPMetrics) RecordRequest(method, host string, latency time.Duration, err error) {
	atomic.AddInt64(&hm.totalRequests, 1)

	// Update rate tracking
	hm.updateRequestRate()

	if err != nil {
		atomic.AddInt64(&hm.failedRequests, 1)
		hm.recordError(err)
	} else {
		atomic.AddInt64(&hm.successfulRequests, 1)
	}

	// Record latency
	hm.recordLatency(method, host, latency)
}

// RecordConnectionReuse tracks whether a connection was reused or newly created,
// helping monitor connection pooling effectiveness.
func (hm *HTTPMetrics) RecordConnectionReuse(reused bool) {
	if reused {
		atomic.AddInt64(&hm.connectionsReused, 1)
	} else {
		atomic.AddInt64(&hm.connectionsCreated, 1)
	}
}

// RecordHTTP2Usage records HTTP/2 connection and stream statistics
// for monitoring multiplexing efficiency.
func (hm *HTTPMetrics) RecordHTTP2Usage(streams int) {
	atomic.AddInt64(&hm.http2Connections, 1)
	atomic.AddInt64(&hm.http2Streams, int64(streams))
}

// recordLatency records latency metrics
func (hm *HTTPMetrics) recordLatency(method, host string, latency time.Duration) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Update global latency samples
	hm.latencySamples[hm.sampleIndex] = latency
	hm.sampleIndex = (hm.sampleIndex + 1) % hm.maxSamples

	// Update per-endpoint bucket
	key := method + ":" + host
	bucket, exists := hm.latencyBuckets[key]
	if !exists {
		bucket = &LatencyBucket{
			host:       host,
			method:     method,
			minLatency: latency,
			maxLatency: latency,
			samples:    make([]time.Duration, 0, 100),
		}
		hm.latencyBuckets[key] = bucket
	}

	bucket.count++
	bucket.totalLatency += int64(latency)

	if latency < bucket.minLatency {
		bucket.minLatency = latency
	}
	if latency > bucket.maxLatency {
		bucket.maxLatency = latency
	}

	// Keep last 100 samples per endpoint
	if len(bucket.samples) >= 100 {
		bucket.samples = bucket.samples[1:]
	}
	bucket.samples = append(bucket.samples, latency)
}

// recordError records error metrics
func (hm *HTTPMetrics) recordError(err error) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	errorType := "unknown"
	if err != nil {
		errorType = err.Error()
		if len(errorType) > 50 {
			errorType = errorType[:50]
		}
	}

	hm.errorsByType[errorType]++
}

// updateRequestRate updates the request rate calculation
func (hm *HTTPMetrics) updateRequestRate() {
	now := time.Now()
	second := now.Unix() % 60

	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Check if we've moved to a new second
	if now.Sub(hm.lastRateCalc) >= time.Second {
		// Reset the current second's counter
		hm.rateWindow[second] = 0
		hm.lastRateCalc = now

		// Calculate average rate over the window
		var total int64
		for _, count := range hm.rateWindow {
			total += count
		}
		hm.requestsPerSecond = float64(total) / 60.0
	}

	// Increment current second's counter
	hm.rateWindow[second]++
}

// GetAverageLatency returns the average latency
func (hm *HTTPMetrics) GetAverageLatency() time.Duration {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	var total time.Duration
	var count int

	for _, sample := range hm.latencySamples {
		if sample > 0 {
			total += sample
			count++
		}
	}

	if count == 0 {
		return 0
	}

	return total / time.Duration(count)
}

// GetP95Latency returns the 95th percentile latency
func (hm *HTTPMetrics) GetP95Latency() time.Duration {
	return hm.getPercentileLatency(0.95)
}

// GetP99Latency returns the 99th percentile latency
func (hm *HTTPMetrics) GetP99Latency() time.Duration {
	return hm.getPercentileLatency(0.99)
}

// getPercentileLatency calculates a specific percentile latency
func (hm *HTTPMetrics) getPercentileLatency(percentile float64) time.Duration {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	// Collect valid samples
	validSamples := make([]time.Duration, 0, len(hm.latencySamples))
	for _, sample := range hm.latencySamples {
		if sample > 0 {
			validSamples = append(validSamples, sample)
		}
	}

	if len(validSamples) == 0 {
		return 0
	}

	// Sort samples
	sort.Slice(validSamples, func(i, j int) bool {
		return validSamples[i] < validSamples[j]
	})

	// Calculate percentile index
	index := int(float64(len(validSamples)-1) * percentile)
	return validSamples[index]
}

// GetEndpointMetrics returns metrics for a specific endpoint
func (hm *HTTPMetrics) GetEndpointMetrics(method, host string) EndpointMetrics {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	key := method + ":" + host
	bucket, exists := hm.latencyBuckets[key]
	if !exists {
		return EndpointMetrics{}
	}

	avgLatency := time.Duration(0)
	if bucket.count > 0 {
		avgLatency = time.Duration(bucket.totalLatency) / time.Duration(bucket.count)
	}

	return EndpointMetrics{
		Host:           host,
		Method:         method,
		RequestCount:   bucket.count,
		AverageLatency: avgLatency,
		MinLatency:     bucket.minLatency,
		MaxLatency:     bucket.maxLatency,
	}
}

// GetRequestRate returns the current request rate per second
func (hm *HTTPMetrics) GetRequestRate() float64 {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.requestsPerSecond
}

// GetConnectionReuseRate returns the connection reuse rate
func (hm *HTTPMetrics) GetConnectionReuseRate() float64 {
	created := atomic.LoadInt64(&hm.connectionsCreated)
	reused := atomic.LoadInt64(&hm.connectionsReused)

	total := created + reused
	if total == 0 {
		return 0
	}

	return float64(reused) / float64(total) * 100
}

// GetHTTP2Stats returns HTTP/2 usage statistics
func (hm *HTTPMetrics) GetHTTP2Stats() HTTP2Stats {
	return HTTP2Stats{
		Connections:    atomic.LoadInt64(&hm.http2Connections),
		TotalStreams:   atomic.LoadInt64(&hm.http2Streams),
		StreamsPerConn: 0,
	}
}

// GetErrorStats returns error statistics
func (hm *HTTPMetrics) GetErrorStats() map[string]int64 {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	// Create a copy to avoid holding the lock
	errorsCopy := make(map[string]int64)
	for errorType, count := range hm.errorsByType {
		errorsCopy[errorType] = count
	}

	return errorsCopy
}

// Reset resets all metrics
func (hm *HTTPMetrics) Reset() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	atomic.StoreInt64(&hm.totalRequests, 0)
	atomic.StoreInt64(&hm.successfulRequests, 0)
	atomic.StoreInt64(&hm.failedRequests, 0)
	atomic.StoreInt64(&hm.connectionsCreated, 0)
	atomic.StoreInt64(&hm.connectionsReused, 0)
	atomic.StoreInt64(&hm.http2Connections, 0)
	atomic.StoreInt64(&hm.http2Streams, 0)

	hm.latencyBuckets = make(map[string]*LatencyBucket)
	hm.latencySamples = make([]time.Duration, hm.maxSamples)
	hm.sampleIndex = 0
	hm.errorsByType = make(map[string]int64)
	hm.rateWindow = make([]int64, 60)
}

// EndpointMetrics represents metrics for a specific endpoint
type EndpointMetrics struct {
	Host           string        `json:"host"`
	Method         string        `json:"method"`
	RequestCount   int64         `json:"request_count"`
	AverageLatency time.Duration `json:"average_latency"`
	MinLatency     time.Duration `json:"min_latency"`
	MaxLatency     time.Duration `json:"max_latency"`
}

// HTTP2Stats represents HTTP/2 usage statistics
type HTTP2Stats struct {
	Connections    int64   `json:"connections"`
	TotalStreams   int64   `json:"total_streams"`
	StreamsPerConn float64 `json:"streams_per_conn"`
}
