// Package clients provides circuit breaker implementation for HTTP clients
package clients

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// CircuitBreaker is an alias for HTTPCircuitBreaker for backward compatibility
type CircuitBreaker = HTTPCircuitBreaker

// CircuitBreakerConfig is the configuration for circuit breaker
type CircuitBreakerConfig struct {
	FailureThreshold int           // Number of failures before opening
	SuccessThreshold int           // Number of successes before closing
	Timeout          time.Duration // Timeout before retrying
}

// NewCircuitBreaker creates a new circuit breaker with the given configuration.
// This is a compatibility wrapper that creates an HTTPCircuitBreaker with a nop logger.
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	httpConfig := &HTTPConfig{
		FailureThreshold: config.FailureThreshold,
		SuccessThreshold: config.SuccessThreshold,
		Timeout:          config.Timeout,
	}
	return NewHTTPCircuitBreaker(httpConfig, zap.NewNop())
}

// HTTPCircuitBreaker implements the circuit breaker pattern for HTTP requests
// to prevent cascading failures and provide fault tolerance.
type HTTPCircuitBreaker struct {
	config *HTTPConfig
	logger *zap.Logger

	// State
	state           int32 // 0: closed, 1: open, 2: half-open
	lastStateChange time.Time
	nextRetryTime   time.Time

	// Counters
	consecutiveFailures  int32
	consecutiveSuccesses int32
	requestsInWindow     int32
	failuresInWindow     int32

	// Sliding window
	window          *SlidingWindow
	halfOpenLimit   int32
	halfOpenCounter int32

	mu sync.RWMutex
}

// CircuitState represents the state of a circuit breaker
type CircuitState int32

const (
	// StateClosed allows all requests to pass through
	StateClosed CircuitState = iota
	// StateOpen blocks all requests
	StateOpen
	// StateHalfOpen allows a limited number of requests to test if the service has recovered
	StateHalfOpen
)

// SlidingWindow tracks requests and failures over a time window for calculating failure rates
type SlidingWindow struct {
	buckets        []int64
	failureBuckets []int64
	bucketSize     time.Duration
	windowSize     time.Duration
	currentBucket  int
	lastUpdate     time.Time
	mu             sync.RWMutex
}

// NewHTTPCircuitBreaker creates a new HTTP circuit breaker with the given configuration and logger.
// The circuit breaker starts in the closed state and uses a sliding window to track request failures.
func NewHTTPCircuitBreaker(config *HTTPConfig, logger *zap.Logger) *HTTPCircuitBreaker {
	cb := &HTTPCircuitBreaker{
		config:          config,
		logger:          logger.With(zap.String("component", "circuit_breaker")),
		state:           int32(StateClosed),
		lastStateChange: time.Now(),
		halfOpenLimit:   5, // Allow 5 requests in half-open state
	}

	// Create sliding window (1-minute window with 6 buckets of 10 seconds each)
	cb.window = NewSlidingWindow(10*time.Second, 60*time.Second)

	return cb
}

// Execute runs a function with circuit breaker protection.
// If the circuit is open, it returns an error immediately without executing the function.
// Otherwise, it executes the function and records the result.
func (cb *HTTPCircuitBreaker) Execute(fn func() error) error {
	if !cb.Allow() {
		return fmt.Errorf("circuit breaker is open")
	}

	err := fn()
	if err != nil {
		cb.RecordFailure()
		return err
	}

	cb.RecordSuccess()
	return nil
}

// Allow determines if a request should be allowed based on the current circuit state.
// Returns true if the request can proceed, false if it should be blocked.
func (cb *HTTPCircuitBreaker) Allow() bool {
	state := CircuitState(atomic.LoadInt32(&cb.state))

	switch state {
	case StateClosed:
		return true

	case StateOpen:
		// Check if we should transition to half-open
		cb.mu.RLock()
		shouldRetry := time.Now().After(cb.nextRetryTime)
		cb.mu.RUnlock()

		if shouldRetry {
			cb.transitionToHalfOpen()
			return cb.allowHalfOpen()
		}
		return false

	case StateHalfOpen:
		return cb.allowHalfOpen()

	default:
		return false
	}
}

// RecordSuccess records a successful request and updates the circuit state accordingly.
// In half-open state, enough consecutive successes will close the circuit.
func (cb *HTTPCircuitBreaker) RecordSuccess() {
	state := CircuitState(atomic.LoadInt32(&cb.state))

	// Update sliding window
	cb.window.RecordRequest(true)

	switch state {
	case StateClosed:
		// Reset consecutive failures
		atomic.StoreInt32(&cb.consecutiveFailures, 0)

	case StateHalfOpen:
		// Increment consecutive successes
		successes := atomic.AddInt32(&cb.consecutiveSuccesses, 1)

		// Check if we should close the circuit
		if successes >= int32(cb.config.SuccessThreshold) {
			cb.transitionToClosed()
		}
	}
}

// RecordFailure records a failed request and updates the circuit state accordingly.
// In closed state, too many failures will open the circuit.
// In half-open state, any failure will reopen the circuit.
func (cb *HTTPCircuitBreaker) RecordFailure() {
	state := CircuitState(atomic.LoadInt32(&cb.state))

	// Update sliding window
	cb.window.RecordRequest(false)

	switch state {
	case StateClosed:
		// Increment consecutive failures
		failures := atomic.AddInt32(&cb.consecutiveFailures, 1)

		// Check failure rate
		failureRate := cb.window.GetFailureRate()

		// Open circuit if threshold exceeded
		if failures >= int32(cb.config.FailureThreshold) || failureRate > 0.5 {
			cb.transitionToOpen()
		}

	case StateHalfOpen:
		// Any failure in half-open state reopens the circuit
		cb.transitionToOpen()
	}
}

// allowHalfOpen checks if a request is allowed in half-open state
func (cb *HTTPCircuitBreaker) allowHalfOpen() bool {
	// Limit number of requests in half-open state
	current := atomic.LoadInt32(&cb.halfOpenCounter)
	if current >= cb.halfOpenLimit {
		return false
	}

	atomic.AddInt32(&cb.halfOpenCounter, 1)
	return true
}

// transitionToOpen transitions to open state
func (cb *HTTPCircuitBreaker) transitionToOpen() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if !atomic.CompareAndSwapInt32(&cb.state, int32(StateHalfOpen), int32(StateOpen)) {
		atomic.CompareAndSwapInt32(&cb.state, int32(StateClosed), int32(StateOpen))
	}

	cb.lastStateChange = time.Now()
	cb.nextRetryTime = time.Now().Add(cb.config.Timeout)
	atomic.StoreInt32(&cb.consecutiveSuccesses, 0)
	atomic.StoreInt32(&cb.halfOpenCounter, 0)

	cb.logger.Warn("circuit breaker opened",
		zap.Time("retry_after", cb.nextRetryTime),
		zap.Int32("consecutive_failures", atomic.LoadInt32(&cb.consecutiveFailures)))
}

// transitionToHalfOpen transitions to half-open state
func (cb *HTTPCircuitBreaker) transitionToHalfOpen() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if atomic.CompareAndSwapInt32(&cb.state, int32(StateOpen), int32(StateHalfOpen)) {
		cb.lastStateChange = time.Now()
		atomic.StoreInt32(&cb.consecutiveFailures, 0)
		atomic.StoreInt32(&cb.consecutiveSuccesses, 0)
		atomic.StoreInt32(&cb.halfOpenCounter, 0)

		cb.logger.Info("circuit breaker half-open")
	}
}

// transitionToClosed transitions to closed state
func (cb *HTTPCircuitBreaker) transitionToClosed() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if atomic.CompareAndSwapInt32(&cb.state, int32(StateHalfOpen), int32(StateClosed)) {
		cb.lastStateChange = time.Now()
		atomic.StoreInt32(&cb.consecutiveFailures, 0)
		atomic.StoreInt32(&cb.halfOpenCounter, 0)

		cb.logger.Info("circuit breaker closed")
	}
}

// GetState returns the current state of the circuit breaker along with statistics
// about requests, failures, and state transitions.
func (cb *HTTPCircuitBreaker) GetState() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	state := CircuitState(atomic.LoadInt32(&cb.state))
	stateStr := "unknown"

	switch state {
	case StateClosed:
		stateStr = "closed"
	case StateOpen:
		stateStr = "open"
	case StateHalfOpen:
		stateStr = "half_open"
	}

	stats := cb.window.GetStats()

	return CircuitBreakerState{
		State:                stateStr,
		LastStateChange:      cb.lastStateChange,
		ConsecutiveFailures:  atomic.LoadInt32(&cb.consecutiveFailures),
		ConsecutiveSuccesses: atomic.LoadInt32(&cb.consecutiveSuccesses),
		TotalRequests:        stats.TotalRequests,
		FailedRequests:       stats.FailedRequests,
		FailureRate:          stats.FailureRate,
		NextRetryTime:        cb.nextRetryTime,
	}
}

// NewSlidingWindow creates a new sliding window for tracking request statistics.
// bucketSize determines the granularity of time buckets, and windowSize is the total time window.
func NewSlidingWindow(bucketSize, windowSize time.Duration) *SlidingWindow {
	numBuckets := int(windowSize / bucketSize)
	return &SlidingWindow{
		buckets:        make([]int64, numBuckets),
		failureBuckets: make([]int64, numBuckets),
		bucketSize:     bucketSize,
		windowSize:     windowSize,
		lastUpdate:     time.Now(),
	}
}

// RecordRequest records a request result in the sliding window.
// success indicates whether the request was successful.
func (sw *SlidingWindow) RecordRequest(success bool) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.updateBuckets()

	sw.buckets[sw.currentBucket]++
	if !success {
		sw.failureBuckets[sw.currentBucket]++
	}
}

// updateBuckets updates the current bucket based on time
func (sw *SlidingWindow) updateBuckets() {
	now := time.Now()
	elapsed := now.Sub(sw.lastUpdate)

	if elapsed >= sw.bucketSize {
		// Move to next bucket(s)
		bucketsToAdvance := int(elapsed / sw.bucketSize)
		if bucketsToAdvance > len(sw.buckets) {
			bucketsToAdvance = len(sw.buckets)
		}

		for i := 0; i < bucketsToAdvance; i++ {
			sw.currentBucket = (sw.currentBucket + 1) % len(sw.buckets)
			sw.buckets[sw.currentBucket] = 0
			sw.failureBuckets[sw.currentBucket] = 0
		}

		sw.lastUpdate = now
	}
}

// GetFailureRate calculates and returns the current failure rate across the entire window.
// Returns 0 if no requests have been recorded.
func (sw *SlidingWindow) GetFailureRate() float64 {
	sw.mu.RLock()
	defer sw.mu.RUnlock()

	var totalRequests, totalFailures int64
	for i := range sw.buckets {
		totalRequests += sw.buckets[i]
		totalFailures += sw.failureBuckets[i]
	}

	if totalRequests == 0 {
		return 0
	}

	return float64(totalFailures) / float64(totalRequests)
}

// GetStats returns comprehensive statistics about requests in the sliding window,
// including total requests, failed requests, and failure rate.
func (sw *SlidingWindow) GetStats() WindowStats {
	sw.mu.RLock()
	defer sw.mu.RUnlock()

	var totalRequests, totalFailures int64
	for i := range sw.buckets {
		totalRequests += sw.buckets[i]
		totalFailures += sw.failureBuckets[i]
	}

	failureRate := float64(0)
	if totalRequests > 0 {
		failureRate = float64(totalFailures) / float64(totalRequests)
	}

	return WindowStats{
		TotalRequests:  totalRequests,
		FailedRequests: totalFailures,
		FailureRate:    failureRate,
	}
}

// CircuitBreakerState represents the current state and statistics of a circuit breaker
type CircuitBreakerState struct {
	State                string    `json:"state"`
	LastStateChange      time.Time `json:"last_state_change"`
	ConsecutiveFailures  int32     `json:"consecutive_failures"`
	ConsecutiveSuccesses int32     `json:"consecutive_successes"`
	TotalRequests        int64     `json:"total_requests"`
	FailedRequests       int64     `json:"failed_requests"`
	FailureRate          float64   `json:"failure_rate"`
	NextRetryTime        time.Time `json:"next_retry_time,omitempty"`
}

// WindowStats represents statistics collected over a sliding time window
type WindowStats struct {
	TotalRequests  int64   `json:"total_requests"`
	FailedRequests int64   `json:"failed_requests"`
	FailureRate    float64 `json:"failure_rate"`
}
