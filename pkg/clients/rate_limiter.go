// Package clients provides rate limiting implementations
package clients

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// NewRateLimiter creates a new rate limiter with the specified rate (requests per second)
// and burst size (maximum requests that can be made at once).
func NewRateLimiter(rate int, burst int) RateLimiter {
	return NewTokenBucketRateLimiter(float64(rate), burst)
}

// RateLimiter defines the interface for rate limiting implementations.
// It supports immediate checks, blocking waits, and future reservations.
type RateLimiter interface {
	// Allow checks if a request is allowed
	Allow() bool

	// Wait blocks until a request is allowed
	Wait(ctx context.Context) error

	// Reserve reserves a future request
	Reserve() Reservation

	// SetRate updates the rate limit
	SetRate(rate float64)

	// SetBurst updates the burst size
	SetBurst(burst int)

	// GetStats returns rate limiter statistics
	GetStats() RateLimiterStats
}

// Reservation represents a rate limiter reservation for future use.
// It allows checking validity, delay time, and cancellation.
type Reservation interface {
	// OK returns whether the reservation is valid
	OK() bool

	// Delay returns the delay before the request can proceed
	Delay() time.Duration

	// Cancel cancels the reservation
	Cancel()
}

// RateLimiterStats provides detailed statistics about rate limiter performance
// and current state for monitoring and debugging.
type RateLimiterStats struct {
	Rate            float64       `json:"rate"`
	Burst           int           `json:"burst"`
	AllowedRequests int64         `json:"allowed_requests"`
	BlockedRequests int64         `json:"blocked_requests"`
	CurrentTokens   float64       `json:"current_tokens"`
	LastRefill      time.Time     `json:"last_refill"`
	AverageWaitTime time.Duration `json:"average_wait_time"`
}

// TokenBucketRateLimiter implements the token bucket algorithm for rate limiting.
// Tokens are added at a constant rate and consumed by requests.
type TokenBucketRateLimiter struct {
	rate     float64
	burst    int
	tokens   float64
	lastTime time.Time

	// Stats
	allowedRequests int64
	blockedRequests int64
	totalWaitTime   int64

	mu sync.Mutex
}

// NewTokenBucketRateLimiter creates a new token bucket rate limiter with the specified
// rate (tokens per second) and burst capacity (maximum tokens).
func NewTokenBucketRateLimiter(rate float64, burst int) *TokenBucketRateLimiter {
	return &TokenBucketRateLimiter{
		rate:     rate,
		burst:    burst,
		tokens:   float64(burst),
		lastTime: time.Now(),
	}
}

// Allow checks if a request is allowed immediately.
// Returns true if a token is available and consumes it, false otherwise.
func (tb *TokenBucketRateLimiter) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= 1.0 {
		tb.tokens--
		atomic.AddInt64(&tb.allowedRequests, 1)
		return true
	}

	atomic.AddInt64(&tb.blockedRequests, 1)
	return false
}

// Wait blocks until a request is allowed
func (tb *TokenBucketRateLimiter) Wait(ctx context.Context) error {
	start := time.Now()

	for {
		tb.mu.Lock()
		tb.refill()

		if tb.tokens >= 1.0 {
			tb.tokens--
			atomic.AddInt64(&tb.allowedRequests, 1)
			atomic.AddInt64(&tb.totalWaitTime, time.Since(start).Nanoseconds())
			tb.mu.Unlock()
			return nil
		}

		// Calculate wait time
		deficit := 1.0 - tb.tokens
		waitTime := time.Duration(deficit / tb.rate * float64(time.Second))
		tb.mu.Unlock()

		// Wait with context
		timer := time.NewTimer(waitTime)
		defer timer.Stop()

		select {
		case <-timer.C:
			continue
		case <-ctx.Done():
			atomic.AddInt64(&tb.blockedRequests, 1)
			return ctx.Err()
		}
	}
}

// Reserve reserves a future request
func (tb *TokenBucketRateLimiter) Reserve() Reservation {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	now := time.Now()

	if tb.tokens >= 1.0 {
		tb.tokens--
		atomic.AddInt64(&tb.allowedRequests, 1)
		return &tokenReservation{
			ok:    true,
			delay: 0,
			at:    now,
		}
	}

	// Calculate when token will be available
	deficit := 1.0 - tb.tokens
	delay := time.Duration(deficit / tb.rate * float64(time.Second))

	// Reserve the token
	tb.tokens = 0

	return &tokenReservation{
		ok:      true,
		delay:   delay,
		at:      now.Add(delay),
		limiter: tb,
	}
}

// refill adds tokens based on elapsed time
func (tb *TokenBucketRateLimiter) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastTime).Seconds()

	tb.tokens += elapsed * tb.rate
	if tb.tokens > float64(tb.burst) {
		tb.tokens = float64(tb.burst)
	}

	tb.lastTime = now
}

// SetRate updates the rate limit
func (tb *TokenBucketRateLimiter) SetRate(rate float64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.rate = rate
}

// SetBurst updates the burst size
func (tb *TokenBucketRateLimiter) SetBurst(burst int) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.burst = burst
	if tb.tokens > float64(burst) {
		tb.tokens = float64(burst)
	}
}

// GetStats returns rate limiter statistics
func (tb *TokenBucketRateLimiter) GetStats() RateLimiterStats {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	allowed := atomic.LoadInt64(&tb.allowedRequests)
	blocked := atomic.LoadInt64(&tb.blockedRequests)
	totalWait := atomic.LoadInt64(&tb.totalWaitTime)

	avgWait := time.Duration(0)
	if allowed > 0 {
		avgWait = time.Duration(totalWait / allowed)
	}

	return RateLimiterStats{
		Rate:            tb.rate,
		Burst:           tb.burst,
		AllowedRequests: allowed,
		BlockedRequests: blocked,
		CurrentTokens:   tb.tokens,
		LastRefill:      tb.lastTime,
		AverageWaitTime: avgWait,
	}
}

// tokenReservation implements the Reservation interface
type tokenReservation struct {
	ok        bool
	delay     time.Duration
	at        time.Time
	limiter   *TokenBucketRateLimiter
	canceled bool
	mu        sync.Mutex
}

func (r *tokenReservation) OK() bool {
	return r.ok && !r.canceled
}

func (r *tokenReservation) Delay() time.Duration {
	return r.delay
}

func (r *tokenReservation) Cancel() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.canceled && r.delay > 0 {
		r.canceled = true
		// Return the reserved token
		if r.limiter != nil {
			r.limiter.mu.Lock()
			r.limiter.tokens++
			r.limiter.mu.Unlock()
		}
	}
}

// AdaptiveRateLimiter adjusts rate based on response times and errors
type AdaptiveRateLimiter struct {
	baseRate    float64
	currentRate float64
	minRate     float64
	maxRate     float64

	// Adaptation parameters
	increaseMultiplier float64
	decreaseMultiplier float64

	// Monitoring window
	window           *ResponseTimeWindow
	errorThreshold   float64
	latencyThreshold time.Duration

	// Underlying rate limiter
	limiter RateLimiter

	// Stats
	adaptations    int64
	lastAdaptation time.Time

	mu sync.RWMutex
}

// NewAdaptiveRateLimiter creates a new adaptive rate limiter
func NewAdaptiveRateLimiter(baseRate float64, burst int) *AdaptiveRateLimiter {
	return &AdaptiveRateLimiter{
		baseRate:           baseRate,
		currentRate:        baseRate,
		minRate:            baseRate * 0.1, // 10% of base rate
		maxRate:            baseRate * 2.0, // 200% of base rate
		increaseMultiplier: 1.1,
		decreaseMultiplier: 0.9,
		errorThreshold:     0.05, // 5% error rate
		latencyThreshold:   time.Second,
		window:             NewResponseTimeWindow(60 * time.Second),
		limiter:            NewTokenBucketRateLimiter(baseRate, burst),
		lastAdaptation:     time.Now(),
	}
}

// Allow checks if a request is allowed
func (ar *AdaptiveRateLimiter) Allow() bool {
	ar.maybeAdapt()
	return ar.limiter.Allow()
}

// Wait blocks until a request is allowed
func (ar *AdaptiveRateLimiter) Wait(ctx context.Context) error {
	ar.maybeAdapt()
	return ar.limiter.Wait(ctx)
}

// Reserve reserves a future request
func (ar *AdaptiveRateLimiter) Reserve() Reservation {
	ar.maybeAdapt()
	return ar.limiter.Reserve()
}

// RecordResponse records a response for adaptation
func (ar *AdaptiveRateLimiter) RecordResponse(latency time.Duration, success bool) {
	ar.window.Record(latency, success)
}

// maybeAdapt checks if rate should be adapted
func (ar *AdaptiveRateLimiter) maybeAdapt() {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	// Don't adapt too frequently
	if time.Since(ar.lastAdaptation) < 5*time.Second {
		return
	}

	stats := ar.window.GetStats()

	// Decrease rate if error rate is high or latency is high
	if stats.ErrorRate > ar.errorThreshold || stats.P95Latency > ar.latencyThreshold {
		newRate := ar.currentRate * ar.decreaseMultiplier
		if newRate < ar.minRate {
			newRate = ar.minRate
		}

		if newRate != ar.currentRate {
			ar.currentRate = newRate
			ar.limiter.SetRate(newRate)
			atomic.AddInt64(&ar.adaptations, 1)
			ar.lastAdaptation = time.Now()
		}
	} else if stats.ErrorRate < ar.errorThreshold/2 && stats.P95Latency < ar.latencyThreshold/2 {
		// Increase rate if performance is good
		newRate := ar.currentRate * ar.increaseMultiplier
		if newRate > ar.maxRate {
			newRate = ar.maxRate
		}

		if newRate != ar.currentRate {
			ar.currentRate = newRate
			ar.limiter.SetRate(newRate)
			atomic.AddInt64(&ar.adaptations, 1)
			ar.lastAdaptation = time.Now()
		}
	}
}

// SetRate updates the base rate
func (ar *AdaptiveRateLimiter) SetRate(rate float64) {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	ar.baseRate = rate
	ar.currentRate = rate
	ar.minRate = rate * 0.1
	ar.maxRate = rate * 2.0
	ar.limiter.SetRate(rate)
}

// SetBurst updates the burst size
func (ar *AdaptiveRateLimiter) SetBurst(burst int) {
	ar.limiter.SetBurst(burst)
}

// GetStats returns rate limiter statistics
func (ar *AdaptiveRateLimiter) GetStats() RateLimiterStats {
	ar.mu.RLock()
	defer ar.mu.RUnlock()

	stats := ar.limiter.GetStats()
	stats.Rate = ar.currentRate // Override with current adaptive rate

	return stats
}

// ResponseTimeWindow tracks response times in a sliding window
type ResponseTimeWindow struct {
	latencies  []time.Duration
	successes  []bool
	timestamps []time.Time
	size       int
	duration   time.Duration
	index      int
	mu         sync.RWMutex
}

// NewResponseTimeWindow creates a new response time window
func NewResponseTimeWindow(duration time.Duration) *ResponseTimeWindow {
	size := 1000 // Keep last 1000 samples
	return &ResponseTimeWindow{
		latencies:  make([]time.Duration, size),
		successes:  make([]bool, size),
		timestamps: make([]time.Time, size),
		size:       size,
		duration:   duration,
	}
}

// Record records a response
func (rtw *ResponseTimeWindow) Record(latency time.Duration, success bool) {
	rtw.mu.Lock()
	defer rtw.mu.Unlock()

	rtw.latencies[rtw.index] = latency
	rtw.successes[rtw.index] = success
	rtw.timestamps[rtw.index] = time.Now()
	rtw.index = (rtw.index + 1) % rtw.size
}

// GetStats returns window statistics
func (rtw *ResponseTimeWindow) GetStats() ResponseTimeStats {
	rtw.mu.RLock()
	defer rtw.mu.RUnlock()

	now := time.Now()
	cutoff := now.Add(-rtw.duration)

	var validLatencies []time.Duration
	var errorCount int
	var totalCount int

	for i := 0; i < rtw.size; i++ {
		if !rtw.timestamps[i].IsZero() && rtw.timestamps[i].After(cutoff) {
			validLatencies = append(validLatencies, rtw.latencies[i])
			totalCount++
			if !rtw.successes[i] {
				errorCount++
			}
		}
	}

	errorRate := float64(0)
	if totalCount > 0 {
		errorRate = float64(errorCount) / float64(totalCount)
	}

	return ResponseTimeStats{
		Count:       totalCount,
		ErrorRate:   errorRate,
		P95Latency:  calculatePercentile(validLatencies, 0.95),
		P99Latency:  calculatePercentile(validLatencies, 0.99),
		MeanLatency: calculateMean(validLatencies),
	}
}

// calculatePercentile calculates a percentile from latencies
func calculatePercentile(latencies []time.Duration, percentile float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	// Simple implementation - in production use a better algorithm
	index := int(float64(len(latencies)-1) * percentile)
	return latencies[index]
}

// calculateMean calculates mean latency
func calculateMean(latencies []time.Duration) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	var sum time.Duration
	for _, latency := range latencies {
		sum += latency
	}

	return sum / time.Duration(len(latencies))
}

// ResponseTimeStats represents response time statistics
type ResponseTimeStats struct {
	Count       int           `json:"count"`
	ErrorRate   float64       `json:"error_rate"`
	P95Latency  time.Duration `json:"p95_latency"`
	P99Latency  time.Duration `json:"p99_latency"`
	MeanLatency time.Duration `json:"mean_latency"`
}

// DistributedRateLimiter provides distributed rate limiting
type DistributedRateLimiter struct {
	nodeID       string
	localLimiter RateLimiter
	globalRate   float64
	nodeCount    int

	// Coordination
	coordinator  RateLimiterCoordinator
	lastSync     time.Time
	syncInterval time.Duration

	mu sync.RWMutex
}

// RateLimiterCoordinator coordinates distributed rate limiting
type RateLimiterCoordinator interface {
	// RegisterNode registers a node in the cluster
	RegisterNode(nodeID string) error

	// GetNodeCount returns the current number of nodes
	GetNodeCount() (int, error)

	// ReportUsage reports usage statistics
	ReportUsage(nodeID string, used int64, timestamp time.Time) error

	// GetGlobalUsage returns global usage statistics
	GetGlobalUsage() (int64, error)
}

// NewDistributedRateLimiter creates a new distributed rate limiter
func NewDistributedRateLimiter(nodeID string, globalRate float64, coordinator RateLimiterCoordinator) (*DistributedRateLimiter, error) {
	// Register with coordinator
	if err := coordinator.RegisterNode(nodeID); err != nil {
		return nil, fmt.Errorf("failed to register node: %w", err)
	}

	// Get initial node count
	nodeCount, err := coordinator.GetNodeCount()
	if err != nil {
		return nil, fmt.Errorf("failed to get node count: %w", err)
	}

	// Calculate per-node rate
	nodeRate := globalRate / float64(nodeCount)
	burst := int(nodeRate) + 1

	return &DistributedRateLimiter{
		nodeID:       nodeID,
		localLimiter: NewTokenBucketRateLimiter(nodeRate, burst),
		globalRate:   globalRate,
		nodeCount:    nodeCount,
		coordinator:  coordinator,
		syncInterval: 5 * time.Second,
		lastSync:     time.Now(),
	}, nil
}

// Allow checks if a request is allowed
func (dr *DistributedRateLimiter) Allow() bool {
	dr.maybeSync()
	return dr.localLimiter.Allow()
}

// Wait blocks until a request is allowed
func (dr *DistributedRateLimiter) Wait(ctx context.Context) error {
	dr.maybeSync()
	return dr.localLimiter.Wait(ctx)
}

// Reserve reserves a future request
func (dr *DistributedRateLimiter) Reserve() Reservation {
	dr.maybeSync()
	return dr.localLimiter.Reserve()
}

// maybeSync synchronizes with the coordinator if needed
func (dr *DistributedRateLimiter) maybeSync() {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	if time.Since(dr.lastSync) < dr.syncInterval {
		return
	}

	// Get current node count
	if nodeCount, err := dr.coordinator.GetNodeCount(); err == nil && nodeCount != dr.nodeCount {
		dr.nodeCount = nodeCount

		// Recalculate per-node rate
		nodeRate := dr.globalRate / float64(nodeCount)
		dr.localLimiter.SetRate(nodeRate)
		dr.localLimiter.SetBurst(int(nodeRate) + 1)
	}

	// Report usage
	stats := dr.localLimiter.GetStats()
	dr.coordinator.ReportUsage(dr.nodeID, stats.AllowedRequests, time.Now())

	dr.lastSync = time.Now()
}

// SetRate updates the global rate
func (dr *DistributedRateLimiter) SetRate(rate float64) {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	dr.globalRate = rate
	nodeRate := rate / float64(dr.nodeCount)
	dr.localLimiter.SetRate(nodeRate)
}

// SetBurst updates the burst size
func (dr *DistributedRateLimiter) SetBurst(burst int) {
	dr.localLimiter.SetBurst(burst)
}

// GetStats returns rate limiter statistics
func (dr *DistributedRateLimiter) GetStats() RateLimiterStats {
	stats := dr.localLimiter.GetStats()

	// Add distributed information
	dr.mu.RLock()
	stats.Rate = dr.globalRate
	dr.mu.RUnlock()

	return stats
}
