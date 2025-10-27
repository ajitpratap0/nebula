// Package pipeline implements advanced error recovery and dead letter queue mechanisms
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// ErrorManager manages error recovery, retries, and dead letter queues
type ErrorManager struct {
	config *StreamingConfig
	logger *zap.Logger

	// Error processing
	deadLetterQueue *DeadLetterQueue
	retryManager    *RetryManager
	circuitBreaker  *PipelineCircuitBreaker
	errorCorrelator *ErrorCorrelator

	// Metrics
	errorsProcessed    int64
	errorsRecovered    int64
	errorsDeadLettered int64

	// State
	isRunning int32
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// DeadLetterQueue manages records that couldn't be processed
type DeadLetterQueue struct {
	records []*DeadLetterRecord
	maxSize int
	mu      sync.RWMutex
	logger  *zap.Logger
	metrics *DLQMetrics
}

// DLQMetrics tracks dead letter queue metrics
type DLQMetrics struct {
	totalRecords   int64
	expiredRecords int64
	retriedRecords int64 //nolint:unused // Reserved for retry metrics tracking
	lastCleanup    time.Time
}

// RetryManager handles retry logic with exponential backoff and jitter
type RetryManager struct {
	config        *StreamingConfig
	logger        *zap.Logger
	retryAttempts map[string]*RetryState
	mu            sync.RWMutex
}

// RetryState tracks retry attempts for a specific error
type RetryState struct {
	attempts     int
	firstAttempt time.Time
	lastAttempt  time.Time
	nextRetry    time.Time
	backoffDelay time.Duration
	errorType    string
	context      map[string]interface{} //nolint:unused // Reserved for error context information
}

// PipelineCircuitBreaker provides circuit breaker functionality for error management
type PipelineCircuitBreaker struct {
	state           CircuitBreakerState
	config          *CircuitBreakerConfig
	logger          *zap.Logger
	mu              sync.RWMutex
	lastStateChange time.Time
}

// CircuitBreakerConfig configures circuit breaker behavior
type CircuitBreakerConfig struct {
	FailureThreshold    int           `json:"failure_threshold"`
	SuccessThreshold    int           `json:"success_threshold"`
	Timeout             time.Duration `json:"timeout"`
	HalfOpenMaxRequests int           `json:"half_open_max_requests"`
	WindowSize          time.Duration `json:"window_size"`
}

// ErrorCorrelator groups and analyzes related errors
type ErrorCorrelator struct {
	errorPatterns map[string]*ErrorPattern
	logger        *zap.Logger
	mu            sync.RWMutex
}

// ErrorPattern represents a pattern of related errors
type ErrorPattern struct {
	Type            string                 `json:"type"`
	Count           int64                  `json:"count"`
	FirstSeen       time.Time              `json:"first_seen"`
	LastSeen        time.Time              `json:"last_seen"`
	Frequency       float64                `json:"frequency"`
	Impact          string                 `json:"impact"`
	Characteristics map[string]interface{} `json:"characteristics"`
	SuggestedAction string                 `json:"suggested_action"`
}

// NewErrorManager creates a new error manager
func NewErrorManager(config *StreamingConfig, logger *zap.Logger) *ErrorManager {
	em := &ErrorManager{
		config: config,
		logger: logger.With(zap.String("component", "error_manager")),
		stopCh: make(chan struct{}),
	}

	em.deadLetterQueue = &DeadLetterQueue{
		records: make([]*DeadLetterRecord, 0, config.DeadLetterQueueSize),
		maxSize: config.DeadLetterQueueSize,
		logger:  em.logger.With(zap.String("subcomponent", "dlq")),
		metrics: &DLQMetrics{},
	}

	em.retryManager = &RetryManager{
		config:        config,
		logger:        em.logger.With(zap.String("subcomponent", "retry")),
		retryAttempts: make(map[string]*RetryState),
	}

	if config.CircuitBreakerEnabled {
		em.circuitBreaker = &PipelineCircuitBreaker{
			config: &CircuitBreakerConfig{
				FailureThreshold:    5,
				SuccessThreshold:    3,
				Timeout:             30 * time.Second,
				HalfOpenMaxRequests: 5,
				WindowSize:          60 * time.Second,
			},
			logger: em.logger.With(zap.String("subcomponent", "circuit_breaker")),
		}
		em.circuitBreaker.state = CircuitBreakerState{State: "closed"}
	}

	em.errorCorrelator = &ErrorCorrelator{
		errorPatterns: make(map[string]*ErrorPattern),
		logger:        em.logger.With(zap.String("subcomponent", "correlator")),
	}

	return em
}

// ProcessErrors processes errors from the pipeline
func (em *ErrorManager) ProcessErrors(ctx context.Context, errorCh <-chan *StreamingError, parentWG *sync.WaitGroup) {
	if parentWG != nil {
		defer parentWG.Done()
	}

	if !atomic.CompareAndSwapInt32(&em.isRunning, 0, 1) {
		em.logger.Error("error manager already running")
		return
	}
	defer atomic.StoreInt32(&em.isRunning, 0)

	em.logger.Info("error manager started")

	// Start background tasks
	em.wg.Add(2)
	go em.cleanupDeadLetterQueue(ctx)
	go em.retryProcessor(ctx)

	for {
		select {
		case streamErr, ok := <-errorCh:
			if !ok {
				em.logger.Info("error channel closed")
				return
			}

			em.processError(ctx, streamErr)

		case <-ctx.Done():
			em.logger.Info("error manager context canceled")
			return

		case <-em.stopCh:
			em.logger.Info("error manager stop signal received")
			return
		}
	}
}

// processError handles a single error
func (em *ErrorManager) processError(ctx context.Context, streamErr *StreamingError) {
	atomic.AddInt64(&em.errorsProcessed, 1)

	em.logger.Debug("processing error",
		zap.String("type", streamErr.Type),
		zap.Error(streamErr.Error),
		zap.Int("worker_id", streamErr.WorkerID))

	// Update error correlation
	em.errorCorrelator.recordError(streamErr)

	// Check circuit breaker
	if em.circuitBreaker != nil {
		if !em.circuitBreaker.allowRequest() {
			em.logger.Warn("circuit breaker open, sending error to dead letter queue",
				zap.String("error_type", streamErr.Type))
			em.sendToDeadLetterQueue(streamErr)
			return
		}
	}

	// Determine if error is recoverable
	if !streamErr.Recoverable {
		em.sendToDeadLetterQueue(streamErr)
		return
	}

	// Try to recover the error
	if em.tryRecover(ctx, streamErr) {
		atomic.AddInt64(&em.errorsRecovered, 1)
		if em.circuitBreaker != nil {
			em.circuitBreaker.recordSuccess()
		}
	} else {
		if em.circuitBreaker != nil {
			em.circuitBreaker.recordFailure()
		}
		em.sendToDeadLetterQueue(streamErr)
	}
}

// tryRecover attempts to recover from an error
func (em *ErrorManager) tryRecover(ctx context.Context, streamErr *StreamingError) bool {
	errorID := em.generateErrorID(streamErr)

	// Check if we should retry
	if !em.retryManager.shouldRetry(errorID, streamErr) {
		em.logger.Debug("max retries exceeded",
			zap.String("error_id", errorID),
			zap.String("error_type", streamErr.Type))
		return false
	}

	// Calculate backoff delay
	delay := em.retryManager.calculateBackoff(errorID, streamErr.Retries)

	em.logger.Debug("scheduling retry",
		zap.String("error_id", errorID),
		zap.Duration("delay", delay),
		zap.Int("attempt", streamErr.Retries+1))

	// Schedule retry
	select {
	case <-time.After(delay):
		return em.attemptRecovery(ctx, streamErr)
	case <-ctx.Done():
		return false
	}
}

// attemptRecovery attempts to recover specific error types
func (em *ErrorManager) attemptRecovery(ctx context.Context, streamErr *StreamingError) bool {
	switch streamErr.Type {
	case "extract_error":
		return em.recoverExtractError(ctx, streamErr)
	case "transform_error":
		return em.recoverTransformError(ctx, streamErr)
	case "load_error":
		return em.recoverLoadError(ctx, streamErr)
	default:
		em.logger.Warn("unknown error type for recovery",
			zap.String("type", streamErr.Type))
		return false
	}
}

// recoverExtractError attempts to recover from extraction errors
func (em *ErrorManager) recoverExtractError(ctx context.Context, streamErr *StreamingError) bool {
	// Implement extraction error recovery logic
	// For now, this is a placeholder
	em.logger.Debug("attempting extract error recovery")

	// Simulate recovery attempt
	time.Sleep(100 * time.Millisecond)

	// In a real implementation, this would:
	// 1. Retry the extraction with different parameters
	// 2. Skip problematic records
	// 3. Switch to alternative data sources
	// 4. Apply data sanitization

	return false // Placeholder - always fails for now
}

// recoverTransformError attempts to recover from transformation errors
func (em *ErrorManager) recoverTransformError(ctx context.Context, streamErr *StreamingError) bool {
	// Implement transformation error recovery logic
	em.logger.Debug("attempting transform error recovery")

	// In a real implementation, this would:
	// 1. Apply alternative transformation rules
	// 2. Use default values for missing fields
	// 3. Skip validation for non-critical fields
	// 4. Apply data type conversions

	return false // Placeholder - always fails for now
}

// recoverLoadError attempts to recover from loading errors
func (em *ErrorManager) recoverLoadError(ctx context.Context, streamErr *StreamingError) bool {
	// Implement loading error recovery logic
	em.logger.Debug("attempting load error recovery")

	// In a real implementation, this would:
	// 1. Retry with exponential backoff
	// 2. Switch to alternative destinations
	// 3. Batch smaller chunks of data
	// 4. Apply data deduplication

	return false // Placeholder - always fails for now
}

// sendToDeadLetterQueue sends an error to the dead letter queue
func (em *ErrorManager) sendToDeadLetterQueue(streamErr *StreamingError) {
	dlqRecord := &DeadLetterRecord{
		Error:        streamErr,
		Attempts:     streamErr.Retries,
		FirstFailure: streamErr.Timestamp,
		LastFailure:  time.Now(),
		TTL:          time.Now().Add(24 * time.Hour),
		Reason:       "max_retries_exceeded",
		Stage:        streamErr.Type,
		WorkerID:     streamErr.WorkerID,
	}

	em.deadLetterQueue.add(dlqRecord)
	atomic.AddInt64(&em.errorsDeadLettered, 1)

	em.logger.Warn("error sent to dead letter queue",
		zap.String("error_type", streamErr.Type),
		zap.Error(streamErr.Error),
		zap.Int("attempts", streamErr.Retries))
}

// generateErrorID generates a unique ID for error tracking
func (em *ErrorManager) generateErrorID(streamErr *StreamingError) string {
	return fmt.Sprintf("%s_%d_%d", streamErr.Type, streamErr.WorkerID, streamErr.Timestamp.Unix())
}

// Dead Letter Queue implementation

// add adds a record to the dead letter queue
func (dlq *DeadLetterQueue) add(record *DeadLetterRecord) {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	// Check if queue is full
	if len(dlq.records) >= dlq.maxSize {
		// Remove oldest record
		dlq.records = dlq.records[1:]
		dlq.logger.Warn("dead letter queue full, removing oldest record")
	}

	dlq.records = append(dlq.records, record)
	atomic.AddInt64(&dlq.metrics.totalRecords, 1)

	dlq.logger.Debug("record added to dead letter queue",
		zap.String("reason", record.Reason),
		zap.Time("ttl", record.TTL))
}

// cleanup removes expired records from the dead letter queue
func (dlq *DeadLetterQueue) cleanup() {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	now := time.Now()
	filtered := make([]*DeadLetterRecord, 0, len(dlq.records))

	for _, record := range dlq.records {
		if now.Before(record.TTL) {
			filtered = append(filtered, record)
		} else {
			atomic.AddInt64(&dlq.metrics.expiredRecords, 1)
		}
	}

	removedCount := len(dlq.records) - len(filtered)
	dlq.records = filtered
	dlq.metrics.lastCleanup = now

	if removedCount > 0 {
		dlq.logger.Info("cleaned up expired dead letter records",
			zap.Int("removed", removedCount),
			zap.Int("remaining", len(filtered)))
	}
}

// Retry Manager implementation

// shouldRetry determines if an error should be retried
func (rm *RetryManager) shouldRetry(errorID string, streamErr *StreamingError) bool {
	rm.mu.RLock()
	retryState, exists := rm.retryAttempts[errorID]
	rm.mu.RUnlock()

	if !exists {
		// First attempt
		rm.mu.Lock()
		rm.retryAttempts[errorID] = &RetryState{
			attempts:     0,
			firstAttempt: time.Now(),
			errorType:    streamErr.Type,
		}
		rm.mu.Unlock()
		return true
	}

	return retryState.attempts < rm.config.MaxRetryAttempts
}

// calculateBackoff calculates the backoff delay for a retry
func (rm *RetryManager) calculateBackoff(errorID string, attempt int) time.Duration {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	retryState := rm.retryAttempts[errorID]
	retryState.attempts++
	retryState.lastAttempt = time.Now()

	// Exponential backoff with jitter
	baseDelay := rm.config.RetryBackoffBase
	exponentialDelay := time.Duration(1<<uint(attempt)) * baseDelay //nolint:gosec // G115: Safe conversion as attempt is bounded by MaxRetries

	// Add jitter (±25%)
	jitter := time.Duration(time.Now().UnixNano()%int64(exponentialDelay/4)) - exponentialDelay/8
	delay := exponentialDelay + jitter

	// Cap at maximum delay
	if delay > rm.config.RetryBackoffMax {
		delay = rm.config.RetryBackoffMax
	}

	retryState.backoffDelay = delay
	retryState.nextRetry = time.Now().Add(delay)

	return delay
}

// Circuit Breaker implementation

// allowRequest checks if a request should be allowed through the circuit breaker
func (cb *PipelineCircuitBreaker) allowRequest() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state.State {
	case "closed":
		return true

	case "open":
		if now.After(cb.state.NextRetry) {
			cb.state.State = "half_open"
			cb.state.RequestCount = 0
			cb.lastStateChange = now
			cb.logger.Info("circuit breaker transitioning to half-open")
		}
		return cb.state.State == "half_open"

	case "half_open":
		return cb.state.RequestCount < cb.config.HalfOpenMaxRequests

	default:
		return false
	}
}

// recordSuccess records a successful request
func (cb *PipelineCircuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state.SuccessCount++
	cb.state.RequestCount++

	if cb.state.State == "half_open" && cb.state.SuccessCount >= cb.config.SuccessThreshold {
		cb.state.State = "closed"
		cb.state.FailureCount = 0
		cb.state.SuccessCount = 0
		cb.lastStateChange = time.Now()
		cb.logger.Info("circuit breaker closed")
	}
}

// recordFailure records a failed request
func (cb *PipelineCircuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state.FailureCount++
	cb.state.RequestCount++
	cb.state.LastFailure = time.Now()

	if cb.state.State == "closed" && cb.state.FailureCount >= cb.config.FailureThreshold {
		cb.state.State = "open"
		cb.state.NextRetry = time.Now().Add(cb.config.Timeout)
		cb.lastStateChange = time.Now()
		cb.logger.Warn("circuit breaker opened",
			zap.Int("failure_count", cb.state.FailureCount))
	} else if cb.state.State == "half_open" {
		cb.state.State = "open"
		cb.state.NextRetry = time.Now().Add(cb.config.Timeout)
		cb.lastStateChange = time.Now()
		cb.logger.Warn("circuit breaker re-opened from half-open")
	}
}

// Error Correlator implementation

// recordError records an error for pattern analysis
func (ec *ErrorCorrelator) recordError(streamErr *StreamingError) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	pattern, exists := ec.errorPatterns[streamErr.Type]
	if !exists {
		pattern = &ErrorPattern{
			Type:            streamErr.Type,
			FirstSeen:       streamErr.Timestamp,
			Characteristics: make(map[string]interface{}),
		}
		ec.errorPatterns[streamErr.Type] = pattern
	}

	pattern.Count++
	pattern.LastSeen = streamErr.Timestamp

	// Calculate frequency (errors per minute)
	duration := pattern.LastSeen.Sub(pattern.FirstSeen)
	if duration > 0 {
		pattern.Frequency = float64(pattern.Count) / duration.Minutes()
	}

	// Update characteristics
	pattern.Characteristics["worker_id"] = streamErr.WorkerID
	pattern.Characteristics["last_error"] = streamErr.Error.Error()

	ec.logger.Debug("error pattern updated",
		zap.String("type", streamErr.Type),
		zap.Int64("count", pattern.Count),
		zap.Float64("frequency", pattern.Frequency))
}

// Background tasks

// cleanupDeadLetterQueue periodically cleans up expired records
func (em *ErrorManager) cleanupDeadLetterQueue(ctx context.Context) {
	defer em.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			em.deadLetterQueue.cleanup()

		case <-ctx.Done():
			return

		case <-em.stopCh:
			return
		}
	}
}

// retryProcessor processes scheduled retries
func (em *ErrorManager) retryProcessor(ctx context.Context) {
	defer em.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			em.processScheduledRetries(ctx)

		case <-ctx.Done():
			return

		case <-em.stopCh:
			return
		}
	}
}

// processScheduledRetries processes any scheduled retries
func (em *ErrorManager) processScheduledRetries(ctx context.Context) {
	em.retryManager.mu.RLock()
	defer em.retryManager.mu.RUnlock()

	now := time.Now()
	for errorID, retryState := range em.retryManager.retryAttempts {
		if now.After(retryState.nextRetry) && retryState.attempts > 0 {
			em.logger.Debug("processing scheduled retry",
				zap.String("error_id", errorID),
				zap.Int("attempt", retryState.attempts))

			// In a real implementation, this would re-inject the failed records
			// into the appropriate pipeline stage for retry
		}
	}
}

// Stop stops the error manager
func (em *ErrorManager) Stop() {
	if !atomic.CompareAndSwapInt32(&em.isRunning, 1, 0) {
		return
	}

	em.logger.Info("stopping error manager")
	close(em.stopCh)
	em.wg.Wait()
}

// GetStats returns error management statistics
func (em *ErrorManager) GetStats() ErrorManagerStats {
	dlqSize := 0
	if em.deadLetterQueue != nil {
		em.deadLetterQueue.mu.RLock()
		dlqSize = len(em.deadLetterQueue.records)
		em.deadLetterQueue.mu.RUnlock()
	}

	circuitBreakerState := ""
	if em.circuitBreaker != nil {
		em.circuitBreaker.mu.RLock()
		circuitBreakerState = em.circuitBreaker.state.State
		em.circuitBreaker.mu.RUnlock()
	}

	return ErrorManagerStats{
		ErrorsProcessed:     atomic.LoadInt64(&em.errorsProcessed),
		ErrorsRecovered:     atomic.LoadInt64(&em.errorsRecovered),
		ErrorsDeadLettered:  atomic.LoadInt64(&em.errorsDeadLettered),
		DeadLetterQueueSize: dlqSize,
		CircuitBreakerState: circuitBreakerState,
		ActiveRetries:       len(em.retryManager.retryAttempts),
		ErrorPatterns:       len(em.errorCorrelator.errorPatterns),
	}
}

// ErrorManagerStats represents error manager statistics
type ErrorManagerStats struct {
	ErrorsProcessed     int64  `json:"errors_processed"`
	ErrorsRecovered     int64  `json:"errors_recovered"`
	ErrorsDeadLettered  int64  `json:"errors_dead_lettered"`
	DeadLetterQueueSize int    `json:"dead_letter_queue_size"`
	CircuitBreakerState string `json:"circuit_breaker_state"`
	ActiveRetries       int    `json:"active_retries"`
	ErrorPatterns       int    `json:"error_patterns"`
}
