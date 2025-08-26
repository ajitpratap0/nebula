// Package base provides the foundational BaseConnector that all Nebula connectors
// inherit from. It implements common functionality including circuit breakers,
// rate limiting, health monitoring, metrics collection, and error handling.
//
// # Overview
//
// BaseConnector provides:
//   - Automatic circuit breaker protection
//   - Configurable rate limiting
//   - Health monitoring with periodic checks
//   - Comprehensive metrics collection
//   - Retry logic with exponential backoff
//   - Connection pooling support
//   - Performance optimization
//   - Progress reporting
//   - State management
//
// # Usage
//
// All connectors should embed BaseConnector to inherit its functionality:
//
//	type MyConnector struct {
//	    *base.BaseConnector
//	    // connector-specific fields
//	}
//
//	func NewMyConnector() *MyConnector {
//	    return &MyConnector{
//	        BaseConnector: base.NewBaseConnector("my-connector", core.TypeSource, "1.0.0"),
//	    }
//	}
//
// # Lifecycle
//
// 1. Create with NewBaseConnector
// 2. Initialize with Initialize() - sets up all production features
// 3. Use throughout connector operations
// 4. Close with Close() - cleans up all resources
//
// # Production Features
//
// Circuit Breaker: Prevents cascading failures by stopping requests when error rate is high
// Rate Limiting: Enforces request rate limits to prevent overwhelming external systems
// Health Checks: Periodic health monitoring with automatic status updates
// Metrics: Comprehensive performance and operational metrics
// Error Handling: Intelligent retry logic with categorized error handling
package base

import (
	"context"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/clients"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/errors"
	"github.com/ajitpratap0/nebula/pkg/logger"
	"github.com/ajitpratap0/nebula/pkg/metrics"
	"github.com/ajitpratap0/nebula/pkg/models"
	"go.uber.org/zap"
)

// BaseConnector provides common functionality for all connectors including
// production-grade features like circuit breakers, rate limiting, health
// monitoring, and comprehensive metrics collection.
type BaseConnector struct {
	// Core fields
	name          string             // Unique connector identifier
	connectorType core.ConnectorType // Source or Destination
	version       string             // Connector version
	config        *config.BaseConfig // Unified configuration
	logger        *zap.Logger        // Structured logger

	// State management
	state      core.State    // Connector state for incremental sync
	position   core.Position // Current processing position
	stateMutex sync.RWMutex  // Protects state access

	// Resource management
	ctx        context.Context    // Connector context
	cancel     context.CancelFunc // Context cancellation
	closed     bool               // Shutdown flag
	closeMutex sync.Mutex         // Protects close operation

	// Production features
	circuitBreaker   *clients.CircuitBreaker // Failure protection
	rateLimiter      clients.RateLimiter     // Request rate control
	healthChecker    *HealthChecker          // Health monitoring
	metricsCollector *metrics.Collector      // Metrics collection
	errorHandler     *ErrorHandler           // Error handling logic

	// Connection management
	connectionPool core.ConnectionPool // Connection pooling
	retryPolicy    *RetryPolicy        // Retry configuration

	// Performance optimization
	batchBuilder core.BatchBuilder     // Batch construction
	optimizer    *PerformanceOptimizer // Dynamic optimization

	// Progress tracking
	progressReporter *ProgressReporter // Progress updates

	// Data quality
	qualityChecker core.DataQualityChecker // Data validation
}

// NewBaseConnector creates a new base connector with the specified name, type, and version.
// This should be called by connector implementations during construction.
//
// Example:
//
//	func NewPostgresSource() *PostgresSource {
//	    return &PostgresSource{
//	        BaseConnector: base.NewBaseConnector("postgres", core.TypeSource, "2.0.0"),
//	    }
//	}
func NewBaseConnector(name string, connectorType core.ConnectorType, version string) *BaseConnector {
	return &BaseConnector{
		name:          name,
		connectorType: connectorType,
		version:       version,
		state:         make(core.State),
		logger:        logger.Get().With(zap.String("connector", name)),
	}
}

// Initialize sets up all production features of the base connector including
// circuit breakers, rate limiting, health monitoring, and metrics collection.
// This must be called before using the connector.
//
// The initialization process:
// 1. Sets up context for lifecycle management
// 2. Configures circuit breaker with failure thresholds
// 3. Initializes rate limiter if configured
// 4. Starts health monitoring
// 5. Sets up metrics collection
// 6. Configures error handling and retry policies
// 7. Initializes performance optimization
//
// Example:
//
//	connector := NewMyConnector()
//	if err := connector.Initialize(ctx, config); err != nil {
//	    return nil, errors.Wrap(err, errors.ErrorTypeConfig, "failed to initialize connector")
//	}
//	defer connector.Close(ctx)
func (bc *BaseConnector) Initialize(ctx context.Context, config *config.BaseConfig) error {
	bc.config = config
	bc.ctx, bc.cancel = context.WithCancel(ctx)

	// Initialize circuit breaker for failure protection
	bc.circuitBreaker = clients.NewCircuitBreaker(clients.CircuitBreakerConfig{
		FailureThreshold: 5,                // Open after 5 consecutive failures
		SuccessThreshold: 3,                // Close after 3 consecutive successes
		Timeout:          30 * time.Second, // Half-open timeout
	})

	// Initialize rate limiter if configured
	if config.Reliability.RateLimitPerSec > 0 {
		bc.rateLimiter = clients.NewRateLimiter(
			config.Reliability.RateLimitPerSec,
			config.Reliability.RateLimitPerSec*2, // Allow bursts up to 2x the limit
		)
	}

	// Initialize health checker with periodic monitoring
	bc.healthChecker = NewHealthChecker(bc.name, 30*time.Second)
	bc.healthChecker.Start(bc.ctx)

	// Initialize metrics collector for observability
	bc.metricsCollector = metrics.NewCollector(bc.name)

	// Initialize error handler with retry configuration
	bc.errorHandler = NewErrorHandler(
		bc.logger,
		config.Reliability.RetryAttempts,
		config.Reliability.RetryDelay,
	)

	// Initialize retry policy for transient failures
	bc.retryPolicy = NewRetryPolicy(
		config.Reliability.RetryAttempts,
		config.Reliability.RetryDelay,
	)

	// Initialize performance optimizer for dynamic tuning
	bc.optimizer = NewPerformanceOptimizer(bc.metricsCollector)

	// Initialize progress reporter for user feedback
	bc.progressReporter = NewProgressReporter(bc.logger, bc.metricsCollector)

	bc.logger.Info("connector initialized",
		zap.String("type", string(bc.connectorType)),
		zap.String("version", bc.version))

	return nil
}

// Name returns the connector name
func (bc *BaseConnector) Name() string {
	return bc.name
}

// Type returns the connector type
func (bc *BaseConnector) Type() core.ConnectorType {
	return bc.connectorType
}

// Version returns the connector version
func (bc *BaseConnector) Version() string {
	return bc.version
}

// GetState returns the current state
func (bc *BaseConnector) GetState() core.State {
	bc.stateMutex.RLock()
	defer bc.stateMutex.RUnlock()

	// Return a copy to prevent external modification
	stateCopy := make(core.State)
	for k, v := range bc.state {
		stateCopy[k] = v
	}
	return stateCopy
}

// SetState updates the connector state
func (bc *BaseConnector) SetState(state core.State) error {
	bc.stateMutex.Lock()
	defer bc.stateMutex.Unlock()

	bc.state = state
	bc.logger.Debug("state updated", zap.Any("state", state))
	return nil
}

// GetPosition returns the current position
func (bc *BaseConnector) GetPosition() core.Position {
	bc.stateMutex.RLock()
	defer bc.stateMutex.RUnlock()
	return bc.position
}

// SetPosition updates the current position
func (bc *BaseConnector) SetPosition(position core.Position) error {
	bc.stateMutex.Lock()
	defer bc.stateMutex.Unlock()

	bc.position = position
	bc.logger.Debug("position updated", zap.String("position", position.String()))
	return nil
}

// Health performs a health check
func (bc *BaseConnector) Health(ctx context.Context) error {
	if bc.closed {
		return errors.New(errors.ErrorTypeConnection, "connector is closed")
	}

	status := bc.healthChecker.GetStatus()
	if status.Status != "healthy" {
		return errors.Wrap(status.Error, errors.ErrorTypeHealth, "health check failed")
	}

	return nil
}

// Metrics returns current metrics
func (bc *BaseConnector) Metrics() map[string]interface{} {
	metrics := bc.metricsCollector.GetAll()

	// Add base metrics
	metrics["name"] = bc.name
	metrics["type"] = bc.connectorType
	metrics["version"] = bc.version
	metrics["uptime"] = time.Since(bc.metricsCollector.StartTime()).Seconds()

	// Add circuit breaker status
	if bc.circuitBreaker != nil {
		cbState := bc.circuitBreaker.GetState()
		metrics["circuit_breaker_state"] = cbState.State
		metrics["circuit_breaker_failure_rate"] = cbState.FailureRate
	}

	// Add rate limiter status
	if bc.rateLimiter != nil {
		rlStats := bc.rateLimiter.GetStats()
		metrics["rate_limit"] = rlStats.Rate
		metrics["rate_limit_burst"] = rlStats.Burst
		metrics["rate_limiter_allowed"] = rlStats.AllowedRequests
		metrics["rate_limiter_blocked"] = rlStats.BlockedRequests
	}

	// Add connection pool stats
	if bc.connectionPool != nil {
		stats := bc.connectionPool.Stats()
		metrics["pool_active"] = stats.Active
		metrics["pool_idle"] = stats.Idle
		metrics["pool_total"] = stats.Total
	}

	// Add health status
	if bc.healthChecker != nil {
		status := bc.healthChecker.GetStatus()
		metrics["health_status"] = status.Status
		metrics["health_check_count"] = bc.healthChecker.CheckCount()
		metrics["health_failure_count"] = bc.healthChecker.FailureCount()
	}

	return metrics
}

// Close shuts down the connector
func (bc *BaseConnector) Close(ctx context.Context) error {
	bc.closeMutex.Lock()
	defer bc.closeMutex.Unlock()

	if bc.closed {
		return nil
	}

	bc.logger.Info("closing connector")

	// Cancel context to stop background operations
	if bc.cancel != nil {
		bc.cancel()
	}

	// Stop health checker
	if bc.healthChecker != nil {
		bc.healthChecker.Stop()
	}

	// Close connection pool
	if bc.connectionPool != nil {
		if err := bc.connectionPool.Close(); err != nil {
			bc.logger.Error("failed to close connection pool", zap.Error(err))
		}
	}

	bc.closed = true
	bc.logger.Info("connector closed")

	return nil
}

// ExecuteWithRetry executes a function with automatic retry logic including
// exponential backoff. Retries are attempted for retryable errors based on
// the configured retry policy.
//
// Example:
//
//	err := connector.ExecuteWithRetry(ctx, func() error {
//	    return apiClient.FetchData()
//	})
//	if err != nil {
//	    logger.Error("operation failed after retries", zap.Error(err))
//	}
func (bc *BaseConnector) ExecuteWithRetry(ctx context.Context, fn func() error) error {
	return bc.retryPolicy.Execute(ctx, fn)
}

// ExecuteWithCircuitBreaker executes a function with circuit breaker protection.
// If the circuit is open due to excessive failures, the function won't be executed
// and an error will be returned immediately.
//
// Example:
//
//	err := connector.ExecuteWithCircuitBreaker(func() error {
//	    return externalService.Call()
//	})
//	if err != nil {
//	    if errors.IsType(err, errors.ErrorTypeRateLimit) {
//	        // Circuit is open, back off
//	    }
//	}
func (bc *BaseConnector) ExecuteWithCircuitBreaker(fn func() error) error {
	return bc.circuitBreaker.Execute(fn)
}

// RateLimit enforces the configured rate limit, blocking if necessary.
// Returns immediately if no rate limiter is configured.
//
// Example:
//
//	for _, record := range records {
//	    if err := connector.RateLimit(ctx); err != nil {
//	        return err // Context cancelled
//	    }
//	    process(record)
//	}
func (bc *BaseConnector) RateLimit(ctx context.Context) error {
	if bc.rateLimiter == nil {
		return nil
	}
	return bc.rateLimiter.Wait(ctx)
}

// RecordMetric records a metric
func (bc *BaseConnector) RecordMetric(name string, value interface{}, metricType core.MetricType) {
	bc.metricsCollector.Record(name, value)
}

// HandleError handles an error with the configured error handler
func (bc *BaseConnector) HandleError(ctx context.Context, err error, record *models.Record) error {
	return bc.errorHandler.HandleError(ctx, err, record)
}

// ShouldRetry checks if an error should be retried
func (bc *BaseConnector) ShouldRetry(err error) bool {
	return bc.errorHandler.ShouldRetry(err)
}

// ReportProgress reports operation progress
func (bc *BaseConnector) ReportProgress(processed, total int64) {
	bc.progressReporter.ReportProgress(processed, total)
}

// OptimizeBatchSize returns an optimized batch size based on current metrics
func (bc *BaseConnector) OptimizeBatchSize() int {
	if bc.optimizer == nil || bc.config == nil {
		return 1000 // default
	}

	currentMetrics := bc.Metrics()
	optimized := bc.optimizer.OptimizeBatchSize(bc.config.Performance.BatchSize, currentMetrics)

	// Apply bounds
	if optimized < 100 {
		optimized = 100
	} else if optimized > 100000 {
		optimized = 100000
	}

	return optimized
}

// OptimizeConcurrency returns an optimized concurrency level
func (bc *BaseConnector) OptimizeConcurrency() int {
	if bc.optimizer == nil || bc.config == nil {
		return 10 // default
	}

	currentMetrics := bc.Metrics()
	optimized := bc.optimizer.OptimizeConcurrency(bc.config.Performance.MaxConcurrency, currentMetrics)

	// Apply bounds
	if optimized < 1 {
		optimized = 1
	} else if optimized > 1000 {
		optimized = 1000
	}

	return optimized
}

// GetLogger returns the connector logger
func (bc *BaseConnector) GetLogger() *zap.Logger {
	return bc.logger
}

// GetConfig returns the connector configuration
func (bc *BaseConnector) GetConfig() *config.BaseConfig {
	return bc.config
}

// GetContext returns the connector context
func (bc *BaseConnector) GetContext() context.Context {
	return bc.ctx
}

// IsHealthy returns true if the connector is healthy
func (bc *BaseConnector) IsHealthy() bool {
	if bc.closed {
		return false
	}

	if bc.healthChecker != nil {
		status := bc.healthChecker.GetStatus()
		return status.Status == "healthy"
	}

	return true
}

// UpdateHealth updates the health status
func (bc *BaseConnector) UpdateHealth(healthy bool, details map[string]interface{}) {
	if bc.healthChecker != nil {
		bc.healthChecker.UpdateStatus(healthy, details)
	}
}

// GetCircuitBreaker returns the circuit breaker
func (bc *BaseConnector) GetCircuitBreaker() *clients.CircuitBreaker {
	return bc.circuitBreaker
}

// GetRateLimiter returns the rate limiter
func (bc *BaseConnector) GetRateLimiter() clients.RateLimiter {
	return bc.rateLimiter
}

// GetErrorHandler returns the error handler
func (bc *BaseConnector) GetErrorHandler() *ErrorHandler {
	return bc.errorHandler
}

// GetMetricsCollector returns the metrics collector
func (bc *BaseConnector) GetMetricsCollector() *metrics.Collector {
	return bc.metricsCollector
}

// SetConnectionPool sets the connection pool
func (bc *BaseConnector) SetConnectionPool(pool core.ConnectionPool) {
	bc.connectionPool = pool
}

// GetConnectionPool returns the connection pool
func (bc *BaseConnector) GetConnectionPool() core.ConnectionPool {
	return bc.connectionPool
}

// SetBatchBuilder sets the batch builder
func (bc *BaseConnector) SetBatchBuilder(builder core.BatchBuilder) {
	bc.batchBuilder = builder
}

// GetBatchBuilder returns the batch builder
func (bc *BaseConnector) GetBatchBuilder() core.BatchBuilder {
	return bc.batchBuilder
}

// SetQualityChecker sets the data quality checker
func (bc *BaseConnector) SetQualityChecker(checker core.DataQualityChecker) {
	bc.qualityChecker = checker
}

// GetQualityChecker returns the data quality checker
func (bc *BaseConnector) GetQualityChecker() core.DataQualityChecker {
	return bc.qualityChecker
}

// Validate validates the connector configuration
func (bc *BaseConnector) Validate() error {
	if bc.config == nil {
		return errors.New(errors.ErrorTypeConfig, "configuration is required")
	}

	if bc.config.Name == "" {
		return errors.New(errors.ErrorTypeConfig, "connector name is required")
	}

	if bc.config.Performance.BatchSize <= 0 {
		bc.config.Performance.BatchSize = 1000
	}

	if bc.config.Performance.MaxConcurrency <= 0 {
		bc.config.Performance.MaxConcurrency = 10
	}

	if bc.config.Performance.BufferSize <= 0 {
		bc.config.Performance.BufferSize = 10000
	}

	return nil
}
