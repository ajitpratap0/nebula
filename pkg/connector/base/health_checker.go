package base

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/logger"
	"go.uber.org/zap"
)

// HealthChecker performs periodic health checks
type HealthChecker struct {
	name             string
	interval         time.Duration
	status           *core.HealthStatus
	statusMutex      sync.RWMutex
	checkFunc        func(ctx context.Context) error
	logger           *zap.Logger
	stopCh           chan struct{}
	wg               sync.WaitGroup
	checkCount       int64
	failureCount     int64
	consecutiveFails int
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(name string, interval time.Duration) *HealthChecker {
	return &HealthChecker{
		name:     name,
		interval: interval,
		status: &core.HealthStatus{
			Status:    "healthy",
			Timestamp: time.Now(),
			Details:   make(map[string]interface{}),
		},
		logger: logger.Get().With(zap.String("component", "health_checker"), zap.String("connector", name)),
		stopCh: make(chan struct{}),
	}
}

// SetCheckFunc sets the health check function
func (hc *HealthChecker) SetCheckFunc(fn func(ctx context.Context) error) {
	hc.checkFunc = fn
}

// Start begins periodic health checks
func (hc *HealthChecker) Start(ctx context.Context) {
	hc.wg.Add(1)
	go func() {
		defer hc.wg.Done()
		ticker := time.NewTicker(hc.interval)
		defer ticker.Stop()

		// Initial check
		hc.performCheck(ctx)

		for {
			select {
			case <-ctx.Done():
				return
			case <-hc.stopCh:
				return
			case <-ticker.C:
				hc.performCheck(ctx)
			}
		}
	}()
}

// Stop stops the health checker
func (hc *HealthChecker) Stop() {
	close(hc.stopCh)
	hc.wg.Wait()
}

// performCheck executes a health check
func (hc *HealthChecker) performCheck(ctx context.Context) {
	atomic.AddInt64(&hc.checkCount, 1)

	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var err error
	if hc.checkFunc != nil {
		err = hc.checkFunc(checkCtx)
	}

	hc.statusMutex.Lock()
	defer hc.statusMutex.Unlock()

	hc.status.Timestamp = time.Now()

	if err != nil {
		atomic.AddInt64(&hc.failureCount, 1)
		hc.consecutiveFails++

		if hc.consecutiveFails >= 3 {
			hc.status.Status = "unhealthy"
		} else {
			hc.status.Status = "degraded"
		}

		hc.status.Error = err
		hc.status.Details["consecutive_failures"] = hc.consecutiveFails
		hc.status.Details["last_error"] = err.Error()

		hc.logger.Warn("health check failed",
			zap.Error(err),
			zap.String("status", hc.status.Status),
			zap.Int("consecutive_failures", hc.consecutiveFails))
	} else {
		hc.consecutiveFails = 0
		hc.status.Status = "healthy"
		hc.status.Error = nil
		delete(hc.status.Details, "consecutive_failures")
		delete(hc.status.Details, "last_error")

		hc.logger.Debug("health check passed")
	}

	hc.status.Details["check_count"] = atomic.LoadInt64(&hc.checkCount)
	hc.status.Details["failure_count"] = atomic.LoadInt64(&hc.failureCount)
}

// GetStatus returns the current health status
func (hc *HealthChecker) GetStatus() *core.HealthStatus {
	hc.statusMutex.RLock()
	defer hc.statusMutex.RUnlock()

	// Return a copy
	statusCopy := &core.HealthStatus{
		Status:    hc.status.Status,
		Timestamp: hc.status.Timestamp,
		Details:   make(map[string]interface{}),
		Error:     hc.status.Error,
	}

	for k, v := range hc.status.Details {
		statusCopy.Details[k] = v
	}

	return statusCopy
}

// UpdateStatus manually updates the health status
func (hc *HealthChecker) UpdateStatus(healthy bool, details map[string]interface{}) {
	hc.statusMutex.Lock()
	defer hc.statusMutex.Unlock()

	hc.status.Timestamp = time.Now()

	if healthy {
		hc.status.Status = "healthy"
		hc.status.Error = nil
		hc.consecutiveFails = 0
	} else {
		hc.status.Status = "unhealthy"
	}

	// Merge details
	for k, v := range details {
		hc.status.Details[k] = v
	}
}

// CheckCount returns the total number of health checks performed
func (hc *HealthChecker) CheckCount() int64 {
	return atomic.LoadInt64(&hc.checkCount)
}

// FailureCount returns the total number of failed health checks
func (hc *HealthChecker) FailureCount() int64 {
	return atomic.LoadInt64(&hc.failureCount)
}

// IsHealthy returns true if the service is healthy
func (hc *HealthChecker) IsHealthy() bool {
	hc.statusMutex.RLock()
	defer hc.statusMutex.RUnlock()
	return hc.status.Status == "healthy"
}
