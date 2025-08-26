// Package pipeline implements backpressure control for streaming pipelines
package pipeline

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

// BackpressureController manages flow control and backpressure in the pipeline
type BackpressureController struct {
	name     string
	strategy BackpressureStrategy
	config   *BackpressureConfig
	logger   *zap.Logger

	// Metrics
	metrics *BackpressureMetrics

	// State
	isActive   int32
	severity   int32 // 0-10, where 10 is most severe
	lastUpdate time.Time

	// Control channels
	controlChan chan ControlSignal

	// Synchronization
	mu sync.RWMutex
}

// BackpressureConfig configures the backpressure controller
type BackpressureConfig struct {
	// Thresholds
	ActivationThreshold   float64 `json:"activation_threshold"`   // 0.0-1.0 buffer utilization
	DeactivationThreshold float64 `json:"deactivation_threshold"` // 0.0-1.0 buffer utilization
	CriticalThreshold     float64 `json:"critical_threshold"`     // 0.0-1.0 buffer utilization

	// Timing
	CheckInterval  time.Duration `json:"check_interval"`
	CooldownPeriod time.Duration `json:"cooldown_period"`

	// Strategies
	Strategy       BackpressureStrategy `json:"strategy"`
	AdaptiveConfig *AdaptiveConfig      `json:"adaptive_config,omitempty"`

	// Limits
	MaxDropRate      float64       `json:"max_drop_rate"`      // For drop strategy
	MaxThrottleDelay time.Duration `json:"max_throttle_delay"` // For throttle strategy

	// Emergency
	EmergencyShutdown  bool    `json:"emergency_shutdown"`
	EmergencyThreshold float64 `json:"emergency_threshold"`
}

// AdaptiveConfig configures adaptive backpressure behavior
type AdaptiveConfig struct {
	// Learning parameters
	LearningRate  float64 `json:"learning_rate"`
	HistoryWindow int     `json:"history_window"`

	// Adjustment factors
	ScaleUpFactor   float64 `json:"scale_up_factor"`
	ScaleDownFactor float64 `json:"scale_down_factor"`

	// Prediction
	UsePrediction     bool          `json:"use_prediction"`
	PredictionHorizon time.Duration `json:"prediction_horizon"`
}

// ControlSignal represents a flow control signal
type ControlSignal struct {
	Type       ControlType            `json:"type"`
	Severity   int                    `json:"severity"`
	Action     string                 `json:"action"`
	Parameters map[string]interface{} `json:"parameters"`
	Timestamp  time.Time              `json:"timestamp"`
	Source     string                 `json:"source"`
	Target     string                 `json:"target,omitempty"`
}

// ControlType defines types of control signals
type ControlType string

const (
	ControlTypeThrottle ControlType = "throttle"
	ControlTypePause    ControlType = "pause"
	ControlTypeResume   ControlType = "resume"
	ControlTypeDrop     ControlType = "drop"
	ControlTypeReroute  ControlType = "reroute"
	ControlTypeScale    ControlType = "scale"
)

// BackpressureMetrics tracks backpressure-related metrics
type BackpressureMetrics struct {
	// Activation metrics
	activationCount   int64
	deactivationCount int64
	totalActiveTime   int64 // nanoseconds
	lastActivation    time.Time

	// Drop metrics (for drop strategy)
	recordsDropped int64
	batchesDropped int64

	// Throttle metrics (for throttle strategy)
	throttleEvents    int64
	totalThrottleTime int64 // nanoseconds
	maxThrottleDelay  int64 // nanoseconds

	// Adaptive metrics
	adjustmentCount    int64
	predictionAccuracy float64

	// Buffer metrics
	bufferUtilization []float64 // Rolling window
	bufferIndex       int

	mu sync.RWMutex
}

// DefaultBackpressureConfig returns a sensible default configuration
func DefaultBackpressureConfig() *BackpressureConfig {
	return &BackpressureConfig{
		ActivationThreshold:   0.8,  // Activate at 80% buffer utilization
		DeactivationThreshold: 0.6,  // Deactivate at 60% buffer utilization
		CriticalThreshold:     0.95, // Critical at 95% buffer utilization
		CheckInterval:         100 * time.Millisecond,
		CooldownPeriod:        5 * time.Second,
		Strategy:              BackpressureAdaptive,
		AdaptiveConfig: &AdaptiveConfig{
			LearningRate:      0.1,
			HistoryWindow:     100,
			ScaleUpFactor:     1.5,
			ScaleDownFactor:   0.8,
			UsePrediction:     true,
			PredictionHorizon: 30 * time.Second,
		},
		MaxDropRate:        0.1, // Max 10% drop rate
		MaxThrottleDelay:   5 * time.Second,
		EmergencyShutdown:  true,
		EmergencyThreshold: 0.99, // Emergency at 99% buffer utilization
	}
}

// NewBackpressureController creates a new backpressure controller
func NewBackpressureController(name string, config *BackpressureConfig, logger *zap.Logger) *BackpressureController {
	if config == nil {
		config = DefaultBackpressureConfig()
	}

	return &BackpressureController{
		name:        name,
		strategy:    config.Strategy,
		config:      config,
		logger:      logger.With(zap.String("component", "backpressure")),
		metrics:     newBackpressureMetrics(),
		controlChan: make(chan ControlSignal, 100),
		lastUpdate:  time.Now(),
	}
}

// newBackpressureMetrics creates a new metrics instance
func newBackpressureMetrics() *BackpressureMetrics {
	return &BackpressureMetrics{
		bufferUtilization: make([]float64, 100), // 100-sample rolling window
	}
}

// Start begins monitoring and controlling backpressure
func (bc *BackpressureController) Start(ctx context.Context) error {
	bc.logger.Info("starting backpressure controller",
		zap.String("strategy", string(bc.strategy)),
		zap.Float64("activation_threshold", bc.config.ActivationThreshold))

	// Start monitoring goroutine
	go bc.monitor(ctx)

	// Start control signal processor
	go bc.processControlSignals(ctx)

	return nil
}

// UpdateBufferMetrics updates buffer utilization metrics
func (bc *BackpressureController) UpdateBufferMetrics(utilization float64, bufferSize, maxSize int) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Update metrics
	bc.metrics.mu.Lock()
	bc.metrics.bufferUtilization[bc.metrics.bufferIndex] = utilization
	bc.metrics.bufferIndex = (bc.metrics.bufferIndex + 1) % len(bc.metrics.bufferUtilization)
	bc.metrics.mu.Unlock()

	// Check if we need to activate/deactivate backpressure
	wasActive := atomic.LoadInt32(&bc.isActive) == 1
	shouldActivate := utilization >= bc.config.ActivationThreshold
	shouldDeactivate := utilization <= bc.config.DeactivationThreshold

	// Update severity based on utilization
	severity := bc.calculateSeverity(utilization)
	atomic.StoreInt32(&bc.severity, severity)

	// Handle state transitions
	if !wasActive && shouldActivate {
		bc.activate(utilization, severity)
	} else if wasActive && shouldDeactivate {
		bc.deactivate(utilization)
	} else if wasActive {
		// Update existing backpressure if severity changed significantly
		bc.adjust(utilization, severity)
	}

	// Check for emergency conditions
	if bc.config.EmergencyShutdown && utilization >= bc.config.EmergencyThreshold {
		bc.triggerEmergency(utilization)
	}

	bc.lastUpdate = time.Now()
}

// Apply applies backpressure to a record based on current strategy
func (bc *BackpressureController) Apply(ctx context.Context, record *models.Record) (bool, error) {
	if atomic.LoadInt32(&bc.isActive) == 0 {
		return true, nil // No backpressure, allow record through
	}

	severity := atomic.LoadInt32(&bc.severity)

	switch bc.strategy {
	case BackpressureDrop:
		return bc.applyDropStrategy(severity, record)

	case BackpressureBlock:
		return bc.applyBlockStrategy(ctx, severity)

	case BackpressureThrottle:
		return bc.applyThrottleStrategy(ctx, severity)

	case BackpressureAdaptive:
		return bc.applyAdaptiveStrategy(ctx, severity, record)

	default:
		return true, nil
	}
}

// ApplyBatch applies backpressure to a batch of records
func (bc *BackpressureController) ApplyBatch(ctx context.Context, records []*models.Record) ([]*models.Record, error) {
	if atomic.LoadInt32(&bc.isActive) == 0 {
		return records, nil // No backpressure
	}

	severity := atomic.LoadInt32(&bc.severity)

	switch bc.strategy {
	case BackpressureDrop:
		return bc.applyBatchDropStrategy(severity, records)

	case BackpressureAdaptive:
		return bc.applyBatchAdaptiveStrategy(ctx, severity, records)

	default:
		// For block and throttle, apply to entire batch
		allowed, err := bc.Apply(ctx, nil)
		if err != nil || !allowed {
			return nil, err
		}
		return records, nil
	}
}

// GetControlChannel returns the control signal channel
func (bc *BackpressureController) GetControlChannel() <-chan ControlSignal {
	return bc.controlChan
}

// GetMetrics returns current backpressure metrics
func (bc *BackpressureController) GetMetrics() BackpressureMetrics {
	bc.metrics.mu.RLock()
	defer bc.metrics.mu.RUnlock()

	// Create a copy of metrics
	metrics := *bc.metrics

	// Copy buffer utilization slice
	metrics.bufferUtilization = make([]float64, len(bc.metrics.bufferUtilization))
	copy(metrics.bufferUtilization, bc.metrics.bufferUtilization)

	return metrics
}

// IsActive returns whether backpressure is currently active
func (bc *BackpressureController) IsActive() bool {
	return atomic.LoadInt32(&bc.isActive) == 1
}

// GetSeverity returns current backpressure severity (0-10)
func (bc *BackpressureController) GetSeverity() int {
	return int(atomic.LoadInt32(&bc.severity))
}

// Private methods

func (bc *BackpressureController) monitor(ctx context.Context) {
	ticker := time.NewTicker(bc.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			// Periodic health check and adjustment
			bc.performHealthCheck()
		}
	}
}

func (bc *BackpressureController) processControlSignals(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case signal := <-bc.controlChan:
			bc.handleControlSignal(signal)
		}
	}
}

func (bc *BackpressureController) calculateSeverity(utilization float64) int32 {
	// Map utilization to severity (0-10)
	if utilization >= bc.config.CriticalThreshold {
		return 10
	}

	// Linear mapping between activation threshold and critical threshold
	if utilization <= bc.config.ActivationThreshold {
		return 0
	}

	range_ := bc.config.CriticalThreshold - bc.config.ActivationThreshold
	normalized := (utilization - bc.config.ActivationThreshold) / range_

	return int32(normalized * 10)
}

func (bc *BackpressureController) activate(utilization float64, severity int32) {
	atomic.StoreInt32(&bc.isActive, 1)

	bc.metrics.mu.Lock()
	bc.metrics.activationCount++
	bc.metrics.lastActivation = time.Now()
	bc.metrics.mu.Unlock()

	bc.logger.Warn("backpressure activated",
		zap.Float64("utilization", utilization),
		zap.Int32("severity", severity))

	// Send control signal
	bc.controlChan <- ControlSignal{
		Type:      ControlTypePause,
		Severity:  int(severity),
		Action:    "activate",
		Timestamp: time.Now(),
		Source:    bc.name,
		Parameters: map[string]interface{}{
			"utilization": utilization,
			"strategy":    string(bc.strategy),
		},
	}
}

func (bc *BackpressureController) deactivate(utilization float64) {
	atomic.StoreInt32(&bc.isActive, 0)
	atomic.StoreInt32(&bc.severity, 0)

	bc.metrics.mu.Lock()
	bc.metrics.deactivationCount++
	if !bc.metrics.lastActivation.IsZero() {
		activeTime := time.Since(bc.metrics.lastActivation)
		atomic.AddInt64(&bc.metrics.totalActiveTime, activeTime.Nanoseconds())
	}
	bc.metrics.mu.Unlock()

	bc.logger.Info("backpressure deactivated",
		zap.Float64("utilization", utilization))

	// Send control signal
	bc.controlChan <- ControlSignal{
		Type:      ControlTypeResume,
		Severity:  0,
		Action:    "deactivate",
		Timestamp: time.Now(),
		Source:    bc.name,
		Parameters: map[string]interface{}{
			"utilization": utilization,
		},
	}
}

func (bc *BackpressureController) adjust(utilization float64, severity int32) {
	// Only adjust if severity changed by more than 2 levels
	oldSeverity := atomic.LoadInt32(&bc.severity)
	if math.Abs(float64(severity-oldSeverity)) < 2 {
		return
	}

	bc.logger.Debug("adjusting backpressure",
		zap.Float64("utilization", utilization),
		zap.Int32("old_severity", oldSeverity),
		zap.Int32("new_severity", severity))

	// Send control signal for adjustment
	bc.controlChan <- ControlSignal{
		Type:      ControlTypeScale,
		Severity:  int(severity),
		Action:    "adjust",
		Timestamp: time.Now(),
		Source:    bc.name,
		Parameters: map[string]interface{}{
			"utilization":  utilization,
			"old_severity": oldSeverity,
			"new_severity": severity,
		},
	}
}

func (bc *BackpressureController) triggerEmergency(utilization float64) {
	bc.logger.Error("emergency backpressure triggered",
		zap.Float64("utilization", utilization),
		zap.Float64("threshold", bc.config.EmergencyThreshold))

	// Send emergency control signal
	bc.controlChan <- ControlSignal{
		Type:      ControlTypePause,
		Severity:  10,
		Action:    "emergency",
		Timestamp: time.Now(),
		Source:    bc.name,
		Parameters: map[string]interface{}{
			"utilization": utilization,
			"emergency":   true,
		},
	}
}

// Strategy implementations

func (bc *BackpressureController) applyDropStrategy(severity int32, record *models.Record) (bool, error) {
	// Calculate drop probability based on severity
	dropProbability := float64(severity) / 10.0 * bc.config.MaxDropRate

	// Random drop based on probability
	if randomFloat() < dropProbability {
		atomic.AddInt64(&bc.metrics.recordsDropped, 1)
		return false, nil
	}

	return true, nil
}

func (bc *BackpressureController) applyBlockStrategy(ctx context.Context, severity int32) (bool, error) {
	// Block until backpressure is released or context is canceled
	for atomic.LoadInt32(&bc.isActive) == 1 {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(10 * time.Millisecond):
			// Check again
		}
	}

	return true, nil
}

func (bc *BackpressureController) applyThrottleStrategy(ctx context.Context, severity int32) (bool, error) {
	// Calculate delay based on severity
	maxDelay := bc.config.MaxThrottleDelay
	delay := time.Duration(float64(severity) / 10.0 * float64(maxDelay))

	if delay > 0 {
		atomic.AddInt64(&bc.metrics.throttleEvents, 1)
		atomic.AddInt64(&bc.metrics.totalThrottleTime, delay.Nanoseconds())

		if delay.Nanoseconds() > atomic.LoadInt64(&bc.metrics.maxThrottleDelay) {
			atomic.StoreInt64(&bc.metrics.maxThrottleDelay, delay.Nanoseconds())
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(delay):
			return true, nil
		}
	}

	return true, nil
}

func (bc *BackpressureController) applyAdaptiveStrategy(ctx context.Context, severity int32, record *models.Record) (bool, error) {
	// Adaptive strategy combines multiple approaches based on conditions

	// Low severity: throttle
	if severity <= 3 {
		return bc.applyThrottleStrategy(ctx, severity)
	}

	// Medium severity: selective drop
	if severity <= 7 {
		// Drop low-priority records
		if record != nil && record.Metadata.Custom != nil {
			if priority, ok := record.Metadata.Custom["priority"].(int); ok && priority < 5 {
				return bc.applyDropStrategy(severity, record)
			}
		}
		return bc.applyThrottleStrategy(ctx, severity-2)
	}

	// High severity: aggressive drop + throttle
	dropped, _ := bc.applyDropStrategy(severity, record)
	if !dropped {
		return false, nil
	}

	// Still throttle even if not dropped
	return bc.applyThrottleStrategy(ctx, severity/2)
}

func (bc *BackpressureController) applyBatchDropStrategy(severity int32, records []*models.Record) ([]*models.Record, error) {
	dropProbability := float64(severity) / 10.0 * bc.config.MaxDropRate

	kept := pool.GetBatchSlice(len(records))
	defer pool.PutBatchSlice(kept)
	dropped := 0

	for _, record := range records {
		if randomFloat() >= dropProbability {
			kept = append(kept, record)
		} else {
			dropped++
		}
	}

	if dropped > 0 {
		atomic.AddInt64(&bc.metrics.recordsDropped, int64(dropped))
		atomic.AddInt64(&bc.metrics.batchesDropped, 1)
	}

	return kept, nil
}

func (bc *BackpressureController) applyBatchAdaptiveStrategy(ctx context.Context, severity int32, records []*models.Record) ([]*models.Record, error) {
	// For batches, we can be smarter about what to keep/drop

	if severity <= 3 {
		// Low severity: keep all, just throttle
		if _, err := bc.applyThrottleStrategy(ctx, severity); err != nil {
			return nil, err
		}
		return records, nil
	}

	// Medium to high severity: prioritize records
	priority := make([]struct {
		record   *models.Record
		priority float64
	}, len(records))

	for i, record := range records {
		priority[i].record = record
		priority[i].priority = bc.calculateRecordPriority(record)
	}

	// Sort by priority (higher priority first)
	// For simplicity, we'll just keep the top percentage based on severity
	keepRatio := 1.0 - (float64(severity) / 10.0 * bc.config.MaxDropRate)
	keepCount := int(float64(len(records)) * keepRatio)

	if keepCount < len(records) {
		atomic.AddInt64(&bc.metrics.recordsDropped, int64(len(records)-keepCount))
		atomic.AddInt64(&bc.metrics.batchesDropped, 1)
	}

	// Return the kept records
	kept := pool.GetBatchSlice(keepCount)

	defer pool.PutBatchSlice(kept)
	for i := 0; i < keepCount && i < len(priority); i++ {
		kept = append(kept, priority[i].record)
	}

	return kept, nil
}

func (bc *BackpressureController) calculateRecordPriority(record *models.Record) float64 {
	if record == nil || record.Metadata.Custom == nil {
		return 0.5 // Default priority
	}

	// Check explicit priority
	if priority, ok := record.Metadata.Custom["priority"].(float64); ok {
		return priority / 10.0 // Normalize to 0-1
	}

	// Check timestamp freshness
	recordTime := record.GetTimestamp()
	if recordTime.After(time.Now().Add(-1 * time.Minute)) {
		return 0.8 // Recent records have higher priority
	}

	// Check size (smaller records have slightly higher priority during backpressure)
	if size, ok := record.Metadata.Custom["size"].(int); ok && size < 1000 {
		return 0.6
	}

	return 0.5
}

func (bc *BackpressureController) performHealthCheck() {
	// Analyze buffer utilization trends
	bc.metrics.mu.RLock()
	utilization := bc.calculateAverageUtilization()
	trend := bc.calculateUtilizationTrend()
	bc.metrics.mu.RUnlock()

	// Predict future utilization if enabled
	if bc.config.AdaptiveConfig != nil && bc.config.AdaptiveConfig.UsePrediction {
		predictedUtilization := bc.predictUtilization(utilization, trend)

		// Proactive backpressure based on prediction
		if predictedUtilization >= bc.config.ActivationThreshold {
			bc.logger.Debug("predicted high utilization",
				zap.Float64("current", utilization),
				zap.Float64("predicted", predictedUtilization),
				zap.Float64("trend", trend))
		}
	}
}

func (bc *BackpressureController) calculateAverageUtilization() float64 {
	sum := 0.0
	count := 0

	for _, u := range bc.metrics.bufferUtilization {
		if u > 0 {
			sum += u
			count++
		}
	}

	if count == 0 {
		return 0
	}

	return sum / float64(count)
}

func (bc *BackpressureController) calculateUtilizationTrend() float64 {
	// Simple linear regression to determine trend
	n := len(bc.metrics.bufferUtilization)
	if n < 2 {
		return 0
	}

	// Calculate slope of utilization over time
	sumX, sumY, sumXY, sumX2 := 0.0, 0.0, 0.0, 0.0
	validPoints := 0

	for i, y := range bc.metrics.bufferUtilization {
		if y > 0 {
			x := float64(i)
			sumX += x
			sumY += y
			sumXY += x * y
			sumX2 += x * x
			validPoints++
		}
	}

	if validPoints < 2 {
		return 0
	}

	np := float64(validPoints)
	slope := (np*sumXY - sumX*sumY) / (np*sumX2 - sumX*sumX)

	return slope
}

func (bc *BackpressureController) predictUtilization(current, trend float64) float64 {
	if bc.config.AdaptiveConfig == nil {
		return current
	}

	// Simple linear prediction
	horizonSeconds := bc.config.AdaptiveConfig.PredictionHorizon.Seconds()
	checkIntervalSeconds := bc.config.CheckInterval.Seconds()

	steps := horizonSeconds / checkIntervalSeconds
	predicted := current + (trend * steps)

	// Clamp to valid range
	if predicted < 0 {
		predicted = 0
	} else if predicted > 1 {
		predicted = 1
	}

	return predicted
}

func (bc *BackpressureController) handleControlSignal(signal ControlSignal) {
	bc.logger.Debug("handling control signal",
		zap.String("type", string(signal.Type)),
		zap.String("action", signal.Action),
		zap.Int("severity", signal.Severity))

	// Process based on signal type
	switch signal.Type {
	case ControlTypeScale:
		// Adjust internal parameters based on scaling signal
		if bc.config.AdaptiveConfig != nil {
			bc.adjustAdaptiveParameters(signal)
		}

	case ControlTypePause:
		// Force activation if not already active
		if atomic.LoadInt32(&bc.isActive) == 0 {
			bc.activate(0.9, int32(signal.Severity))
		}

	case ControlTypeResume:
		// Force deactivation
		if atomic.LoadInt32(&bc.isActive) == 1 {
			bc.deactivate(0.5)
		}
	}
}

func (bc *BackpressureController) adjustAdaptiveParameters(signal ControlSignal) {
	// Adjust learning rate or other parameters based on performance
	if bc.config.AdaptiveConfig == nil {
		return
	}

	// This is where we could implement more sophisticated adaptive behavior
	// For now, we'll just track the adjustment
	atomic.AddInt64(&bc.metrics.adjustmentCount, 1)
}

// randomFloat returns a random float between 0 and 1
func randomFloat() float64 {
	return float64(time.Now().UnixNano()%1000) / 1000.0
}
