// Package pipeline implements adaptive buffer management
package pipeline

import (
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// AdaptiveBuffer manages dynamic buffer sizing based on throughput and utilization
type AdaptiveBuffer struct {
	config *StreamingConfig
	logger *zap.Logger

	// Buffer state
	currentSize    int64
	targetSize     int64
	utilization    float64
	maxUtilization float64
	metrics        *BufferMetrics

	// Control parameters
	growthFactor   float64
	shrinkFactor   float64
	lastResize     time.Time
	resizeCooldown time.Duration

	// Monitoring
	samples      []float64
	sampleIndex  int
	sampleWindow time.Duration

	mu sync.RWMutex
}

// NewAdaptiveBuffer creates a new adaptive buffer manager
func NewAdaptiveBuffer(config *StreamingConfig, logger *zap.Logger) *AdaptiveBuffer {
	return &AdaptiveBuffer{
		config:         config,
		logger:         logger.With(zap.String("component", "adaptive_buffer")),
		currentSize:    int64(config.InitialBufferSize),
		targetSize:     int64(config.InitialBufferSize),
		growthFactor:   config.BufferGrowthFactor,
		shrinkFactor:   0.8,
		resizeCooldown: config.BufferShrinkDelay,
		samples:        make([]float64, 60), // 60 samples for rolling average
		sampleWindow:   time.Second,
		metrics: &BufferMetrics{
			CurrentSize:  config.InitialBufferSize,
			MaxSize:      config.MaxBufferSize,
			LastResize:   time.Now(),
			ResizeReason: "initial",
		},
	}
}

// UpdateUtilization updates buffer utilization and triggers resize if needed
func (ab *AdaptiveBuffer) UpdateUtilization(utilization float64) {
	ab.mu.Lock()
	defer ab.mu.Unlock()

	ab.utilization = utilization

	// Update rolling average
	ab.samples[ab.sampleIndex] = utilization
	ab.sampleIndex = (ab.sampleIndex + 1) % len(ab.samples)

	// Calculate average utilization
	var sum float64
	validSamples := 0
	for _, sample := range ab.samples {
		if sample > 0 {
			sum += sample
			validSamples++
		}
	}

	if validSamples > 0 {
		avgUtilization := sum / float64(validSamples)
		ab.checkResize(avgUtilization)
	}

	// Update metrics
	ab.metrics.Utilization = utilization
	if utilization > ab.maxUtilization {
		ab.maxUtilization = utilization
	}
}

// checkResize determines if buffer should be resized
func (ab *AdaptiveBuffer) checkResize(avgUtilization float64) {
	now := time.Now()

	// Check cooldown period
	if now.Sub(ab.lastResize) < ab.resizeCooldown {
		return
	}

	currentSize := atomic.LoadInt64(&ab.currentSize)
	var newSize int64
	var reason string

	// Determine resize action based on utilization
	switch {
	case avgUtilization > 0.8 && currentSize < int64(ab.config.MaxBufferSize):
		// Growth conditions
		newSize = int64(float64(currentSize) * ab.growthFactor)
		if newSize > int64(ab.config.MaxBufferSize) {
			newSize = int64(ab.config.MaxBufferSize)
		}
		reason = "high_utilization"
		ab.metrics.GrowthEvents++
	case avgUtilization < 0.4 && currentSize > int64(ab.config.InitialBufferSize):
		// Shrink conditions
		newSize = int64(float64(currentSize) * ab.shrinkFactor)
		if newSize < int64(ab.config.InitialBufferSize) {
			newSize = int64(ab.config.InitialBufferSize)
		}
		reason = "low_utilization"
		ab.metrics.ShrinkEvents++
	default:
		// No resize needed
		return
	}

	// Apply resize
	if newSize != currentSize {
		ab.resize(newSize, reason)
	}
}

// resize changes the buffer size
func (ab *AdaptiveBuffer) resize(newSize int64, reason string) {
	oldSize := atomic.SwapInt64(&ab.currentSize, newSize)
	atomic.StoreInt64(&ab.targetSize, newSize)

	ab.lastResize = time.Now()
	ab.metrics.CurrentSize = int(newSize)
	ab.metrics.LastResize = ab.lastResize
	ab.metrics.ResizeReason = reason
	ab.metrics.TargetSize = int(newSize)

	ab.logger.Info("buffer resized",
		zap.Int64("old_size", oldSize),
		zap.Int64("new_size", newSize),
		zap.String("reason", reason),
		zap.Float64("utilization", ab.utilization))
}

// GetCurrentSize returns the current buffer size
func (ab *AdaptiveBuffer) GetCurrentSize() int {
	return int(atomic.LoadInt64(&ab.currentSize))
}

// GetTargetSize returns the target buffer size
func (ab *AdaptiveBuffer) GetTargetSize() int {
	return int(atomic.LoadInt64(&ab.targetSize))
}

// GetUtilization returns current utilization
func (ab *AdaptiveBuffer) GetUtilization() float64 {
	ab.mu.RLock()
	defer ab.mu.RUnlock()
	return ab.utilization
}

// GetMetrics returns buffer metrics
func (ab *AdaptiveBuffer) GetMetrics() *BufferMetrics {
	ab.mu.RLock()
	defer ab.mu.RUnlock()

	// Create a copy to avoid race conditions
	metrics := *ab.metrics
	metrics.CurrentSize = int(atomic.LoadInt64(&ab.currentSize))
	metrics.TargetSize = int(atomic.LoadInt64(&ab.targetSize))
	metrics.Utilization = ab.utilization

	return &metrics
}

// RecordOverflow records a buffer overflow event
func (ab *AdaptiveBuffer) RecordOverflow() {
	ab.mu.Lock()
	defer ab.mu.Unlock()

	ab.metrics.OverflowEvents++

	ab.logger.Warn("buffer overflow detected",
		zap.Int("current_size", int(atomic.LoadInt64(&ab.currentSize))),
		zap.Float64("utilization", ab.utilization))
}

// ShouldGrow returns true if buffer should grow
func (ab *AdaptiveBuffer) ShouldGrow() bool {
	ab.mu.RLock()
	defer ab.mu.RUnlock()

	currentSize := atomic.LoadInt64(&ab.currentSize)
	return ab.utilization > 0.8 &&
		currentSize < int64(ab.config.MaxBufferSize) &&
		time.Since(ab.lastResize) > ab.resizeCooldown
}

// ShouldShrink returns true if buffer should shrink
func (ab *AdaptiveBuffer) ShouldShrink() bool {
	ab.mu.RLock()
	defer ab.mu.RUnlock()

	currentSize := atomic.LoadInt64(&ab.currentSize)
	return ab.utilization < 0.4 &&
		currentSize > int64(ab.config.InitialBufferSize) &&
		time.Since(ab.lastResize) > ab.resizeCooldown
}
