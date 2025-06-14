package base

import (
	"math"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/metrics"
)

// PerformanceOptimizer provides performance optimization recommendations
type PerformanceOptimizer struct {
	metricsCollector *metrics.Collector
	history          *performanceHistory
	mu               sync.RWMutex
}

// performanceHistory tracks historical performance data
type performanceHistory struct {
	batchSizes     []int
	throughputs    []float64
	latencies      []time.Duration
	errorRates     []float64
	memoryUsages   []int64
	cpuUsages      []float64
	timestamps     []time.Time
	maxHistorySize int
}

// NewPerformanceOptimizer creates a new performance optimizer
func NewPerformanceOptimizer(collector *metrics.Collector) *PerformanceOptimizer {
	return &PerformanceOptimizer{
		metricsCollector: collector,
		history: &performanceHistory{
			maxHistorySize: 100,
			batchSizes:     make([]int, 0, 100),
			throughputs:    make([]float64, 0, 100),
			latencies:      make([]time.Duration, 0, 100),
			errorRates:     make([]float64, 0, 100),
			memoryUsages:   make([]int64, 0, 100),
			cpuUsages:      make([]float64, 0, 100),
			timestamps:     make([]time.Time, 0, 100),
		},
	}
}

// OptimizeBatchSize recommends an optimal batch size
func (po *PerformanceOptimizer) OptimizeBatchSize(current int, metrics map[string]interface{}) int {
	po.mu.Lock()
	defer po.mu.Unlock()

	// Record current metrics
	po.recordMetrics(current, metrics)

	// Not enough history to optimize
	if len(po.history.batchSizes) < 5 {
		return current
	}

	// Find the batch size with best throughput/latency ratio
	bestSize := current
	bestScore := 0.0

	// Group by batch size and calculate average performance
	sizePerformance := make(map[int][]float64)

	for i := range po.history.batchSizes {
		size := po.history.batchSizes[i]
		throughput := po.history.throughputs[i]
		latency := float64(po.history.latencies[i].Milliseconds())
		errorRate := po.history.errorRates[i]

		// Calculate performance score (higher is better)
		// Prioritize throughput, penalize high latency and errors
		score := throughput / (1 + latency/1000) * (1 - errorRate)

		sizePerformance[size] = append(sizePerformance[size], score)
	}

	// Find best performing batch size
	for size, scores := range sizePerformance {
		avgScore := average(scores)
		if avgScore > bestScore {
			bestScore = avgScore
			bestSize = size
		}
	}

	// Apply gradual adjustment (don't change too drastically)
	if bestSize != current {
		diff := bestSize - current
		adjustment := int(float64(diff) * 0.3) // 30% adjustment
		if adjustment == 0 && diff != 0 {
			if diff > 0 {
				adjustment = 1
			} else {
				adjustment = -1
			}
		}
		bestSize = current + adjustment
	}

	// Apply bounds
	if bestSize < 100 {
		bestSize = 100
	} else if bestSize > 50000 {
		bestSize = 50000
	}

	return bestSize
}

// OptimizeConcurrency recommends an optimal concurrency level
func (po *PerformanceOptimizer) OptimizeConcurrency(current int, metrics map[string]interface{}) int {
	po.mu.RLock()
	defer po.mu.RUnlock()

	// Extract relevant metrics
	cpuUsage := getFloat64(metrics, "cpu_usage", 0.5)
	errorRate := getFloat64(metrics, "error_rate", 0)
	queueDepth := getInt(metrics, "queue_depth", 0)

	optimal := current

	// If CPU usage is low and queue is building up, increase concurrency
	if cpuUsage < 0.6 && queueDepth > current*100 {
		optimal = int(float64(current) * 1.2)
	} else if cpuUsage > 0.9 || errorRate > 0.05 {
		// If CPU is high or errors are increasing, decrease concurrency
		optimal = int(float64(current) * 0.8)
	}

	// Apply bounds
	if optimal < 1 {
		optimal = 1
	} else if optimal > 100 {
		optimal = 100
	}

	return optimal
}

// OptimizeBufferSize recommends an optimal buffer size
func (po *PerformanceOptimizer) OptimizeBufferSize(current int, metrics map[string]interface{}) int {
	po.mu.RLock()
	defer po.mu.RUnlock()

	// Extract relevant metrics
	memoryUsage := getInt64(metrics, "memory_usage", 0)
	bufferUtilization := getFloat64(metrics, "buffer_utilization", 0.5)

	optimal := current

	// If buffer is frequently full, increase size
	if bufferUtilization > 0.8 && memoryUsage < 1<<30 { // Less than 1GB
		optimal = int(float64(current) * 1.5)
	} else if bufferUtilization < 0.2 && current > 1000 {
		// If buffer is rarely used, decrease size
		optimal = int(float64(current) * 0.7)
	}

	// Apply bounds based on available memory
	maxBuffer := int(memoryUsage / 1000) // Rough estimate
	if maxBuffer < 1000 {
		maxBuffer = 1000
	}

	if optimal < 100 {
		optimal = 100
	} else if optimal > maxBuffer {
		optimal = maxBuffer
	}

	return optimal
}

// SuggestOptimizations provides optimization suggestions
func (po *PerformanceOptimizer) SuggestOptimizations(metrics map[string]interface{}) []string {
	suggestions := []string{}

	// Analyze metrics
	cpuUsage := getFloat64(metrics, "cpu_usage", 0.5)
	memoryUsage := getInt64(metrics, "memory_usage", 0)
	errorRate := getFloat64(metrics, "error_rate", 0)
	throughput := getFloat64(metrics, "throughput", 0)
	latencyP99 := getFloat64(metrics, "latency_p99_ms", 0)
	gcPauseTime := getFloat64(metrics, "gc_pause_ms", 0)

	// CPU optimization suggestions
	if cpuUsage > 0.9 {
		suggestions = append(suggestions, "High CPU usage detected. Consider reducing concurrency or batch size.")
	} else if cpuUsage < 0.3 {
		suggestions = append(suggestions, "Low CPU usage. Consider increasing concurrency to improve throughput.")
	}

	// Memory optimization suggestions
	if memoryUsage > 2<<30 { // > 2GB
		suggestions = append(suggestions, "High memory usage. Consider reducing buffer sizes or implementing memory pooling.")
	}

	if gcPauseTime > 100 {
		suggestions = append(suggestions, "High GC pause time. Consider optimizing memory allocations.")
	}

	// Error rate suggestions
	if errorRate > 0.05 {
		suggestions = append(suggestions, "High error rate detected. Check logs and consider reducing load.")
	}

	// Latency suggestions
	if latencyP99 > 1000 {
		suggestions = append(suggestions, "High latency detected. Consider optimizing batch processing or reducing batch size.")
	}

	// Throughput suggestions
	po.mu.RLock()
	avgThroughput := average(po.history.throughputs)
	po.mu.RUnlock()

	if len(po.history.throughputs) > 10 && throughput < avgThroughput*0.8 {
		suggestions = append(suggestions, "Throughput degradation detected. Review recent changes and system resources.")
	}

	return suggestions
}

// recordMetrics records current performance metrics
func (po *PerformanceOptimizer) recordMetrics(batchSize int, metrics map[string]interface{}) {
	// Extract metrics
	throughput := getFloat64(metrics, "throughput", 0)
	latency := time.Duration(getFloat64(metrics, "latency_ms", 0)) * time.Millisecond
	errorRate := getFloat64(metrics, "error_rate", 0)
	memoryUsage := getInt64(metrics, "memory_usage", 0)
	cpuUsage := getFloat64(metrics, "cpu_usage", 0)

	// Add to history
	po.history.batchSizes = append(po.history.batchSizes, batchSize)
	po.history.throughputs = append(po.history.throughputs, throughput)
	po.history.latencies = append(po.history.latencies, latency)
	po.history.errorRates = append(po.history.errorRates, errorRate)
	po.history.memoryUsages = append(po.history.memoryUsages, memoryUsage)
	po.history.cpuUsages = append(po.history.cpuUsages, cpuUsage)
	po.history.timestamps = append(po.history.timestamps, time.Now())

	// Trim history if needed
	if len(po.history.batchSizes) > po.history.maxHistorySize {
		trim := len(po.history.batchSizes) - po.history.maxHistorySize
		po.history.batchSizes = po.history.batchSizes[trim:]
		po.history.throughputs = po.history.throughputs[trim:]
		po.history.latencies = po.history.latencies[trim:]
		po.history.errorRates = po.history.errorRates[trim:]
		po.history.memoryUsages = po.history.memoryUsages[trim:]
		po.history.cpuUsages = po.history.cpuUsages[trim:]
		po.history.timestamps = po.history.timestamps[trim:]
	}
}

// GetPerformanceTrend returns the performance trend
func (po *PerformanceOptimizer) GetPerformanceTrend() string {
	po.mu.RLock()
	defer po.mu.RUnlock()

	if len(po.history.throughputs) < 10 {
		return "insufficient_data"
	}

	// Compare recent performance to historical average
	recentStart := len(po.history.throughputs) - 5
	recentThroughput := average(po.history.throughputs[recentStart:])
	historicalThroughput := average(po.history.throughputs[:recentStart])

	ratio := recentThroughput / historicalThroughput

	switch {
	case ratio > 1.1:
		return "improving"
	case ratio < 0.9:
		return "degrading"
	default:
		return "stable"
	}
}

// Helper functions

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func getFloat64(m map[string]interface{}, key string, defaultValue float64) float64 {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case float64:
			return val
		case float32:
			return float64(val)
		case int:
			return float64(val)
		case int64:
			return float64(val)
		}
	}
	return defaultValue
}

func getInt(m map[string]interface{}, key string, defaultValue int) int {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case int:
			return val
		case int64:
			return int(val)
		case float64:
			return int(val)
		}
	}
	return defaultValue
}

func getInt64(m map[string]interface{}, key string, defaultValue int64) int64 {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case int64:
			return val
		case int:
			return int64(val)
		case float64:
			return int64(val)
		}
	}
	return defaultValue
}

// PerformanceReport generates a performance report
type PerformanceReport struct {
	Timestamp         time.Time
	AverageThroughput float64
	AverageLatency    time.Duration
	P99Latency        time.Duration
	ErrorRate         float64
	TotalProcessed    int64
	TotalErrors       int64
	Trend             string
	Recommendations   []string
}

// GenerateReport creates a performance report
func (po *PerformanceOptimizer) GenerateReport(metrics map[string]interface{}) *PerformanceReport {
	po.mu.RLock()
	defer po.mu.RUnlock()

	report := &PerformanceReport{
		Timestamp:       time.Now(),
		Trend:           po.GetPerformanceTrend(),
		Recommendations: po.SuggestOptimizations(metrics),
	}

	// Calculate averages from history
	if len(po.history.throughputs) > 0 {
		report.AverageThroughput = average(po.history.throughputs)

		// Calculate average latency
		totalLatency := time.Duration(0)
		for _, lat := range po.history.latencies {
			totalLatency += lat
		}
		report.AverageLatency = totalLatency / time.Duration(len(po.history.latencies))

		// Calculate P99 latency
		latencies := make([]time.Duration, len(po.history.latencies))
		copy(latencies, po.history.latencies)
		p99Index := int(math.Ceil(float64(len(latencies))*0.99)) - 1
		if p99Index >= 0 && p99Index < len(latencies) {
			report.P99Latency = latencies[p99Index]
		}

		// Calculate error rate
		report.ErrorRate = average(po.history.errorRates)
	}

	// Extract totals from metrics
	report.TotalProcessed = getInt64(metrics, "total_processed", 0)
	report.TotalErrors = getInt64(metrics, "total_errors", 0)

	return report
}
