package base

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/metrics"
	"go.uber.org/zap"
)

// ProgressReporter tracks and reports progress of operations
type ProgressReporter struct {
	logger           *zap.Logger
	metricsCollector *metrics.Collector

	// Progress tracking
	totalRecords     int64
	processedRecords int64
	startTime        time.Time
	lastReportTime   time.Time
	reportInterval   time.Duration

	// Performance metrics
	throughputHistory []float64
	latencyHistory    []time.Duration
	historyMutex      sync.RWMutex

	// Reporting control
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewProgressReporter creates a new progress reporter
func NewProgressReporter(logger *zap.Logger, collector *metrics.Collector) *ProgressReporter {
	return &ProgressReporter{
		logger:            logger,
		metricsCollector:  collector,
		startTime:         time.Now(),
		lastReportTime:    time.Now(),
		reportInterval:    10 * time.Second,
		stopCh:            make(chan struct{}),
		throughputHistory: make([]float64, 0, 100),
		latencyHistory:    make([]time.Duration, 0, 100),
	}
}

// Start begins periodic progress reporting
func (pr *ProgressReporter) Start() {
	pr.wg.Add(1)
	go func() {
		defer pr.wg.Done()
		ticker := time.NewTicker(pr.reportInterval)
		defer ticker.Stop()

		for {
			select {
			case <-pr.stopCh:
				return
			case <-ticker.C:
				pr.reportCurrentProgress()
			}
		}
	}()
}

// Stop stops progress reporting
func (pr *ProgressReporter) Stop() {
	close(pr.stopCh)
	pr.wg.Wait()

	// Final report
	pr.reportFinalProgress()
}

// SetTotal sets the total number of records to process
func (pr *ProgressReporter) SetTotal(total int64) {
	atomic.StoreInt64(&pr.totalRecords, total)
}

// ReportProgress updates the progress
func (pr *ProgressReporter) ReportProgress(processed, total int64) {
	atomic.StoreInt64(&pr.processedRecords, processed)
	if total > 0 {
		atomic.StoreInt64(&pr.totalRecords, total)
	}
}

// IncrementProcessed increments the processed count
func (pr *ProgressReporter) IncrementProcessed(count int64) {
	atomic.AddInt64(&pr.processedRecords, count)
}

// ReportThroughput records throughput
func (pr *ProgressReporter) ReportThroughput(recordsPerSecond float64) {
	pr.historyMutex.Lock()
	defer pr.historyMutex.Unlock()

	pr.throughputHistory = append(pr.throughputHistory, recordsPerSecond)

	// Keep last 100 entries
	if len(pr.throughputHistory) > 100 {
		pr.throughputHistory = pr.throughputHistory[1:]
	}

	// Update metrics
	pr.metricsCollector.RecordGauge("throughput", recordsPerSecond)
}

// ReportLatency records processing latency
func (pr *ProgressReporter) ReportLatency(latency time.Duration) {
	pr.historyMutex.Lock()
	defer pr.historyMutex.Unlock()

	pr.latencyHistory = append(pr.latencyHistory, latency)

	// Keep last 100 entries
	if len(pr.latencyHistory) > 100 {
		pr.latencyHistory = pr.latencyHistory[1:]
	}

	// Update metrics
	pr.metricsCollector.RecordHistogram("processing_latency_ms", float64(latency.Milliseconds()))
}

// GetProgress returns current progress
func (pr *ProgressReporter) GetProgress() (processed, total int64) {
	return atomic.LoadInt64(&pr.processedRecords), atomic.LoadInt64(&pr.totalRecords)
}

// GetElapsedTime returns time since start
func (pr *ProgressReporter) GetElapsedTime() time.Duration {
	return time.Since(pr.startTime)
}

// GetETA estimates time remaining
func (pr *ProgressReporter) GetETA() time.Duration {
	processed := atomic.LoadInt64(&pr.processedRecords)
	total := atomic.LoadInt64(&pr.totalRecords)

	if processed == 0 || total == 0 || processed >= total {
		return 0
	}

	elapsed := time.Since(pr.startTime)
	rate := float64(processed) / elapsed.Seconds()

	if rate == 0 {
		return 0
	}

	remaining := total - processed
	return time.Duration(float64(remaining)/rate) * time.Second
}

// GetAverageThroughput returns average throughput
func (pr *ProgressReporter) GetAverageThroughput() float64 {
	pr.historyMutex.RLock()
	defer pr.historyMutex.RUnlock()

	if len(pr.throughputHistory) == 0 {
		// Calculate from total progress
		processed := atomic.LoadInt64(&pr.processedRecords)
		elapsed := time.Since(pr.startTime).Seconds()
		if elapsed > 0 {
			return float64(processed) / elapsed
		}
		return 0
	}

	sum := 0.0
	for _, t := range pr.throughputHistory {
		sum += t
	}
	return sum / float64(len(pr.throughputHistory))
}

// GetAverageLatency returns average latency
func (pr *ProgressReporter) GetAverageLatency() time.Duration {
	pr.historyMutex.RLock()
	defer pr.historyMutex.RUnlock()

	if len(pr.latencyHistory) == 0 {
		return 0
	}

	total := time.Duration(0)
	for _, l := range pr.latencyHistory {
		total += l
	}
	return total / time.Duration(len(pr.latencyHistory))
}

// reportCurrentProgress logs current progress
func (pr *ProgressReporter) reportCurrentProgress() {
	processed := atomic.LoadInt64(&pr.processedRecords)
	total := atomic.LoadInt64(&pr.totalRecords)

	// Calculate metrics
	elapsed := time.Since(pr.startTime)
	intervalElapsed := time.Since(pr.lastReportTime)
	throughput := pr.GetAverageThroughput()
	eta := pr.GetETA()

	// Determine progress percentage
	var percentage float64
	if total > 0 {
		percentage = float64(processed) / float64(total) * 100
	}

	// Log progress
	fields := []zap.Field{
		zap.Int64("processed", processed),
		zap.Float64("throughput", throughput),
		zap.Duration("elapsed", elapsed),
		zap.Duration("interval", intervalElapsed),
	}

	if total > 0 {
		fields = append(fields,
			zap.Int64("total", total),
			zap.Float64("percentage", percentage),
			zap.Duration("eta", eta),
		)
	}

	pr.logger.Info("progress update", fields...)

	// Update metrics
	pr.metricsCollector.RecordGauge("progress_percentage", percentage)
	pr.metricsCollector.RecordGauge("eta_seconds", eta.Seconds())
	pr.metricsCollector.RecordCounter("records_processed", float64(processed))

	pr.lastReportTime = time.Now()
}

// reportFinalProgress logs final progress summary
func (pr *ProgressReporter) reportFinalProgress() {
	processed := atomic.LoadInt64(&pr.processedRecords)
	total := atomic.LoadInt64(&pr.totalRecords)
	elapsed := time.Since(pr.startTime)

	avgThroughput := pr.GetAverageThroughput()
	avgLatency := pr.GetAverageLatency()

	fields := []zap.Field{
		zap.Int64("total_processed", processed),
		zap.Duration("total_time", elapsed),
		zap.Float64("avg_throughput", avgThroughput),
		zap.Duration("avg_latency", avgLatency),
	}

	if total > 0 {
		fields = append(fields,
			zap.Int64("expected_total", total),
			zap.Float64("completion_percentage", float64(processed)/float64(total)*100),
		)
	}

	pr.logger.Info("processing completed", fields...)

	// Final metrics
	pr.metricsCollector.RecordGauge("final_throughput", avgThroughput)
	pr.metricsCollector.RecordHistogram("final_latency_ms", float64(avgLatency.Milliseconds()))
	pr.metricsCollector.RecordCounter("total_processed", float64(processed))
	pr.metricsCollector.RecordHistogram("total_duration_seconds", elapsed.Seconds())
}

// ProgressSnapshot represents a point-in-time progress snapshot
type ProgressSnapshot struct {
	Timestamp        time.Time
	ProcessedRecords int64
	TotalRecords     int64
	Percentage       float64
	Throughput       float64
	AverageLatency   time.Duration
	ElapsedTime      time.Duration
	ETA              time.Duration
}

// GetSnapshot returns a progress snapshot
func (pr *ProgressReporter) GetSnapshot() *ProgressSnapshot {
	processed, total := pr.GetProgress()

	snapshot := &ProgressSnapshot{
		Timestamp:        time.Now(),
		ProcessedRecords: processed,
		TotalRecords:     total,
		Throughput:       pr.GetAverageThroughput(),
		AverageLatency:   pr.GetAverageLatency(),
		ElapsedTime:      pr.GetElapsedTime(),
		ETA:              pr.GetETA(),
	}

	if total > 0 {
		snapshot.Percentage = float64(processed) / float64(total) * 100
	}

	return snapshot
}

// SetReportInterval sets the progress reporting interval
func (pr *ProgressReporter) SetReportInterval(interval time.Duration) {
	pr.reportInterval = interval
}
