// Package performance provides profiling and optimization tools for Nebula
package performance

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/pool"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"
)

// Profiler provides performance profiling capabilities
type Profiler struct {
	name            string
	startTime       time.Time
	cpuProfile      *pprof.Profile
	memStats        runtime.MemStats
	metrics         *Metrics
	sampling        bool
	samplingStop    chan struct{}
	resourceMonitor *ResourceMonitor
	mu              sync.RWMutex
}

// Metrics tracks performance metrics
type Metrics struct {
	// Throughput metrics
	RecordsProcessed int64
	BytesProcessed   int64
	RecordsPerSecond float64
	BytesPerSecond   float64

	// Latency metrics
	MinLatency time.Duration
	MaxLatency time.Duration
	AvgLatency time.Duration
	P50Latency time.Duration
	P95Latency time.Duration
	P99Latency time.Duration

	// Resource metrics
	CPUUsagePercent float64
	MemoryUsageMB   uint64
	GoroutineCount  int
	GCCount         uint32
	GCPauseTotalNs  uint64

	// Error metrics
	ErrorCount int64
	RetryCount int64

	// Custom metrics
	CustomMetrics map[string]interface{}
}

// ProfilerConfig configures the profiler
type ProfilerConfig struct {
	Name             string
	EnableCPUProfile bool
	EnableMemProfile bool
	EnableTrace      bool
	SamplingInterval time.Duration
	ResourceMonitor  bool
}

// DefaultProfilerConfig returns default configuration
func DefaultProfilerConfig(name string) *ProfilerConfig {
	return &ProfilerConfig{
		Name:             name,
		EnableCPUProfile: true,
		EnableMemProfile: true,
		EnableTrace:      false,
		SamplingInterval: 100 * time.Millisecond,
		ResourceMonitor:  true,
	}
}

// NewProfiler creates a new profiler
func NewProfiler(config *ProfilerConfig) *Profiler {
	if config == nil {
		config = DefaultProfilerConfig("default")
	}

	p := &Profiler{
		name:      config.Name,
		startTime: time.Now(),
		metrics: &Metrics{
			MinLatency:    time.Duration(1<<63 - 1),
			CustomMetrics: make(map[string]interface{}),
		},
	}

	if config.EnableCPUProfile {
		p.cpuProfile = pprof.NewProfile(config.Name + "_cpu")
	}

	if config.ResourceMonitor {
		p.resourceMonitor = NewResourceMonitor()
		p.startSampling(config.SamplingInterval)
	}

	return p
}

// Start begins profiling
func (p *Profiler) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.startTime = time.Now()
	runtime.ReadMemStats(&p.memStats)

	if p.cpuProfile != nil {
		pprof.StartCPUProfile(nil)
	}
}

// Stop stops profiling and returns metrics
func (p *Profiler) Stop() *Metrics {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cpuProfile != nil {
		pprof.StopCPUProfile()
	}

	if p.sampling {
		close(p.samplingStop)
		p.sampling = false
	}

	// Calculate final metrics
	elapsed := time.Since(p.startTime)
	p.metrics.RecordsPerSecond = float64(p.metrics.RecordsProcessed) / elapsed.Seconds()
	p.metrics.BytesPerSecond = float64(p.metrics.BytesProcessed) / elapsed.Seconds()

	// Get final memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	p.metrics.MemoryUsageMB = (memStats.HeapAlloc) / 1024 / 1024
	p.metrics.GCCount = memStats.NumGC - p.memStats.NumGC
	p.metrics.GCPauseTotalNs = memStats.PauseTotalNs - p.memStats.PauseTotalNs

	return p.metrics
}

// RecordLatency records operation latency
func (p *Profiler) RecordLatency(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if d < p.metrics.MinLatency {
		p.metrics.MinLatency = d
	}
	if d > p.metrics.MaxLatency {
		p.metrics.MaxLatency = d
	}

	// Simple moving average
	if p.metrics.AvgLatency == 0 {
		p.metrics.AvgLatency = d
	} else {
		p.metrics.AvgLatency = (p.metrics.AvgLatency + d) / 2
	}
}

// IncrementRecords increments record counter
func (p *Profiler) IncrementRecords(count int64) {
	atomic.AddInt64(&p.metrics.RecordsProcessed, count)
}

// IncrementBytes increments byte counter
func (p *Profiler) IncrementBytes(bytes int64) {
	atomic.AddInt64(&p.metrics.BytesProcessed, bytes)
}

// IncrementErrors increments error counter
func (p *Profiler) IncrementErrors(count int64) {
	atomic.AddInt64(&p.metrics.ErrorCount, count)
}

// SetCustomMetric sets a custom metric
func (p *Profiler) SetCustomMetric(key string, value interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metrics.CustomMetrics[key] = value
}

// GetMetrics returns current metrics
func (p *Profiler) GetMetrics() *Metrics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Return copy to avoid race conditions
	m := *p.metrics
	m.CustomMetrics = pool.GetMap()
	for k, v := range p.metrics.CustomMetrics {
		m.CustomMetrics[k] = v
	}
	return &m
}

// startSampling starts resource sampling
func (p *Profiler) startSampling(interval time.Duration) {
	p.samplingStop = make(chan struct{})
	p.sampling = true

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.sampleResources()
			case <-p.samplingStop:
				return
			}
		}
	}()
}

// sampleResources samples current resource usage
func (p *Profiler) sampleResources() {
	// CPU usage
	cpuPercent, _ := cpu.Percent(0, false)
	if len(cpuPercent) > 0 {
		p.metrics.CPUUsagePercent = cpuPercent[0]
	}

	// Memory usage
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	p.metrics.MemoryUsageMB = memStats.HeapAlloc / 1024 / 1024

	// Goroutines
	p.metrics.GoroutineCount = runtime.NumGoroutine()
}

// ResourceMonitor monitors system resources
type ResourceMonitor struct {
	process      *process.Process
	startCPUTime float64
	startTime    time.Time
	mu           sync.RWMutex
}

// NewResourceMonitor creates a resource monitor
func NewResourceMonitor() *ResourceMonitor {
	proc, _ := process.NewProcess(int32(os.Getpid()))
	cpuTime, _ := proc.Times()

	return &ResourceMonitor{
		process:      proc,
		startCPUTime: cpuTime.Total(),
		startTime:    time.Now(),
	}
}

// GetResourceUsage returns current resource usage
func (rm *ResourceMonitor) GetResourceUsage() (*ResourceUsage, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	usage := &ResourceUsage{}

	// CPU usage
	cpuTime, err := rm.process.Times()
	if err == nil {
		elapsed := time.Since(rm.startTime).Seconds()
		usage.CPUPercent = ((cpuTime.Total() - rm.startCPUTime) / elapsed) * 100
	}

	// Memory usage
	memInfo, err := rm.process.MemoryInfo()
	if err == nil {
		usage.MemoryRSS = memInfo.RSS
		usage.MemoryVMS = memInfo.VMS
	}

	// System memory
	vmStat, err := mem.VirtualMemory()
	if err == nil {
		usage.SystemMemoryPercent = vmStat.UsedPercent
		usage.SystemMemoryAvailable = vmStat.Available
	}

	// Goroutines and threads
	usage.GoroutineCount = runtime.NumGoroutine()
	usage.ThreadCount, _ = rm.process.NumThreads()

	// File descriptors
	usage.OpenFDs, _ = rm.process.NumFDs()

	return usage, nil
}

// ResourceUsage contains resource usage information
type ResourceUsage struct {
	CPUPercent            float64
	MemoryRSS             uint64
	MemoryVMS             uint64
	SystemMemoryPercent   float64
	SystemMemoryAvailable uint64
	GoroutineCount        int
	ThreadCount           int32
	OpenFDs               int32
}

// LatencyTracker tracks latency percentiles
type LatencyTracker struct {
	samples []time.Duration
	mu      sync.Mutex
}

// NewLatencyTracker creates a latency tracker
func NewLatencyTracker() *LatencyTracker {
	return &LatencyTracker{
		samples: make([]time.Duration, 0, 10000),
	}
}

// Record records a latency sample
func (lt *LatencyTracker) Record(d time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.samples = append(lt.samples, d)

	// Keep last 10000 samples
	if len(lt.samples) > 10000 {
		lt.samples = lt.samples[len(lt.samples)-10000:]
	}
}

// GetPercentiles returns latency percentiles
func (lt *LatencyTracker) GetPercentiles() (p50, p95, p99 time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if len(lt.samples) == 0 {
		return 0, 0, 0
	}

	// Simple percentile calculation
	sorted := make([]time.Duration, len(lt.samples))
	copy(sorted, lt.samples)

	// Sort samples
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	p99 = sorted[len(sorted)*99/100]

	return
}

// ProfileResult contains profiling results
type ProfileResult struct {
	Name      string
	Duration  time.Duration
	Metrics   *Metrics
	Resources *ResourceUsage
	Report    string
}

// GenerateReport generates a performance report
func (p *Profiler) GenerateReport() *ProfileResult {
	metrics := p.GetMetrics()
	resources, _ := p.resourceMonitor.GetResourceUsage()

	report := fmt.Sprintf(`
Performance Profile: %s
========================
Duration: %v

Throughput:
- Records: %d (%.2f/sec)
- Bytes: %d (%.2f MB/sec)

Latency:
- Min: %v
- Max: %v
- Avg: %v

Resources:
- CPU: %.2f%%
- Memory: %d MB
- Goroutines: %d
- GC Count: %d
- GC Pause: %v

Errors:
- Count: %d
- Retries: %d
`,
		p.name,
		time.Since(p.startTime),
		metrics.RecordsProcessed,
		metrics.RecordsPerSecond,
		metrics.BytesProcessed,
		metrics.BytesPerSecond/1024/1024,
		metrics.MinLatency,
		metrics.MaxLatency,
		metrics.AvgLatency,
		metrics.CPUUsagePercent,
		metrics.MemoryUsageMB,
		metrics.GoroutineCount,
		metrics.GCCount,
		time.Duration(metrics.GCPauseTotalNs),
		metrics.ErrorCount,
		metrics.RetryCount,
	)

	return &ProfileResult{
		Name:      p.name,
		Duration:  time.Since(p.startTime),
		Metrics:   metrics,
		Resources: resources,
		Report:    report,
	}
}

// Benchmark provides benchmarking utilities
type Benchmark struct {
	name     string
	profiler *Profiler
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewBenchmark creates a new benchmark
func NewBenchmark(name string) *Benchmark {
	ctx, cancel := context.WithCancel(context.Background())
	return &Benchmark{
		name:     name,
		profiler: NewProfiler(DefaultProfilerConfig(name)),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Run runs a benchmark function
func (b *Benchmark) Run(fn func() error, duration time.Duration) (*ProfileResult, error) {
	b.profiler.Start()
	defer b.profiler.Stop()

	start := time.Now()
	errors := 0

	for time.Since(start) < duration {
		select {
		case <-b.ctx.Done():
			return b.profiler.GenerateReport(), nil
		default:
			if err := fn(); err != nil {
				errors++
				b.profiler.IncrementErrors(1)
			}
		}
	}

	return b.profiler.GenerateReport(), nil
}

// Stop stops the benchmark
func (b *Benchmark) Stop() {
	b.cancel()
}
