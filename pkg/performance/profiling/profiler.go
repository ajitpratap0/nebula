package profiling

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/metrics"
	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	"go.uber.org/zap"
)

// ProfileType represents the type of profiling to perform
type ProfileType string

const (
	CPUProfile       ProfileType = "cpu"
	MemoryProfile    ProfileType = "memory"
	BlockProfile     ProfileType = "block"
	MutexProfile     ProfileType = "mutex"
	GoroutineProfile ProfileType = "goroutine"
	TraceProfile     ProfileType = "trace"
	AllProfiles      ProfileType = "all"
)

// ProfileConfig contains configuration for profiling
type ProfileConfig struct {
	// Profile types to collect
	Types []ProfileType

	// Output directory for profile files
	OutputDir string

	// Duration for CPU profiling
	CPUDuration time.Duration

	// Memory profile rate (0 = default rate)
	MemProfileRate int

	// Block profile rate (0-100, 0 = disabled)
	BlockProfileRate int

	// Mutex profile fraction (0-100, 0 = disabled)
	MutexProfileFraction int

	// Whether to collect runtime metrics
	CollectRuntimeMetrics bool

	// Sampling interval for runtime metrics
	MetricsSamplingInterval time.Duration

	// Whether to generate flame graphs
	GenerateFlameGraphs bool
}

// DefaultProfileConfig returns a default profiling configuration
func DefaultProfileConfig() *ProfileConfig {
	return &ProfileConfig{
		Types:                   []ProfileType{CPUProfile, MemoryProfile},
		OutputDir:               "./profiles",
		CPUDuration:             30 * time.Second,
		MemProfileRate:          512 * 1024, // 512KB
		BlockProfileRate:        1,
		MutexProfileFraction:    1,
		CollectRuntimeMetrics:   true,
		MetricsSamplingInterval: 100 * time.Millisecond,
		GenerateFlameGraphs:     false,
	}
}

// Profiler provides comprehensive profiling capabilities
type Profiler struct {
	config    *ProfileConfig
	logger    *zap.Logger
	collector *metrics.Collector
	stopChan  chan struct{}
	wg        sync.WaitGroup
	startTime time.Time
	cpuFile   *os.File
	traceFile *os.File

	// Runtime metrics
	metricsData *RuntimeMetrics
	metricsMu   sync.RWMutex
}

// RuntimeMetrics contains runtime performance metrics
type RuntimeMetrics struct {
	// Memory metrics
	AllocBytes      uint64
	TotalAllocBytes uint64
	SysBytes        uint64
	NumGC           uint32
	GCPauseTotal    time.Duration
	GCPauseLast     time.Duration

	// Goroutine metrics
	NumGoroutines int

	// CPU metrics
	NumCPU     int
	GOMAXPROCS int

	// Samples over time
	Samples []MetricsSample
}

// MetricsSample represents a point-in-time metrics sample
type MetricsSample struct {
	Timestamp     time.Time
	AllocBytes    uint64
	NumGoroutines int
	GCPauseNs     uint64
}

// NewProfiler creates a new profiler instance
func NewProfiler(config *ProfileConfig, logger *zap.Logger) *Profiler {
	if config == nil {
		config = DefaultProfileConfig()
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	return &Profiler{
		config:    config,
		logger:    logger,
		collector: metrics.NewCollector("profiler"),
		stopChan:  make(chan struct{}),
		metricsData: &RuntimeMetrics{
			NumCPU:     runtime.NumCPU(),
			GOMAXPROCS: runtime.GOMAXPROCS(0),
			Samples:    make([]MetricsSample, 0, 1000),
		},
	}
}

// Start begins profiling
func (p *Profiler) Start(ctx context.Context) error {
	p.startTime = time.Now()

	// Create output directory
	if err := os.MkdirAll(p.config.OutputDir, 0755); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to create profile directory")
	}

	// Configure profile rates
	if p.config.BlockProfileRate > 0 {
		runtime.SetBlockProfileRate(p.config.BlockProfileRate)
	}

	if p.config.MutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(p.config.MutexProfileFraction)
	}

	// Start profiling based on requested types
	for _, profileType := range p.config.Types {
		switch profileType {
		case CPUProfile:
			if err := p.startCPUProfile(); err != nil {
				return err
			}

		case TraceProfile:
			if err := p.startTrace(); err != nil {
				return err
			}

		case AllProfiles:
			if err := p.startCPUProfile(); err != nil {
				return err
			}
			if err := p.startTrace(); err != nil {
				return err
			}
		}
	}

	// Start runtime metrics collection
	if p.config.CollectRuntimeMetrics {
		p.wg.Add(1)
		go p.collectRuntimeMetrics(ctx)
	}

	p.logger.Info("profiling started",
		zap.String("output_dir", p.config.OutputDir),
		zap.Any("types", p.config.Types),
		zap.Duration("cpu_duration", p.config.CPUDuration))

	return nil
}

// Stop stops profiling and saves results
func (p *Profiler) Stop() error {
	close(p.stopChan)

	// Stop CPU profiling
	if p.cpuFile != nil {
		pprof.StopCPUProfile()
		p.cpuFile.Close()
		p.logger.Info("CPU profile saved",
			zap.String("file", p.cpuFile.Name()))
	}

	// Stop trace
	if p.traceFile != nil {
		trace.Stop()
		p.traceFile.Close()
		p.logger.Info("trace saved",
			zap.String("file", p.traceFile.Name()))
	}

	// Wait for metrics collection to stop
	p.wg.Wait()

	// Save other profiles
	for _, profileType := range p.config.Types {
		switch profileType {
		case MemoryProfile:
			if err := p.saveMemoryProfile(); err != nil {
				p.logger.Error("failed to save memory profile", zap.Error(err))
			}

		case BlockProfile:
			if err := p.saveBlockProfile(); err != nil {
				p.logger.Error("failed to save block profile", zap.Error(err))
			}

		case MutexProfile:
			if err := p.saveMutexProfile(); err != nil {
				p.logger.Error("failed to save mutex profile", zap.Error(err))
			}

		case GoroutineProfile:
			if err := p.saveGoroutineProfile(); err != nil {
				p.logger.Error("failed to save goroutine profile", zap.Error(err))
			}

		case AllProfiles:
			p.saveAllProfiles()
		}
	}

	// Generate analysis report
	if err := p.generateReport(); err != nil {
		p.logger.Error("failed to generate report", zap.Error(err))
	}

	duration := time.Since(p.startTime)
	p.logger.Info("profiling completed",
		zap.Duration("duration", duration),
		zap.String("output_dir", p.config.OutputDir))

	return nil
}

// GetRuntimeMetrics returns current runtime metrics
func (p *Profiler) GetRuntimeMetrics() *RuntimeMetrics {
	p.metricsMu.RLock()
	defer p.metricsMu.RUnlock()

	// Return a copy to avoid race conditions
	metrics := *p.metricsData
	metrics.Samples = make([]MetricsSample, len(p.metricsData.Samples))
	copy(metrics.Samples, p.metricsData.Samples)

	return &metrics
}

// Private methods

func (p *Profiler) startCPUProfile() error {
	filename := fmt.Sprintf("%s/cpu_%s.prof", p.config.OutputDir, p.timestamp())
	file, err := os.Create(filename)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to create CPU profile file")
	}

	p.cpuFile = file

	if err := pprof.StartCPUProfile(file); err != nil {
		_ = file.Close() // Ignore close error
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to start CPU profiling")
	}

	// Stop CPU profiling after duration
	if p.config.CPUDuration > 0 {
		time.AfterFunc(p.config.CPUDuration, func() {
			pprof.StopCPUProfile()
		})
	}

	return nil
}

func (p *Profiler) startTrace() error {
	filename := fmt.Sprintf("%s/trace_%s.out", p.config.OutputDir, p.timestamp())
	file, err := os.Create(filename)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to create trace file")
	}

	p.traceFile = file

	if err := trace.Start(file); err != nil {
		_ = file.Close() // Ignore close error
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to start tracing")
	}

	return nil
}

func (p *Profiler) saveMemoryProfile() error {
	filename := fmt.Sprintf("%s/memory_%s.prof", p.config.OutputDir, p.timestamp())
	file, err := os.Create(filename)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to create memory profile file")
	}
	defer file.Close() // Ignore close error

	runtime.GC() // Force GC before heap profile
	if err := pprof.WriteHeapProfile(file); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to write memory profile")
	}

	p.logger.Info("memory profile saved", zap.String("file", filename))
	return nil
}

func (p *Profiler) saveBlockProfile() error {
	filename := fmt.Sprintf("%s/block_%s.prof", p.config.OutputDir, p.timestamp())
	file, err := os.Create(filename)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to create block profile file")
	}
	defer file.Close() // Ignore close error

	if err := pprof.Lookup("block").WriteTo(file, 0); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to write block profile")
	}

	p.logger.Info("block profile saved", zap.String("file", filename))
	return nil
}

func (p *Profiler) saveMutexProfile() error {
	filename := fmt.Sprintf("%s/mutex_%s.prof", p.config.OutputDir, p.timestamp())
	file, err := os.Create(filename)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to create mutex profile file")
	}
	defer file.Close() // Ignore close error

	if err := pprof.Lookup("mutex").WriteTo(file, 0); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to write mutex profile")
	}

	p.logger.Info("mutex profile saved", zap.String("file", filename))
	return nil
}

func (p *Profiler) saveGoroutineProfile() error {
	filename := fmt.Sprintf("%s/goroutine_%s.prof", p.config.OutputDir, p.timestamp())
	file, err := os.Create(filename)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to create goroutine profile file")
	}
	defer file.Close() // Ignore close error

	if err := pprof.Lookup("goroutine").WriteTo(file, 2); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to write goroutine profile")
	}

	p.logger.Info("goroutine profile saved", zap.String("file", filename))
	return nil
}

func (p *Profiler) saveAllProfiles() {
	p.saveMemoryProfile()
	p.saveBlockProfile()
	p.saveMutexProfile()
	p.saveGoroutineProfile()
}

func (p *Profiler) collectRuntimeMetrics(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.MetricsSamplingInterval)
	defer ticker.Stop()

	var memStats runtime.MemStats

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopChan:
			return
		case <-ticker.C:
			runtime.ReadMemStats(&memStats)

			p.metricsMu.Lock()

			// Update current metrics
			p.metricsData.AllocBytes = memStats.Alloc
			p.metricsData.TotalAllocBytes = memStats.TotalAlloc
			p.metricsData.SysBytes = memStats.Sys
			p.metricsData.NumGC = memStats.NumGC
			p.metricsData.GCPauseTotal = time.Duration(memStats.PauseTotalNs)
			if memStats.NumGC > 0 {
				p.metricsData.GCPauseLast = time.Duration(memStats.PauseNs[(memStats.NumGC+255)%256])
			}
			p.metricsData.NumGoroutines = runtime.NumGoroutine()

			// Add sample
			p.metricsData.Samples = append(p.metricsData.Samples, MetricsSample{
				Timestamp:     time.Now(),
				AllocBytes:    memStats.Alloc,
				NumGoroutines: runtime.NumGoroutine(),
				GCPauseNs:     memStats.PauseTotalNs,
			})

			p.metricsMu.Unlock()

			// Record metrics
			p.collector.RecordGauge("memory.alloc_mb", float64(memStats.Alloc)/(1024*1024))
			p.collector.RecordGauge("memory.sys_mb", float64(memStats.Sys)/(1024*1024))
			p.collector.RecordGauge("goroutines", float64(runtime.NumGoroutine()))
			p.collector.RecordCounter("gc.runs", float64(memStats.NumGC))
		}
	}
}

func (p *Profiler) generateReport() error {
	metrics := p.GetRuntimeMetrics()

	filename := fmt.Sprintf("%s/report_%s.txt", p.config.OutputDir, p.timestamp())
	file, err := os.Create(filename)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to create report file")
	}
	defer file.Close() // Ignore close error

	fmt.Fprintf(file, "Nebula Performance Profile Report\n")
	fmt.Fprintf(file, "=================================\n\n")
	fmt.Fprintf(file, "Duration: %v\n", time.Since(p.startTime))
	fmt.Fprintf(file, "CPU Count: %d\n", metrics.NumCPU)
	fmt.Fprintf(file, "GOMAXPROCS: %d\n\n", metrics.GOMAXPROCS)

	fmt.Fprintf(file, "Memory Statistics:\n")
	fmt.Fprintf(file, "-----------------\n")
	fmt.Fprintf(file, "Current Allocated: %.2f MB\n", float64(metrics.AllocBytes)/(1024*1024))
	fmt.Fprintf(file, "Total Allocated: %.2f MB\n", float64(metrics.TotalAllocBytes)/(1024*1024))
	fmt.Fprintf(file, "System Memory: %.2f MB\n", float64(metrics.SysBytes)/(1024*1024))
	fmt.Fprintf(file, "GC Runs: %d\n", metrics.NumGC)
	fmt.Fprintf(file, "GC Pause Total: %v\n", metrics.GCPauseTotal)
	fmt.Fprintf(file, "GC Pause Last: %v\n\n", metrics.GCPauseLast)

	fmt.Fprintf(file, "Goroutine Statistics:\n")
	fmt.Fprintf(file, "--------------------\n")
	fmt.Fprintf(file, "Current Goroutines: %d\n\n", metrics.NumGoroutines)

	// Analysis
	fmt.Fprintf(file, "Performance Analysis:\n")
	fmt.Fprintf(file, "--------------------\n")

	if len(metrics.Samples) > 0 {
		// Memory growth analysis
		firstSample := metrics.Samples[0]
		lastSample := metrics.Samples[len(metrics.Samples)-1]
		memGrowth := float64(lastSample.AllocBytes-firstSample.AllocBytes) / (1024 * 1024)

		fmt.Fprintf(file, "Memory Growth: %.2f MB\n", memGrowth)
		fmt.Fprintf(file, "Goroutine Growth: %d\n", lastSample.NumGoroutines-firstSample.NumGoroutines)

		// GC pressure
		gcPressure := float64(lastSample.GCPauseNs-firstSample.GCPauseNs) / 1e6 // Convert to ms
		fmt.Fprintf(file, "GC Pause Growth: %.2f ms\n", gcPressure)
	}

	p.logger.Info("performance report generated", zap.String("file", filename))
	return nil
}

func (p *Profiler) timestamp() string {
	return time.Now().Format("20060102_150405")
}

// ProfilePipeline profiles a complete pipeline execution
func ProfilePipeline(ctx context.Context, pipelineFunc func() error, config *ProfileConfig) (*RuntimeMetrics, error) {
	profiler := NewProfiler(config, zap.NewNop())

	if err := profiler.Start(ctx); err != nil {
		return nil, err
	}

	// Execute the pipeline
	pipelineErr := pipelineFunc()

	if err := profiler.Stop(); err != nil {
		return nil, err
	}

	return profiler.GetRuntimeMetrics(), pipelineErr
}
