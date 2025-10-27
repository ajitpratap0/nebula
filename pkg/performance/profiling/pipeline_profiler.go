package profiling

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	"go.uber.org/zap"
)

// PipelineProfiler profiles pipeline performance with detailed metrics
type PipelineProfiler struct {
	logger   *zap.Logger
	profiler *Profiler
	analyzer *BottleneckAnalyzer

	// Pipeline metrics
	recordsProcessed int64
	bytesProcessed   int64
	errorsCount      int64
	startTime        time.Time
	endTime          time.Time

	// Stage metrics
	stageMetrics map[string]*StageMetrics
	stageMutex   sync.RWMutex

	// Connector metrics
	sourceMetrics *ConnectorMetrics
	destMetrics   *ConnectorMetrics

	// Channel metrics
	channelMetrics *ChannelMetrics
}

// StageMetrics tracks metrics for a pipeline stage
type StageMetrics struct {
	Name           string
	RecordsIn      int64
	RecordsOut     int64
	ProcessingTime time.Duration
	ErrorCount     int64
	AvgLatency     time.Duration
	MaxLatency     time.Duration
	MinLatency     time.Duration
	lastUpdate     time.Time
	latencySum     time.Duration
	latencyCount   int64
}

// ConnectorMetrics tracks connector-specific metrics
type ConnectorMetrics struct {
	Name             string
	Type             string
	RecordsProcessed int64
	BytesProcessed   int64
	Errors           int64
	ConnectionTime   time.Duration
	TotalTime        time.Duration
	BatchCount       int64
	AvgBatchSize     float64
}

// ChannelMetrics tracks channel utilization
type ChannelMetrics struct {
	BufferSize     int
	MaxUtilization int
	AvgUtilization float64
	BlockedTime    time.Duration
	measurements   []int
	mu             sync.Mutex
}

// ProfileResult contains complete pipeline profiling results
type ProfileResult struct {
	// Overall metrics
	Duration       time.Duration
	Throughput     float64 // records/sec
	ByteThroughput float64 // bytes/sec
	ErrorRate      float64 // errors/total

	// Stage breakdown
	StageMetrics map[string]*StageMetrics

	// Connector metrics
	SourceMetrics *ConnectorMetrics
	DestMetrics   *ConnectorMetrics

	// Channel metrics
	ChannelMetrics *ChannelMetrics

	// System metrics
	RuntimeMetrics *RuntimeMetrics

	// Bottleneck analysis
	BottleneckAnalysis *AnalysisResult

	// Recommendations
	Recommendations []string
}

// NewPipelineProfiler creates a new pipeline profiler
func NewPipelineProfiler(profileConfig *ProfileConfig) *PipelineProfiler {
	if profileConfig == nil {
		profileConfig = DefaultProfileConfig()
	}

	logger := zap.NewNop()

	return &PipelineProfiler{
		logger:       logger,
		profiler:     NewProfiler(profileConfig, logger),
		analyzer:     NewBottleneckAnalyzer(profileConfig.OutputDir, logger),
		stageMetrics: make(map[string]*StageMetrics),
		channelMetrics: &ChannelMetrics{
			measurements: make([]int, 0, 1000),
		},
	}
}

// Start begins profiling
func (p *PipelineProfiler) Start(ctx context.Context) error {
	p.startTime = time.Now()

	// Start system profiling
	if err := p.profiler.Start(ctx); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeInternal, "failed to start profiler")
	}

	p.logger.Info("pipeline profiling started")
	return nil
}

// Stop stops profiling and generates report
func (p *PipelineProfiler) Stop() (*ProfileResult, error) {
	p.endTime = time.Now()

	// Stop system profiling
	if err := p.profiler.Stop(); err != nil {
		p.logger.Error("failed to stop profiler", zap.Error(err))
	}

	// Get runtime metrics
	runtimeMetrics := p.profiler.GetRuntimeMetrics()

	// Perform bottleneck analysis
	analysisResult, err := p.analyzer.Analyze(runtimeMetrics)
	if err != nil {
		p.logger.Error("failed to analyze bottlenecks", zap.Error(err))
	}

	// Calculate overall metrics
	duration := p.endTime.Sub(p.startTime)
	throughput := float64(atomic.LoadInt64(&p.recordsProcessed)) / duration.Seconds()
	byteThroughput := float64(atomic.LoadInt64(&p.bytesProcessed)) / duration.Seconds()

	errorRate := float64(0)
	if p.recordsProcessed > 0 {
		errorRate = float64(atomic.LoadInt64(&p.errorsCount)) / float64(p.recordsProcessed)
	}

	// Calculate channel utilization
	p.channelMetrics.calculateAverages()

	// Generate recommendations
	recommendations := p.generateRecommendations(throughput, analysisResult)

	result := &ProfileResult{
		Duration:           duration,
		Throughput:         throughput,
		ByteThroughput:     byteThroughput,
		ErrorRate:          errorRate,
		StageMetrics:       p.getStageMetricsCopy(),
		SourceMetrics:      p.sourceMetrics,
		DestMetrics:        p.destMetrics,
		ChannelMetrics:     p.channelMetrics,
		RuntimeMetrics:     runtimeMetrics,
		BottleneckAnalysis: analysisResult,
		Recommendations:    recommendations,
	}

	// Save detailed report
	if err := p.saveReport(result); err != nil {
		p.logger.Error("failed to save profiling report", zap.Error(err))
	}

	p.logger.Info("pipeline profiling completed",
		zap.Duration("duration", duration),
		zap.Float64("throughput", throughput),
		zap.Float64("error_rate", errorRate))

	return result, nil
}

// ProfileSource profiles a source connector
func (p *PipelineProfiler) ProfileSource(source core.Source) core.Source {
	// Try to get name from connector interface
	name := "source"
	if connector, ok := source.(core.Connector); ok {
		name = connector.Name()
	}

	return &profiledSource{
		Source:   source,
		profiler: p,
		metrics: &ConnectorMetrics{
			Name: name,
			Type: "source",
		},
	}
}

// ProfileDestination profiles a destination connector
func (p *PipelineProfiler) ProfileDestination(dest core.Destination) core.Destination {
	// Try to get name from connector interface
	name := "destination"
	if connector, ok := dest.(core.Connector); ok {
		name = connector.Name()
	}

	return &profiledDestination{
		Destination: dest,
		profiler:    p,
		metrics: &ConnectorMetrics{
			Name: name,
			Type: "destination",
		},
	}
}

// RecordStageMetrics records metrics for a pipeline stage
func (p *PipelineProfiler) RecordStageMetrics(stageName string, recordsIn, recordsOut int64, processingTime time.Duration) {
	p.stageMutex.Lock()
	defer p.stageMutex.Unlock()

	metrics, exists := p.stageMetrics[stageName]
	if !exists {
		metrics = &StageMetrics{
			Name:       stageName,
			MinLatency: processingTime,
		}
		p.stageMetrics[stageName] = metrics
	}

	metrics.RecordsIn += recordsIn
	metrics.RecordsOut += recordsOut
	metrics.ProcessingTime += processingTime

	// Update latency stats
	metrics.latencySum += processingTime
	metrics.latencyCount++
	metrics.AvgLatency = metrics.latencySum / time.Duration(metrics.latencyCount)

	if processingTime > metrics.MaxLatency {
		metrics.MaxLatency = processingTime
	}
	if processingTime < metrics.MinLatency {
		metrics.MinLatency = processingTime
	}

	metrics.lastUpdate = time.Now()
}

// RecordChannelUtilization records channel buffer utilization
func (p *PipelineProfiler) RecordChannelUtilization(current, capacity int) {
	p.channelMetrics.mu.Lock()
	defer p.channelMetrics.mu.Unlock()

	utilization := current
	p.channelMetrics.measurements = append(p.channelMetrics.measurements, utilization)

	if capacity > p.channelMetrics.BufferSize {
		p.channelMetrics.BufferSize = capacity
	}

	if utilization > p.channelMetrics.MaxUtilization {
		p.channelMetrics.MaxUtilization = utilization
	}
}

// RecordRecord increments record counter
func (p *PipelineProfiler) RecordRecord(bytes int64) {
	atomic.AddInt64(&p.recordsProcessed, 1)
	atomic.AddInt64(&p.bytesProcessed, bytes)
}

// RecordError increments error counter
func (p *PipelineProfiler) RecordError() {
	atomic.AddInt64(&p.errorsCount, 1)
}

// Private methods

func (p *PipelineProfiler) getStageMetricsCopy() map[string]*StageMetrics {
	p.stageMutex.RLock()
	defer p.stageMutex.RUnlock()

	stagesCopy := make(map[string]*StageMetrics)
	for k, v := range p.stageMetrics {
		metricsCopy := *v
		stagesCopy[k] = &metricsCopy
	}
	return stagesCopy
}

func (p *PipelineProfiler) generateRecommendations(throughput float64, analysis *AnalysisResult) []string {
	recommendations := make([]string, 0)

	// Add bottleneck-based recommendations
	if analysis != nil && len(analysis.Recommendations) > 0 {
		recommendations = append(recommendations, analysis.Recommendations...)
	}

	// Throughput-based recommendations
	if throughput < 10000 { // Less than 10K records/sec
		recommendations = append(recommendations,
			"Consider increasing batch sizes for better throughput",
			"Enable compression for network transfers",
			"Use connection pooling for database operations")
	}

	// Stage-based recommendations
	p.stageMutex.RLock()
	for _, stage := range p.stageMetrics {
		if stage.AvgLatency > 100*time.Millisecond {
			recommendations = append(recommendations,
				fmt.Sprintf("Optimize stage '%s' - average latency %.2fms", stage.Name, stage.AvgLatency.Seconds()*1000))
		}

		if stage.RecordsIn > 0 && stage.RecordsOut < stage.RecordsIn/2 {
			recommendations = append(recommendations,
				fmt.Sprintf("Stage '%s' filtering >50%% of records - consider moving filter earlier", stage.Name))
		}
	}
	p.stageMutex.RUnlock()

	// Channel-based recommendations
	if p.channelMetrics.AvgUtilization > float64(p.channelMetrics.BufferSize)*0.8 {
		recommendations = append(recommendations,
			"Channel buffer frequently full - increase buffer size or add backpressure")
	}

	return recommendations
}

func (p *PipelineProfiler) saveReport(result *ProfileResult) error {
	// Generate filename with timestamp
	filename := fmt.Sprintf("pipeline_profile_%s.json", time.Now().Format("20060102_150405"))

	// Marshal result to JSON using jsonpool
	data, err := jsonpool.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal profile result: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filename, data, 0o600); err != nil {
		return fmt.Errorf("failed to write profile report: %w", err)
	}

	p.logger.Info("saved profile report",
		zap.String("filename", filename),
		zap.Int("size", len(data)))

	return nil
}

func (c *ChannelMetrics) calculateAverages() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.measurements) == 0 {
		return
	}

	sum := 0
	for _, m := range c.measurements {
		sum += m
	}
	c.AvgUtilization = float64(sum) / float64(len(c.measurements))
}

// profiledSource wraps a source connector with profiling
type profiledSource struct {
	core.Source
	profiler *PipelineProfiler
	metrics  *ConnectorMetrics
}

func (s *profiledSource) Initialize(ctx context.Context, config *config.BaseConfig) error {
	start := time.Now()
	err := s.Source.Initialize(ctx, config)
	s.metrics.ConnectionTime = time.Since(start)

	if s.profiler.sourceMetrics == nil {
		s.profiler.sourceMetrics = s.metrics
	}

	return err
}

func (s *profiledSource) Read(ctx context.Context) (*core.RecordStream, error) {
	stream, err := s.Source.Read(ctx)
	if err != nil {
		return nil, err
	}

	// Wrap the stream to collect metrics
	recordChan := make(chan *models.Record, 10000)
	profiledStream := &core.RecordStream{
		Records: recordChan,
		Errors:  stream.Errors,
	}

	go func() {
		for record := range stream.Records {
			s.metrics.RecordsProcessed++
			s.profiler.RecordRecord(int64(len(record.Data)))
			recordChan <- record
		}
		close(recordChan)
	}()

	return profiledStream, nil
}

// profiledDestination wraps a destination connector with profiling
type profiledDestination struct {
	core.Destination
	profiler *PipelineProfiler
	metrics  *ConnectorMetrics
}

func (d *profiledDestination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	start := time.Now()
	err := d.Destination.Initialize(ctx, config)
	d.metrics.ConnectionTime = time.Since(start)

	if d.profiler.destMetrics == nil {
		d.profiler.destMetrics = d.metrics
	}

	return err
}

func (d *profiledDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	// Wrap the stream to collect metrics
	recordChan := make(chan *models.Record, 10000)
	profiledStream := &core.RecordStream{
		Records: recordChan,
		Errors:  stream.Errors,
	}

	go func() {
		for record := range stream.Records {
			d.metrics.RecordsProcessed++
			d.profiler.RecordRecord(int64(len(record.Data)))
			recordChan <- record
		}
		close(recordChan)
	}()

	return d.Destination.Write(ctx, profiledStream)
}
