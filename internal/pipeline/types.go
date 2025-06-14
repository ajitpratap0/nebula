// Package pipeline defines types for streaming pipeline architecture
package pipeline

import (
	"time"

	"github.com/ajitpratap0/nebula/pkg/models"
)

// StreamingRecord is deprecated - use models.Record instead
// Kept as type alias for backward compatibility during migration
type StreamingRecord = models.Record

// StreamingError represents an error in the streaming pipeline with recovery context
type StreamingError struct {
	Type        string                 `json:"type"`
	Error       error                  `json:"error"`
	Records     []*models.Record       `json:"records,omitempty"`
	WorkerID    int                    `json:"worker_id,omitempty"`
	Timestamp   time.Time              `json:"timestamp"`
	Retries     int                    `json:"retries"`
	LastRetry   time.Time              `json:"last_retry,omitempty"`
	Recoverable bool                   `json:"recoverable"`
	Context     map[string]interface{} `json:"context,omitempty"`
}

// StreamingStats represents real-time pipeline statistics
type StreamingStats struct {
	// Throughput metrics
	RecordsProcessed int64         `json:"records_processed"`
	BatchesProcessed int64         `json:"batches_processed"`
	ThroughputRPS    float64       `json:"throughput_rps"`
	AvgBatchSize     float64       `json:"avg_batch_size"`
	AvgBatchDuration time.Duration `json:"avg_batch_duration"`

	// Error and recovery metrics
	ErrorsTotal     int64 `json:"errors_total"`
	ErrorsRecovered int64 `json:"errors_recovered"`
	DeadLetterCount int64 `json:"dead_letter_count"`
	RetryAttempts   int64 `json:"retry_attempts"`

	// Backpressure metrics
	BackpressureEvents int64   `json:"backpressure_events"`
	BufferUtilization  float64 `json:"buffer_utilization"`
	FlowControlActive  bool    `json:"flow_control_active"`

	// Performance metrics
	MemoryUsage     int64   `json:"memory_usage_bytes"`
	CPUUtilization  float64 `json:"cpu_utilization"`
	NetworkBytesIn  int64   `json:"network_bytes_in"`
	NetworkBytesOut int64   `json:"network_bytes_out"`

	// Timing metrics
	StartTime  time.Time     `json:"start_time"`
	LastUpdate time.Time     `json:"last_update"`
	Uptime     time.Duration `json:"uptime"`

	// Health metrics
	IsHealthy          bool `json:"is_healthy"`
	SourceHealthy      bool `json:"source_healthy"`
	DestinationHealthy bool `json:"destination_healthy"`
}

// DataFlowStage represents a stage in the data flow pipeline
type DataFlowStage struct {
	Name          string                 `json:"name"`
	Type          string                 `json:"type"` // extract, transform, load
	Parallelism   int                    `json:"parallelism"`
	BufferSize    int                    `json:"buffer_size"`
	Config        map[string]interface{} `json:"config"`
	Dependencies  []string               `json:"dependencies"`
	ErrorHandling ErrorHandlingStrategy  `json:"error_handling"`
}

// ErrorHandlingStrategy defines how errors are handled for a stage
type ErrorHandlingStrategy struct {
	Strategy       string        `json:"strategy"` // retry, skip, stop, dead_letter
	MaxRetries     int           `json:"max_retries"`
	RetryDelay     time.Duration `json:"retry_delay"`
	DeadLetterTTL  time.Duration `json:"dead_letter_ttl"`
	CircuitBreaker bool          `json:"circuit_breaker"`
}

// BackpressureStrategy defines different backpressure handling approaches
type BackpressureStrategy string

const (
	BackpressureDrop     BackpressureStrategy = "drop"
	BackpressureBlock    BackpressureStrategy = "block"
	BackpressureAdaptive BackpressureStrategy = "adaptive"
	BackpressureThrottle BackpressureStrategy = "throttle"
)

// FlowControlState represents the current state of flow control
type FlowControlState struct {
	Active            bool          `json:"active"`
	Reason            string        `json:"reason"`
	Severity          int           `json:"severity"` // 1-10, 10 being most severe
	RecommendedAction string        `json:"recommended_action"`
	EstimatedDuration time.Duration `json:"estimated_duration"`
	AffectedStages    []string      `json:"affected_stages"`
}

// PipelineFusionConfig defines how stages can be fused for optimization
type PipelineFusionConfig struct {
	Enabled           bool              `json:"enabled"`
	FusableStages     [][]string        `json:"fusable_stages"`
	FusionStrategies  map[string]string `json:"fusion_strategies"`
	OptimizationLevel int               `json:"optimization_level"` // 1-3
	ZeroCopyEnabled   bool              `json:"zero_copy_enabled"`
	SIMDEnabled       bool              `json:"simd_enabled"`
}

// DeadLetterRecord represents a record that couldn't be processed
type DeadLetterRecord struct {
	OriginalRecord *models.Record  `json:"original_record"`
	Error          *StreamingError `json:"error"`
	Attempts       int             `json:"attempts"`
	FirstFailure   time.Time       `json:"first_failure"`
	LastFailure    time.Time       `json:"last_failure"`
	TTL            time.Time       `json:"ttl"`
	Reason         string          `json:"reason"`
	Stage          string          `json:"stage"`
	WorkerID       int             `json:"worker_id"`
}

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState struct {
	State        string    `json:"state"` // closed, open, half_open
	FailureCount int       `json:"failure_count"`
	SuccessCount int       `json:"success_count"`
	LastFailure  time.Time `json:"last_failure"`
	NextRetry    time.Time `json:"next_retry"`
	WindowStart  time.Time `json:"window_start"`
	WindowEnd    time.Time `json:"window_end"`
	ErrorRate    float64   `json:"error_rate"`
	RequestCount int       `json:"request_count"`
}

// BufferMetrics represents metrics for adaptive buffering
type BufferMetrics struct {
	CurrentSize    int       `json:"current_size"`
	MaxSize        int       `json:"max_size"`
	Utilization    float64   `json:"utilization"`
	GrowthEvents   int64     `json:"growth_events"`
	ShrinkEvents   int64     `json:"shrink_events"`
	OverflowEvents int64     `json:"overflow_events"`
	LastResize     time.Time `json:"last_resize"`
	ResizeReason   string    `json:"resize_reason"`
	TargetSize     int       `json:"target_size"`
	PendingResize  bool      `json:"pending_resize"`
}

// PerformanceProfile represents performance characteristics
type PerformanceProfile struct {
	TargetThroughput  float64            `json:"target_throughput_rps"`
	MaxLatency        time.Duration      `json:"max_latency"`
	MemoryBudget      int64              `json:"memory_budget_bytes"`
	CPUBudget         float64            `json:"cpu_budget_percent"`
	NetworkBudget     int64              `json:"network_budget_bps"`
	OptimizationLevel int                `json:"optimization_level"`
	ProfileName       string             `json:"profile_name"`
	CustomMetrics     map[string]float64 `json:"custom_metrics"`
}

// PipelineTopology represents the overall pipeline structure
type PipelineTopology struct {
	Stages       []DataFlowStage   `json:"stages"`
	Connections  []StageConnection `json:"connections"`
	FusionGroups [][]string        `json:"fusion_groups"`
	Parallelism  map[string]int    `json:"parallelism"`
	BufferSizes  map[string]int    `json:"buffer_sizes"`
	ErrorFlow    []ErrorFlowRule   `json:"error_flow"`
}

// StageConnection represents a connection between pipeline stages
type StageConnection struct {
	From        string `json:"from"`
	To          string `json:"to"`
	BufferSize  int    `json:"buffer_size"`
	Partitioned bool   `json:"partitioned"`
	PartitionBy string `json:"partition_by,omitempty"`
}

// ErrorFlowRule defines how errors flow through the pipeline
type ErrorFlowRule struct {
	ErrorType string   `json:"error_type"`
	FromStage string   `json:"from_stage"`
	ToStages  []string `json:"to_stages"`
	Strategy  string   `json:"strategy"`
	Condition string   `json:"condition,omitempty"`
}
