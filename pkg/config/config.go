// Package config provides the unified configuration system for Nebula
package config

import (
	"fmt"
	"runtime"
	"time"
)

// BaseConfig is the single unified configuration structure that all connectors use
type BaseConfig struct {
	// Core identification
	Name    string `yaml:"name" json:"name"`
	Type    string `yaml:"type" json:"type"`
	Version string `yaml:"version" json:"version"`

	// Performance settings
	Performance PerformanceConfig `yaml:"performance" json:"performance"`

	// Timeouts and connection settings
	Timeouts TimeoutConfig `yaml:"timeouts" json:"timeouts"`

	// Reliability and error handling
	Reliability ReliabilityConfig `yaml:"reliability" json:"reliability"`

	// Security and authentication
	Security SecurityConfig `yaml:"security" json:"security"`

	// Observability and monitoring
	Observability ObservabilityConfig `yaml:"observability" json:"observability"`

	// Memory management
	Memory MemoryConfig `yaml:"memory" json:"memory"`

	// Advanced features
	Advanced AdvancedConfig `yaml:"advanced" json:"advanced"`
}

// PerformanceConfig contains all performance-related settings
type PerformanceConfig struct {
	BatchSize       int           `yaml:"batch_size" json:"batch_size"`
	BufferSize      int           `yaml:"buffer_size" json:"buffer_size"`
	Workers         int           `yaml:"workers" json:"workers"`
	MaxConcurrency  int           `yaml:"max_concurrency" json:"max_concurrency"`
	FlushInterval   time.Duration `yaml:"flush_interval" json:"flush_interval"`
	MemoryLimitMB   int           `yaml:"memory_limit_mb" json:"memory_limit_mb"`
	EnableStreaming bool          `yaml:"enable_streaming" json:"enable_streaming"`
	StreamingMode   bool          `yaml:"streaming_mode" json:"streaming_mode"`
	AsyncOperations bool          `yaml:"async_operations" json:"async_operations"`
}

// TimeoutConfig contains all timeout-related settings
type TimeoutConfig struct {
	Request      time.Duration `yaml:"request" json:"request"`
	Connection   time.Duration `yaml:"connection" json:"connection"`
	Idle         time.Duration `yaml:"idle" json:"idle"`
	ReadTimeout  time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout" json:"write_timeout"`
	KeepAlive    time.Duration `yaml:"keep_alive" json:"keep_alive"`
}

// ReliabilityConfig contains reliability and error handling settings
type ReliabilityConfig struct {
	RetryAttempts   int           `yaml:"retry_attempts" json:"retry_attempts"`
	RetryDelay      time.Duration `yaml:"retry_delay" json:"retry_delay"`
	RetryMultiplier float64       `yaml:"retry_multiplier" json:"retry_multiplier"`
	MaxRetryDelay   time.Duration `yaml:"max_retry_delay" json:"max_retry_delay"`
	CircuitBreaker  bool          `yaml:"circuit_breaker" json:"circuit_breaker"`
	RateLimitPerSec int           `yaml:"rate_limit_per_sec" json:"rate_limit_per_sec"`
	HealthCheck     bool          `yaml:"health_check" json:"health_check"`
	FailFast        bool          `yaml:"fail_fast" json:"fail_fast"`
}

// SecurityConfig contains security and authentication settings
type SecurityConfig struct {
	EnableTLS       bool              `yaml:"enable_tls" json:"enable_tls"`
	TLSSkipVerify   bool              `yaml:"tls_skip_verify" json:"tls_skip_verify"`
	AuthType        string            `yaml:"auth_type" json:"auth_type"`
	Credentials     map[string]string `yaml:"credentials" json:"credentials"`
	CertificatePath string            `yaml:"certificate_path" json:"certificate_path"`
	KeyPath         string            `yaml:"key_path" json:"key_path"`
	CAPath          string            `yaml:"ca_path" json:"ca_path"`
}

// ObservabilityConfig contains monitoring and observability settings
type ObservabilityConfig struct {
	EnableMetrics     bool          `yaml:"enable_metrics" json:"enable_metrics"`
	EnableTracing     bool          `yaml:"enable_tracing" json:"enable_tracing"`
	EnableLogging     bool          `yaml:"enable_logging" json:"enable_logging"`
	MetricsInterval   time.Duration `yaml:"metrics_interval" json:"metrics_interval"`
	LogLevel          string        `yaml:"log_level" json:"log_level"`
	TracingSampleRate float64       `yaml:"tracing_sample_rate" json:"tracing_sample_rate"`
}

// MemoryConfig contains memory management settings
type MemoryConfig struct {
	EnablePools       bool          `yaml:"enable_pools" json:"enable_pools"`
	RecordPoolSize    int           `yaml:"record_pool_size" json:"record_pool_size"`
	BufferPoolSize    int           `yaml:"buffer_pool_size" json:"buffer_pool_size"`
	EnableBufferReuse bool          `yaml:"enable_buffer_reuse" json:"enable_buffer_reuse"`
	MinBufferSize     int           `yaml:"min_buffer_size" json:"min_buffer_size"`
	MaxBufferSize     int           `yaml:"max_buffer_size" json:"max_buffer_size"`
	GCInterval        time.Duration `yaml:"gc_interval" json:"gc_interval"`
}

// AdvancedConfig contains optional advanced features
type AdvancedConfig struct {
	EnableCompression     bool   `yaml:"enable_compression" json:"enable_compression"`
	CompressionAlgorithm  string `yaml:"compression_algorithm" json:"compression_algorithm"`
	CompressionLevel      int    `yaml:"compression_level" json:"compression_level"`
	CompressionThreshold  int    `yaml:"compression_threshold" json:"compression_threshold"`
	EnableBulkOperations  bool   `yaml:"enable_bulk_operations" json:"enable_bulk_operations"`
	EnableTransactions    bool   `yaml:"enable_transactions" json:"enable_transactions"`
	EnableUpsert          bool   `yaml:"enable_upsert" json:"enable_upsert"`
	EnableSchemaEvolution bool   `yaml:"enable_schema_evolution" json:"enable_schema_evolution"`
	StorageMode           string `yaml:"storage_mode" json:"storage_mode"` // row, columnar, hybrid
	Debug                 bool   `yaml:"debug" json:"debug"`
}

// NewBaseConfig creates a new BaseConfig with sensible defaults
func NewBaseConfig(name, connectorType string) *BaseConfig {
	return &BaseConfig{
		Name:    name,
		Type:    connectorType,
		Version: "1.0.0",
		Performance: PerformanceConfig{
			BatchSize:       1000,
			BufferSize:      10000,
			Workers:         runtime.NumCPU(),
			MaxConcurrency:  10,
			FlushInterval:   10 * time.Second,
			MemoryLimitMB:   1024,
			EnableStreaming: true,
			StreamingMode:   false, // Default to batch mode
			AsyncOperations: true,
		},
		Timeouts: TimeoutConfig{
			Request:      30 * time.Second,
			Connection:   10 * time.Second,
			Idle:         5 * time.Minute,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			KeepAlive:    30 * time.Second,
		},
		Reliability: ReliabilityConfig{
			RetryAttempts:   3,
			RetryDelay:      time.Second,
			RetryMultiplier: 2.0,
			MaxRetryDelay:   60 * time.Second,
			CircuitBreaker:  true,
			RateLimitPerSec: 0,
			HealthCheck:     true,
			FailFast:        false,
		},
		Security: SecurityConfig{
			EnableTLS:     true,
			TLSSkipVerify: false,
			Credentials:   make(map[string]string),
		},
		Observability: ObservabilityConfig{
			EnableMetrics:     true,
			EnableTracing:     false,
			EnableLogging:     true,
			MetricsInterval:   30 * time.Second,
			LogLevel:          "info",
			TracingSampleRate: 0.1,
		},
		Memory: MemoryConfig{
			EnablePools:       true,
			RecordPoolSize:    1000,
			BufferPoolSize:    100,
			EnableBufferReuse: true,
			MinBufferSize:     1024,
			MaxBufferSize:     1048576, // 1MB
			GCInterval:        time.Minute,
		},
		Advanced: AdvancedConfig{
			EnableCompression:     false,
			CompressionAlgorithm:  "gzip",
			CompressionLevel:      6,
			CompressionThreshold:  1024,
			EnableBulkOperations:  true,
			EnableTransactions:    false,
			EnableUpsert:          false,
			EnableSchemaEvolution: true,
			StorageMode:           "hybrid", // Default to hybrid mode
			Debug:                 false,
		},
	}
}

// Validate validates the configuration
func (bc *BaseConfig) Validate() error {
	if bc.Name == "" {
		return fmt.Errorf("name is required")
	}
	if bc.Type == "" {
		return fmt.Errorf("type is required")
	}
	if bc.Performance.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be positive")
	}
	if bc.Performance.BufferSize <= 0 {
		return fmt.Errorf("buffer_size must be positive")
	}
	if bc.Performance.MaxConcurrency <= 0 {
		return fmt.Errorf("max_concurrency must be positive")
	}
	if bc.Reliability.RetryAttempts < 0 {
		return fmt.Errorf("retry_attempts cannot be negative")
	}
	if bc.Reliability.RateLimitPerSec < 0 {
		return fmt.Errorf("rate_limit_per_sec cannot be negative")
	}
	return nil
}

// GetWorkers returns the number of workers, ensuring it's at least 1
func (p *PerformanceConfig) GetWorkers() int {
	if p.Workers <= 0 {
		return runtime.NumCPU()
	}
	return p.Workers
}

// IsRateLimited returns true if rate limiting is enabled
func (r *ReliabilityConfig) IsRateLimited() bool {
	return r.RateLimitPerSec > 0
}

// HasCredentials returns true if credentials are configured
func (s *SecurityConfig) HasCredentials() bool {
	return len(s.Credentials) > 0
}

// IsCompressionEnabled returns true if compression should be used
func (a *AdvancedConfig) IsCompressionEnabled() bool {
	return a.EnableCompression && a.CompressionAlgorithm != ""
}