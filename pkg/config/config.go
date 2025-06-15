// Package config provides the unified configuration system for Nebula.
// It defines a single BaseConfig structure that all connectors must use,
// ensuring consistent configuration across the entire system.
//
// The configuration is organized into logical sections:
//   - Performance: Batch sizes, concurrency, streaming settings
//   - Timeouts: Connection and operation timeouts
//   - Reliability: Retry logic, circuit breakers, rate limiting
//   - Security: TLS, authentication, credentials
//   - Observability: Metrics, tracing, logging
//   - Memory: Pooling and buffer management
//   - Advanced: Optional features like compression and storage modes
//
// Example usage:
//
//	cfg := config.NewBaseConfig("my-connector", "source")
//	cfg.Performance.BatchSize = 5000
//	cfg.Security.EnableTLS = true
//	
//	if err := cfg.Validate(); err != nil {
//	    log.Fatal(err)
//	}
package config

import (
	"fmt"
	"runtime"
	"time"
)

// BaseConfig is the single unified configuration structure that all connectors use.
// It provides a comprehensive set of configuration options organized into logical
// sections. Connectors should embed this structure with the yaml inline tag.
type BaseConfig struct {
	// Core identification fields
	
	// Name identifies the connector instance
	Name    string `yaml:"name" json:"name"`
	// Type specifies the connector type (e.g., "csv", "postgresql", "s3")
	Type    string `yaml:"type" json:"type"`
	// Version indicates the configuration version
	Version string `yaml:"version" json:"version"`

	// Performance settings control throughput and resource usage
	Performance PerformanceConfig `yaml:"performance" json:"performance"`

	// Timeouts define various timeout durations
	Timeouts TimeoutConfig `yaml:"timeouts" json:"timeouts"`

	// Reliability settings for error handling and resilience
	Reliability ReliabilityConfig `yaml:"reliability" json:"reliability"`

	// Security configuration for authentication and encryption
	Security SecurityConfig `yaml:"security" json:"security"`

	// Observability settings for monitoring and debugging
	Observability ObservabilityConfig `yaml:"observability" json:"observability"`

	// Memory management configuration
	Memory MemoryConfig `yaml:"memory" json:"memory"`

	// Advanced features and optimizations
	Advanced AdvancedConfig `yaml:"advanced" json:"advanced"`
}

// PerformanceConfig contains all performance-related settings.
// These settings control throughput, concurrency, and resource utilization.
type PerformanceConfig struct {
	// BatchSize controls the number of records processed together
	BatchSize       int           `yaml:"batch_size" json:"batch_size"`
	// BufferSize sets the size of internal buffers
	BufferSize      int           `yaml:"buffer_size" json:"buffer_size"`
	// Workers defines the number of concurrent workers
	Workers         int           `yaml:"workers" json:"workers"`
	// MaxConcurrency limits total concurrent operations
	MaxConcurrency  int           `yaml:"max_concurrency" json:"max_concurrency"`
	// FlushInterval triggers periodic batch flushes
	FlushInterval   time.Duration `yaml:"flush_interval" json:"flush_interval"`
	// MemoryLimitMB sets the memory usage limit in megabytes
	MemoryLimitMB   int           `yaml:"memory_limit_mb" json:"memory_limit_mb"`
	// EnableStreaming enables streaming mode if supported
	EnableStreaming bool          `yaml:"enable_streaming" json:"enable_streaming"`
	// StreamingMode forces pure streaming (no batching)
	StreamingMode   bool          `yaml:"streaming_mode" json:"streaming_mode"`
	// AsyncOperations enables asynchronous processing
	AsyncOperations bool          `yaml:"async_operations" json:"async_operations"`
}

// TimeoutConfig contains all timeout-related settings.
// These prevent operations from hanging indefinitely.
type TimeoutConfig struct {
	// Request timeout for individual operations
	Request      time.Duration `yaml:"request" json:"request"`
	// Connection timeout for establishing connections
	Connection   time.Duration `yaml:"connection" json:"connection"`
	// Idle timeout before closing inactive connections
	Idle         time.Duration `yaml:"idle" json:"idle"`
	// ReadTimeout for read operations
	ReadTimeout  time.Duration `yaml:"read_timeout" json:"read_timeout"`
	// WriteTimeout for write operations
	WriteTimeout time.Duration `yaml:"write_timeout" json:"write_timeout"`
	// KeepAlive interval for connection health checks
	KeepAlive    time.Duration `yaml:"keep_alive" json:"keep_alive"`
}

// ReliabilityConfig contains reliability and error handling settings.
// These ensure robust operation in the face of failures.
type ReliabilityConfig struct {
	// RetryAttempts sets maximum retry attempts for failed operations
	RetryAttempts   int           `yaml:"retry_attempts" json:"retry_attempts"`
	// RetryDelay is the initial delay between retries
	RetryDelay      time.Duration `yaml:"retry_delay" json:"retry_delay"`
	// RetryMultiplier increases delay exponentially
	RetryMultiplier float64       `yaml:"retry_multiplier" json:"retry_multiplier"`
	// MaxRetryDelay caps the maximum retry delay
	MaxRetryDelay   time.Duration `yaml:"max_retry_delay" json:"max_retry_delay"`
	// CircuitBreaker enables circuit breaker pattern
	CircuitBreaker  bool          `yaml:"circuit_breaker" json:"circuit_breaker"`
	// RateLimitPerSec limits operations per second (0 = unlimited)
	RateLimitPerSec int           `yaml:"rate_limit_per_sec" json:"rate_limit_per_sec"`
	// HealthCheck enables periodic health checks
	HealthCheck     bool          `yaml:"health_check" json:"health_check"`
	// FailFast stops on first error instead of continuing
	FailFast        bool          `yaml:"fail_fast" json:"fail_fast"`
}

// SecurityConfig contains security and authentication settings.
// These protect data in transit and at rest.
type SecurityConfig struct {
	// EnableTLS enables TLS/SSL encryption
	EnableTLS       bool              `yaml:"enable_tls" json:"enable_tls"`
	// TLSSkipVerify disables certificate verification (insecure)
	TLSSkipVerify   bool              `yaml:"tls_skip_verify" json:"tls_skip_verify"`
	// AuthType specifies authentication method (basic, oauth2, api_key, etc.)
	AuthType        string            `yaml:"auth_type" json:"auth_type"`
	// Credentials stores authentication credentials (use env vars in production)
	Credentials     map[string]string `yaml:"credentials" json:"credentials"`
	// CertificatePath for client certificate
	CertificatePath string            `yaml:"certificate_path" json:"certificate_path"`
	// KeyPath for client private key
	KeyPath         string            `yaml:"key_path" json:"key_path"`
	// CAPath for custom CA certificate
	CAPath          string            `yaml:"ca_path" json:"ca_path"`
}

// ObservabilityConfig contains monitoring and observability settings.
// These enable tracking of connector behavior and performance.
type ObservabilityConfig struct {
	// EnableMetrics activates metrics collection
	EnableMetrics     bool          `yaml:"enable_metrics" json:"enable_metrics"`
	// EnableTracing activates distributed tracing
	EnableTracing     bool          `yaml:"enable_tracing" json:"enable_tracing"`
	// EnableLogging controls logging output
	EnableLogging     bool          `yaml:"enable_logging" json:"enable_logging"`
	// MetricsInterval sets how often metrics are collected
	MetricsInterval   time.Duration `yaml:"metrics_interval" json:"metrics_interval"`
	// LogLevel sets logging verbosity (debug, info, warn, error)
	LogLevel          string        `yaml:"log_level" json:"log_level"`
	// TracingSampleRate controls trace sampling (0.0-1.0)
	TracingSampleRate float64       `yaml:"tracing_sample_rate" json:"tracing_sample_rate"`
}

// MemoryConfig contains memory management settings.
// These optimize memory usage through pooling and reuse.
type MemoryConfig struct {
	// EnablePools activates object pooling
	EnablePools       bool          `yaml:"enable_pools" json:"enable_pools"`
	// RecordPoolSize sets the record pool capacity
	RecordPoolSize    int           `yaml:"record_pool_size" json:"record_pool_size"`
	// BufferPoolSize sets the buffer pool capacity
	BufferPoolSize    int           `yaml:"buffer_pool_size" json:"buffer_pool_size"`
	// EnableBufferReuse allows buffer recycling
	EnableBufferReuse bool          `yaml:"enable_buffer_reuse" json:"enable_buffer_reuse"`
	// MinBufferSize sets minimum buffer allocation
	MinBufferSize     int           `yaml:"min_buffer_size" json:"min_buffer_size"`
	// MaxBufferSize sets maximum buffer allocation
	MaxBufferSize     int           `yaml:"max_buffer_size" json:"max_buffer_size"`
	// GCInterval triggers periodic garbage collection
	GCInterval        time.Duration `yaml:"gc_interval" json:"gc_interval"`
}

// AdvancedConfig contains optional advanced features.
// These provide additional optimizations and capabilities.
type AdvancedConfig struct {
	// EnableCompression activates data compression
	EnableCompression     bool   `yaml:"enable_compression" json:"enable_compression"`
	// CompressionAlgorithm selects compression type (gzip, snappy, lz4, zstd)
	CompressionAlgorithm  string `yaml:"compression_algorithm" json:"compression_algorithm"`
	// CompressionLevel sets compression ratio vs speed (1-9)
	CompressionLevel      int    `yaml:"compression_level" json:"compression_level"`
	// CompressionThreshold skips compression for small data
	CompressionThreshold  int    `yaml:"compression_threshold" json:"compression_threshold"`
	// EnableBulkOperations allows bulk loading
	EnableBulkOperations  bool   `yaml:"enable_bulk_operations" json:"enable_bulk_operations"`
	// EnableTransactions enables transactional operations
	EnableTransactions    bool   `yaml:"enable_transactions" json:"enable_transactions"`
	// EnableUpsert allows insert-or-update operations
	EnableUpsert          bool   `yaml:"enable_upsert" json:"enable_upsert"`
	// EnableSchemaEvolution allows automatic schema updates
	EnableSchemaEvolution bool   `yaml:"enable_schema_evolution" json:"enable_schema_evolution"`
	// StorageMode selects storage strategy (row, columnar, hybrid)
	StorageMode           string `yaml:"storage_mode" json:"storage_mode"`
	// Debug enables detailed debug output
	Debug                 bool   `yaml:"debug" json:"debug"`
}

// NewBaseConfig creates a new BaseConfig with sensible defaults.
// It initializes all configuration sections with production-ready values
// that work well for most use cases. Specific connectors can override
// these defaults as needed.
//
// Parameters:
//   - name: The connector instance name
//   - connectorType: The type of connector (e.g., "csv", "postgresql")
//
// Example:
//
//	cfg := config.NewBaseConfig("my-source", "postgresql")
//	cfg.Performance.BatchSize = 5000  // Override default
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

// Validate validates the configuration for correctness.
// It checks required fields and ensures values are within acceptable ranges.
// Connectors should call this after loading configuration to catch errors early.
//
// Returns an error if validation fails, nil otherwise.
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