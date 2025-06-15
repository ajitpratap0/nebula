package config_test

import (
	"fmt"
	"log"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
)

// ExampleNewBaseConfig demonstrates creating a new base configuration
// with default values.
func ExampleNewBaseConfig() {
	// Create a new base configuration for a CSV source
	cfg := config.NewBaseConfig("csv", "source")

	// The configuration comes with sensible defaults
	fmt.Printf("Batch Size: %d\n", cfg.Performance.BatchSize)
	fmt.Printf("Connection Timeout: %s\n", cfg.Timeouts.Connection)
	fmt.Printf("Request Timeout: %s\n", cfg.Timeouts.Request)

	// Output:
	// Batch Size: 1000
	// Connection Timeout: 10s
	// Request Timeout: 30s
}

// ExampleBaseConfig_Validate shows how to validate a configuration
// before using it.
func ExampleBaseConfig_Validate() {
	cfg := config.NewBaseConfig("bigquery", "destination")

	// Modify some values
	cfg.Performance.Workers = 16
	cfg.Performance.BatchSize = 10000
	cfg.Timeouts.Request = 2 * time.Minute

	// Validate the configuration
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	fmt.Println("Configuration is valid!")

	// Output:
	// Configuration is valid!
}

// ExampleLoad demonstrates loading configuration from a YAML file
// with environment variable substitution.
func ExampleLoad() {
	// Example configuration structure
	type MyConnectorConfig struct {
		config.BaseConfig `yaml:",inline" json:",inline"`
		APIKey            string `yaml:"api_key" json:"api_key"`
		Endpoint          string `yaml:"endpoint" json:"endpoint"`
	}

	// In practice, you would load from a file:
	// var cfg MyConnectorConfig
	// if err := config.Load("config.yaml", &cfg); err != nil {
	//     log.Fatal(err)
	// }

	// For this example, we'll create one manually
	cfg := MyConnectorConfig{
		BaseConfig: *config.NewBaseConfig("myconnector", "source"),
		APIKey:     "secret-key",
		Endpoint:   "https://api.example.com",
	}

	fmt.Printf("Name: %s\n", cfg.Name)
	fmt.Printf("Type: %s\n", cfg.Type)
	fmt.Printf("Batch Size: %d\n", cfg.Performance.BatchSize)

	// Output:
	// Name: myconnector
	// Type: source
	// Batch Size: 1000
}

// ExampleBaseConfig_performance shows how to configure performance settings
// for high-throughput scenarios.
func ExampleBaseConfig_performance() {
	cfg := config.NewBaseConfig("s3", "destination")

	// Configure for high-throughput batch processing
	cfg.Performance.Workers = 16
	cfg.Performance.BatchSize = 50000
	cfg.Performance.BufferSize = 100000
	cfg.Performance.MaxConcurrency = 100

	// Enable compression through advanced settings
	cfg.Advanced.EnableCompression = true
	cfg.Advanced.CompressionLevel = 6 // Balanced compression

	// Set memory limits
	cfg.Performance.MemoryLimitMB = 2048
	cfg.Memory.RecordPoolSize = 10000

	fmt.Printf("Workers: %d\n", cfg.Performance.Workers)
	fmt.Printf("Batch Size: %d\n", cfg.Performance.BatchSize)
	fmt.Printf("Compression: %v\n", cfg.Advanced.EnableCompression)

	// Output:
	// Workers: 16
	// Batch Size: 50000
	// Compression: true
}

// ExampleBaseConfig_reliability shows how to configure reliability features
// like circuit breakers and retry policies.
func ExampleBaseConfig_reliability() {
	cfg := config.NewBaseConfig("api", "source")

	// Configure circuit breaker
	cfg.Reliability.CircuitBreaker = true
	cfg.Reliability.HealthCheck = true

	// Configure retry policy
	cfg.Reliability.RetryAttempts = 3
	cfg.Reliability.RetryDelay = 1 * time.Second
	cfg.Reliability.MaxRetryDelay = 30 * time.Second
	cfg.Reliability.RetryMultiplier = 2.0

	// Configure rate limiting
	cfg.Reliability.RateLimitPerSec = 100

	fmt.Printf("Circuit Breaker: %v\n", cfg.Reliability.CircuitBreaker)
	fmt.Printf("Max Retry Attempts: %d\n", cfg.Reliability.RetryAttempts)
	fmt.Printf("Rate Limit: %d req/s\n", cfg.Reliability.RateLimitPerSec)

	// Output:
	// Circuit Breaker: true
	// Max Retry Attempts: 3
	// Rate Limit: 100 req/s
}

// ExampleBaseConfig_storageMode demonstrates configuring the hybrid storage engine
// for different workload patterns.
func ExampleBaseConfig_storageMode() {
	// Configure for streaming workload (low latency)
	streamCfg := config.NewBaseConfig("kafka", "source")
	streamCfg.Advanced.StorageMode = "row"
	streamCfg.Performance.StreamingMode = true
	streamCfg.Performance.BatchSize = 100 // Small batches for low latency

	// Configure for batch analytics (high compression)
	batchCfg := config.NewBaseConfig("s3", "source")
	batchCfg.Advanced.StorageMode = "columnar"
	batchCfg.Performance.BatchSize = 50000 // Large batches for efficiency
	batchCfg.Advanced.EnableCompression = true

	// Configure for hybrid mode (automatic optimization)
	hybridCfg := config.NewBaseConfig("postgres", "source")
	hybridCfg.Advanced.StorageMode = "hybrid"
	// The system will automatically switch between row and columnar storage

	fmt.Printf("Streaming mode: %s\n", streamCfg.Advanced.StorageMode)
	fmt.Printf("Batch mode: %s\n", batchCfg.Advanced.StorageMode)
	fmt.Printf("Hybrid mode: %s\n", hybridCfg.Advanced.StorageMode)

	// Output:
	// Streaming mode: row
	// Batch mode: columnar
	// Hybrid mode: hybrid
}