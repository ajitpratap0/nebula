// Package config provides unified configuration management for the Nebula data integration platform.
//
// This package implements the Phase 3 refactoring that eliminated feature proliferation
// by consolidating 64+ different configuration types into a single, unified structure.
//
// # Key Features
//
// - BaseConfig: Single configuration structure that all connectors use
// - Structured sections: Performance, Timeouts, Reliability, Security, Observability, Memory, Advanced
// - Environment variable substitution with ${VAR_NAME} syntax
// - Automatic defaults and validation
// - Clean implementation with no backward compatibility code
//
// # Usage
//
// ## Basic Configuration Loading
//
//	var cfg MyConnectorConfig
//	err := config.Load("config.yaml", &cfg)
//	if err != nil {
//		log.Fatal(err)
//	}
//
// ## Creating Connector Configurations
//
//	type MyConnectorConfig struct {
//		config.BaseConfig `yaml:",inline" json:",inline"`
//
//		// Connector-specific fields
//		DatabaseURL string `yaml:"database_url" json:"database_url"`
//	}
//
//	func NewMyConnector() *MyConnector {
//		cfg := config.NewBaseConfig("my-connector", "source")
//		// cfg now has all sensible defaults
//
//		return &MyConnector{config: cfg}
//	}
//
// ## Environment Variable Substitution
//
//	# config.yaml
//	name: my-connector
//	type: source
//	security:
//	  credentials:
//	    username: ${DB_USERNAME}
//	    password: ${DB_PASSWORD}
//
// # Configuration Structure
//
// All configurations use the BaseConfig pattern:
//
//	type BaseConfig struct {
//		Name    string `yaml:"name" json:"name"`
//		Type    string `yaml:"type" json:"type"`
//		Version string `yaml:"version" json:"version"`
//
//		Performance   PerformanceConfig   `yaml:"performance" json:"performance"`
//		Timeouts      TimeoutConfig       `yaml:"timeouts" json:"timeouts"`
//		Reliability   ReliabilityConfig   `yaml:"reliability" json:"reliability"`
//		Security      SecurityConfig      `yaml:"security" json:"security"`
//		Observability ObservabilityConfig `yaml:"observability" json:"observability"`
//		Memory        MemoryConfig        `yaml:"memory" json:"memory"`
//		Advanced      AdvancedConfig      `yaml:"advanced" json:"advanced"`
//	}
//
// Each section provides structured, validated configuration:
//
// - Performance: Batch sizes, concurrency, memory limits
// - Timeouts: Request, connection, read/write timeouts
// - Reliability: Retry policies, circuit breakers, rate limiting
// - Security: TLS, authentication, certificates
// - Observability: Metrics, logging, tracing
// - Memory: Pool management, buffer reuse, GC settings
// - Advanced: Compression, transactions, schema evolution
//
// # Benefits of Clean Implementation
//
// - Eliminates 64+ configuration types → 1 unified BaseConfig
// - No migration code or backward compatibility cruft
// - Consistent configuration experience across all connectors
// - Automatic validation and default value application
// - Simple environment variable substitution
// - Type-safe configuration with structured validation
//
// # Usage Pattern
//
// 1. Embed BaseConfig in connector-specific configurations
// 2. Use config.Load() for simple YAML loading
// 3. Use config.NewBaseConfig() for programmatic creation
// 4. Environment variables are substituted automatically
// 5. Validation is performed on load
//
// This clean implementation eliminates the configuration proliferation that was
// blocking Nebula from achieving its 1M records/sec performance target.
package config
