// Package sdk provides a comprehensive Software Development Kit for building V2 connectors
// with the Nebula data integration platform.
//
// The SDK follows the builder pattern to make connector development intuitive and includes:
//
// - Builder pattern for source and destination connectors
// - Automatic BaseConnector integration with optimization features
// - Comprehensive testing utilities
// - Configuration validation helpers
// - Schema building utilities
// - Type conversion helpers
// - Real-world examples
//
// Example usage:
//
//	// Create a source connector
//	builder := sdk.NewSourceBuilder().
//		WithName("my-api-source").
//		WithVersion("1.0.0").
//		WithCapabilities("streaming", "batch").
//		WithReadHook(myReadFunction).
//		WithDiscoverHook(myDiscoverFunction)
//
//	source, err := sdk.NewSDKSourceConnector(builder)
//	if err != nil {
//		return err
//	}
//
//	// Register the connector
//	registry := sdk.NewConnectorRegistry()
//	err = registry.RegisterSourceBuilder("my-api", builder)
//	if err != nil {
//		return err
//	}
//
// The SDK automatically provides:
//
// - Circuit breaker protection
// - Rate limiting
// - Health checks
// - Metrics collection
// - Error handling
// - Memory optimization
// - Connection pooling
// - State management
//
// For comprehensive examples, see the examples.go file.
package sdk

import (
	"context"
	"fmt"
	"time"

	"github.com/ajitpratap0/nebula/pkg/compression"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/ajitpratap0/nebula/pkg/logger"
	"go.uber.org/zap"
)

// Version contains the SDK version
const Version = "2.0.0"

// SDK provides the main entry point for the connector SDK
type SDK struct {
	logger            *zap.Logger
	// OptimizationLayer removed - using unified pool system
	registry          *ConnectorRegistry
	validator         *ConfigValidator
	typeConverter     *TypeConverter
	reflectionHelper  *ReflectionHelper
}

// NewSDK creates a new SDK instance
func NewSDK() *SDK {
	return &SDK{
		logger:           logger.Get().With(zap.String("component", "connector_sdk")),
		registry:         NewConnectorRegistry(),
		validator:        NewConfigValidator(),
		typeConverter:    NewTypeConverter(),
		reflectionHelper: NewReflectionHelper(),
	}
}

// Initialize initializes the SDK with unified pool system
func (sdk *SDK) Initialize() error {
	// No additional initialization needed - unified pool is global
	sdk.logger.Info("SDK initialized",
		zap.String("version", Version),
		zap.Bool("unified_pool_enabled", true))

	return nil
}

// GetRegistry returns the connector registry
func (sdk *SDK) GetRegistry() *ConnectorRegistry {
	return sdk.registry
}

// GetValidator returns the configuration validator
func (sdk *SDK) GetValidator() *ConfigValidator {
	return sdk.validator
}

// GetTypeConverter returns the type converter
func (sdk *SDK) GetTypeConverter() *TypeConverter {
	return sdk.typeConverter
}

// GetReflectionHelper returns the reflection helper
func (sdk *SDK) GetReflectionHelper() *ReflectionHelper {
	return sdk.reflectionHelper
}

// GetPoolStats returns unified pool statistics
func (sdk *SDK) GetPoolStats() map[string]pool.Stats {
	return pool.GetGlobalStats()
}

// CreateSourceBuilder creates a new source builder with SDK defaults
func (sdk *SDK) CreateSourceBuilder(name string) *SourceBuilder {
	builder := NewSourceBuilder().
		WithName(name)

	// Unified pool system is always enabled

	return builder
}

// CreateDestinationBuilder creates a new destination builder with SDK defaults
func (sdk *SDK) CreateDestinationBuilder(name string) *DestinationBuilder {
	builder := NewDestinationBuilder().
		WithName(name)

	// Unified pool system is always enabled

	return builder
}

// ValidateConnectorConfig validates a connector configuration
func (sdk *SDK) ValidateConnectorConfig(config *config.BaseConfig, requiredProperties ...string) error {
	// Basic validation
	if config.Name == "" {
		return fmt.Errorf("connector name is required")
	}

	if config.Type != string(core.ConnectorTypeSource) && config.Type != string(core.ConnectorTypeDestination) {
		return fmt.Errorf("invalid connector type: %s", config.Type)
	}

	// Validate required properties
	return sdk.validator.ValidateRequired(config, requiredProperties...)
}

// Close shuts down the SDK
func (sdk *SDK) Close() error {
	// No cleanup needed for unified pool system
	sdk.logger.Info("SDK closed")
	return nil
}

// Global SDK instance for convenience
var globalSDK *SDK

// InitializeGlobalSDK initializes the global SDK instance
func InitializeGlobalSDK() error {
	globalSDK = NewSDK()
	return globalSDK.Initialize()
}

// GetGlobalSDK returns the global SDK instance
func GetGlobalSDK() *SDK {
	if globalSDK == nil {
		globalSDK = NewSDK()
		globalSDK.Initialize() // Use unified pool system
	}
	return globalSDK
}

// QuickStart provides a quick start interface for common connector development patterns
type QuickStart struct {
	sdk *SDK
}

// NewQuickStart creates a new quick start helper
func NewQuickStart() *QuickStart {
	return &QuickStart{
		sdk: GetGlobalSDK(),
	}
}

// CreateSimpleSourceConnector creates a simple source connector with minimal configuration
func (qs *QuickStart) CreateSimpleSourceConnector(
	name, description string,
	readFunc func(context.Context) (*core.RecordStream, error),
	discoverFunc func(context.Context) (*core.Schema, error)) (core.Source, error) {

	builder := qs.sdk.CreateSourceBuilder(name).
		WithDescription(description).
		WithCapabilities("streaming").
		WithReadHook(readFunc).
		WithDiscoverHook(discoverFunc)

	return NewSDKSourceConnector(builder)
}

// CreateSimpleDestinationConnector creates a simple destination connector with minimal configuration
func (qs *QuickStart) CreateSimpleDestinationConnector(
	name, description string,
	writeFunc func(context.Context, *core.RecordStream) error,
	createSchemaFunc func(context.Context, *core.Schema) error) (core.Destination, error) {

	builder := qs.sdk.CreateDestinationBuilder(name).
		WithDescription(description).
		WithCapabilities("streaming").
		WithWriteHook(writeFunc).
		WithCreateSchemaHook(createSchemaFunc)

	return NewSDKDestinationConnector(builder)
}

// RegisterSimpleSource registers a simple source connector
func (qs *QuickStart) RegisterSimpleSource(
	registryName, connectorName, description string,
	readFunc func(context.Context) (*core.RecordStream, error),
	discoverFunc func(context.Context) (*core.Schema, error)) error {

	builder := qs.sdk.CreateSourceBuilder(connectorName).
		WithDescription(description).
		WithCapabilities("streaming").
		WithReadHook(readFunc).
		WithDiscoverHook(discoverFunc)

	return qs.sdk.GetRegistry().RegisterSourceBuilder(registryName, builder)
}

// RegisterSimpleDestination registers a simple destination connector
func (qs *QuickStart) RegisterSimpleDestination(
	registryName, connectorName, description string,
	writeFunc func(context.Context, *core.RecordStream) error,
	createSchemaFunc func(context.Context, *core.Schema) error) error {

	builder := qs.sdk.CreateDestinationBuilder(connectorName).
		WithDescription(description).
		WithCapabilities("streaming").
		WithWriteHook(writeFunc).
		WithCreateSchemaHook(createSchemaFunc)

	return qs.sdk.GetRegistry().RegisterDestinationBuilder(registryName, builder)
}

// ConnectorTemplate provides templates for common connector patterns
type ConnectorTemplate struct{}

// NewConnectorTemplate creates a new connector template helper
func NewConnectorTemplate() *ConnectorTemplate {
	return &ConnectorTemplate{}
}

// DatabaseSourceTemplate creates a template for database source connectors
func (ct *ConnectorTemplate) DatabaseSourceTemplate(name string) *SourceBuilder {
	return NewSourceBuilder().
		WithName(name).
		WithCapabilities("streaming", "batch", "incremental", "cdc").
		WithIncremental(true).
		WithBatch(true).
		WithRealtime(true).
		WithCDC(true).
		WithConfigProperty("connection_string", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"description": "Database connection string",
		}).
		WithConfigProperty("table", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"description": "Source table name",
		}).
		WithConfigProperty("incremental_column", map[string]interface{}{
			"type":        "string",
			"required":    false,
			"description": "Column for incremental sync",
		}).
		WithDefaults(
			5000,           // batch size
			20000,          // buffer size
			10,             // max concurrency
			60*time.Second, // request timeout
			5*time.Second,  // retry delay
			3,              // retry attempts
			1000,           // rate limit
		).
		WithCompression(true, compression.Snappy, compression.Default).
		WithCompressionThreshold(2048) // Compress data larger than 2KB
}

// APISourceTemplate creates a template for API source connectors
func (ct *ConnectorTemplate) APISourceTemplate(name string) *SourceBuilder {
	return NewSourceBuilder().
		WithName(name).
		WithCapabilities("streaming", "batch", "incremental").
		WithIncremental(true).
		WithBatch(true).
		WithConfigProperty("base_url", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"description": "API base URL",
		}).
		WithConfigProperty("api_key", map[string]interface{}{
			"type":        "string",
			"required":    false,
			"description": "API key for authentication",
		}).
		WithConfigProperty("endpoint", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"description": "API endpoint",
		}).
		WithDefaults(
			1000,           // batch size
			5000,           // buffer size
			5,              // max concurrency
			30*time.Second, // request timeout
			3*time.Second,  // retry delay
			5,              // retry attempts
			100,            // rate limit
		).
		WithCompression(true, compression.LZ4, compression.Default).
		WithCompressionThreshold(1024) // Compress data larger than 1KB
}

// FileSourceTemplate creates a template for file source connectors
func (ct *ConnectorTemplate) FileSourceTemplate(name string) *SourceBuilder {
	return NewSourceBuilder().
		WithName(name).
		WithCapabilities("streaming", "batch").
		WithBatch(true).
		WithConfigProperty("file_path", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"description": "File path",
		}).
		WithConfigProperty("format", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"enum":        []string{"csv", "json", "parquet", "avro"},
			"description": "File format",
		}).
		WithDefaults(
			10000,         // batch size
			50000,         // buffer size
			1,             // max concurrency (file reading is usually single-threaded)
			5*time.Minute, // request timeout
			time.Second,   // retry delay
			3,             // retry attempts
			0,             // no rate limit for files
		)
}

// DatabaseDestinationTemplate creates a template for database destination connectors
func (ct *ConnectorTemplate) DatabaseDestinationTemplate(name string) *DestinationBuilder {
	return NewDestinationBuilder().
		WithName(name).
		WithCapabilities("streaming", "batch", "transactions", "upsert", "bulk_load").
		WithBatch(true).
		WithStreaming(true).
		WithTransactions(true).
		WithUpsert(true).
		WithBulkLoad(true).
		WithConfigProperty("connection_string", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"description": "Database connection string",
		}).
		WithConfigProperty("table", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"description": "Target table name",
		}).
		WithConfigProperty("upsert_keys", map[string]interface{}{
			"type":        "array",
			"required":    false,
			"description": "Keys for upsert operations",
		}).
		WithDefaults(
			5000,          // batch size
			25000,         // buffer size
			10,            // max concurrency
			2*time.Minute, // request timeout
			5*time.Second, // retry delay
			3,             // retry attempts
			0,             // no rate limit
		)
}

// FileDestinationTemplate creates a template for file destination connectors
func (ct *ConnectorTemplate) FileDestinationTemplate(name string) *DestinationBuilder {
	return NewDestinationBuilder().
		WithName(name).
		WithCapabilities("streaming", "batch", "bulk_load").
		WithBatch(true).
		WithStreaming(true).
		WithBulkLoad(true).
		WithConfigProperty("output_path", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"description": "Output file path",
		}).
		WithConfigProperty("format", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"enum":        []string{"csv", "json", "parquet", "avro"},
			"description": "Output file format",
		}).
		WithConfigProperty("compression", map[string]interface{}{
			"type":        "string",
			"required":    false,
			"enum":        []string{"none", "gzip", "snappy", "lz4"},
			"description": "Compression type",
		}).
		WithDefaults(
			10000,         // batch size
			50000,         // buffer size
			1,             // max concurrency
			5*time.Minute, // request timeout
			time.Second,   // retry delay
			3,             // retry attempts
			0,             // no rate limit
		)
}

// GetVersion returns the SDK version
func GetVersion() string {
	return Version
}

// GetInfo returns SDK information
func GetInfo() map[string]interface{} {
	return map[string]interface{}{
		"version":     Version,
		"description": "Nebula Connector SDK for building high-performance V2 connectors",
		"features": []string{
			"Builder pattern for intuitive development",
			"Automatic BaseConnector integration",
			"Built-in optimization layer",
			"Comprehensive testing utilities",
			"Configuration validation",
			"Schema building helpers",
			"Type conversion utilities",
			"Real-world examples and templates",
		},
		"capabilities": []string{
			"Circuit breaker protection",
			"Rate limiting",
			"Health checks",
			"Metrics collection",
			"Error handling",
			"Memory optimization",
			"Connection pooling",
			"State management",
		},
	}
}
