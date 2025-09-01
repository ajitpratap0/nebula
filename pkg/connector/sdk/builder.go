package sdk

import (
	"context"
	"time"

	"github.com/ajitpratap0/nebula/pkg/compression"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/base"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/logger"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

// ConnectorBuilder provides a fluent interface for building V2 connectors.
// It simplifies the creation of custom connectors by providing sensible defaults
// and a builder pattern for configuration.
type ConnectorBuilder struct {
	name          string
	connectorType core.ConnectorType
	version       string
	description   string
	author        string
	capabilities  []string
	configSchema  map[string]interface{}

	// Configuration defaults
	defaultBatchSize      int
	defaultBufferSize     int
	defaultMaxConcurrency int
	defaultRequestTimeout time.Duration
	defaultRetryAttempts  int
	defaultRetryDelay     time.Duration
	defaultRateLimit      int

	// Unified pool system is always enabled

	// Error handling
	errorHandler func(error) error

	// Custom lifecycle hooks
	initHook   func(context.Context, *config.BaseConfig) error
	closeHook  func(context.Context) error
	healthHook func(context.Context) error

	// Validation
	configValidator func(*config.BaseConfig) error

	logger *zap.Logger
}

// NewConnectorBuilder creates a new connector builder with default configuration.
// The builder starts with sensible defaults for batch size (1000), buffer size (10000),
// concurrency (10), and other settings that can be customized using the builder methods.
func NewConnectorBuilder() *ConnectorBuilder {
	return &ConnectorBuilder{
		version:               "1.0.0",
		defaultBatchSize:      1000,
		defaultBufferSize:     10000,
		defaultMaxConcurrency: 10,
		defaultRequestTimeout: 30 * time.Second,
		defaultRetryAttempts:  3,
		defaultRetryDelay:     time.Second,
		defaultRateLimit:      0, // No limit by default
		// Unified pool system is always enabled
		capabilities: make([]string, 0),
		configSchema: make(map[string]interface{}),
		logger:       logger.Get().With(zap.String("component", "connector_builder")),
	}
}

// WithName sets the connector name. This is required and should be unique
// across all connectors in the registry.
func (cb *ConnectorBuilder) WithName(name string) *ConnectorBuilder {
	cb.name = name
	return cb
}

// WithType sets the connector type (source or destination).
// This determines which interfaces the connector must implement.
func (cb *ConnectorBuilder) WithType(connectorType core.ConnectorType) *ConnectorBuilder {
	cb.connectorType = connectorType
	return cb
}

// WithVersion sets the connector version using semantic versioning (e.g., "1.0.0").
// Defaults to "1.0.0" if not specified.
func (cb *ConnectorBuilder) WithVersion(version string) *ConnectorBuilder {
	cb.version = version
	return cb
}

// WithDescription sets a human-readable description of the connector's purpose
// and functionality. This is used for documentation and discovery.
func (cb *ConnectorBuilder) WithDescription(description string) *ConnectorBuilder {
	cb.description = description
	return cb
}

// WithAuthor sets the connector author information (name, email, or organization).
// This helps users know who to contact for support.
func (cb *ConnectorBuilder) WithAuthor(author string) *ConnectorBuilder {
	cb.author = author
	return cb
}

// WithCapability adds a single capability to the connector's capability list.
// Common capabilities include "incremental", "batch", "streaming", "cdc", etc.
func (cb *ConnectorBuilder) WithCapability(capability string) *ConnectorBuilder {
	cb.capabilities = append(cb.capabilities, capability)
	return cb
}

// WithCapabilities sets all capabilities at once, replacing any previously set capabilities.
// Use this when you know all capabilities upfront.
func (cb *ConnectorBuilder) WithCapabilities(capabilities ...string) *ConnectorBuilder {
	cb.capabilities = capabilities
	return cb
}

// WithConfigSchema sets the complete configuration schema for the connector.
// The schema should follow JSON Schema format for validation and documentation.
func (cb *ConnectorBuilder) WithConfigSchema(schema map[string]interface{}) *ConnectorBuilder {
	cb.configSchema = schema
	return cb
}

// WithConfigProperty adds a single configuration property to the schema.
// Each property should include type, description, and validation rules.
func (cb *ConnectorBuilder) WithConfigProperty(name string, schema map[string]interface{}) *ConnectorBuilder {
	if cb.configSchema == nil {
		cb.configSchema = pool.GetMap()
	}
	cb.configSchema[name] = schema
	return cb
}

// WithDefaults sets default configuration values for performance and reliability.
// These defaults override the builder's initial defaults.
func (cb *ConnectorBuilder) WithDefaults(
	batchSize, bufferSize, maxConcurrency int,
	requestTimeout, retryDelay time.Duration,
	retryAttempts, rateLimit int) *ConnectorBuilder {

	cb.defaultBatchSize = batchSize
	cb.defaultBufferSize = bufferSize
	cb.defaultMaxConcurrency = maxConcurrency
	cb.defaultRequestTimeout = requestTimeout
	cb.defaultRetryAttempts = retryAttempts
	cb.defaultRetryDelay = retryDelay
	cb.defaultRateLimit = rateLimit
	return cb
}

// WithOptimization is deprecated - unified pool system is always enabled.
// This method is kept for backward compatibility but has no effect.
// Deprecated: The unified pool system is always enabled in the current architecture.
func (cb *ConnectorBuilder) WithOptimization(enabled bool, config interface{}) *ConnectorBuilder {
	// No-op: unified pool system is always enabled
	return cb
}

// WithCompression enables/disables compression with specified algorithm and level
func (cb *ConnectorBuilder) WithCompression(enabled bool, algorithm compression.Algorithm, level compression.Level) *ConnectorBuilder {
	// Compression is handled separately from pool system
	// This is kept for compatibility but doesn't affect pool behavior
	return cb
}

// WithCompressionThreshold sets the minimum size threshold for compression
func (cb *ConnectorBuilder) WithCompressionThreshold(threshold int) *ConnectorBuilder {
	// Compression thresholds are handled separately from pool system
	// This is kept for compatibility
	return cb
}

// WithErrorHandler sets a custom error handler for processing connector errors.
// The handler can transform, log, or wrap errors before they're returned.
func (cb *ConnectorBuilder) WithErrorHandler(handler func(error) error) *ConnectorBuilder {
	cb.errorHandler = handler
	return cb
}

// WithInitHook sets a custom initialization hook called during connector startup.
// Use this for custom setup like establishing connections or loading resources.
func (cb *ConnectorBuilder) WithInitHook(hook func(context.Context, *config.BaseConfig) error) *ConnectorBuilder {
	cb.initHook = hook
	return cb
}

// WithCloseHook sets a custom close hook called during connector shutdown.
// Use this for cleanup like closing connections or releasing resources.
func (cb *ConnectorBuilder) WithCloseHook(hook func(context.Context) error) *ConnectorBuilder {
	cb.closeHook = hook
	return cb
}

// WithHealthHook sets a custom health check hook for monitoring connector status.
// The hook should return an error if the connector is unhealthy.
func (cb *ConnectorBuilder) WithHealthHook(hook func(context.Context) error) *ConnectorBuilder {
	cb.healthHook = hook
	return cb
}

// WithConfigValidator sets a configuration validator to ensure config correctness.
// The validator is called after loading configuration and before initialization.
func (cb *ConnectorBuilder) WithConfigValidator(validator func(*config.BaseConfig) error) *ConnectorBuilder {
	cb.configValidator = validator
	return cb
}

// Validate validates the builder configuration to ensure all required fields are set.
// Returns an error if name, type, or version are missing or invalid.
func (cb *ConnectorBuilder) Validate() error {
	if cb.name == "" {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "connector name is required")
	}

	if cb.connectorType != core.ConnectorTypeSource && cb.connectorType != core.ConnectorTypeDestination {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "invalid connector type")
	}

	if cb.version == "" {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "connector version is required")
	}

	return nil
}

// CreateBaseConfig creates a base configuration with the builder's default values.
// The returned config can be further customized before using with the connector.
func (cb *ConnectorBuilder) CreateBaseConfig(name string) *config.BaseConfig {
	cfg := config.NewBaseConfig(name, string(cb.connectorType))
	cfg.Version = cb.version

	// Override with builder defaults
	if cb.defaultBatchSize > 0 {
		cfg.Performance.BatchSize = cb.defaultBatchSize
	}
	if cb.defaultBufferSize > 0 {
		cfg.Performance.BufferSize = cb.defaultBufferSize
	}
	if cb.defaultMaxConcurrency > 0 {
		cfg.Performance.MaxConcurrency = cb.defaultMaxConcurrency
	}
	if cb.defaultRequestTimeout > 0 {
		cfg.Timeouts.Request = cb.defaultRequestTimeout
	}
	if cb.defaultRetryAttempts > 0 {
		cfg.Reliability.RetryAttempts = cb.defaultRetryAttempts
	}
	if cb.defaultRetryDelay > 0 {
		cfg.Reliability.RetryDelay = cb.defaultRetryDelay
	}
	if cb.defaultRateLimit > 0 {
		cfg.Reliability.RateLimitPerSec = cb.defaultRateLimit
	}

	// Set default flush interval
	cfg.Performance.FlushInterval = 10 * time.Second

	return cfg
}

// CreateBaseConnector creates a BaseConnector instance with the builder's configuration.
// The BaseConnector provides common functionality like circuit breakers and rate limiting.
func (cb *ConnectorBuilder) CreateBaseConnector() *base.BaseConnector {
	baseConnector := base.NewBaseConnector(cb.name, cb.connectorType, cb.version)

	// Unified pool system is always enabled
	cb.logger.Debug("unified pool system enabled for connector", zap.String("name", cb.name))

	return baseConnector
}

// GetMetadata returns the connector's metadata for registration and discovery.
// This includes name, type, version, capabilities, and configuration schema.
func (cb *ConnectorBuilder) GetMetadata() *ConnectorMetadata {
	return &ConnectorMetadata{
		Name:         cb.name,
		Type:         cb.connectorType,
		Version:      cb.version,
		Description:  cb.description,
		Author:       cb.author,
		Capabilities: cb.capabilities,
		ConfigSchema: cb.configSchema,
	}
}

// ConnectorMetadata holds metadata about a connector for registration and discovery.
// This information is used by the connector registry and management tools.
type ConnectorMetadata struct {
	Name         string                 `json:"name"`
	Type         core.ConnectorType     `json:"type"`
	Version      string                 `json:"version"`
	Description  string                 `json:"description"`
	Author       string                 `json:"author"`
	Capabilities []string               `json:"capabilities"`
	ConfigSchema map[string]interface{} `json:"config_schema"`
}

// SourceBuilder provides specialized building for source connectors.
// It extends ConnectorBuilder with source-specific capabilities and hooks.
type SourceBuilder struct {
	*ConnectorBuilder

	// Source-specific capabilities
	supportsIncremental bool
	supportsRealtime    bool
	supportsBatch       bool
	supportsCDC         bool

	// Source-specific hooks
	discoverHook  func(context.Context) (*core.Schema, error)
	readHook      func(context.Context) (*core.RecordStream, error)
	readBatchHook func(context.Context, int) (*core.BatchStream, error)
	subscribeHook func(context.Context, []string) (*core.ChangeStream, error)

	// State management hooks
	getPositionHook func() core.Position
	setPositionHook func(core.Position) error
	getStateHook    func() core.State
	setStateHook    func(core.State) error
}

// NewSourceBuilder creates a new source builder with default source capabilities.
// The builder starts with batch support enabled by default.
func NewSourceBuilder() *SourceBuilder {
	cb := NewConnectorBuilder()
	cb.connectorType = core.ConnectorTypeSource

	return &SourceBuilder{
		ConnectorBuilder: cb,
		supportsBatch:    true, // Default capability
	}
}

// Override ConnectorBuilder methods to return SourceBuilder for method chaining

// WithName sets the connector name
func (sb *SourceBuilder) WithName(name string) *SourceBuilder {
	sb.ConnectorBuilder.WithName(name)
	return sb
}

// WithVersion sets the connector version
func (sb *SourceBuilder) WithVersion(version string) *SourceBuilder {
	sb.ConnectorBuilder.WithVersion(version)
	return sb
}

// WithDescription sets the connector description
func (sb *SourceBuilder) WithDescription(description string) *SourceBuilder {
	sb.ConnectorBuilder.WithDescription(description)
	return sb
}

// WithAuthor sets the connector author
func (sb *SourceBuilder) WithAuthor(author string) *SourceBuilder {
	sb.ConnectorBuilder.WithAuthor(author)
	return sb
}

// WithConfigProperty adds a single configuration property
func (sb *SourceBuilder) WithConfigProperty(name string, schema map[string]interface{}) *SourceBuilder {
	sb.ConnectorBuilder.WithConfigProperty(name, schema)
	return sb
}

// WithDefaults sets default configuration values
func (sb *SourceBuilder) WithDefaults(
	batchSize, bufferSize, maxConcurrency int,
	requestTimeout, retryDelay time.Duration,
	retryAttempts, rateLimit int) *SourceBuilder {

	sb.ConnectorBuilder.WithDefaults(batchSize, bufferSize, maxConcurrency, requestTimeout, retryDelay, retryAttempts, rateLimit)
	return sb
}

// WithConfigValidator sets a configuration validator
func (sb *SourceBuilder) WithConfigValidator(validator func(*config.BaseConfig) error) *SourceBuilder {
	sb.ConnectorBuilder.WithConfigValidator(validator)
	return sb
}

// WithCompression enables/disables compression with specified algorithm and level
func (sb *SourceBuilder) WithCompression(enabled bool, algorithm compression.Algorithm, level compression.Level) *SourceBuilder {
	sb.ConnectorBuilder.WithCompression(enabled, algorithm, level)
	return sb
}

// WithCompressionThreshold sets the minimum size threshold for compression
func (sb *SourceBuilder) WithCompressionThreshold(threshold int) *SourceBuilder {
	sb.ConnectorBuilder.WithCompressionThreshold(threshold)
	return sb
}

// WithInitHook sets a custom initialization hook
func (sb *SourceBuilder) WithInitHook(hook func(context.Context, *config.BaseConfig) error) *SourceBuilder {
	sb.ConnectorBuilder.WithInitHook(hook)
	return sb
}

// WithCloseHook sets a custom close hook
func (sb *SourceBuilder) WithCloseHook(hook func(context.Context) error) *SourceBuilder {
	sb.ConnectorBuilder.WithCloseHook(hook)
	return sb
}

// WithHealthHook sets a custom health check hook
func (sb *SourceBuilder) WithHealthHook(hook func(context.Context) error) *SourceBuilder {
	sb.ConnectorBuilder.WithHealthHook(hook)
	return sb
}

// WithCapabilities sets all capabilities at once
func (sb *SourceBuilder) WithCapabilities(capabilities ...string) *SourceBuilder {
	sb.ConnectorBuilder.WithCapabilities(capabilities...)
	return sb
}

// WithIncremental enables incremental sync support
func (sb *SourceBuilder) WithIncremental(supported bool) *SourceBuilder {
	sb.supportsIncremental = supported
	if supported {
		sb.WithCapability("incremental")
	}
	return sb
}

// WithRealtime enables real-time sync support
func (sb *SourceBuilder) WithRealtime(supported bool) *SourceBuilder {
	sb.supportsRealtime = supported
	if supported {
		sb.WithCapability("realtime")
	}
	return sb
}

// WithBatch enables batch processing support
func (sb *SourceBuilder) WithBatch(supported bool) *SourceBuilder {
	sb.supportsBatch = supported
	if supported {
		sb.WithCapability("batch")
	}
	return sb
}

// WithCDC enables Change Data Capture support
func (sb *SourceBuilder) WithCDC(supported bool) *SourceBuilder {
	sb.supportsCDC = supported
	if supported {
		sb.WithCapability("cdc")
	}
	return sb
}

// WithDiscoverHook sets the schema discovery hook
func (sb *SourceBuilder) WithDiscoverHook(hook func(context.Context) (*core.Schema, error)) *SourceBuilder {
	sb.discoverHook = hook
	return sb
}

// WithReadHook sets the record reading hook
func (sb *SourceBuilder) WithReadHook(hook func(context.Context) (*core.RecordStream, error)) *SourceBuilder {
	sb.readHook = hook
	return sb
}

// WithReadBatchHook sets the batch reading hook
func (sb *SourceBuilder) WithReadBatchHook(hook func(context.Context, int) (*core.BatchStream, error)) *SourceBuilder {
	sb.readBatchHook = hook
	return sb
}

// WithSubscribeHook sets the subscription hook for CDC
func (sb *SourceBuilder) WithSubscribeHook(hook func(context.Context, []string) (*core.ChangeStream, error)) *SourceBuilder {
	sb.subscribeHook = hook
	return sb
}

// WithStateManagement sets state management hooks
func (sb *SourceBuilder) WithStateManagement(
	getPosition func() core.Position,
	setPosition func(core.Position) error,
	getState func() core.State,
	setState func(core.State) error) *SourceBuilder {

	sb.getPositionHook = getPosition
	sb.setPositionHook = setPosition
	sb.getStateHook = getState
	sb.setStateHook = setState
	return sb
}

// DestinationBuilder provides specialized building for destination connectors.
// It extends ConnectorBuilder with destination-specific capabilities and hooks.
type DestinationBuilder struct {
	*ConnectorBuilder

	// Destination-specific capabilities
	supportsBulkLoad     bool
	supportsTransactions bool
	supportsUpsert       bool
	supportsBatch        bool
	supportsStreaming    bool

	// Destination-specific hooks
	createSchemaHook     func(context.Context, *core.Schema) error
	writeHook            func(context.Context, *core.RecordStream) error
	writeBatchHook       func(context.Context, *core.BatchStream) error
	bulkLoadHook         func(context.Context, interface{}, string) error
	beginTransactionHook func(context.Context) (core.Transaction, error)
	upsertHook           func(context.Context, []*models.Record, []string) error
	alterSchemaHook      func(context.Context, *core.Schema, *core.Schema) error
	dropSchemaHook       func(context.Context, *core.Schema) error
}

// NewDestinationBuilder creates a new destination builder with default capabilities.
// The builder starts with batch and streaming support enabled by default.
func NewDestinationBuilder() *DestinationBuilder {
	cb := NewConnectorBuilder()
	cb.connectorType = core.ConnectorTypeDestination

	return &DestinationBuilder{
		ConnectorBuilder:  cb,
		supportsBatch:     true, // Default capability
		supportsStreaming: true, // Default capability
	}
}

// Override ConnectorBuilder methods to return DestinationBuilder for method chaining

// WithName sets the connector name
func (db *DestinationBuilder) WithName(name string) *DestinationBuilder {
	db.ConnectorBuilder.WithName(name)
	return db
}

// WithVersion sets the connector version
func (db *DestinationBuilder) WithVersion(version string) *DestinationBuilder {
	db.ConnectorBuilder.WithVersion(version)
	return db
}

// WithDescription sets the connector description
func (db *DestinationBuilder) WithDescription(description string) *DestinationBuilder {
	db.ConnectorBuilder.WithDescription(description)
	return db
}

// WithAuthor sets the connector author
func (db *DestinationBuilder) WithAuthor(author string) *DestinationBuilder {
	db.ConnectorBuilder.WithAuthor(author)
	return db
}

// WithConfigProperty adds a single configuration property
func (db *DestinationBuilder) WithConfigProperty(name string, schema map[string]interface{}) *DestinationBuilder {
	db.ConnectorBuilder.WithConfigProperty(name, schema)
	return db
}

// WithDefaults sets default configuration values
func (db *DestinationBuilder) WithDefaults(
	batchSize, bufferSize, maxConcurrency int,
	requestTimeout, retryDelay time.Duration,
	retryAttempts, rateLimit int) *DestinationBuilder {

	db.ConnectorBuilder.WithDefaults(batchSize, bufferSize, maxConcurrency, requestTimeout, retryDelay, retryAttempts, rateLimit)
	return db
}

// WithConfigValidator sets a configuration validator
func (db *DestinationBuilder) WithConfigValidator(validator func(*config.BaseConfig) error) *DestinationBuilder {
	db.ConnectorBuilder.WithConfigValidator(validator)
	return db
}

// WithCompression enables/disables compression with specified algorithm and level
func (db *DestinationBuilder) WithCompression(enabled bool, algorithm compression.Algorithm, level compression.Level) *DestinationBuilder {
	db.ConnectorBuilder.WithCompression(enabled, algorithm, level)
	return db
}

// WithCompressionThreshold sets the minimum size threshold for compression
func (db *DestinationBuilder) WithCompressionThreshold(threshold int) *DestinationBuilder {
	db.ConnectorBuilder.WithCompressionThreshold(threshold)
	return db
}

// WithInitHook sets a custom initialization hook
func (db *DestinationBuilder) WithInitHook(hook func(context.Context, *config.BaseConfig) error) *DestinationBuilder {
	db.ConnectorBuilder.WithInitHook(hook)
	return db
}

// WithCloseHook sets a custom close hook
func (db *DestinationBuilder) WithCloseHook(hook func(context.Context) error) *DestinationBuilder {
	db.ConnectorBuilder.WithCloseHook(hook)
	return db
}

// WithHealthHook sets a custom health check hook
func (db *DestinationBuilder) WithHealthHook(hook func(context.Context) error) *DestinationBuilder {
	db.ConnectorBuilder.WithHealthHook(hook)
	return db
}

// WithCapabilities sets all capabilities at once
func (db *DestinationBuilder) WithCapabilities(capabilities ...string) *DestinationBuilder {
	db.ConnectorBuilder.WithCapabilities(capabilities...)
	return db
}

// WithBulkLoad enables bulk loading support
func (db *DestinationBuilder) WithBulkLoad(supported bool) *DestinationBuilder {
	db.supportsBulkLoad = supported
	if supported {
		db.WithCapability("bulk_load")
	}
	return db
}

// WithTransactions enables transaction support
func (db *DestinationBuilder) WithTransactions(supported bool) *DestinationBuilder {
	db.supportsTransactions = supported
	if supported {
		db.WithCapability("transactions")
	}
	return db
}

// WithUpsert enables upsert support
func (db *DestinationBuilder) WithUpsert(supported bool) *DestinationBuilder {
	db.supportsUpsert = supported
	if supported {
		db.WithCapability("upsert")
	}
	return db
}

// WithBatch enables batch processing support
func (db *DestinationBuilder) WithBatch(supported bool) *DestinationBuilder {
	db.supportsBatch = supported
	if supported {
		db.WithCapability("batch")
	}
	return db
}

// WithStreaming enables streaming support
func (db *DestinationBuilder) WithStreaming(supported bool) *DestinationBuilder {
	db.supportsStreaming = supported
	if supported {
		db.WithCapability("streaming")
	}
	return db
}

// WithCreateSchemaHook sets the schema creation hook
func (db *DestinationBuilder) WithCreateSchemaHook(hook func(context.Context, *core.Schema) error) *DestinationBuilder {
	db.createSchemaHook = hook
	return db
}

// WithWriteHook sets the record writing hook
func (db *DestinationBuilder) WithWriteHook(hook func(context.Context, *core.RecordStream) error) *DestinationBuilder {
	db.writeHook = hook
	return db
}

// WithWriteBatchHook sets the batch writing hook
func (db *DestinationBuilder) WithWriteBatchHook(hook func(context.Context, *core.BatchStream) error) *DestinationBuilder {
	db.writeBatchHook = hook
	return db
}

// WithBulkLoadHook sets the bulk loading hook
func (db *DestinationBuilder) WithBulkLoadHook(hook func(context.Context, interface{}, string) error) *DestinationBuilder {
	db.bulkLoadHook = hook
	return db
}

// WithTransactionHook sets the transaction management hook
func (db *DestinationBuilder) WithTransactionHook(hook func(context.Context) (core.Transaction, error)) *DestinationBuilder {
	db.beginTransactionHook = hook
	return db
}

// WithUpsertHook sets the upsert hook
func (db *DestinationBuilder) WithUpsertHook(hook func(context.Context, []*models.Record, []string) error) *DestinationBuilder {
	db.upsertHook = hook
	return db
}

// WithSchemaManagement sets schema management hooks
func (db *DestinationBuilder) WithSchemaManagement(
	alterSchema func(context.Context, *core.Schema, *core.Schema) error,
	dropSchema func(context.Context, *core.Schema) error) *DestinationBuilder {

	db.alterSchemaHook = alterSchema
	db.dropSchemaHook = dropSchema
	return db
}
