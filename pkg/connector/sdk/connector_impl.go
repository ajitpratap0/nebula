package sdk

import (
	"context"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/base"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/errors"
	"github.com/ajitpratap0/nebula/pkg/models"
	"go.uber.org/zap"
)

// SDKSourceConnector implements the core.Source interface using the SDK builder pattern
type SDKSourceConnector struct {
	*base.BaseConnector
	builder *SourceBuilder
	// OptimizationLayer removed - using unified pool system
}

// NewSDKSourceConnector creates a new SDK-based source connector
func NewSDKSourceConnector(builder *SourceBuilder) (*SDKSourceConnector, error) {
	if err := builder.Validate(); err != nil {
		return nil, err
	}

	baseConnector := builder.CreateBaseConnector()

	connector := &SDKSourceConnector{
		BaseConnector: baseConnector,
		builder:       builder,
	}

	// Optimization layer removed - using unified pool system for memory management

	return connector, nil
}

// Initialize initializes the source connector
func (sc *SDKSourceConnector) Initialize(ctx context.Context, config *config.BaseConfig) error {
	// Initialize base connector
	if err := sc.BaseConnector.Initialize(ctx, config); err != nil {
		return err
	}

	// Optimization layer removed - using unified pool system for memory management

	// Validate configuration if validator is provided
	if sc.builder.configValidator != nil {
		if err := sc.builder.configValidator(config); err != nil {
			return errors.Wrap(err, errors.ErrorTypeConfig, "configuration validation failed")
		}
	}

	// Call custom initialization hook if provided
	if sc.builder.initHook != nil {
		return sc.builder.initHook(ctx, config)
	}

	return nil
}

// Discover discovers the schema
func (sc *SDKSourceConnector) Discover(ctx context.Context) (*core.Schema, error) {
	if sc.builder.discoverHook == nil {
		return nil, errors.New(errors.ErrorTypeCapability, "schema discovery not implemented")
	}
	return sc.builder.discoverHook(ctx)
}

// Read reads records from the source
func (sc *SDKSourceConnector) Read(ctx context.Context) (*core.RecordStream, error) {
	if sc.builder.readHook == nil {
		return nil, errors.New(errors.ErrorTypeCapability, "record reading not implemented")
	}
	return sc.builder.readHook(ctx)
}

// ReadBatch reads records in batches
func (sc *SDKSourceConnector) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	if sc.builder.readBatchHook == nil {
		return nil, errors.New(errors.ErrorTypeCapability, "batch reading not implemented")
	}
	return sc.builder.readBatchHook(ctx, batchSize)
}

// GetPosition returns the current position
func (sc *SDKSourceConnector) GetPosition() core.Position {
	if sc.builder.getPositionHook != nil {
		return sc.builder.getPositionHook()
	}
	return sc.BaseConnector.GetPosition()
}

// SetPosition sets the current position
func (sc *SDKSourceConnector) SetPosition(position core.Position) error {
	if sc.builder.setPositionHook != nil {
		return sc.builder.setPositionHook(position)
	}
	return sc.BaseConnector.SetPosition(position)
}

// GetState returns the current state
func (sc *SDKSourceConnector) GetState() core.State {
	if sc.builder.getStateHook != nil {
		return sc.builder.getStateHook()
	}
	return sc.BaseConnector.GetState()
}

// SetState sets the current state
func (sc *SDKSourceConnector) SetState(state core.State) error {
	if sc.builder.setStateHook != nil {
		return sc.builder.setStateHook(state)
	}
	return sc.BaseConnector.SetState(state)
}

// SupportsIncremental returns whether incremental sync is supported
func (sc *SDKSourceConnector) SupportsIncremental() bool {
	return sc.builder.supportsIncremental
}

// SupportsRealtime returns whether real-time sync is supported
func (sc *SDKSourceConnector) SupportsRealtime() bool {
	return sc.builder.supportsRealtime
}

// SupportsBatch returns whether batch processing is supported
func (sc *SDKSourceConnector) SupportsBatch() bool {
	return sc.builder.supportsBatch
}

// Subscribe subscribes to real-time changes
func (sc *SDKSourceConnector) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	if !sc.builder.supportsRealtime || !sc.builder.supportsCDC {
		return nil, errors.New(errors.ErrorTypeCapability, "real-time subscription not supported")
	}

	if sc.builder.subscribeHook == nil {
		return nil, errors.New(errors.ErrorTypeCapability, "subscription not implemented")
	}

	return sc.builder.subscribeHook(ctx, tables)
}

// Close closes the source connector
func (sc *SDKSourceConnector) Close(ctx context.Context) error {
	// Call custom close hook if provided
	if sc.builder.closeHook != nil {
		if err := sc.builder.closeHook(ctx); err != nil {
			sc.GetLogger().Error("custom close hook failed", zap.Error(err))
		}
	}

	// Optimization layer removed - using unified pool system for memory management

	// Close base connector
	return sc.BaseConnector.Close(ctx)
}

// Health performs a health check
func (sc *SDKSourceConnector) Health(ctx context.Context) error {
	// Check base health first
	if err := sc.BaseConnector.Health(ctx); err != nil {
		return err
	}

	// Call custom health hook if provided
	if sc.builder.healthHook != nil {
		return sc.builder.healthHook(ctx)
	}

	return nil
}

// GetOptimizationLayer removed - using unified pool system for memory management

// SDKDestinationConnector implements the core.Destination interface using the SDK builder pattern
type SDKDestinationConnector struct {
	*base.BaseConnector
	builder *DestinationBuilder
	// OptimizationLayer removed - using unified pool system
}

// NewSDKDestinationConnector creates a new SDK-based destination connector
func NewSDKDestinationConnector(builder *DestinationBuilder) (*SDKDestinationConnector, error) {
	if err := builder.Validate(); err != nil {
		return nil, err
	}

	baseConnector := builder.CreateBaseConnector()

	connector := &SDKDestinationConnector{
		BaseConnector: baseConnector,
		builder:       builder,
	}

	// Optimization layer removed - using unified pool system for memory management

	return connector, nil
}

// Initialize initializes the destination connector
func (dc *SDKDestinationConnector) Initialize(ctx context.Context, config *config.BaseConfig) error {
	// Initialize base connector
	if err := dc.BaseConnector.Initialize(ctx, config); err != nil {
		return err
	}

	// Optimization layer removed - using unified pool system for memory management

	// Validate configuration if validator is provided
	if dc.builder.configValidator != nil {
		if err := dc.builder.configValidator(config); err != nil {
			return errors.Wrap(err, errors.ErrorTypeConfig, "configuration validation failed")
		}
	}

	// Call custom initialization hook if provided
	if dc.builder.initHook != nil {
		return dc.builder.initHook(ctx, config)
	}

	return nil
}

// CreateSchema creates a schema
func (dc *SDKDestinationConnector) CreateSchema(ctx context.Context, schema *core.Schema) error {
	if dc.builder.createSchemaHook == nil {
		return errors.New(errors.ErrorTypeCapability, "schema creation not implemented")
	}
	return dc.builder.createSchemaHook(ctx, schema)
}

// Write writes records to the destination
func (dc *SDKDestinationConnector) Write(ctx context.Context, stream *core.RecordStream) error {
	if dc.builder.writeHook == nil {
		return errors.New(errors.ErrorTypeCapability, "record writing not implemented")
	}
	return dc.builder.writeHook(ctx, stream)
}

// WriteBatch writes batches of records
func (dc *SDKDestinationConnector) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	if dc.builder.writeBatchHook == nil {
		return errors.New(errors.ErrorTypeCapability, "batch writing not implemented")
	}
	return dc.builder.writeBatchHook(ctx, stream)
}

// SupportsBulkLoad returns whether bulk loading is supported
func (dc *SDKDestinationConnector) SupportsBulkLoad() bool {
	return dc.builder.supportsBulkLoad
}

// SupportsTransactions returns whether transactions are supported
func (dc *SDKDestinationConnector) SupportsTransactions() bool {
	return dc.builder.supportsTransactions
}

// SupportsUpsert returns whether upsert operations are supported
func (dc *SDKDestinationConnector) SupportsUpsert() bool {
	return dc.builder.supportsUpsert
}

// SupportsBatch returns whether batch operations are supported
func (dc *SDKDestinationConnector) SupportsBatch() bool {
	return dc.builder.supportsBatch
}

// SupportsStreaming returns whether streaming is supported
func (dc *SDKDestinationConnector) SupportsStreaming() bool {
	return dc.builder.supportsStreaming
}

// BulkLoad performs bulk loading
func (dc *SDKDestinationConnector) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	if !dc.builder.supportsBulkLoad || dc.builder.bulkLoadHook == nil {
		return errors.New(errors.ErrorTypeCapability, "bulk loading not supported")
	}
	return dc.builder.bulkLoadHook(ctx, reader, format)
}

// BeginTransaction begins a transaction
func (dc *SDKDestinationConnector) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	if !dc.builder.supportsTransactions || dc.builder.beginTransactionHook == nil {
		return nil, errors.New(errors.ErrorTypeCapability, "transactions not supported")
	}
	return dc.builder.beginTransactionHook(ctx)
}

// Upsert performs upsert operations
func (dc *SDKDestinationConnector) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	if !dc.builder.supportsUpsert || dc.builder.upsertHook == nil {
		return errors.New(errors.ErrorTypeCapability, "upsert operations not supported")
	}
	return dc.builder.upsertHook(ctx, records, keys)
}

// AlterSchema alters the schema
func (dc *SDKDestinationConnector) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	if dc.builder.alterSchemaHook == nil {
		return errors.New(errors.ErrorTypeCapability, "schema alteration not implemented")
	}
	return dc.builder.alterSchemaHook(ctx, oldSchema, newSchema)
}

// DropSchema drops the schema
func (dc *SDKDestinationConnector) DropSchema(ctx context.Context, schema *core.Schema) error {
	if dc.builder.dropSchemaHook == nil {
		return errors.New(errors.ErrorTypeCapability, "schema dropping not implemented")
	}
	return dc.builder.dropSchemaHook(ctx, schema)
}

// Close closes the destination connector
func (dc *SDKDestinationConnector) Close(ctx context.Context) error {
	// Call custom close hook if provided
	if dc.builder.closeHook != nil {
		if err := dc.builder.closeHook(ctx); err != nil {
			dc.GetLogger().Error("custom close hook failed", zap.Error(err))
		}
	}

	// Optimization layer removed - using unified pool system for memory management

	// Close base connector
	return dc.BaseConnector.Close(ctx)
}

// Health performs a health check
func (dc *SDKDestinationConnector) Health(ctx context.Context) error {
	// Check base health first
	if err := dc.BaseConnector.Health(ctx); err != nil {
		return err
	}

	// Call custom health hook if provided
	if dc.builder.healthHook != nil {
		return dc.builder.healthHook(ctx)
	}

	return nil
}

// GetOptimizationLayer removed - using unified pool system for memory management
