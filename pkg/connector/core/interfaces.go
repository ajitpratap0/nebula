// Package core defines the core interfaces and types for Nebula connectors.
// It provides a unified abstraction for all data sources and destinations,
// enabling consistent behavior across different connector implementations.
//
// The package includes:
//   - Source and Destination interfaces for data integration
//   - Schema definitions for data structure management
//   - Streaming interfaces for real-time data processing
//   - Health monitoring and metrics collection
//   - Advanced features like CDC, transactions, and bulk loading
//
// All connectors must implement these interfaces to integrate with Nebula's
// high-performance data pipeline architecture.
package core

import (
	"context"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// ConnectorType represents the type of connector (source or destination).
type ConnectorType string

const (
	// ConnectorTypeSource indicates a source connector that reads data
	ConnectorTypeSource      ConnectorType = "source"
	// ConnectorTypeDestination indicates a destination connector that writes data
	ConnectorTypeDestination ConnectorType = "destination"
)

// State represents connector state for resumable operations.
// It stores arbitrary key-value pairs that connectors can use to
// track their progress and resume from the last known position.
type State map[string]interface{}

// Position represents a position in the data stream for incremental reads.
// Implementations should be comparable and serializable to support
// checkpoint/resume functionality.
type Position interface {
	// String returns a string representation of the position
	String() string
	// Compare returns -1 if this < other, 0 if equal, 1 if this > other
	Compare(other Position) int
}

// Schema represents the structure of data being processed.
// It includes metadata about the schema version and lifecycle.
type Schema struct {
	// Name is the schema identifier (e.g., table name, collection name)
	Name        string
	// Description provides human-readable information about the schema
	Description string
	// Fields defines the structure of the data
	Fields      []Field
	// Version tracks schema evolution
	Version     int
	// CreatedAt is when the schema was first created
	CreatedAt   time.Time
	// UpdatedAt is when the schema was last modified
	UpdatedAt   time.Time
}

// Field represents a field in the schema with type and constraint information.
type Field struct {
	// Name is the field identifier
	Name        string
	// Type specifies the data type
	Type        FieldType
	// Description provides human-readable field information
	Description string
	// Nullable indicates if the field can contain null values
	Nullable    bool
	// Primary indicates if this field is part of the primary key
	Primary     bool
	// Unique indicates if values must be unique
	Unique      bool
	// Default specifies the default value if not provided
	Default     interface{}
}

// FieldType represents the data type of a field.
// These types map to common data types across different systems.
type FieldType string

const (
	// FieldTypeString represents text data
	FieldTypeString    FieldType = "string"
	// FieldTypeInt represents integer numbers
	FieldTypeInt       FieldType = "int"
	// FieldTypeFloat represents floating-point numbers
	FieldTypeFloat     FieldType = "float"
	// FieldTypeBool represents boolean values
	FieldTypeBool      FieldType = "bool"
	// FieldTypeTimestamp represents date and time with timezone
	FieldTypeTimestamp FieldType = "timestamp"
	// FieldTypeDate represents date without time
	FieldTypeDate      FieldType = "date"
	// FieldTypeTime represents time without date
	FieldTypeTime      FieldType = "time"
	// FieldTypeJSON represents JSON structured data
	FieldTypeJSON      FieldType = "json"
	// FieldTypeBinary represents binary data
	FieldTypeBinary    FieldType = "binary"
)


// RecordStream represents a stream of individual records for real-time processing.
// It provides separate channels for records and errors to enable concurrent
// processing while handling errors gracefully.
type RecordStream struct {
	// Records channel delivers individual records
	Records <-chan *pool.Record
	// Errors channel delivers any errors encountered during streaming
	Errors  <-chan error
}

// BatchStream represents a stream of record batches for efficient bulk processing.
// Batching improves throughput by amortizing overhead across multiple records.
type BatchStream struct {
	// Batches channel delivers slices of records
	Batches <-chan []*pool.Record
	// Errors channel delivers any errors encountered during streaming
	Errors  <-chan error
}

// ChangeStream represents a stream of change events for CDC (Change Data Capture).
// It enables real-time synchronization by capturing database changes as they occur.
type ChangeStream struct {
	// Changes channel delivers CDC events
	Changes <-chan *ChangeEvent
	// Errors channel delivers any errors encountered during streaming
	Errors  <-chan error
}

// ChangeEvent represents a change data capture event with before/after states.
// It captures the complete context of a database change for accurate replication.
type ChangeEvent struct {
	// Type indicates the kind of change (insert, update, delete)
	Type      ChangeType
	// Table identifies where the change occurred
	Table     string
	// Timestamp indicates when the change happened
	Timestamp time.Time
	// Position enables resumable CDC by tracking progress
	Position  Position
	// Before contains the record state before the change (nil for inserts)
	Before    map[string]interface{}
	// After contains the record state after the change (nil for deletes)
	After     map[string]interface{}
}

// ChangeType represents the type of database change in CDC.
type ChangeType string

const (
	// ChangeTypeInsert indicates a new record was added
	ChangeTypeInsert ChangeType = "insert"
	// ChangeTypeUpdate indicates an existing record was modified
	ChangeTypeUpdate ChangeType = "update"
	// ChangeTypeDelete indicates a record was removed
	ChangeTypeDelete ChangeType = "delete"
)

// Transaction represents a database transaction for atomic operations.
// Implementations must ensure ACID properties are maintained.
type Transaction interface {
	// Commit finalizes all operations in the transaction
	Commit(ctx context.Context) error
	// Rollback cancels all operations in the transaction
	Rollback(ctx context.Context) error
}

// Source is the interface that all source connectors must implement.
// It defines methods for reading data from external systems with support
// for batch processing, streaming, CDC, and incremental synchronization.
type Source interface {
	// Core functionality
	
	// Initialize prepares the source connector with configuration
	Initialize(ctx context.Context, config *config.BaseConfig) error
	
	// Discover retrieves the schema of available data
	Discover(ctx context.Context) (*Schema, error)
	
	// Read starts streaming individual records
	Read(ctx context.Context) (*RecordStream, error)
	
	// ReadBatch reads records in batches for improved efficiency
	ReadBatch(ctx context.Context, batchSize int) (*BatchStream, error)
	
	// Close cleanly shuts down the connector and releases resources
	Close(ctx context.Context) error

	// State management for resumable operations
	
	// GetPosition returns the current read position
	GetPosition() Position
	
	// SetPosition sets the read position for incremental sync
	SetPosition(position Position) error
	
	// GetState returns the full connector state
	GetState() State
	
	// SetState restores connector state from a previous run
	SetState(state State) error

	// Capabilities indicate what features the source supports
	
	// SupportsIncremental indicates if incremental sync is available
	SupportsIncremental() bool
	
	// SupportsRealtime indicates if real-time streaming is available
	SupportsRealtime() bool
	
	// SupportsBatch indicates if batch reading is available
	SupportsBatch() bool

	// Real-time/CDC support
	
	// Subscribe starts CDC streaming for specified tables
	Subscribe(ctx context.Context, tables []string) (*ChangeStream, error)

	// Health and metrics
	
	// Health checks if the source is operational
	Health(ctx context.Context) error
	
	// Metrics returns performance and operational metrics
	Metrics() map[string]interface{}
}

// Destination is the interface that all destination connectors must implement.
// It defines methods for writing data to external systems with support for
// streaming, batching, transactions, bulk loading, and schema management.
type Destination interface {
	// Core functionality
	
	// Initialize prepares the destination connector with configuration
	Initialize(ctx context.Context, config *config.BaseConfig) error
	
	// CreateSchema creates the target schema in the destination
	CreateSchema(ctx context.Context, schema *Schema) error
	
	// Write processes a stream of individual records
	Write(ctx context.Context, stream *RecordStream) error
	
	// WriteBatch processes batches of records for improved efficiency
	WriteBatch(ctx context.Context, stream *BatchStream) error
	
	// Close cleanly shuts down the connector and releases resources
	Close(ctx context.Context) error

	// Capabilities indicate what features the destination supports
	
	// SupportsBulkLoad indicates if bulk loading is available
	SupportsBulkLoad() bool
	
	// SupportsTransactions indicates if transactional writes are supported
	SupportsTransactions() bool
	
	// SupportsUpsert indicates if upsert operations are supported
	SupportsUpsert() bool
	
	// SupportsBatch indicates if batch writing is available
	SupportsBatch() bool
	
	// SupportsStreaming indicates if streaming writes are supported
	SupportsStreaming() bool

	// Advanced operations
	
	// BulkLoad performs high-speed data loading from files or readers
	BulkLoad(ctx context.Context, reader interface{}, format string) error
	
	// BeginTransaction starts a new transaction for atomic operations
	BeginTransaction(ctx context.Context) (Transaction, error)
	
	// Upsert performs insert-or-update operations based on key columns
	Upsert(ctx context.Context, records []*pool.Record, keys []string) error

	// Schema operations
	
	// AlterSchema modifies an existing schema structure
	AlterSchema(ctx context.Context, oldSchema, newSchema *Schema) error
	
	// DropSchema removes a schema from the destination
	DropSchema(ctx context.Context, schema *Schema) error

	// Health and metrics
	
	// Health checks if the destination is operational
	Health(ctx context.Context) error
	
	// Metrics returns performance and operational metrics
	Metrics() map[string]interface{}
}

// Connector is the base interface for all connectors.
// Both Source and Destination connectors should embed this interface
// to provide consistent metadata and lifecycle management.
type Connector interface {
	// Metadata
	
	// Name returns the connector's unique identifier
	Name() string
	
	// Type returns whether this is a source or destination
	Type() ConnectorType
	
	// Version returns the connector's version string
	Version() string

	// Lifecycle
	
	// Initialize prepares the connector with configuration
	Initialize(ctx context.Context, config *config.BaseConfig) error
	
	// Close cleanly shuts down the connector
	Close(ctx context.Context) error

	// Health and monitoring
	
	// Health checks connector operational status
	Health(ctx context.Context) error
	
	// Metrics returns performance and operational metrics
	Metrics() map[string]interface{}
}

// ConnectorFactory creates connector instances dynamically.
// It provides a registry of available connectors and instantiation methods.
type ConnectorFactory interface {
	// CreateSource instantiates a source connector by type
	CreateSource(connectorType string, config *config.BaseConfig) (Source, error)
	
	// CreateDestination instantiates a destination connector by type
	CreateDestination(connectorType string, config *config.BaseConfig) (Destination, error)
	
	// ListSources returns available source connector types
	ListSources() []string
	
	// ListDestinations returns available destination connector types
	ListDestinations() []string
}

// HealthStatus represents the health status of a connector.
// It provides detailed information about the connector's operational state.
type HealthStatus struct {
	// Status indicates the overall health ("healthy", "unhealthy", "degraded")
	Status    string                 `json:"status"`
	// Timestamp of the health check
	Timestamp time.Time              `json:"timestamp"`
	// Details provides additional health information
	Details   map[string]interface{} `json:"details"`
	// Error contains any error information if unhealthy
	Error     error                  `json:"error,omitempty"`
}

// MetricType represents the type of metric for monitoring.
type MetricType string

const (
	// MetricTypeCounter is for monotonically increasing values
	MetricTypeCounter   MetricType = "counter"
	// MetricTypeGauge is for values that can go up or down
	MetricTypeGauge     MetricType = "gauge"
	// MetricTypeHistogram is for distributions of values
	MetricTypeHistogram MetricType = "histogram"
	// MetricTypeSummary is for statistical summaries
	MetricTypeSummary   MetricType = "summary"
)

// Metric represents a single metric measurement.
// Metrics are used for monitoring connector performance and behavior.
type Metric struct {
	// Name identifies the metric
	Name        string            `json:"name"`
	// Type specifies the metric type
	Type        MetricType        `json:"type"`
	// Value contains the metric measurement
	Value       interface{}       `json:"value"`
	// Labels provide dimensional metadata
	Labels      map[string]string `json:"labels"`
	// Timestamp indicates when the metric was collected
	Timestamp   time.Time         `json:"timestamp"`
	// Description explains what the metric measures
	Description string            `json:"description"`
}

// StreamController provides control over data streams.
// It enables pausing, resuming, and canceling streaming operations.
type StreamController interface {
	// Pause temporarily stops the stream
	Pause() error
	// Resume continues a paused stream
	Resume() error
	// Cancel permanently stops the stream
	Cancel() error
	// Status returns the current stream state
	Status() string
}

// PositionManager manages position/checkpoint state for resumable operations.
// It enables connectors to track progress and recover from failures.
type PositionManager interface {
	// SavePosition persists the current position
	SavePosition(ctx context.Context, position Position) error
	// LoadPosition retrieves the last saved position
	LoadPosition(ctx context.Context) (Position, error)
	// ResetPosition clears the saved position
	ResetPosition(ctx context.Context) error
}

// SchemaRegistry manages schema versions for evolution support.
// It tracks schema changes over time and enables compatibility checking.
type SchemaRegistry interface {
	// RegisterSchema adds a new schema version
	RegisterSchema(ctx context.Context, schema *Schema) error
	// GetSchema retrieves a specific schema version
	GetSchema(ctx context.Context, name string, version int) (*Schema, error)
	// ListSchemas returns all registered schemas
	ListSchemas(ctx context.Context) ([]*Schema, error)
	// GetLatestSchema returns the most recent schema version
	GetLatestSchema(ctx context.Context, name string) (*Schema, error)
}

// ErrorHandler defines how connectors handle errors.
// It provides retry logic and error recording capabilities.
type ErrorHandler interface {
	// HandleError processes an error with optional record context
	HandleError(ctx context.Context, err error, record *pool.Record) error
	// ShouldRetry determines if an error is retryable
	ShouldRetry(err error) bool
	// GetRetryDelay calculates backoff delay for retries
	GetRetryDelay(attempt int) time.Duration
	// RecordError logs error details for analysis
	RecordError(err error, details map[string]interface{})
}

// RateLimiter provides rate limiting capabilities.
// It helps prevent overwhelming external systems with requests.
type RateLimiter interface {
	// Allow checks if a request can proceed
	Allow() bool
	// Wait blocks until rate limit allows proceeding
	Wait(ctx context.Context) error
	// Limit returns the current rate limit
	Limit() int
	// SetLimit updates the rate limit
	SetLimit(limit int)
}

// CircuitBreaker provides circuit breaker pattern implementation.
// It prevents cascading failures by failing fast when errors exceed thresholds.
type CircuitBreaker interface {
	// Allow checks if requests can proceed
	Allow() error
	// MarkSuccess records a successful operation
	MarkSuccess()
	// MarkFailed records a failed operation
	MarkFailed()
	// State returns the current circuit state (open/closed/half-open)
	State() string
	// Reset manually resets the circuit breaker
	Reset()
}

// ConnectionPool manages connection pooling for efficient resource usage.
// It reuses connections to reduce overhead and improve performance.
type ConnectionPool interface {
	// Get retrieves a connection from the pool
	Get(ctx context.Context) (interface{}, error)
	// Put returns a connection to the pool
	Put(conn interface{}) error
	// Close shuts down the pool and all connections
	Close() error
	// Stats returns pool usage statistics
	Stats() PoolStats
}

// PoolStats represents connection pool statistics.
type PoolStats struct {
	// Active is the number of connections in use
	Active   int
	// Idle is the number of available connections
	Idle     int
	// Total is the total number of connections
	Total    int
	// MaxSize is the maximum pool size
	MaxSize  int
	// Waits is the number of times waited for a connection
	Waits    int64
	// Timeouts is the number of wait timeouts
	Timeouts int64
}

// BatchBuilder helps build record batches efficiently.
// It accumulates records until batch size limits are reached.
type BatchBuilder interface {
	// Add appends a record to the batch
	Add(record *pool.Record) error
	// Build returns the completed batch
	Build() ([]*pool.Record, error)
	// Reset clears the batch builder
	Reset()
	// Size returns the current batch size
	Size() int
	// IsFull checks if the batch is at capacity
	IsFull() bool
}

// Optimizer provides performance optimization hints.
// It analyzes metrics to suggest configuration improvements.
type Optimizer interface {
	// OptimizeBatchSize suggests optimal batch size based on metrics
	OptimizeBatchSize(current int, metrics map[string]interface{}) int
	// OptimizeConcurrency suggests optimal worker count
	OptimizeConcurrency(current int, metrics map[string]interface{}) int
	// OptimizeBufferSize suggests optimal buffer size
	OptimizeBufferSize(current int, metrics map[string]interface{}) int
	// SuggestOptimizations provides general optimization recommendations
	SuggestOptimizations(metrics map[string]interface{}) []string
}

// ProgressReporter reports progress of operations.
// It enables monitoring of long-running data transfers.
type ProgressReporter interface {
	// ReportProgress updates processed/total counts
	ReportProgress(processed int64, total int64)
	// ReportThroughput updates processing speed
	ReportThroughput(recordsPerSecond float64)
	// ReportLatency updates processing latency
	ReportLatency(latency time.Duration)
	// GetProgress returns current progress
	GetProgress() (processed int64, total int64)
}

// DataQualityChecker performs data quality checks.
// It validates records against defined rules and constraints.
type DataQualityChecker interface {
	// CheckRecord validates a single record
	CheckRecord(record *pool.Record) error
	// CheckBatch validates multiple records
	CheckBatch(records []*pool.Record) []error
	// GetQualityMetrics returns quality statistics
	GetQualityMetrics() map[string]interface{}
	// SetRules configures validation rules
	SetRules(rules map[string]interface{})
}

// TransformFunc is a function that transforms records.
// It modifies record data or structure during processing.
type TransformFunc func(record *pool.Record) (*pool.Record, error)

// FilterFunc is a function that filters records.
// It returns true to keep the record, false to drop it.
type FilterFunc func(record *pool.Record) bool

// Pipeline represents a data processing pipeline.
// It chains transforms and filters for complex processing logic.
type Pipeline interface {
	// AddTransform adds a transformation step
	AddTransform(name string, transform TransformFunc)
	// AddFilter adds a filtering step
	AddFilter(name string, filter FilterFunc)
	// Process applies the pipeline to a record stream
	Process(ctx context.Context, input *RecordStream) (*RecordStream, error)
	// ProcessBatch applies the pipeline to a batch stream
	ProcessBatch(ctx context.Context, input *BatchStream) (*BatchStream, error)
}

// ConnectorMetadata provides metadata about a connector.
// It describes connector capabilities and configuration requirements.
type ConnectorMetadata struct {
	// Name is the connector identifier
	Name          string                 `json:"name"`
	// Type indicates source or destination
	Type          ConnectorType          `json:"type"`
	// Version is the connector version
	Version       string                 `json:"version"`
	// Description explains the connector's purpose
	Description   string                 `json:"description"`
	// Author identifies the connector creator
	Author        string                 `json:"author"`
	// Documentation links to detailed docs
	Documentation string                 `json:"documentation"`
	// Capabilities lists supported features
	Capabilities  []string               `json:"capabilities"`
	// ConfigSchema describes configuration options
	ConfigSchema  map[string]interface{} `json:"config_schema"`
}

// ConnectorRegistry manages available connectors.
// It provides discovery and instantiation of connector types.
type ConnectorRegistry interface {
	// Register adds a new connector type
	Register(name string, factory func() Connector) error
	// Get retrieves a connector by name
	Get(name string) (Connector, error)
	// List returns all available connectors
	List() []ConnectorMetadata
	// Exists checks if a connector is registered
	Exists(name string) bool
}
