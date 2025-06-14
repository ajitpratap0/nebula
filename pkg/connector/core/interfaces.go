package core

import (
	"context"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// ConnectorType represents the type of connector
type ConnectorType string

const (
	ConnectorTypeSource      ConnectorType = "source"
	ConnectorTypeDestination ConnectorType = "destination"
)

// State represents connector state
type State map[string]interface{}

// Position represents a position in the data stream
type Position interface {
	// String returns a string representation of the position
	String() string
	// Compare returns -1 if this < other, 0 if equal, 1 if this > other
	Compare(other Position) int
}

// Schema represents the data schema
type Schema struct {
	Name        string
	Description string
	Fields      []Field
	Version     int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Field represents a field in the schema
type Field struct {
	Name        string
	Type        FieldType
	Description string
	Nullable    bool
	Primary     bool
	Unique      bool
	Default     interface{}
}

// FieldType represents the data type of a field
type FieldType string

const (
	FieldTypeString    FieldType = "string"
	FieldTypeInt       FieldType = "int"
	FieldTypeFloat     FieldType = "float"
	FieldTypeBool      FieldType = "bool"
	FieldTypeTimestamp FieldType = "timestamp"
	FieldTypeDate      FieldType = "date"
	FieldTypeTime      FieldType = "time"
	FieldTypeJSON      FieldType = "json"
	FieldTypeBinary    FieldType = "binary"
)


// RecordStream represents a stream of records
type RecordStream struct {
	Records <-chan *pool.Record
	Errors  <-chan error
}

// BatchStream represents a stream of record batches
type BatchStream struct {
	Batches <-chan []*pool.Record
	Errors  <-chan error
}

// ChangeStream represents a stream of change events (for CDC)
type ChangeStream struct {
	Changes <-chan *ChangeEvent
	Errors  <-chan error
}

// ChangeEvent represents a change data capture event
type ChangeEvent struct {
	Type      ChangeType
	Table     string
	Timestamp time.Time
	Position  Position
	Before    map[string]interface{}
	After     map[string]interface{}
}

// ChangeType represents the type of change
type ChangeType string

const (
	ChangeTypeInsert ChangeType = "insert"
	ChangeTypeUpdate ChangeType = "update"
	ChangeTypeDelete ChangeType = "delete"
)

// Transaction represents a database transaction
type Transaction interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Source is the interface that all source connectors must implement
type Source interface {
	// Core functionality
	Initialize(ctx context.Context, config *config.BaseConfig) error
	Discover(ctx context.Context) (*Schema, error)
	Read(ctx context.Context) (*RecordStream, error)
	ReadBatch(ctx context.Context, batchSize int) (*BatchStream, error)
	Close(ctx context.Context) error

	// State management
	GetPosition() Position
	SetPosition(position Position) error
	GetState() State
	SetState(state State) error

	// Capabilities
	SupportsIncremental() bool
	SupportsRealtime() bool
	SupportsBatch() bool

	// Real-time/CDC support
	Subscribe(ctx context.Context, tables []string) (*ChangeStream, error)

	// Health and metrics
	Health(ctx context.Context) error
	Metrics() map[string]interface{}
}

// Destination is the interface that all destination connectors must implement
type Destination interface {
	// Core functionality
	Initialize(ctx context.Context, config *config.BaseConfig) error
	CreateSchema(ctx context.Context, schema *Schema) error
	Write(ctx context.Context, stream *RecordStream) error
	WriteBatch(ctx context.Context, stream *BatchStream) error
	Close(ctx context.Context) error

	// Capabilities
	SupportsBulkLoad() bool
	SupportsTransactions() bool
	SupportsUpsert() bool
	SupportsBatch() bool
	SupportsStreaming() bool

	// Advanced operations
	BulkLoad(ctx context.Context, reader interface{}, format string) error
	BeginTransaction(ctx context.Context) (Transaction, error)
	Upsert(ctx context.Context, records []*pool.Record, keys []string) error

	// Schema operations
	AlterSchema(ctx context.Context, oldSchema, newSchema *Schema) error
	DropSchema(ctx context.Context, schema *Schema) error

	// Health and metrics
	Health(ctx context.Context) error
	Metrics() map[string]interface{}
}

// Connector is the base interface for all connectors
type Connector interface {
	// Metadata
	Name() string
	Type() ConnectorType
	Version() string

	// Lifecycle
	Initialize(ctx context.Context, config *config.BaseConfig) error
	Close(ctx context.Context) error

	// Health and monitoring
	Health(ctx context.Context) error
	Metrics() map[string]interface{}
}

// ConnectorFactory creates connector instances
type ConnectorFactory interface {
	CreateSource(connectorType string, config *config.BaseConfig) (Source, error)
	CreateDestination(connectorType string, config *config.BaseConfig) (Destination, error)
	ListSources() []string
	ListDestinations() []string
}

// HealthStatus represents the health status of a connector
type HealthStatus struct {
	Status    string                 `json:"status"` // "healthy", "unhealthy", "degraded"
	Timestamp time.Time              `json:"timestamp"`
	Details   map[string]interface{} `json:"details"`
	Error     error                  `json:"error,omitempty"`
}

// MetricType represents the type of metric
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
	MetricTypeSummary   MetricType = "summary"
)

// Metric represents a single metric
type Metric struct {
	Name        string            `json:"name"`
	Type        MetricType        `json:"type"`
	Value       interface{}       `json:"value"`
	Labels      map[string]string `json:"labels"`
	Timestamp   time.Time         `json:"timestamp"`
	Description string            `json:"description"`
}

// StreamController provides control over data streams
type StreamController interface {
	Pause() error
	Resume() error
	Cancel() error
	Status() string
}

// PositionManager manages position/checkpoint state
type PositionManager interface {
	SavePosition(ctx context.Context, position Position) error
	LoadPosition(ctx context.Context) (Position, error)
	ResetPosition(ctx context.Context) error
}

// SchemaRegistry manages schema versions
type SchemaRegistry interface {
	RegisterSchema(ctx context.Context, schema *Schema) error
	GetSchema(ctx context.Context, name string, version int) (*Schema, error)
	ListSchemas(ctx context.Context) ([]*Schema, error)
	GetLatestSchema(ctx context.Context, name string) (*Schema, error)
}

// ErrorHandler defines how connectors handle errors
type ErrorHandler interface {
	HandleError(ctx context.Context, err error, record *pool.Record) error
	ShouldRetry(err error) bool
	GetRetryDelay(attempt int) time.Duration
	RecordError(err error, details map[string]interface{})
}

// RateLimiter provides rate limiting capabilities
type RateLimiter interface {
	Allow() bool
	Wait(ctx context.Context) error
	Limit() int
	SetLimit(limit int)
}

// CircuitBreaker provides circuit breaker pattern
type CircuitBreaker interface {
	Allow() error
	MarkSuccess()
	MarkFailed()
	State() string
	Reset()
}

// ConnectionPool manages connection pooling
type ConnectionPool interface {
	Get(ctx context.Context) (interface{}, error)
	Put(conn interface{}) error
	Close() error
	Stats() PoolStats
}

// PoolStats represents connection pool statistics
type PoolStats struct {
	Active   int
	Idle     int
	Total    int
	MaxSize  int
	Waits    int64
	Timeouts int64
}

// BatchBuilder helps build record batches efficiently
type BatchBuilder interface {
	Add(record *pool.Record) error
	Build() ([]*pool.Record, error)
	Reset()
	Size() int
	IsFull() bool
}

// Optimizer provides performance optimization hints
type Optimizer interface {
	OptimizeBatchSize(current int, metrics map[string]interface{}) int
	OptimizeConcurrency(current int, metrics map[string]interface{}) int
	OptimizeBufferSize(current int, metrics map[string]interface{}) int
	SuggestOptimizations(metrics map[string]interface{}) []string
}

// ProgressReporter reports progress of operations
type ProgressReporter interface {
	ReportProgress(processed int64, total int64)
	ReportThroughput(recordsPerSecond float64)
	ReportLatency(latency time.Duration)
	GetProgress() (processed int64, total int64)
}

// DataQualityChecker performs data quality checks
type DataQualityChecker interface {
	CheckRecord(record *pool.Record) error
	CheckBatch(records []*pool.Record) []error
	GetQualityMetrics() map[string]interface{}
	SetRules(rules map[string]interface{})
}

// TransformFunc is a function that transforms records
type TransformFunc func(record *pool.Record) (*pool.Record, error)

// FilterFunc is a function that filters records
type FilterFunc func(record *pool.Record) bool

// Pipeline represents a data processing pipeline
type Pipeline interface {
	AddTransform(name string, transform TransformFunc)
	AddFilter(name string, filter FilterFunc)
	Process(ctx context.Context, input *RecordStream) (*RecordStream, error)
	ProcessBatch(ctx context.Context, input *BatchStream) (*BatchStream, error)
}

// ConnectorMetadata provides metadata about a connector
type ConnectorMetadata struct {
	Name          string                 `json:"name"`
	Type          ConnectorType          `json:"type"`
	Version       string                 `json:"version"`
	Description   string                 `json:"description"`
	Author        string                 `json:"author"`
	Documentation string                 `json:"documentation"`
	Capabilities  []string               `json:"capabilities"`
	ConfigSchema  map[string]interface{} `json:"config_schema"`
}

// ConnectorRegistry manages available connectors
type ConnectorRegistry interface {
	Register(name string, factory func() Connector) error
	Get(name string) (Connector, error)
	List() []ConnectorMetadata
	Exists(name string) bool
}
