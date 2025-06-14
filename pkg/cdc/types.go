// Package cdc provides Change Data Capture functionality for real-time data replication
package cdc

import (
	"context"
	"fmt"
	"time"

	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
)

// CDCConnector defines the interface for Change Data Capture connectors
type CDCConnector interface {
	// Connect establishes connection to the data source
	Connect(config CDCConfig) error

	// Subscribe starts listening to changes on specified tables/collections
	Subscribe(tables []string) error

	// ReadChanges returns a channel of change events
	ReadChanges(ctx context.Context) (<-chan ChangeEvent, error)

	// GetPosition returns the current replication position
	GetPosition() Position

	// Acknowledge confirms processing of events up to the given position
	Acknowledge(position Position) error

	// Stop gracefully shuts down the connector
	Stop() error

	// Health returns the health status of the connector
	Health() HealthStatus
}

// CDCConfig contains configuration for CDC connectors
type CDCConfig struct {
	Type          ConnectorType          `json:"type" yaml:"type"`
	ConnectionStr string                 `json:"connection_string" yaml:"connection_string"`
	Database      string                 `json:"database" yaml:"database"`
	Tables        []string               `json:"tables" yaml:"tables"`
	StartPosition *Position              `json:"start_position,omitempty" yaml:"start_position,omitempty"`
	BatchSize     int                    `json:"batch_size" yaml:"batch_size"`
	PollInterval  time.Duration          `json:"poll_interval" yaml:"poll_interval"`
	BufferSize    int                    `json:"buffer_size" yaml:"buffer_size"`
	Options       map[string]interface{} `json:"options,omitempty" yaml:"options,omitempty"`
}

// ConnectorType represents the type of CDC connector
type ConnectorType string

const (
	ConnectorPostgreSQL ConnectorType = "postgresql"
	ConnectorMySQL      ConnectorType = "mysql"
	ConnectorMongoDB    ConnectorType = "mongodb"
	ConnectorKafka      ConnectorType = "kafka"
)

// OperationType represents the type of database operation
type OperationType string

const (
	OperationInsert OperationType = "INSERT"
	OperationUpdate OperationType = "UPDATE"
	OperationDelete OperationType = "DELETE"
	OperationDDL    OperationType = "DDL"    // Schema changes
	OperationCommit OperationType = "COMMIT" // Transaction commit
)

// ChangeEvent represents a single change event from the database
type ChangeEvent struct {
	ID            string                 `json:"id"`
	Operation     OperationType          `json:"operation"`
	Database      string                 `json:"database"`
	Table         string                 `json:"table"`
	Schema        string                 `json:"schema,omitempty"`
	Before        map[string]interface{} `json:"before,omitempty"`
	After         map[string]interface{} `json:"after,omitempty"`
	Timestamp     time.Time              `json:"timestamp"`
	Position      Position               `json:"position"`
	TransactionID string                 `json:"transaction_id,omitempty"`

	// Additional metadata
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	Source   SourceInfo             `json:"source"`
}

// Position represents a replication position in the change stream
type Position struct {
	Type     string                 `json:"type"`
	Value    interface{}            `json:"value"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// SourceInfo contains information about the data source
type SourceInfo struct {
	Name          string        `json:"name"`
	Database      string        `json:"database"`
	Table         string        `json:"table"`
	ConnectorType ConnectorType `json:"connector_type"`
	Version       string        `json:"version,omitempty"`
	Timestamp     time.Time     `json:"timestamp"`
}

// HealthStatus represents the health of a CDC connector
type HealthStatus struct {
	Status     string                 `json:"status"`
	Message    string                 `json:"message,omitempty"`
	LastEvent  time.Time              `json:"last_event"`
	EventCount int64                  `json:"event_count"`
	ErrorCount int64                  `json:"error_count"`
	Lag        time.Duration          `json:"lag,omitempty"`
	Details    map[string]interface{} `json:"details,omitempty"`
}

// EventFilter defines filtering criteria for change events
type EventFilter struct {
	IncludeTables []string          `json:"include_tables,omitempty"`
	ExcludeTables []string          `json:"exclude_tables,omitempty"`
	Operations    []OperationType   `json:"operations,omitempty"`
	Conditions    []FilterCondition `json:"conditions,omitempty"`
}

// FilterCondition represents a single filtering condition
type FilterCondition struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"` // eq, ne, lt, gt, in, like, etc.
	Value    interface{} `json:"value"`
}

// EventProcessor defines the interface for processing change events
type EventProcessor interface {
	Process(ctx context.Context, event ChangeEvent) error
	ProcessBatch(ctx context.Context, events []ChangeEvent) error
	Stop() error
}

// EventHandler is a function type for handling change events
type EventHandler func(ctx context.Context, event ChangeEvent) error

// BatchEventHandler is a function type for handling batches of change events
type BatchEventHandler func(ctx context.Context, events []ChangeEvent) error

// StreamingConfig contains configuration for event streaming
type StreamingConfig struct {
	MaxBatchSize    int           `json:"max_batch_size"`
	BatchTimeout    time.Duration `json:"batch_timeout"`
	MaxRetries      int           `json:"max_retries"`
	RetryBackoff    time.Duration `json:"retry_backoff"`
	DeadLetterQueue string        `json:"dead_letter_queue,omitempty"`
	ParallelWorkers int           `json:"parallel_workers"`
	OrderingKey     string        `json:"ordering_key,omitempty"`
	CompressionType string        `json:"compression_type,omitempty"`
	ExactlyOnce     bool          `json:"exactly_once"`
}

// Checkpoint represents a savepoint in the event stream
type Checkpoint struct {
	ID          string                 `json:"id"`
	Position    Position               `json:"position"`
	Timestamp   time.Time              `json:"timestamp"`
	EventCount  int64                  `json:"event_count"`
	ProcessedAt time.Time              `json:"processed_at"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// EventMetrics contains metrics for CDC operations
type EventMetrics struct {
	EventsReceived    int64         `json:"events_received"`
	EventsProcessed   int64         `json:"events_processed"`
	EventsFiltered    int64         `json:"events_filtered"`
	EventsErrored     int64         `json:"events_errored"`
	ProcessingLatency time.Duration `json:"processing_latency"`
	ThroughputRPS     float64       `json:"throughput_rps"`
	LastEventTime     time.Time     `json:"last_event_time"`
	BacklogSize       int64         `json:"backlog_size"`
}

// TransactionInfo contains information about database transactions
type TransactionInfo struct {
	ID         string    `json:"id"`
	StartTime  time.Time `json:"start_time"`
	EndTime    time.Time `json:"end_time"`
	EventCount int       `json:"event_count"`
	Size       int64     `json:"size"`
}

// SchemaChange represents a DDL change event
type SchemaChange struct {
	Type      string                 `json:"type"`   // CREATE, ALTER, DROP
	Object    string                 `json:"object"` // TABLE, INDEX, etc.
	Name      string                 `json:"name"`
	Statement string                 `json:"statement"`
	Before    map[string]interface{} `json:"before,omitempty"`
	After     map[string]interface{} `json:"after,omitempty"`
}

// ConvertToRecord converts a ChangeEvent to the unified Nebula Record
func (ce *ChangeEvent) ConvertToRecord() (*models.Record, error) {
	// Create data based on operation type
	var data map[string]interface{}

	switch ce.Operation {
	case OperationInsert:
		data = ce.After
	case OperationUpdate:
		data = ce.After // Main data is the after state
	case OperationDelete:
		data = ce.Before
	default:
		data = ce.After
		if data == nil {
			data = ce.Before
		}
	}

	// Create unified record using the new CDC constructor
	record := models.NewCDCRecord(ce.Database, ce.Table, string(ce.Operation), ce.Before, data)
	
	// Set additional CDC metadata
	record.ID = ce.ID
	record.SetCDCPosition(ce.Position.String())
	record.SetCDCTransaction(ce.TransactionID)
	record.Metadata.Schema = ce.Schema
	record.SetTimestamp(ce.Timestamp)

	// Add custom metadata to the Custom map
	for k, v := range ce.Metadata {
		record.SetMetadata(k, v)
	}

	// Set source info in custom metadata
	record.SetMetadata("source_name", ce.Source.Name)
	record.SetMetadata("source_database", ce.Source.Database)
	record.SetMetadata("source_table", ce.Source.Table)
	record.SetMetadata("source_connector_type", string(ce.Source.ConnectorType))
	record.SetMetadata("source_version", ce.Source.Version)

	// Set schema if available
	if ce.Database != "" && ce.Table != "" {
		record.Schema = &models.Schema{
			Name:    fmt.Sprintf("%s.%s", ce.Database, ce.Table),
			Version: "1.0",
			Fields:  []models.Field{}, // TODO: populate from actual schema
		}
	}

	return record, nil
}

// String returns a string representation of the Position
func (p Position) String() string {
	data, _ := jsonpool.Marshal(p.Value)
	return fmt.Sprintf("%s:%s", p.Type, string(data))
}

// IsValid checks if the position is valid
func (p Position) IsValid() bool {
	return p.Type != "" && p.Value != nil
}

// Compare compares two positions (returns -1, 0, 1 for less, equal, greater)
func (p Position) Compare(other Position) int {
	if p.Type != other.Type {
		return -1 // Different types can't be compared
	}

	// Type-specific comparison logic would go here
	// For now, we'll do a simple string comparison
	thisStr := fmt.Sprintf("%v", p.Value)
	otherStr := fmt.Sprintf("%v", other.Value)

	if thisStr < otherStr {
		return -1
	} else if thisStr > otherStr {
		return 1
	}
	return 0
}

// IsHealthy returns true if the health status indicates the connector is healthy
func (h HealthStatus) IsHealthy() bool {
	return h.Status == "healthy" || h.Status == "running"
}

// AddCondition adds a filter condition
func (f *EventFilter) AddCondition(field, operator string, value interface{}) {
	f.Conditions = append(f.Conditions, FilterCondition{
		Field:    field,
		Operator: operator,
		Value:    value,
	})
}

// ShouldInclude checks if an event should be included based on the filter
func (f *EventFilter) ShouldInclude(event ChangeEvent) bool {
	// Check table inclusion/exclusion
	if len(f.IncludeTables) > 0 {
		found := false
		for _, table := range f.IncludeTables {
			if table == event.Table {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(f.ExcludeTables) > 0 {
		for _, table := range f.ExcludeTables {
			if table == event.Table {
				return false
			}
		}
	}

	// Check operation types
	if len(f.Operations) > 0 {
		found := false
		for _, op := range f.Operations {
			if op == event.Operation {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check custom conditions
	for _, condition := range f.Conditions {
		if !f.evaluateCondition(condition, event) {
			return false
		}
	}

	return true
}

// evaluateCondition evaluates a single filter condition
func (f *EventFilter) evaluateCondition(condition FilterCondition, event ChangeEvent) bool {
	// Get the field value from the event
	var fieldValue interface{}

	// Check in After data first, then Before data
	if event.After != nil {
		if val, exists := event.After[condition.Field]; exists {
			fieldValue = val
		}
	}
	if fieldValue == nil && event.Before != nil {
		if val, exists := event.Before[condition.Field]; exists {
			fieldValue = val
		}
	}

	// Evaluate the condition based on operator
	switch condition.Operator {
	case "eq", "=":
		return fmt.Sprintf("%v", fieldValue) == fmt.Sprintf("%v", condition.Value)
	case "ne", "!=":
		return fmt.Sprintf("%v", fieldValue) != fmt.Sprintf("%v", condition.Value)
	case "gt", ">":
		return compareValues(fieldValue, condition.Value) > 0
	case "lt", "<":
		return compareValues(fieldValue, condition.Value) < 0
	case "gte", ">=":
		return compareValues(fieldValue, condition.Value) >= 0
	case "lte", "<=":
		return compareValues(fieldValue, condition.Value) <= 0
	case "in":
		// Check if fieldValue is in the list
		if values, ok := condition.Value.([]interface{}); ok {
			for _, v := range values {
				if fmt.Sprintf("%v", fieldValue) == fmt.Sprintf("%v", v) {
					return true
				}
			}
		}
		return false
	case "like":
		// Simple pattern matching (could be enhanced with regex)
		pattern := fmt.Sprintf("%v", condition.Value)
		value := fmt.Sprintf("%v", fieldValue)
		// For now, just check if pattern is contained in value
		return len(pattern) > 0 && len(value) >= len(pattern) &&
			(pattern == "%" || fmt.Sprintf("%v", fieldValue) == pattern)
	}

	return true
}

// compareValues compares two values and returns -1, 0, 1
func compareValues(a, b interface{}) int {
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)

	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// Validate validates the CDC configuration
func (c *CDCConfig) Validate() error {
	if c.Type == "" {
		return fmt.Errorf("connector type is required")
	}

	if c.ConnectionStr == "" {
		return fmt.Errorf("connection string is required")
	}

	if c.Database == "" {
		return fmt.Errorf("database name is required")
	}

	if len(c.Tables) == 0 {
		return fmt.Errorf("at least one table must be specified")
	}

	if c.BatchSize <= 0 {
		c.BatchSize = 1000 // Default batch size
	}

	if c.PollInterval <= 0 {
		c.PollInterval = 1 * time.Second // Default poll interval
	}

	if c.BufferSize <= 0 {
		c.BufferSize = 10000 // Default buffer size
	}

	return nil
}

// Validate validates the streaming configuration
func (c *StreamingConfig) Validate() error {
	if c.MaxBatchSize <= 0 {
		c.MaxBatchSize = 1000
	}

	if c.BatchTimeout <= 0 {
		c.BatchTimeout = 5 * time.Second
	}

	if c.MaxRetries < 0 {
		c.MaxRetries = 3
	}

	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 1 * time.Second
	}

	if c.ParallelWorkers <= 0 {
		c.ParallelWorkers = 1
	}

	return nil
}
