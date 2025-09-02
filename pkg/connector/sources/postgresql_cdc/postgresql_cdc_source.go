// Package postgresql_cdc provides a PostgreSQL Change Data Capture (CDC) source connector for Nebula.
// It captures real-time changes from PostgreSQL databases using logical replication,
// enabling streaming data integration for data warehouses and lakes.
//
// # Features
//
//   - Real-time change data capture using PostgreSQL logical replication
//   - Support for INSERT, UPDATE, DELETE, and DDL operations
//   - Automatic schema discovery and evolution
//   - Exactly-once delivery semantics with checkpointing
//   - Table filtering and column selection
//   - Transaction consistency preservation
//   - Automatic reconnection and failure recovery
//   - Comprehensive monitoring and metrics
//
// # Prerequisites
//
// PostgreSQL configuration requirements:
//   - PostgreSQL 10+ with logical replication enabled
//   - wal_level = logical
//   - max_replication_slots >= 1
//   - max_wal_senders >= 1
//   - User with REPLICATION privilege
//
// # Configuration
//
// The PostgreSQL CDC source uses these configuration fields:
//
//	config.Security.Credentials["connection_string"] = "postgres://user:pass@host:5432/db"
//	config.Security.Credentials["slot_name"] = "nebula_slot"  // replication slot name
//	config.Security.Credentials["publication_name"] = "nebula_pub"  // publication name
//	config.Security.Credentials["tables"] = "public.users,public.orders"  // comma-separated
//	config.Security.Credentials["start_lsn"] = "0/1234567"  // optional start position
//
// # Example Usage
//
//	cfg := config.NewBaseConfig("pg_cdc", "row")
//	cfg.Security.Credentials["connection_string"] = "postgres://localhost/mydb"
//	cfg.Security.Credentials["slot_name"] = "nebula_slot"
//	cfg.Security.Credentials["publication_name"] = "nebula_pub"
//	cfg.Security.Credentials["tables"] = "public.users,public.products"
//	cfg.Advanced.StreamingMode = true
//
//	source, err := postgresql_cdc.NewPostgreSQLCDCSource(cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ctx := context.Background()
//	if err := source.Initialize(ctx, cfg); err != nil {
//	    log.Fatal(err)
//	}
//	defer source.Close(ctx)
//
//	// Read CDC events
//	for {
//	    records, err := source.Read(ctx)
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//
//	    for _, record := range records {
//	        if record.IsCDC() {
//	            fmt.Printf("Operation: %s, Table: %s\n",
//	                record.GetCDCOperation(),
//	                record.GetCDCTable())
//	        }
//	        record.Release()
//	    }
//	}
package postgresqlcdc

import (
	"context"
	"fmt"

	"github.com/ajitpratap0/nebula/pkg/cdc"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/baseconnector"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"
	"strings"
)

// PostgreSQLCDCSource is a production-ready PostgreSQL CDC source connector that captures
// real-time changes from PostgreSQL databases using logical replication.
//
// The connector provides:
//   - Real-time streaming of database changes
//   - Automatic reconnection and failure recovery
//   - Schema discovery and evolution support
//   - Transaction consistency preservation
//   - Checkpoint management for exactly-once delivery
//   - Comprehensive health monitoring and metrics
type PostgreSQLCDCSource struct {
	*baseconnector.BaseConnector

	// CDC connector
	cdcConnector *cdc.PostgreSQLConnector // Underlying CDC implementation

	// Configuration
	cdcConfig cdc.CDCConfig // CDC-specific configuration

	// Streaming state
	recordStream chan *models.Record // Channel for streaming records
	errorStream  chan error          // Channel for async errors

	// Position tracking
	currentPosition core.Position // Current replication position

	// State management
	state core.State // Connector state for persistence

	// Statistics
	eventsProcessed int64 // Total CDC events processed
}

// NewPostgreSQLCDCSource creates a new PostgreSQL CDC source connector
func NewPostgreSQLCDCSource(config *config.BaseConfig) (core.Source, error) {
	base := baseconnector.NewBaseConnector("postgresql_cdc", core.ConnectorTypeSource, "2.0.0")

	source := &PostgreSQLCDCSource{
		BaseConnector: base,
		recordStream:  make(chan *models.Record, 10000),
		errorStream:   make(chan error, 100),
	}

	return source, nil
}

// Initialize sets up the PostgreSQL CDC source connector
func (s *PostgreSQLCDCSource) Initialize(ctx context.Context, config *config.BaseConfig) error {
	// Initialize base connector first
	if err := s.BaseConnector.Initialize(ctx, config); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig, "failed to initialize base connector")
	}

	// Extract CDC configuration from connector config
	cdcConfig, err := s.extractCDCConfig(config)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig, "failed to extract CDC configuration")
	}

	s.cdcConfig = cdcConfig

	// Create CDC connector
	s.cdcConnector = cdc.NewPostgreSQLConnector(s.GetLogger())

	// Connect to PostgreSQL
	if err := s.ExecuteWithCircuitBreaker(func() error {
		return s.cdcConnector.Connect(cdcConfig)
	}); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to connect to PostgreSQL")
	}

	// Subscribe to tables
	if err := s.cdcConnector.Subscribe(cdcConfig.Tables); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig, "failed to subscribe to tables")
	}

	s.UpdateHealth(true, map[string]interface{}{
		"database": cdcConfig.Database,
		"tables":   cdcConfig.Tables,
		"position": s.currentPosition,
	})

	s.GetLogger().Info("PostgreSQL CDC source initialized",
		zap.String("database", cdcConfig.Database),
		zap.Strings("tables", cdcConfig.Tables),
		zap.Int("batch_size", cdcConfig.BatchSize))

	return nil
}

// Discover discovers the schema of the PostgreSQL database
func (s *PostgreSQLCDCSource) Discover(ctx context.Context) (*core.Schema, error) {
	// Build schema from CDC configuration and table metadata
	schema := &core.Schema{
		Name:    stringpool.Sprintf("postgresql_cdc_%s", s.cdcConfig.Database),
		Version: 1,
		Fields:  make([]core.Field, 0),
	}

	// Add standard CDC fields that are always present
	cdcFields := []core.Field{
		{Name: "operation", Type: core.FieldTypeString, Nullable: false},
		{Name: "table", Type: core.FieldTypeString, Nullable: false},
		{Name: "database", Type: core.FieldTypeString, Nullable: false},
		{Name: "timestamp", Type: core.FieldTypeTimestamp, Nullable: false},
		{Name: "before", Type: core.FieldTypeJSON, Nullable: true},
		{Name: "after", Type: core.FieldTypeJSON, Nullable: true},
		{Name: "position", Type: core.FieldTypeJSON, Nullable: false},
		{Name: "transaction_id", Type: core.FieldTypeString, Nullable: true},
	}

	schema.Fields = append(schema.Fields, cdcFields...)

	s.GetLogger().Info("schema discovered",
		zap.String("schema_name", schema.Name),
		zap.Int("field_count", len(schema.Fields)))

	return schema, nil
}

// Read starts streaming change events as records
func (s *PostgreSQLCDCSource) Read(ctx context.Context) (*core.RecordStream, error) {
	// Start CDC event streaming in background goroutine
	go s.streamEvents(ctx)

	stream := &core.RecordStream{
		Records: s.recordStream,
		Errors:  s.errorStream,
	}

	s.GetLogger().Info("started reading CDC events")

	return stream, nil
}

// ReadBatch reads a batch of change events
func (s *PostgreSQLCDCSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	batchChan := make(chan []*models.Record, 100)
	errorChan := make(chan error, 10)

	// Start batch collection in background
	go s.collectBatches(ctx, batchSize, batchChan, errorChan)

	stream := &core.BatchStream{
		Batches: batchChan,
		Errors:  errorChan,
	}

	return stream, nil
}

// GetPosition returns the current replication position
func (s *PostgreSQLCDCSource) GetPosition() core.Position {
	if s.cdcConnector != nil {
		cdcPosition := s.cdcConnector.GetPosition()
		return &simplePosition{
			posType:  "postgresql_lsn",
			value:    cdcPosition.Value,
			metadata: cdcPosition.Metadata,
		}
	}
	return s.currentPosition
}

// SetPosition sets the replication position
func (s *PostgreSQLCDCSource) SetPosition(position core.Position) error {
	s.currentPosition = position

	if s.cdcConnector != nil {
		// Convert core.Position back to cdc.Position
		if simplePos, ok := position.(*simplePosition); ok {
			cdcPosition := cdc.Position{
				Type:     simplePos.posType,
				Value:    simplePos.value,
				Metadata: simplePos.metadata,
			}

			return s.cdcConnector.Acknowledge(cdcPosition)
		}
	}

	return nil
}

// GetState returns the current connector state
func (s *PostgreSQLCDCSource) GetState() core.State {
	if s.state == nil {
		s.state = make(core.State)
	}

	// Include position in state
	s.state["position"] = s.currentPosition
	s.state["events_processed"] = s.eventsProcessed
	s.state["database"] = s.cdcConfig.Database
	s.state["tables"] = s.cdcConfig.Tables

	return s.state
}

// SetState sets the connector state
func (s *PostgreSQLCDCSource) SetState(state core.State) error {
	s.state = state

	// Extract position from state if available
	if positionData, ok := state["position"]; ok {
		if position, ok := positionData.(core.Position); ok {
			s.currentPosition = position
		}
	}

	// Extract events processed count if available
	if eventsData, ok := state["events_processed"]; ok {
		if events, ok := eventsData.(int64); ok {
			s.eventsProcessed = events
		}
	}

	return nil
}

// Subscribe enables real-time change streaming for specified tables
func (s *PostgreSQLCDCSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	if s.cdcConnector == nil {
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeConnection, "CDC connector not initialized")
	}

	// Subscribe to tables via CDC connector
	if err := s.cdcConnector.Subscribe(tables); err != nil {
		return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig, "failed to subscribe to tables")
	}

	// Create change stream
	changeCh := make(chan *core.ChangeEvent, 1000)
	errorCh := make(chan error, 100)

	// Start converting CDC events to core change events
	go s.streamChangeEvents(ctx, changeCh, errorCh)

	stream := &core.ChangeStream{
		Changes: changeCh,
		Errors:  errorCh,
	}

	s.GetLogger().Info("subscribed to CDC changes", zap.Strings("tables", tables))

	return stream, nil
}

// SupportsIncremental returns true as CDC inherently supports incremental sync
func (s *PostgreSQLCDCSource) SupportsIncremental() bool {
	return true
}

// SupportsRealtime returns true as CDC provides real-time streaming
func (s *PostgreSQLCDCSource) SupportsRealtime() bool {
	return true
}

// SupportsBatch returns true as CDC can batch events
func (s *PostgreSQLCDCSource) SupportsBatch() bool {
	return true
}

// Close shuts down the PostgreSQL CDC source connector
func (s *PostgreSQLCDCSource) Close(ctx context.Context) error {
	if s.cdcConnector != nil {
		if err := s.cdcConnector.Stop(); err != nil {
			s.GetLogger().Warn("error stopping CDC connector", zap.Error(err))
		}
	}

	// Close channels
	close(s.recordStream)
	close(s.errorStream)

	return s.BaseConnector.Close(ctx)
}

// Health returns the health status of the CDC connector
func (s *PostgreSQLCDCSource) Health(ctx context.Context) error {
	if s.cdcConnector == nil {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConnection, "CDC connector not initialized")
	}

	health := s.cdcConnector.Health()
	if !health.IsHealthy() {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConnection, health.Message)
	}

	return nil
}

// Metrics returns connector metrics
func (s *PostgreSQLCDCSource) Metrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"events_processed": s.eventsProcessed,
		"current_position": s.currentPosition,
	}

	if s.cdcConnector != nil {
		health := s.cdcConnector.Health()
		metrics["event_count"] = health.EventCount
		metrics["error_count"] = health.ErrorCount
		metrics["last_event"] = health.LastEvent
		metrics["lag"] = health.Lag
	}

	return metrics
}

// streamEvents converts CDC events to records and streams them
func (s *PostgreSQLCDCSource) streamEvents(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			s.GetLogger().Error("panic in event streaming", zap.Any("panic", r))
		}
	}()

	// Get CDC event stream
	eventStream, err := s.cdcConnector.ReadChanges(ctx)
	if err != nil {
		s.errorStream <- nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to read CDC changes")
		return
	}

	for {
		select {
		case event, ok := <-eventStream:
			if !ok {
				s.GetLogger().Info("CDC event stream closed")
				return
			}

			// Convert CDC event to models.Record
			record, err := event.ConvertToRecord()
			if err != nil {
				s.GetLogger().Warn("failed to convert CDC event to record", zap.Error(err))
				continue
			}

			// Update position
			position := &simplePosition{
				posType:  "postgresql_lsn",
				value:    event.Position.Value,
				metadata: map[string]interface{}{"timestamp": event.Timestamp},
			}
			s.currentPosition = position

			// Send record to stream
			select {
			case s.recordStream <- record:
				s.eventsProcessed++
				s.RecordMetric("events_processed", 1, core.MetricTypeCounter)

			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			s.GetLogger().Info("context canceled, stopping event streaming")
			return
		}
	}
}

// collectBatches collects individual records into batches
func (s *PostgreSQLCDCSource) collectBatches(ctx context.Context, batchSize int, batchChan chan []*models.Record, errorChan chan error) {
	defer close(batchChan)
	defer close(errorChan)

	batch := pool.GetBatchSlice(batchSize)

	defer pool.PutBatchSlice(batch)

	// Start streaming records
	recordStream, err := s.Read(ctx)
	if err != nil {
		errorChan <- err
		return
	}

	for {
		select {
		case record, ok := <-recordStream.Records:
			if !ok {
				// Send final batch if any records remain
				if len(batch) > 0 {
					batchChan <- batch
				}
				return
			}

			batch = append(batch, record)

			// Send batch when full
			if len(batch) >= batchSize {
				batchChan <- batch
				batch = pool.GetBatchSlice(batchSize)
			}

		case err := <-recordStream.Errors:
			errorChan <- err

		case <-ctx.Done():
			return
		}
	}
}

// extractCDCConfig converts connector config to CDC config
func (s *PostgreSQLCDCSource) extractCDCConfig(config *config.BaseConfig) (cdc.CDCConfig, error) {
	var cdcConfig cdc.CDCConfig

	// Extract from Security credentials (for connection details)
	connectionStr, ok := config.Security.Credentials["connection_string"]
	if !ok || connectionStr == "" {
		return cdcConfig, fmt.Errorf("connection_string is required in security.credentials")
	}

	database, ok := config.Security.Credentials["database"]
	if !ok || database == "" {
		return cdcConfig, fmt.Errorf("database is required in security.credentials")
	}

	// Tables should be in credentials as comma-separated string
	tablesStr, ok := config.Security.Credentials["tables"]
	if !ok || tablesStr == "" {
		return cdcConfig, fmt.Errorf("tables is required in security.credentials")
	}

	// Parse comma-separated tables
	var tables []string
	for _, table := range splitAndTrim(tablesStr, ",") {
		if table != "" {
			tables = append(tables, table)
		}
	}

	cdcConfig = cdc.CDCConfig{
		Type:          cdc.ConnectorPostgreSQL,
		ConnectionStr: connectionStr,
		Database:      database,
		Tables:        tables,
		BatchSize:     config.Performance.BatchSize,
		BufferSize:    10000,
	}

	// Optional fields
	if slotName, ok := config.Security.Credentials["slot_name"]; ok && slotName != "" {
		if cdcConfig.Options == nil {
			cdcConfig.Options = pool.GetMap()
		}
		cdcConfig.Options["slot_name"] = slotName
	}

	if publication, ok := config.Security.Credentials["publication"]; ok && publication != "" {
		if cdcConfig.Options == nil {
			cdcConfig.Options = pool.GetMap()
		}
		cdcConfig.Options["publication"] = publication
	}

	if startLSN, ok := config.Security.Credentials["start_lsn"]; ok && startLSN != "" {
		if cdcConfig.Options == nil {
			cdcConfig.Options = pool.GetMap()
		}
		cdcConfig.Options["start_lsn"] = startLSN
	}

	return cdcConfig, nil
}

// splitAndTrim splits a string by delimiter and trims whitespace
func splitAndTrim(s, delim string) []string {
	parts := strings.Split(s, delim)
	var result []string
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// streamChangeEvents converts CDC events to core change events
func (s *PostgreSQLCDCSource) streamChangeEvents(ctx context.Context, changeCh chan *core.ChangeEvent, errorCh chan error) {
	defer close(changeCh)
	defer close(errorCh)

	// Get CDC event stream
	eventStream, err := s.cdcConnector.ReadChanges(ctx)
	if err != nil {
		errorCh <- nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to read CDC changes")
		return
	}

	for {
		select {
		case event, ok := <-eventStream:
			if !ok {
				return
			}

			// Convert CDC event to core change event
			changeEvent := &core.ChangeEvent{
				Type:      s.convertOperationType(event.Operation),
				Table:     event.Table,
				Timestamp: event.Timestamp,
				Position:  s.convertPosition(event.Position),
				Before:    event.Before,
				After:     event.After,
			}

			select {
			case changeCh <- changeEvent:
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

// convertOperationType converts CDC operation type to core change type
func (s *PostgreSQLCDCSource) convertOperationType(op cdc.OperationType) core.ChangeType {
	switch op {
	case cdc.OperationInsert:
		return core.ChangeTypeInsert
	case cdc.OperationUpdate:
		return core.ChangeTypeUpdate
	case cdc.OperationDelete:
		return core.ChangeTypeDelete
	default:
		return core.ChangeTypeUpdate // Default fallback
	}
}

// convertPosition converts CDC position to core position
func (s *PostgreSQLCDCSource) convertPosition(pos cdc.Position) core.Position {
	return &simplePosition{
		posType:  pos.Type,
		value:    pos.Value,
		metadata: pos.Metadata,
	}
}

// simplePosition implements core.Position interface
type simplePosition struct {
	posType  string
	value    interface{}
	metadata map[string]interface{}
}

func (p *simplePosition) String() string {
	return stringpool.Sprintf("%s:%v", p.posType, p.value)
}

func (p *simplePosition) Compare(other core.Position) int {
	if otherPos, ok := other.(*simplePosition); ok {
		if p.posType != otherPos.posType {
			return -1
		}

		// Simple string comparison for now
		thisStr := stringpool.Sprintf("%v", p.value)
		otherStr := stringpool.Sprintf("%v", otherPos.value)

		if thisStr < otherStr {
			return -1
		} else if thisStr > otherStr {
			return 1
		}
		return 0
	}
	return -1
}
