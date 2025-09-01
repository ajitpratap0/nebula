package mysqlcdc

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/cdc"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/base"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"
)

// MySQLCDCSource is a MySQL CDC source connector that implements core.Source
type MySQLCDCSource struct {
	*base.BaseConnector

	// CDC engine
	cdcConnector *cdc.MySQLConnector
	cdcConfig    cdc.CDCConfig

	// State management
	position   core.Position
	positionMu sync.RWMutex
	state      core.State
	stateMu    sync.RWMutex

	// Schema cache
	schema   *core.Schema
	schemaMu sync.RWMutex

	// Configuration
	tables   []string
	database string

	// Stream management
	stopCh    chan struct{}
	running   bool
	runningMu sync.Mutex
}

// NewMySQLCDCSource creates a new MySQL CDC source connector
func NewMySQLCDCSource(config *config.BaseConfig) (core.Source, error) {
	base := base.NewBaseConnector("mysql-cdc", core.ConnectorTypeSource, "1.0.0")

	return &MySQLCDCSource{
		BaseConnector: base,
		stopCh:        make(chan struct{}),
		state:         make(core.State),
	}, nil
}

// Initialize initializes the MySQL CDC connector
func (s *MySQLCDCSource) Initialize(ctx context.Context, config *config.BaseConfig) error {
	// Initialize base connector first
	if err := s.BaseConnector.Initialize(ctx, config); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig, "failed to initialize base connector")
	}

	// Extract CDC configuration from Security credentials
	connectionStr, ok := config.Security.Credentials["connection_string"]
	if !ok || connectionStr == "" {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "missing required property: connection_string in security.credentials")
	}

	database, ok := config.Security.Credentials["database"]
	if !ok || database == "" {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "missing required property: database in security.credentials")
	}
	s.database = database

	// Extract tables from comma-separated string
	tablesStr, ok := config.Security.Credentials["tables"]
	if !ok || tablesStr == "" {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "missing required property: tables in security.credentials")
	}

	// Parse comma-separated tables
	for _, table := range strings.Split(tablesStr, ",") {
		trimmed := strings.TrimSpace(table)
		if trimmed != "" {
			s.tables = append(s.tables, trimmed)
		}
	}
	if len(s.tables) == 0 {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "at least one table must be specified")
	}

	// Create CDC configuration
	s.cdcConfig = cdc.CDCConfig{
		Type:          cdc.ConnectorMySQL,
		ConnectionStr: connectionStr,
		Database:      database,
		Tables:        s.tables,
		BatchSize:     config.Performance.BatchSize,
		BufferSize:    config.Performance.BufferSize,
	}

	// Extract MySQL-specific options
	if serverIDStr, ok := config.Security.Credentials["server_id"]; ok && serverIDStr != "" {
		if s.cdcConfig.Options == nil {
			s.cdcConfig.Options = pool.GetMap()
		}
		s.cdcConfig.Options["server_id"] = serverIDStr
	}

	// Create CDC connector
	s.cdcConnector = cdc.NewMySQLConnector(s.GetLogger())

	// Connect to MySQL
	if err := s.ExecuteWithCircuitBreaker(func() error {
		return s.cdcConnector.Connect(s.cdcConfig)
	}); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to connect to MySQL")
	}

	// Subscribe to tables
	if err := s.cdcConnector.Subscribe(s.tables); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to subscribe to tables")
	}

	s.UpdateHealth(true, map[string]interface{}{
		"database": database,
		"tables":   s.tables,
		"cdc_mode": true,
	})

	s.GetLogger().Info("MySQL CDC source initialized",
		zap.String("database", database),
		zap.Strings("tables", s.tables))

	return nil
}

// All other methods follow the same pattern as PostgreSQLCDCSource
// The main difference is using cdc.MySQLConnector instead of cdc.PostgreSQLConnector

// Discover discovers the schema from MySQL tables
func (s *MySQLCDCSource) Discover(ctx context.Context) (*core.Schema, error) {
	s.schemaMu.RLock()
	if s.schema != nil {
		defer s.schemaMu.RUnlock()
		return s.schema, nil
	}
	s.schemaMu.RUnlock()

	s.schemaMu.Lock()
	defer s.schemaMu.Unlock()

	s.schema = &core.Schema{
		Name:        stringpool.Sprintf("%s_cdc", s.database),
		Description: stringpool.Sprintf("CDC schema for MySQL database %s", s.database),
		Fields:      []core.Field{}, // Will be populated dynamically from change events
		Version:     1,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	return s.schema, nil
}

// Read starts reading changes from MySQL CDC
func (s *MySQLCDCSource) Read(ctx context.Context) (*core.RecordStream, error) {
	// Check if already running
	s.runningMu.Lock()
	if s.running {
		s.runningMu.Unlock()
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeConnection, "CDC reader already running")
	}
	s.running = true
	s.runningMu.Unlock()

	// Create channels for the stream
	recordsChan := make(chan *models.Record, s.cdcConfig.BufferSize)
	errorsChan := make(chan error, 10)

	stream := &core.RecordStream{
		Records: recordsChan,
		Errors:  errorsChan,
	}

	// Start reading changes in background
	go func() {
		defer func() {
			close(recordsChan)
			close(errorsChan)
			s.runningMu.Lock()
			s.running = false
			s.runningMu.Unlock()
		}()

		// Start CDC event reading
		changesChan, err := s.cdcConnector.ReadChanges(ctx)
		if err != nil {
			errorsChan <- nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to start reading changes")
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stopCh:
				return
			case cdcEvent, ok := <-changesChan:
				if !ok {
					return
				}

				// Convert CDC event to Record
				record, err := s.convertCDCEventToRecord(cdcEvent)
				if err != nil {
					errorsChan <- err
					continue
				}

				// Apply rate limiting if configured
				if err := s.RateLimit(ctx); err != nil {
					errorsChan <- err
					return
				}

				// Send record
				select {
				case recordsChan <- record:
					// Update position
					s.updatePosition(cdcEvent.Position)

					// Update metrics
					s.RecordMetric("cdc_events_processed", 1, core.MetricTypeCounter)

				case <-ctx.Done():
					return
				case <-s.stopCh:
					return
				}
			}
		}
	}()

	return stream, nil
}

// ReadBatch reads a batch of changes (delegates to streaming with batching)
func (s *MySQLCDCSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	// Implementation similar to PostgreSQL CDC
	recordStream, err := s.Read(ctx)
	if err != nil {
		return nil, err
	}

	batchesChan := make(chan []*models.Record, 10)
	errorsChan := make(chan error, 10)

	stream := &core.BatchStream{
		Batches: batchesChan,
		Errors:  errorsChan,
	}

	// Convert records to batches
	go func() {
		defer func() {
			close(batchesChan)
			close(errorsChan)
		}()

		batch := pool.GetBatchSlice(batchSize)

		defer pool.PutBatchSlice(batch)
		timeout := time.NewTimer(5 * time.Second)
		defer timeout.Stop()

		for {
			select {
			case record, ok := <-recordStream.Records:
				if !ok {
					// Send any remaining records
					if len(batch) > 0 {
						batchesChan <- batch
					}
					return
				}

				batch = append(batch, record)

				// Send batch if full
				if len(batch) >= batchSize {
					batchesChan <- batch
					batch = pool.GetBatchSlice(batchSize)
					timeout.Reset(5 * time.Second)
				}

			case err := <-recordStream.Errors:
				errorsChan <- err

			case <-timeout.C:
				// Send partial batch on timeout
				if len(batch) > 0 {
					batchesChan <- batch
					batch = pool.GetBatchSlice(batchSize)
				}
				timeout.Reset(5 * time.Second)

			case <-ctx.Done():
				return
			}
		}
	}()

	return stream, nil
}

// Close closes the MySQL CDC connection
func (s *MySQLCDCSource) Close(ctx context.Context) error {
	close(s.stopCh)

	if s.cdcConnector != nil {
		if err := s.cdcConnector.Stop(); err != nil {
			s.GetLogger().Error("failed to stop CDC connector", zap.Error(err))
		}
	}

	s.GetLogger().Info("MySQL CDC source closed")

	// Close base connector
	return s.BaseConnector.Close(ctx)
}

// GetPosition returns the current CDC position
func (s *MySQLCDCSource) GetPosition() core.Position {
	s.positionMu.RLock()
	defer s.positionMu.RUnlock()
	return s.position
}

// SetPosition sets the CDC position
func (s *MySQLCDCSource) SetPosition(position core.Position) error {
	s.positionMu.Lock()
	defer s.positionMu.Unlock()
	s.position = position
	return nil
}

// GetState returns the connector state
func (s *MySQLCDCSource) GetState() core.State {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()

	stateCopy := make(core.State)
	for k, v := range s.state {
		stateCopy[k] = v
	}
	return stateCopy
}

// SetState sets the connector state
func (s *MySQLCDCSource) SetState(state core.State) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	s.state = make(core.State)
	for k, v := range state {
		s.state[k] = v
	}
	return nil
}

// SupportsIncremental returns true as CDC is inherently incremental
func (s *MySQLCDCSource) SupportsIncremental() bool {
	return true
}

// SupportsRealtime returns true as CDC provides real-time changes
func (s *MySQLCDCSource) SupportsRealtime() bool {
	return true
}

// SupportsBatch returns true as we can batch CDC events
func (s *MySQLCDCSource) SupportsBatch() bool {
	return true
}

// Subscribe enables real-time change streaming for specified tables
func (s *MySQLCDCSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
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

// Health returns the health status of the CDC connector
func (s *MySQLCDCSource) Health(ctx context.Context) error {
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
func (s *MySQLCDCSource) Metrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"database": s.database,
		"tables":   s.tables,
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

// Private methods

// convertCDCEventToRecord converts a CDC ChangeEvent to a models.Record
func (s *MySQLCDCSource) convertCDCEventToRecord(event cdc.ChangeEvent) (*models.Record, error) {
	// Use the built-in ConvertToRecord method
	record, err := event.ConvertToRecord()
	if err != nil {
		return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to convert CDC event to record")
	}

	// Add additional metadata
	if record.Metadata.Custom == nil {
		record.Metadata.Custom = pool.GetMap()
	}
	record.Metadata.Custom["connector_type"] = "mysql-cdc"
	record.Metadata.Database = s.database

	return record, nil
}

// updatePosition updates the current position
func (s *MySQLCDCSource) updatePosition(cdcPos cdc.Position) {
	s.positionMu.Lock()
	defer s.positionMu.Unlock()

	// Convert CDC position to core.Position
	s.position = &cdcPositionAdapter{
		cdcPosition: cdcPos,
	}
}

// cdcPositionAdapter adapts cdc.Position to core.Position
type cdcPositionAdapter struct {
	cdcPosition cdc.Position
}

func (p *cdcPositionAdapter) String() string {
	return p.cdcPosition.String()
}

func (p *cdcPositionAdapter) Compare(other core.Position) int {
	if otherAdapter, ok := other.(*cdcPositionAdapter); ok {
		return p.cdcPosition.Compare(otherAdapter.cdcPosition)
	}
	return -1
}

// streamChangeEvents converts CDC events to core change events
func (s *MySQLCDCSource) streamChangeEvents(ctx context.Context, changeCh chan *core.ChangeEvent, errorCh chan error) {
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
func (s *MySQLCDCSource) convertOperationType(op cdc.OperationType) core.ChangeType {
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
func (s *MySQLCDCSource) convertPosition(pos cdc.Position) core.Position {
	return &cdcPositionAdapter{
		cdcPosition: pos,
	}
}
