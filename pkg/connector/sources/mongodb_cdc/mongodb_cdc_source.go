package mongodb_cdc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/cdc"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/base"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/errors"
	"github.com/ajitpratap0/nebula/pkg/models"
	"go.uber.org/zap"
)

// MongoDBCDCSource is a MongoDB CDC source connector that implements core.Source
type MongoDBCDCSource struct {
	*base.BaseConnector

	// CDC engine
	cdcConnector *cdc.MongoDBConnector
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
	collections []string
	database    string

	// Stream management
	stopCh    chan struct{}
	running   bool
	runningMu sync.Mutex
}

// NewMongoDBCDCSource creates a new MongoDB CDC source connector
func NewMongoDBCDCSource(cfg *config.BaseConfig) (core.Source, error) {
	base := base.NewBaseConnector("mongodb-cdc", core.ConnectorTypeSource, "1.0.0")

	return &MongoDBCDCSource{
		BaseConnector: base,
		stopCh:        make(chan struct{}),
		state:         make(core.State),
	}, nil
}

// Initialize initializes the MongoDB CDC connector
func (s *MongoDBCDCSource) Initialize(ctx context.Context, cfg *config.BaseConfig) error {
	// Initialize base connector first
	if err := s.BaseConnector.Initialize(ctx, cfg); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "failed to initialize base connector")
	}

	// Extract CDC configuration from Security credentials
	connectionStr, ok := cfg.Security.Credentials["connection_string"]
	if !ok || connectionStr == "" {
		return errors.New(errors.ErrorTypeConfig, "missing required property: connection_string in security.credentials")
	}

	database, ok := cfg.Security.Credentials["database"]
	if !ok || database == "" {
		return errors.New(errors.ErrorTypeConfig, "missing required property: database in security.credentials")
	}
	s.database = database

	// Extract collections from comma-separated string
	collectionsStr, ok := cfg.Security.Credentials["collections"]
	if ok && collectionsStr != "" {
		// Parse comma-separated collections
		for _, collection := range strings.Split(collectionsStr, ",") {
			trimmed := strings.TrimSpace(collection)
			if trimmed != "" {
				s.collections = append(s.collections, trimmed)
			}
		}
	}
	// MongoDB can monitor all collections if none specified
	// Unlike MySQL/PostgreSQL, this is a valid configuration

	// Create CDC configuration
	s.cdcConfig = cdc.CDCConfig{
		Type:          cdc.ConnectorMongoDB,
		ConnectionStr: connectionStr,
		Database:      database,
		Tables:        s.collections, // Tables field is used for collections in MongoDB
		BatchSize:     cfg.Performance.BatchSize,
		BufferSize:    cfg.Performance.BufferSize,
	}

	// Extract MongoDB-specific options
	if s.cdcConfig.Options == nil {
		s.cdcConfig.Options = pool.GetMap()
	}

	// MongoDB-specific options from config
	mongoOptions := pool.GetMap()

	defer pool.PutMap(mongoOptions)

	if resumeToken, ok := cfg.Security.Credentials["resume_token"]; ok && resumeToken != "" {
		mongoOptions["resume_token"] = resumeToken
	}

	if fullDocument, ok := cfg.Security.Credentials["full_document"]; ok && fullDocument != "" {
		mongoOptions["full_document"] = fullDocument
	}

	if fullDocumentBeforeChange, ok := cfg.Security.Credentials["full_document_before_change"]; ok && fullDocumentBeforeChange != "" {
		mongoOptions["full_document_before_change"] = fullDocumentBeforeChange
	}

	// Use performance batch size from structured config
	if cfg.Performance.BatchSize > 0 {
		mongoOptions["batch_size"] = cfg.Performance.BatchSize
	}

	if maxAwaitTime, ok := cfg.Security.Credentials["max_await_time"]; ok && maxAwaitTime != "" {
		mongoOptions["max_await_time"] = maxAwaitTime
	}

	if includeOperationTypesStr, ok := cfg.Security.Credentials["include_operation_types"]; ok && includeOperationTypesStr != "" {
		// Parse comma-separated operation types
		var operationTypes []string
		for _, opType := range strings.Split(includeOperationTypesStr, ",") {
			trimmed := strings.TrimSpace(opType)
			if trimmed != "" {
				operationTypes = append(operationTypes, trimmed)
			}
		}
		mongoOptions["include_operation_types"] = operationTypes
	}

	s.cdcConfig.Options["mongodb"] = mongoOptions

	// Create CDC connector
	s.cdcConnector = cdc.NewMongoDBConnector(s.GetLogger())

	// Connect to MongoDB
	if err := s.ExecuteWithCircuitBreaker(func() error {
		return s.cdcConnector.Connect(s.cdcConfig)
	}); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to connect to MongoDB")
	}

	// Subscribe to collections
	if err := s.cdcConnector.Subscribe(s.collections); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to subscribe to collections")
	}

	s.UpdateHealth(true, map[string]interface{}{
		"database":       database,
		"collections":    s.collections,
		"cdc_mode":       true,
		"change_streams": true,
	})

	s.GetLogger().Info("MongoDB CDC source initialized",
		zap.String("database", database),
		zap.Strings("collections", s.collections))

	return nil
}

// Discover discovers the schema from MongoDB collections
func (s *MongoDBCDCSource) Discover(ctx context.Context) (*core.Schema, error) {
	s.schemaMu.RLock()
	if s.schema != nil {
		defer s.schemaMu.RUnlock()
		return s.schema, nil
	}
	s.schemaMu.RUnlock()

	s.schemaMu.Lock()
	defer s.schemaMu.Unlock()

	// MongoDB is schema-less, so we create a flexible schema
	s.schema = &core.Schema{
		Name:        fmt.Sprintf("%s_cdc", s.database),
		Description: fmt.Sprintf("CDC schema for MongoDB database %s", s.database),
		Fields: []core.Field{
			{
				Name:        "_id",
				Type:        core.FieldTypeString,
				Description: "MongoDB document ID",
				Primary:     true,
				Nullable:    false,
			},
			{
				Name:        "_data",
				Type:        core.FieldTypeJSON,
				Description: "Document data",
				Nullable:    true,
			},
			{
				Name:        "_operation",
				Type:        core.FieldTypeString,
				Description: "CDC operation type",
				Nullable:    false,
			},
			{
				Name:        "_timestamp",
				Type:        core.FieldTypeTimestamp,
				Description: "Change timestamp",
				Nullable:    false,
			},
		},
		Version:   1,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	return s.schema, nil
}

// Read starts reading changes from MongoDB CDC
func (s *MongoDBCDCSource) Read(ctx context.Context) (*core.RecordStream, error) {
	// Check if already running
	s.runningMu.Lock()
	if s.running {
		s.runningMu.Unlock()
		return nil, errors.New(errors.ErrorTypeConnection, "CDC reader already running")
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
			errorsChan <- errors.Wrap(err, errors.ErrorTypeConnection, "failed to start reading changes")
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
func (s *MongoDBCDCSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	// Implementation similar to PostgreSQL/MySQL CDC
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

// Close closes the MongoDB CDC connection
func (s *MongoDBCDCSource) Close(ctx context.Context) error {
	close(s.stopCh)

	if s.cdcConnector != nil {
		if err := s.cdcConnector.Stop(); err != nil {
			s.GetLogger().Error("failed to stop CDC connector", zap.Error(err))
		}
	}

	s.GetLogger().Info("MongoDB CDC source closed")

	// Close base connector
	return s.BaseConnector.Close(ctx)
}

// GetPosition returns the current CDC position
func (s *MongoDBCDCSource) GetPosition() core.Position {
	s.positionMu.RLock()
	defer s.positionMu.RUnlock()
	return s.position
}

// SetPosition sets the CDC position
func (s *MongoDBCDCSource) SetPosition(position core.Position) error {
	s.positionMu.Lock()
	defer s.positionMu.Unlock()
	s.position = position

	// If we have a CDC connector, acknowledge the position
	if s.cdcConnector != nil && position != nil {
		if cdcPos, ok := position.(*cdcPositionAdapter); ok {
			return s.cdcConnector.Acknowledge(cdcPos.cdcPosition)
		}
	}

	return nil
}

// GetState returns the connector state
func (s *MongoDBCDCSource) GetState() core.State {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()

	stateCopy := make(core.State)
	for k, v := range s.state {
		stateCopy[k] = v
	}
	return stateCopy
}

// SetState sets the connector state
func (s *MongoDBCDCSource) SetState(state core.State) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	s.state = make(core.State)
	for k, v := range state {
		s.state[k] = v
	}
	return nil
}

// SupportsIncremental returns true as CDC is inherently incremental
func (s *MongoDBCDCSource) SupportsIncremental() bool {
	return true
}

// SupportsRealtime returns true as CDC provides real-time changes
func (s *MongoDBCDCSource) SupportsRealtime() bool {
	return true
}

// SupportsBatch returns true as we can batch CDC events
func (s *MongoDBCDCSource) SupportsBatch() bool {
	return true
}

// Subscribe enables real-time change streaming for specified collections
func (s *MongoDBCDCSource) Subscribe(ctx context.Context, collections []string) (*core.ChangeStream, error) {
	if s.cdcConnector == nil {
		return nil, errors.New(errors.ErrorTypeConnection, "CDC connector not initialized")
	}

	// Subscribe to collections via CDC connector
	if err := s.cdcConnector.Subscribe(collections); err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConfig, "failed to subscribe to collections")
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

	s.GetLogger().Info("subscribed to CDC changes", zap.Strings("collections", collections))

	return stream, nil
}

// Health returns the health status of the CDC connector
func (s *MongoDBCDCSource) Health(ctx context.Context) error {
	if s.cdcConnector == nil {
		return errors.New(errors.ErrorTypeConnection, "CDC connector not initialized")
	}

	health := s.cdcConnector.Health()
	if health.Status != "running" && health.Status != "connected" {
		return errors.New(errors.ErrorTypeConnection, health.Message)
	}

	return nil
}

// Metrics returns connector metrics
func (s *MongoDBCDCSource) Metrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"database":    s.database,
		"collections": s.collections,
	}

	if s.cdcConnector != nil {
		health := s.cdcConnector.Health()
		metrics["event_count"] = health.EventCount
		metrics["error_count"] = health.ErrorCount
		metrics["last_event"] = health.LastEvent
		metrics["lag"] = health.Lag
		metrics["status"] = health.Status
	}

	return metrics
}

// Private methods

// convertCDCEventToRecord converts a CDC ChangeEvent to a models.Record
func (s *MongoDBCDCSource) convertCDCEventToRecord(event cdc.ChangeEvent) (*models.Record, error) {
	// Use the built-in ConvertToRecord method
	record, err := event.ConvertToRecord()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeData, "failed to convert CDC event to record")
	}

	// Add additional metadata specific to MongoDB
	if record.Metadata.Custom == nil {
		record.Metadata.Custom = pool.GetMap()
	}
	record.Metadata.Custom["connector_type"] = "mongodb-cdc"
	record.Metadata.Custom["database"] = s.database
	record.Metadata.Custom["change_stream"] = true

	// Add collection info if available
	if event.Table != "" {
		record.Metadata.Custom["collection"] = event.Table
	}

	return record, nil
}

// updatePosition updates the current position
func (s *MongoDBCDCSource) updatePosition(cdcPos cdc.Position) {
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
func (s *MongoDBCDCSource) streamChangeEvents(ctx context.Context, changeCh chan *core.ChangeEvent, errorCh chan error) {
	defer close(changeCh)
	defer close(errorCh)

	// Get CDC event stream
	eventStream, err := s.cdcConnector.ReadChanges(ctx)
	if err != nil {
		errorCh <- errors.Wrap(err, errors.ErrorTypeData, "failed to read CDC changes")
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
				Table:     event.Table, // Collection name in MongoDB
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
func (s *MongoDBCDCSource) convertOperationType(op cdc.OperationType) core.ChangeType {
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
func (s *MongoDBCDCSource) convertPosition(pos cdc.Position) core.Position {
	return &cdcPositionAdapter{
		cdcPosition: pos,
	}
}
