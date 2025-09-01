package cdc

import (
	"context"
	"fmt"
	"sync"
	"time"

	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// MongoDBConnector implements CDC for MongoDB using change streams
type MongoDBConnector struct {
	config CDCConfig
	logger *zap.Logger

	// MongoDB client and database
	client   *mongo.Client
	database *mongo.Database

	// Change stream state
	changeStream *mongo.ChangeStream
	resumeToken  bson.Raw

	// Event streaming
	eventCh chan ChangeEvent
	errorCh chan error
	stopCh  chan struct{}

	// Synchronization
	mutex   sync.RWMutex
	running bool

	// Collection tracking
	collections   []string
	collectionMap map[string]bool

	// Metrics
	metrics      EventMetrics
	metricsMutex sync.Mutex

	// Health status
	health      HealthStatus
	healthMutex sync.Mutex
}

// MongoDBConfig contains MongoDB-specific configuration
type MongoDBConfig struct {
	ResumeToken              string               `json:"resume_token,omitempty"`
	StartAtOperationTime     *primitive.Timestamp `json:"start_at_operation_time,omitempty"`
	FullDocument             string               `json:"full_document"`               // updateLookup, default
	FullDocumentBeforeChange string               `json:"full_document_before_change"` // whenAvailable, required, off
	BatchSize                int32                `json:"batch_size"`
	MaxAwaitTime             time.Duration        `json:"max_await_time"`
	Collation                *options.Collation   `json:"collation,omitempty"`
	IncludeOperationTypes    []string             `json:"include_operation_types,omitempty"`
}

// MongoChangeEvent represents a MongoDB change event
type MongoChangeEvent struct {
	ID                       bson.Raw               `bson:"_id"`
	OperationType            string                 `bson:"operationType"`
	ClusterTime              primitive.Timestamp    `bson:"clusterTime"`
	FullDocument             map[string]interface{} `bson:"fullDocument,omitempty"`
	FullDocumentBeforeChange map[string]interface{} `bson:"fullDocumentBeforeChange,omitempty"`
	DocumentKey              map[string]interface{} `bson:"documentKey,omitempty"`
	UpdateDescription        *UpdateDescription     `bson:"updateDescription,omitempty"`
	Namespace                Namespace              `bson:"ns"`
	TxnNumber                *int64                 `bson:"txnNumber,omitempty"`
	SessionID                *bson.Raw              `bson:"lsid,omitempty"`
}

// UpdateDescription contains information about updated fields
type UpdateDescription struct {
	UpdatedFields   map[string]interface{} `bson:"updatedFields,omitempty"`
	RemovedFields   []string               `bson:"removedFields,omitempty"`
	TruncatedArrays []TruncatedArray       `bson:"truncatedArrays,omitempty"`
}

// TruncatedArray represents a truncated array field
type TruncatedArray struct {
	Field   string `bson:"field"`
	NewSize int32  `bson:"newSize"`
}

// Namespace represents a MongoDB namespace (database.collection)
type Namespace struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

// NewMongoDBConnector creates a new MongoDB CDC connector
func NewMongoDBConnector(logger *zap.Logger) *MongoDBConnector {
	return &MongoDBConnector{
		logger:        logger.With(zap.String("connector", "mongodb")),
		eventCh:       make(chan ChangeEvent, 1000),
		errorCh:       make(chan error, 100),
		stopCh:        make(chan struct{}),
		collectionMap: make(map[string]bool),
		health: HealthStatus{
			Status:    "disconnected",
			Message:   "Not connected",
			LastEvent: time.Time{},
		},
	}
}

// Connect establishes connection to MongoDB and sets up change streams
func (c *MongoDBConnector) Connect(config CDCConfig) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	c.config = config

	// Parse MongoDB-specific options
	mongoConfig := MongoDBConfig{
		FullDocument:             "updateLookup",
		FullDocumentBeforeChange: "whenAvailable",
		BatchSize:                1000,
		MaxAwaitTime:             30 * time.Second,
	}

	if opts, ok := config.Options["mongodb"]; ok {
		if optsMap, ok := opts.(map[string]interface{}); ok {
			if val, ok := optsMap["resume_token"]; ok {
				mongoConfig.ResumeToken = val.(string)
			}
			if val, ok := optsMap["full_document"]; ok {
				mongoConfig.FullDocument = val.(string)
			}
			if val, ok := optsMap["full_document_before_change"]; ok {
				mongoConfig.FullDocumentBeforeChange = val.(string)
			}
			if val, ok := optsMap["batch_size"]; ok {
				if batchSize, ok := val.(float64); ok {
					mongoConfig.BatchSize = int32(batchSize)
				}
			}
			if val, ok := optsMap["max_await_time"]; ok {
				if duration, ok := val.(string); ok {
					if d, err := time.ParseDuration(duration); err == nil {
						mongoConfig.MaxAwaitTime = d
					}
				}
			}
		}
	}

	// Create MongoDB client
	clientOpts := options.Client().ApplyURI(config.ConnectionStr)

	var err error
	c.client, err = mongo.Connect(context.Background(), clientOpts)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Test connection
	err = c.client.Ping(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	// Get database reference
	c.database = c.client.Database(config.Database)

	// Get server info
	var serverStatus bson.M
	err = c.database.RunCommand(context.Background(), bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus)
	if err != nil {
		c.logger.Warn("failed to get server status", zap.Error(err))
	} else {
		if version, ok := serverStatus["version"].(string); ok {
			c.logger.Info("connected to MongoDB", zap.String("version", version))
		}
	}

	// Validate that the deployment supports change streams
	if err := c.validateChangeStreamSupport(); err != nil {
		return fmt.Errorf("change streams not supported: %w", err)
	}

	// Parse resume token if provided
	if mongoConfig.ResumeToken != "" {
		if err := c.parseResumeToken(mongoConfig.ResumeToken); err != nil {
			c.logger.Warn("failed to parse resume token", zap.Error(err))
		}
	}

	c.updateHealth("connected", "Connected to MongoDB", nil)

	c.logger.Info("MongoDB CDC connector initialized successfully")

	return nil
}

// Subscribe starts listening to changes on specified collections
func (c *MongoDBConnector) Subscribe(collections []string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.running {
		return fmt.Errorf("connector is already running")
	}

	// Setup collection tracking
	c.collections = collections
	c.collectionMap = make(map[string]bool)
	for _, coll := range collections {
		c.collectionMap[coll] = true
	}

	// Start change stream
	if err := c.startChangeStream(); err != nil {
		return fmt.Errorf("failed to start change stream: %w", err)
	}

	c.running = true

	// Start change stream processing in a separate goroutine
	go c.processChangeStream()

	c.updateHealth("running", "Subscribed to collections", nil)

	c.logger.Info("subscribed to collections", zap.Strings("collections", collections))

	return nil
}

// ReadChanges returns a channel of change events
func (c *MongoDBConnector) ReadChanges(ctx context.Context) (<-chan ChangeEvent, error) {
	if !c.running {
		return nil, fmt.Errorf("connector is not running")
	}

	return c.eventCh, nil
}

// GetPosition returns the current change stream position
func (c *MongoDBConnector) GetPosition() Position {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	var tokenStr string
	if c.resumeToken != nil {
		if data, err := jsonpool.Marshal(c.resumeToken); err == nil {
			tokenStr = string(data)
		}
	}

	return Position{
		Type:  "mongodb_resume_token",
		Value: tokenStr,
		Metadata: map[string]interface{}{
			"database": c.config.Database,
		},
	}
}

// Acknowledge confirms processing of events up to the given position
func (c *MongoDBConnector) Acknowledge(position Position) error {
	if position.Type != "mongodb_resume_token" {
		return fmt.Errorf("invalid position type: %s", position.Type)
	}

	tokenStr, ok := position.Value.(string)
	if !ok || tokenStr == "" {
		return fmt.Errorf("invalid resume token value")
	}

	var tokenData bson.Raw
	if err := jsonpool.Unmarshal([]byte(tokenStr), &tokenData); err != nil {
		return fmt.Errorf("failed to parse resume token: %w", err)
	}

	c.mutex.Lock()
	c.resumeToken = tokenData
	c.mutex.Unlock()

	c.logger.Debug("acknowledged position", zap.String("resume_token", tokenStr))

	return nil
}

// Stop gracefully shuts down the connector
func (c *MongoDBConnector) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.running {
		return nil
	}

	c.running = false
	close(c.stopCh)

	// Close change stream
	if c.changeStream != nil {
		_ = c.changeStream.Close(context.Background()) // Best effort close
	}

	// Disconnect from MongoDB
	if c.client != nil {
		_ = c.client.Disconnect(context.Background()) // Best effort disconnect
	}

	c.updateHealth("stopped", "Connector stopped", nil)

	c.logger.Info("MongoDB CDC connector stopped")

	return nil
}

// Health returns the health status of the connector
func (c *MongoDBConnector) Health() HealthStatus {
	c.healthMutex.Lock()
	defer c.healthMutex.Unlock()

	// Update metrics in health status
	c.metricsMutex.Lock()
	health := c.health
	health.EventCount = c.metrics.EventsReceived
	health.ErrorCount = c.metrics.EventsErrored
	if !c.metrics.LastEventTime.IsZero() {
		health.Lag = time.Since(c.metrics.LastEventTime)
	}
	c.metricsMutex.Unlock()

	return health
}

// validateChangeStreamSupport validates that the MongoDB deployment supports change streams
func (c *MongoDBConnector) validateChangeStreamSupport() error {
	// Check if we're connected to a replica set or sharded cluster
	var isMaster bson.M
	err := c.database.RunCommand(context.Background(), bson.D{{Key: "isMaster", Value: 1}}).Decode(&isMaster)
	if err != nil {
		return fmt.Errorf("failed to check deployment type: %w", err)
	}

	// Change streams require replica set or sharded cluster
	if setName, ok := isMaster["setName"]; ok && setName != nil {
		c.logger.Info("connected to replica set", zap.Any("setName", setName))
		return nil
	}

	if msg, ok := isMaster["msg"]; ok && msg == "isdbgrid" {
		c.logger.Info("connected to sharded cluster")
		return nil
	}

	return fmt.Errorf("change streams require a replica set or sharded cluster")
}

// parseResumeToken parses a resume token string
func (c *MongoDBConnector) parseResumeToken(tokenStr string) error {
	var tokenData bson.Raw
	if err := jsonpool.Unmarshal([]byte(tokenStr), &tokenData); err != nil {
		return fmt.Errorf("failed to parse resume token JSON: %w", err)
	}

	c.resumeToken = tokenData
	return nil
}

// startChangeStream starts the MongoDB change stream
func (c *MongoDBConnector) startChangeStream() error {
	// Build change stream options
	opts := options.ChangeStream()

	// Set full document options
	mongoConfig := c.getMongoConfig()
	if mongoConfig.FullDocument != "" {
		opts.SetFullDocument(options.FullDocument(mongoConfig.FullDocument))
	}
	// FullDocumentBeforeChange is available in newer versions of MongoDB driver
	// Comment out for now if using older driver version
	// if mongoConfig.FullDocumentBeforeChange != "" {
	// 	opts.SetFullDocumentBeforeChange(options.FullDocumentBeforeChange(mongoConfig.FullDocumentBeforeChange))
	// }

	// Set batch size
	if mongoConfig.BatchSize > 0 {
		opts.SetBatchSize(mongoConfig.BatchSize)
	}

	// Set max await time
	if mongoConfig.MaxAwaitTime > 0 {
		opts.SetMaxAwaitTime(mongoConfig.MaxAwaitTime)
	}

	// Set resume token if available
	if c.resumeToken != nil {
		opts.SetResumeAfter(c.resumeToken)
	}

	// Build pipeline to filter by collections
	pipeline := c.buildChangeStreamPipeline()

	// Start change stream
	var err error
	c.changeStream, err = c.database.Watch(context.Background(), pipeline, opts)
	if err != nil {
		return fmt.Errorf("failed to start change stream: %w", err)
	}

	c.logger.Info("started MongoDB change stream",
		zap.Strings("collections", c.collections))

	return nil
}

// buildChangeStreamPipeline builds the aggregation pipeline for change stream filtering
func (c *MongoDBConnector) buildChangeStreamPipeline() mongo.Pipeline {
	pipeline := mongo.Pipeline{}

	// Filter by collections if specified
	if len(c.collections) > 0 {
		collections := make([]string, len(c.collections))
		for i, coll := range c.collections {
			collections[i] = fmt.Sprintf("%s.%s", c.config.Database, coll)
		}

		matchStage := bson.D{{
			Key: "$match",
			Value: bson.D{{
				Key: "$or",
				Value: []bson.D{
					{{Key: "ns.coll", Value: bson.D{{Key: "$in", Value: c.collections}}}},
				},
			}},
		}}

		pipeline = append(pipeline, matchStage)
	}

	// Add operation type filtering if configured
	mongoConfig := c.getMongoConfig()
	if len(mongoConfig.IncludeOperationTypes) > 0 {
		matchStage := bson.D{{
			Key: "$match",
			Value: bson.D{{
				Key:   "operationType",
				Value: bson.D{{Key: "$in", Value: mongoConfig.IncludeOperationTypes}},
			}},
		}}

		pipeline = append(pipeline, matchStage)
	}

	return pipeline
}

// processChangeStream processes change stream events
func (c *MongoDBConnector) processChangeStream() {
	defer c.updateHealth("stopped", "Change stream processing stopped", nil)

	c.updateHealth("running", "Change stream processing active", nil)

	for {
		select {
		case <-c.stopCh:
			return

		default:
			// Try to get next change with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			hasNext := c.changeStream.TryNext(ctx)
			cancel()

			if !hasNext {
				// Check for errors
				if err := c.changeStream.Err(); err != nil {
					c.logger.Error("change stream error", zap.Error(err))
					c.updateMetrics(func(m *EventMetrics) {
						m.EventsErrored++
					})

					// Try to restart change stream
					if err := c.restartChangeStream(); err != nil {
						c.logger.Error("failed to restart change stream", zap.Error(err))
						time.Sleep(5 * time.Second)
					}
				}
				continue
			}

			// Decode change event
			var changeEvent MongoChangeEvent
			if err := c.changeStream.Decode(&changeEvent); err != nil {
				c.logger.Error("failed to decode change event", zap.Error(err))
				c.updateMetrics(func(m *EventMetrics) {
					m.EventsErrored++
				})
				continue
			}

			// Update resume token
			c.mutex.Lock()
			c.resumeToken = c.changeStream.ResumeToken()
			c.mutex.Unlock()

			// Process the change event
			if err := c.processMongoChangeEvent(changeEvent); err != nil {
				c.logger.Error("failed to process change event", zap.Error(err))
				c.updateMetrics(func(m *EventMetrics) {
					m.EventsErrored++
				})
			}
		}
	}
}

// processMongoChangeEvent processes a single MongoDB change event
func (c *MongoDBConnector) processMongoChangeEvent(changeEvent MongoChangeEvent) error {
	// Skip if collection is not monitored
	if !c.isCollectionMonitored(changeEvent.Namespace.Collection) {
		return nil
	}

	// Convert MongoDB operation type to our operation type
	operation := c.convertOperationType(changeEvent.OperationType)
	if operation == "" {
		return nil // Skip unsupported operations
	}

	// Build before and after data
	var before, after map[string]interface{}

	switch operation {
	case OperationInsert:
		after = changeEvent.FullDocument

	case OperationUpdate:
		// For updates, we might have full document or just the changes
		if changeEvent.FullDocument != nil {
			after = changeEvent.FullDocument
		}
		if changeEvent.FullDocumentBeforeChange != nil {
			before = changeEvent.FullDocumentBeforeChange
		}

		// If we don't have full documents, construct change information
		if after == nil && changeEvent.UpdateDescription != nil {
			after = map[string]interface{}{
				"updatedFields":   changeEvent.UpdateDescription.UpdatedFields,
				"removedFields":   changeEvent.UpdateDescription.RemovedFields,
				"truncatedArrays": changeEvent.UpdateDescription.TruncatedArrays,
			}
		}

	case OperationDelete:
		before = changeEvent.FullDocumentBeforeChange
		if before == nil && changeEvent.DocumentKey != nil {
			// At minimum, include the document key
			before = changeEvent.DocumentKey
		}

	case OperationDDL:
		// DDL operations typically don't have before/after data in the same way
		// The operation details are in the change event itself

	case OperationCommit:
		// Commit operations typically don't have before/after data
		// They represent transaction boundaries
	}

	// Create change event
	event := ChangeEvent{
		ID:        c.generateEventID(changeEvent),
		Operation: operation,
		Database:  changeEvent.Namespace.Database,
		Table:     changeEvent.Namespace.Collection,
		Before:    before,
		After:     after,
		Timestamp: time.Unix(int64(changeEvent.ClusterTime.T), 0),
		Position:  c.GetPosition(),
		Source: SourceInfo{
			Name:          fmt.Sprintf("mongodb_%s", changeEvent.Namespace.Database),
			Database:      changeEvent.Namespace.Database,
			Table:         changeEvent.Namespace.Collection,
			ConnectorType: ConnectorMongoDB,
			Timestamp:     time.Now(),
		},
		Metadata: map[string]interface{}{
			"cluster_time": changeEvent.ClusterTime,
			"document_key": changeEvent.DocumentKey,
		},
	}

	// Add transaction information if available
	if changeEvent.TxnNumber != nil {
		event.TransactionID = fmt.Sprintf("%d", *changeEvent.TxnNumber)
		event.Metadata["txn_number"] = *changeEvent.TxnNumber
	}
	if changeEvent.SessionID != nil {
		event.Metadata["session_id"] = *changeEvent.SessionID
	}

	return c.sendEvent(event)
}

// convertOperationType converts MongoDB operation type to our operation type
func (c *MongoDBConnector) convertOperationType(mongoOp string) OperationType {
	switch mongoOp {
	case "insert":
		return OperationInsert
	case "update", "replace":
		return OperationUpdate
	case "delete":
		return OperationDelete
	case "drop", "rename", "dropDatabase":
		return OperationDDL
	default:
		return "" // Unsupported operation
	}
}

// isCollectionMonitored checks if a collection is being monitored
func (c *MongoDBConnector) isCollectionMonitored(collection string) bool {
	if len(c.collectionMap) == 0 {
		return true // Monitor all collections if none specified
	}

	return c.collectionMap[collection]
}

// restartChangeStream restarts the change stream after an error
func (c *MongoDBConnector) restartChangeStream() error {
	c.logger.Info("restarting change stream")

	// Close existing change stream
	if c.changeStream != nil {
		_ = c.changeStream.Close(context.Background()) // Best effort close
	}

	// Start new change stream
	return c.startChangeStream()
}

// sendEvent sends a change event to the event channel
func (c *MongoDBConnector) sendEvent(event ChangeEvent) error {
	select {
	case c.eventCh <- event:
		c.updateMetrics(func(m *EventMetrics) {
			m.EventsReceived++
			m.EventsProcessed++
			m.LastEventTime = time.Now()
		})

		c.updateHealth("running", "Processing events", nil)

		c.logger.Debug("sent change event",
			zap.String("operation", string(event.Operation)),
			zap.String("collection", event.Table),
			zap.String("event_id", event.ID))

		return nil

	default:
		c.updateMetrics(func(m *EventMetrics) {
			m.EventsErrored++
		})
		return fmt.Errorf("event channel is full")
	}
}

// generateEventID generates a unique event ID
func (c *MongoDBConnector) generateEventID(changeEvent MongoChangeEvent) string {
	// Use the change stream's _id as part of the event ID
	idData, _ := jsonpool.Marshal(changeEvent.ID)
	return fmt.Sprintf("mongo_%d_%s", time.Now().UnixNano(), string(idData))
}

// getMongoConfig extracts MongoDB-specific configuration
func (c *MongoDBConnector) getMongoConfig() MongoDBConfig {
	config := MongoDBConfig{
		FullDocument:             "updateLookup",
		FullDocumentBeforeChange: "whenAvailable",
		BatchSize:                1000,
		MaxAwaitTime:             30 * time.Second,
	}

	if opts, ok := c.config.Options["mongodb"]; ok {
		if optsMap, ok := opts.(map[string]interface{}); ok {
			if val, ok := optsMap["full_document"]; ok {
				config.FullDocument = val.(string)
			}
			if val, ok := optsMap["full_document_before_change"]; ok {
				config.FullDocumentBeforeChange = val.(string)
			}
			if val, ok := optsMap["batch_size"]; ok {
				if batchSize, ok := val.(float64); ok {
					config.BatchSize = int32(batchSize)
				}
			}
			if val, ok := optsMap["include_operation_types"]; ok {
				if opTypes, ok := val.([]interface{}); ok {
					for _, opType := range opTypes {
						if opTypeStr, ok := opType.(string); ok {
							config.IncludeOperationTypes = append(config.IncludeOperationTypes, opTypeStr)
						}
					}
				}
			}
		}
	}

	return config
}

// updateHealth updates the health status
func (c *MongoDBConnector) updateHealth(status, message string, details map[string]interface{}) {
	c.healthMutex.Lock()
	defer c.healthMutex.Unlock()

	c.health.Status = status
	c.health.Message = message
	if details != nil {
		c.health.Details = details
	}
}

// updateMetrics updates the metrics
func (c *MongoDBConnector) updateMetrics(updateFn func(*EventMetrics)) {
	c.metricsMutex.Lock()
	defer c.metricsMutex.Unlock()

	updateFn(&c.metrics)
}
