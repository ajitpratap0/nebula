package cdc

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/errors"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
)

// PostgreSQLConnector implements CDC for PostgreSQL using logical replication
type PostgreSQLConnector struct {
	config CDCConfig
	logger *zap.Logger

	// Database connections
	conn     *pgx.Conn
	replConn *pgconn.PgConn

	// Replication state
	slotName     string
	publication  string
	currentLSN   pglogrepl.LSN
	confirmedLSN pglogrepl.LSN

	// Event streaming
	eventCh chan ChangeEvent
	errorCh chan error
	stopCh  chan struct{}

	// Synchronization
	mutex   sync.RWMutex
	running bool

	// Table schema cache
	schemaCache map[string]*TableSchema
	schemaMutex sync.RWMutex

	// Relation ID to table name mapping
	relationMap     sync.Map // map[uint32]string
	relationMapLock sync.RWMutex

	// Metrics
	metrics      EventMetrics
	metricsMutex sync.Mutex

	// Health status
	health      HealthStatus
	healthMutex sync.Mutex
}

// TableSchema represents PostgreSQL table schema information
type TableSchema struct {
	Name        string
	Columns     []ColumnInfo
	PrimaryKey  []string
	LastUpdated time.Time
}

// ColumnInfo represents PostgreSQL column information
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
	Default  interface{}
	OID      uint32
}

// PostgreSQLConfig contains PostgreSQL-specific configuration
type PostgreSQLConfig struct {
	SlotName       string `json:"slot_name"`
	Publication    string `json:"publication"`
	StartLSN       string `json:"start_lsn,omitempty"`
	TempSlot       bool   `json:"temp_slot"`
	PluginName     string `json:"plugin_name"`
	StatusInterval int    `json:"status_interval"`
}

// NewPostgreSQLConnector creates a new PostgreSQL CDC connector
func NewPostgreSQLConnector(logger *zap.Logger) *PostgreSQLConnector {
	return &PostgreSQLConnector{
		logger:      logger.With(zap.String("connector", "postgresql")),
		eventCh:     make(chan ChangeEvent, 1000),
		errorCh:     make(chan error, 100),
		stopCh:      make(chan struct{}),
		schemaCache: make(map[string]*TableSchema),
		health: HealthStatus{
			Status:    "disconnected",
			Message:   "Not connected",
			LastEvent: time.Time{},
		},
	}
}

// Connect establishes connection to PostgreSQL and sets up logical replication
func (c *PostgreSQLConnector) Connect(config CDCConfig) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if err := config.Validate(); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "invalid config")
	}

	c.config = config

	// Parse PostgreSQL-specific options
	pgConfig := PostgreSQLConfig{
		SlotName:       "nebula_cdc_slot",
		Publication:    "nebula_cdc_pub",
		TempSlot:       false,
		PluginName:     "pgoutput",
		StatusInterval: 10,
	}

	if opts, ok := config.Options["postgresql"]; ok {
		if optsMap, ok := opts.(map[string]interface{}); ok {
			if val, ok := optsMap["slot_name"]; ok {
				pgConfig.SlotName = val.(string)
			}
			if val, ok := optsMap["publication"]; ok {
				pgConfig.Publication = val.(string)
			}
			if val, ok := optsMap["start_lsn"]; ok {
				pgConfig.StartLSN = val.(string)
			}
			if val, ok := optsMap["temp_slot"]; ok {
				pgConfig.TempSlot = val.(bool)
			}
			if val, ok := optsMap["plugin_name"]; ok {
				pgConfig.PluginName = val.(string)
			}
		}
	}

	c.slotName = pgConfig.SlotName
	c.publication = pgConfig.Publication

	// Establish regular connection for metadata queries
	var err error
	c.conn, err = pgx.Connect(context.Background(), config.ConnectionStr)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to connect to PostgreSQL")
	}

	// Test connection
	var version string
	err = c.conn.QueryRow(context.Background(), "SELECT version()").Scan(&version)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeQuery, "failed to query PostgreSQL version")
	}

	c.logger.Info("connected to PostgreSQL", zap.String("version", version))

	// Establish replication connection
	replConfig, err := pgconn.ParseConfig(config.ConnectionStr)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "failed to parse replication config")
	}

	replConfig.RuntimeParams["replication"] = "database"

	c.replConn, err = pgconn.ConnectConfig(context.Background(), replConfig)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to establish replication connection")
	}

	// Create publication if it doesn't exist
	if err := c.ensurePublication(); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "failed to ensure publication")
	}

	// Create replication slot if it doesn't exist
	if err := c.ensureReplicationSlot(pgConfig); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "failed to ensure replication slot")
	}

	// Load table schemas
	if err := c.loadTableSchemas(); err != nil {
		return errors.Wrap(err, errors.ErrorTypeQuery, "failed to load table schemas")
	}

	c.updateHealth("connected", "Connected to PostgreSQL", nil)

	c.logger.Info("PostgreSQL CDC connector initialized successfully",
		zap.String("slot", c.slotName),
		zap.String("publication", c.publication))

	return nil
}

// Subscribe starts listening to changes on specified tables
func (c *PostgreSQLConnector) Subscribe(tables []string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.running {
		return errors.New(errors.ErrorTypeConflict, "connector is already running")
	}

	// Update publication to include specified tables
	if err := c.updatePublication(tables); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "failed to update publication")
	}

	c.running = true

	// Start replication in a separate goroutine
	go c.startReplication()

	c.updateHealth("running", "Subscribed to tables", nil)

	c.logger.Info("subscribed to tables", zap.Strings("tables", tables))

	return nil
}

// ReadChanges returns a channel of change events
func (c *PostgreSQLConnector) ReadChanges(ctx context.Context) (<-chan ChangeEvent, error) {
	if !c.running {
		return nil, errors.New(errors.ErrorTypeConflict, "connector is not running")
	}

	return c.eventCh, nil
}

// GetPosition returns the current replication position
func (c *PostgreSQLConnector) GetPosition() Position {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return Position{
		Type:  "postgresql_lsn",
		Value: c.currentLSN.String(),
		Metadata: map[string]interface{}{
			"confirmed_lsn": c.confirmedLSN.String(),
			"slot_name":     c.slotName,
		},
	}
}

// Acknowledge confirms processing of events up to the given position
func (c *PostgreSQLConnector) Acknowledge(position Position) error {
	if position.Type != "postgresql_lsn" {
		return errors.New(errors.ErrorTypeValidation, stringpool.Sprintf("invalid position type: %s", position.Type))
	}

	lsnStr, ok := position.Value.(string)
	if !ok {
		return errors.New(errors.ErrorTypeData, stringpool.Sprintf("invalid LSN value type: %T", position.Value))
	}

	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to parse LSN")
	}

	c.mutex.Lock()
	c.confirmedLSN = lsn
	c.mutex.Unlock()

	// Send standby status update
	return c.sendStandbyStatus()
}

// Stop gracefully shuts down the connector
func (c *PostgreSQLConnector) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.running {
		return nil
	}

	c.running = false
	close(c.stopCh)

	// Close connections
	if c.replConn != nil {
		if err := c.replConn.Close(context.Background()); err != nil {
			c.logger.Error("failed to close replication connection", zap.Error(err))
		}
	}

	if c.conn != nil {
		if err := c.conn.Close(context.Background()); err != nil {
			c.logger.Error("failed to close connection", zap.Error(err))
		}
	}

	c.updateHealth("stopped", "Connector stopped", nil)

	c.logger.Info("PostgreSQL CDC connector stopped")

	return nil
}

// Health returns the health status of the connector
func (c *PostgreSQLConnector) Health() HealthStatus {
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

// ensurePublication creates the publication if it doesn't exist
func (c *PostgreSQLConnector) ensurePublication() error {
	// Check if publication exists
	var exists bool
	err := c.conn.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
		c.publication).Scan(&exists)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeQuery, "failed to check publication existence")
	}

	if !exists {
		// Create publication for all tables initially
		_, err = c.conn.Exec(context.Background(),
			stringpool.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", c.publication))
		if err != nil {
			return errors.Wrap(err, errors.ErrorTypeQuery, "failed to create publication")
		}

		c.logger.Info("created publication", zap.String("publication", c.publication))
	}

	return nil
}

// updatePublication updates the publication to include specified tables
func (c *PostgreSQLConnector) updatePublication(tables []string) error {
	// Build table list with schema qualification
	var qualifiedTables []string
	for _, table := range tables {
		if !strings.Contains(table, ".") {
			qualifiedTables = append(qualifiedTables, stringpool.Sprintf("public.%s", table))
		} else {
			qualifiedTables = append(qualifiedTables, table)
		}
	}

	// Update publication to include only specified tables
	tableList := stringpool.JoinPooled(qualifiedTables, ", ")
	_, err := c.conn.Exec(context.Background(),
		stringpool.Sprintf("ALTER PUBLICATION %s SET TABLE %s", c.publication, tableList))
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "failed to update publication")
	}

	c.logger.Info("updated publication",
		zap.String("publication", c.publication),
		zap.Strings("tables", qualifiedTables))

	return nil
}

// ensureReplicationSlot creates the replication slot if it doesn't exist
func (c *PostgreSQLConnector) ensureReplicationSlot(config PostgreSQLConfig) error {
	// Check if slot exists
	var exists bool
	err := c.conn.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		c.slotName).Scan(&exists)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeQuery, "failed to check replication slot existence")
	}

	if !exists {
		// Create replication slot
		result, err := pglogrepl.CreateReplicationSlot(context.Background(), c.replConn,
			c.slotName, config.PluginName,
			pglogrepl.CreateReplicationSlotOptions{Temporary: config.TempSlot})
		if err != nil {
			return errors.Wrap(err, errors.ErrorTypeConfig, "failed to create replication slot")
		}

		c.logger.Info("created replication slot",
			zap.String("slot", c.slotName),
			zap.String("plugin", config.PluginName),
			zap.String("consistent_point", result.ConsistentPoint))
	}

	return nil
}

// loadTableSchemas loads schema information for all tables
func (c *PostgreSQLConnector) loadTableSchemas() error {
	// Query table and column information
	query := `
		SELECT 
			t.table_schema,
			t.table_name,
			c.column_name,
			c.data_type,
			c.is_nullable,
			c.column_default,
			pt.oid
		FROM information_schema.tables t
		JOIN information_schema.columns c ON t.table_name = c.table_name 
			AND t.table_schema = c.table_schema
		LEFT JOIN pg_type pt ON c.udt_name = pt.typname
		WHERE t.table_type = 'BASE TABLE'
			AND t.table_schema NOT IN ('information_schema', 'pg_catalog')
		ORDER BY t.table_schema, t.table_name, c.ordinal_position
	`

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeQuery, "failed to query table schemas")
	}
	defer rows.Close()

	c.schemaMutex.Lock()
	defer c.schemaMutex.Unlock()

	schemaMap := make(map[string]*TableSchema)

	for rows.Next() {
		var schema, table, column, dataType, nullable string
		var columnDefault interface{}
		var oid uint32

		err := rows.Scan(&schema, &table, &column, &dataType, &nullable, &columnDefault, &oid)
		if err != nil {
			return errors.Wrap(err, errors.ErrorTypeData, "failed to scan schema row")
		}

		tableName := stringpool.Sprintf("%s.%s", schema, table)

		if _, exists := schemaMap[tableName]; !exists {
			schemaMap[tableName] = &TableSchema{
				Name:        tableName,
				Columns:     make([]ColumnInfo, 0),
				LastUpdated: time.Now(),
			}
		}

		columnInfo := ColumnInfo{
			Name:     column,
			Type:     dataType,
			Nullable: nullable == "YES",
			Default:  columnDefault,
			OID:      oid,
		}

		schemaMap[tableName].Columns = append(schemaMap[tableName].Columns, columnInfo)
	}

	// Load primary key information
	for tableName := range schemaMap {
		parts := strings.Split(tableName, ".")
		if len(parts) != 2 {
			continue
		}

		schema := parts[0]
		table := parts[1]

		pkQuery := `
			SELECT a.attname
			FROM pg_index i
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
			WHERE i.indrelid = $1::regclass AND i.indisprimary
			ORDER BY array_position(i.indkey, a.attnum)
		`

		pkRows, err := c.conn.Query(context.Background(), pkQuery,
			stringpool.Sprintf("%s.%s", schema, table))
		if err != nil {
			c.logger.Warn("failed to load primary key info",
				zap.String("table", tableName), zap.Error(err))
			continue
		}

		var primaryKey []string
		for pkRows.Next() {
			var column string
			if err := pkRows.Scan(&column); err != nil {
				continue
			}
			primaryKey = append(primaryKey, column)
		}
		pkRows.Close()

		schemaMap[tableName].PrimaryKey = primaryKey
	}

	c.schemaCache = schemaMap

	// Load relation OIDs for table name mapping
	relationQuery := `
		SELECT c.oid, n.nspname, c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r', 'p')
			AND n.nspname NOT IN ('pg_catalog', 'information_schema')
	`

	relationRows, err := c.conn.Query(context.Background(), relationQuery)
	if err != nil {
		c.logger.Warn("failed to load relation OIDs", zap.Error(err))
	} else {
		defer relationRows.Close()

		for relationRows.Next() {
			var oid uint32
			var schema, table string
			if err := relationRows.Scan(&oid, &schema, &table); err != nil {
				continue
			}

			tableName := stringpool.Sprintf("%s.%s", schema, table)
			c.relationMap.Store(oid, tableName)
		}
	}

	c.logger.Info("loaded table schemas",
		zap.Int("table_count", len(schemaMap)),
		zap.Int("relation_mappings", func() int {
			count := 0
			c.relationMap.Range(func(_, _ interface{}) bool {
				count++
				return true
			})
			return count
		}()))

	return nil
}

// startReplication starts the logical replication process
func (c *PostgreSQLConnector) startReplication() {
	defer c.updateHealth("stopped", "Replication stopped", nil)

	// Determine starting LSN
	var startLSN pglogrepl.LSN
	if c.config.StartPosition != nil && c.config.StartPosition.Type == "postgresql_lsn" {
		if lsnStr, ok := c.config.StartPosition.Value.(string); ok {
			if lsn, err := pglogrepl.ParseLSN(lsnStr); err == nil {
				startLSN = lsn
			}
		}
	}

	// Start logical replication
	pluginArguments := []string{
		"proto_version '1'",
		stringpool.Sprintf("publication_names '%s'", c.publication),
	}

	err := pglogrepl.StartReplication(context.Background(), c.replConn,
		c.slotName, startLSN, pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArguments,
		})
	if err != nil {
		c.logger.Error("failed to start replication", zap.Error(err))
		return
	}

	c.logger.Info("started logical replication",
		zap.String("start_lsn", startLSN.String()))

	// Status update ticker
	statusTicker := time.NewTicker(10 * time.Second)
	defer statusTicker.Stop()

	c.updateHealth("running", "Replication active", nil)

	for {
		select {
		case <-c.stopCh:
			return

		case <-statusTicker.C:
			if err := c.sendStandbyStatus(); err != nil {
				c.logger.Error("failed to send standby status", zap.Error(err))
			}

		default:
			// Receive messages with timeout
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			msg, err := c.replConn.ReceiveMessage(ctx)
			cancel()

			if err != nil {
				if pgconn.Timeout(err) {
					continue // Timeout is expected
				}
				c.logger.Error("failed to receive replication message", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			if err := c.processReplicationMessage(msg); err != nil {
				c.logger.Error("failed to process replication message", zap.Error(err))
				c.updateMetrics(func(m *EventMetrics) {
					m.EventsErrored++
				})
			}
		}
	}
}

// processReplicationMessage processes a single replication message
func (c *PostgreSQLConnector) processReplicationMessage(msg pgproto3.BackendMessage) error {
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.XLogDataByteID:
			return c.processXLogData(msg.Data[1:])
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			return c.processPrimaryKeepalive(msg.Data[1:])
		}
	}
	return nil
}

// processXLogData processes WAL data messages
func (c *PostgreSQLConnector) processXLogData(data []byte) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to parse XLogData")
	}

	c.mutex.Lock()
	c.currentLSN = xld.WALStart
	c.mutex.Unlock()

	// Parse logical replication message
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to parse logical replication message")
	}

	return c.processLogicalMessage(logicalMsg)
}

// processLogicalMessage processes logical replication messages
func (c *PostgreSQLConnector) processLogicalMessage(msg pglogrepl.Message) error {
	switch msg := msg.(type) {
	case *pglogrepl.RelationMessage:
		return c.processRelationMessage(msg)
	case *pglogrepl.InsertMessage:
		return c.processInsertMessage(msg)
	case *pglogrepl.UpdateMessage:
		return c.processUpdateMessage(msg)
	case *pglogrepl.DeleteMessage:
		return c.processDeleteMessage(msg)
	case *pglogrepl.BeginMessage:
		return c.processBeginMessage(msg)
	case *pglogrepl.CommitMessage:
		return c.processCommitMessage(msg)
	}

	return nil
}

// processRelationMessage processes relation (table) definition messages
func (c *PostgreSQLConnector) processRelationMessage(msg *pglogrepl.RelationMessage) error {
	tableName := stringpool.Sprintf("%s.%s", msg.Namespace, msg.RelationName)

	c.schemaMutex.Lock()
	defer c.schemaMutex.Unlock()

	// Update schema cache with relation information
	schema := &TableSchema{
		Name:        tableName,
		Columns:     make([]ColumnInfo, len(msg.Columns)),
		LastUpdated: time.Now(),
	}

	for i, col := range msg.Columns {
		schema.Columns[i] = ColumnInfo{
			Name: col.Name,
			Type: stringpool.Sprintf("oid_%d", col.DataType),
			OID:  col.DataType,
		}
	}

	c.schemaCache[tableName] = schema

	c.logger.Debug("updated relation schema",
		zap.String("table", tableName),
		zap.Int("columns", len(msg.Columns)))

	return nil
}

// processInsertMessage processes INSERT operations
func (c *PostgreSQLConnector) processInsertMessage(msg *pglogrepl.InsertMessage) error {
	tableName := c.getTableNameByRelationID(msg.RelationID)
	if tableName == "" {
		return errors.New(errors.ErrorTypeData, stringpool.Sprintf("unknown relation ID: %d", msg.RelationID))
	}

	after, err := c.tupleToMap(msg.Tuple, tableName)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to convert tuple to map")
	}

	event := ChangeEvent{
		ID:        c.generateEventID(),
		Operation: OperationInsert,
		Database:  c.config.Database,
		Table:     tableName,
		After:     after,
		Timestamp: time.Now(),
		Position:  c.GetPosition(),
		Source: SourceInfo{
			Name:          stringpool.Sprintf("postgresql_%s", c.config.Database),
			Database:      c.config.Database,
			Table:         tableName,
			ConnectorType: ConnectorPostgreSQL,
			Timestamp:     time.Now(),
		},
	}

	return c.sendEvent(event)
}

// processUpdateMessage processes UPDATE operations
func (c *PostgreSQLConnector) processUpdateMessage(msg *pglogrepl.UpdateMessage) error {
	tableName := c.getTableNameByRelationID(msg.RelationID)
	if tableName == "" {
		return errors.New(errors.ErrorTypeData, stringpool.Sprintf("unknown relation ID: %d", msg.RelationID))
	}

	var before map[string]interface{}
	var err error

	if msg.OldTuple != nil {
		before, err = c.tupleToMap(msg.OldTuple, tableName)
		if err != nil {
			return errors.Wrap(err, errors.ErrorTypeData, "failed to convert old tuple to map")
		}
	}

	after, err := c.tupleToMap(msg.NewTuple, tableName)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to convert new tuple to map")
	}

	event := ChangeEvent{
		ID:        c.generateEventID(),
		Operation: OperationUpdate,
		Database:  c.config.Database,
		Table:     tableName,
		Before:    before,
		After:     after,
		Timestamp: time.Now(),
		Position:  c.GetPosition(),
		Source: SourceInfo{
			Name:          stringpool.Sprintf("postgresql_%s", c.config.Database),
			Database:      c.config.Database,
			Table:         tableName,
			ConnectorType: ConnectorPostgreSQL,
			Timestamp:     time.Now(),
		},
	}

	return c.sendEvent(event)
}

// processDeleteMessage processes DELETE operations
func (c *PostgreSQLConnector) processDeleteMessage(msg *pglogrepl.DeleteMessage) error {
	tableName := c.getTableNameByRelationID(msg.RelationID)
	if tableName == "" {
		return errors.New(errors.ErrorTypeData, stringpool.Sprintf("unknown relation ID: %d", msg.RelationID))
	}

	var before map[string]interface{}
	var err error

	if msg.OldTuple != nil {
		before, err = c.tupleToMap(msg.OldTuple, tableName)
		if err != nil {
			return errors.Wrap(err, errors.ErrorTypeData, "failed to convert old tuple to map")
		}
	}

	event := ChangeEvent{
		ID:        c.generateEventID(),
		Operation: OperationDelete,
		Database:  c.config.Database,
		Table:     tableName,
		Before:    before,
		Timestamp: time.Now(),
		Position:  c.GetPosition(),
		Source: SourceInfo{
			Name:          stringpool.Sprintf("postgresql_%s", c.config.Database),
			Database:      c.config.Database,
			Table:         tableName,
			ConnectorType: ConnectorPostgreSQL,
			Timestamp:     time.Now(),
		},
	}

	return c.sendEvent(event)
}

// processBeginMessage processes transaction begin messages
func (c *PostgreSQLConnector) processBeginMessage(msg *pglogrepl.BeginMessage) error {
	c.logger.Debug("transaction begin",
		zap.Uint32("xid", msg.Xid))
	return nil
}

// processCommitMessage processes transaction commit messages
func (c *PostgreSQLConnector) processCommitMessage(msg *pglogrepl.CommitMessage) error {
	c.logger.Debug("transaction commit",
		zap.Time("timestamp", msg.CommitTime))
	return nil
}

// processPrimaryKeepalive processes primary keepalive messages
func (c *PostgreSQLConnector) processPrimaryKeepalive(data []byte) error {
	keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to parse keepalive")
	}

	if keepalive.ReplyRequested {
		return c.sendStandbyStatus()
	}

	return nil
}

// sendStandbyStatus sends standby status update
func (c *PostgreSQLConnector) sendStandbyStatus() error {
	c.mutex.RLock()
	currentLSN := c.currentLSN
	confirmedLSN := c.confirmedLSN
	c.mutex.RUnlock()

	status := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: currentLSN,
		WALFlushPosition: currentLSN,
		WALApplyPosition: confirmedLSN,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	}
	return pglogrepl.SendStandbyStatusUpdate(context.Background(), c.replConn, status)
}

// tupleToMap converts a pglogrepl.TupleData to a map
func (c *PostgreSQLConnector) tupleToMap(tuple *pglogrepl.TupleData, tableName string) (map[string]interface{}, error) {
	c.schemaMutex.RLock()
	schema, exists := c.schemaCache[tableName]
	c.schemaMutex.RUnlock()

	if !exists {
		return nil, errors.New(errors.ErrorTypeNotFound, stringpool.Sprintf("schema not found for table: %s", tableName))
	}

	result := pool.GetMap()

	defer pool.PutMap(result)

	for i, col := range tuple.Columns {
		if i >= len(schema.Columns) {
			continue
		}

		columnName := schema.Columns[i].Name

		switch col.DataType {
		case 'n': // null
			result[columnName] = nil
		case 'u': // unchanged (for REPLICA IDENTITY)
			// Skip unchanged columns in updates
			continue
		case 't': // text
			result[columnName] = string(col.Data)
		default:
			// Try to decode based on PostgreSQL type
			result[columnName] = c.decodeColumnValue(col.Data, schema.Columns[i])
		}
	}

	return result, nil
}

// decodeColumnValue decodes a column value based on its PostgreSQL type
func (c *PostgreSQLConnector) decodeColumnValue(data []byte, column ColumnInfo) interface{} {
	if len(data) == 0 {
		return nil
	}

	value := string(data)

	// Basic type conversions - this could be enhanced with proper PostgreSQL type handling
	switch column.Type {
	case "integer", "bigint", "smallint":
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i
		}
	case "numeric", "decimal", "real", "double precision":
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
	case "boolean":
		return value == "t" || value == "true"
	case "timestamp", "timestamptz", "date", "time", "timetz":
		if t, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
			return t
		}
		if t, err := time.Parse("2006-01-02", value); err == nil {
			return t
		}
	case "json", "jsonb":
		var result interface{}
		if err := jsonpool.Unmarshal(data, &result); err == nil {
			return result
		}
	}

	return value
}

// getTableNameByRelationID gets table name by relation ID
func (c *PostgreSQLConnector) getTableNameByRelationID(relationID uint32) string {
	// Check the relation map first
	if tableName, ok := c.relationMap.Load(relationID); ok {
		return tableName.(string)
	}

	// If not found, try to look it up and cache it
	var schema, table string
	err := c.conn.QueryRow(context.Background(), `
		SELECT n.nspname, c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.oid = $1
	`, relationID).Scan(&schema, &table)

	if err == nil {
		tableName := stringpool.Sprintf("%s.%s", schema, table)
		c.relationMap.Store(relationID, tableName)
		return tableName
	}

	c.logger.Warn("failed to resolve relation ID",
		zap.Uint32("relation_id", relationID),
		zap.Error(err))

	return ""
}

// sendEvent sends a change event to the event channel
func (c *PostgreSQLConnector) sendEvent(event ChangeEvent) error {
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
			zap.String("table", event.Table),
			zap.String("event_id", event.ID))

		return nil

	default:
		c.updateMetrics(func(m *EventMetrics) {
			m.EventsErrored++
		})
		return errors.New(errors.ErrorTypeRateLimit, "event channel is full")
	}
}

// generateEventID generates a unique event ID
func (c *PostgreSQLConnector) generateEventID() string {
	return stringpool.Sprintf("pg_%d_%s", time.Now().UnixNano(), c.currentLSN.String())
}

// updateHealth updates the health status
func (c *PostgreSQLConnector) updateHealth(status, message string, details map[string]interface{}) {
	c.healthMutex.Lock()
	defer c.healthMutex.Unlock()

	c.health.Status = status
	c.health.Message = message
	if details != nil {
		c.health.Details = details
	}
}

// updateMetrics updates the metrics
func (c *PostgreSQLConnector) updateMetrics(updateFn func(*EventMetrics)) {
	c.metricsMutex.Lock()
	defer c.metricsMutex.Unlock()

	updateFn(&c.metrics)
}
