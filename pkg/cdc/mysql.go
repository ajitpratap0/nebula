package cdc

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/errors"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

// MySQLConnector implements CDC for MySQL using binary log replication
type MySQLConnector struct {
	config CDCConfig
	logger *zap.Logger

	// MySQL connections
	db     *sql.DB
	canal  *canal.Canal
	syncer *replication.BinlogSyncer

	// Replication state
	position mysql.Position
	serverID uint32

	// Event streaming
	eventCh chan ChangeEvent
	errorCh chan error
	stopCh  chan struct{}

	// Synchronization
	mutex   sync.RWMutex
	running bool

	// Table schema cache
	schemaCache map[string]*schema.Table
	schemaMutex sync.RWMutex

	// Metrics
	metrics      EventMetrics
	metricsMutex sync.Mutex

	// Health status
	health      HealthStatus
	healthMutex sync.Mutex
}

// MySQLConfig contains MySQL-specific configuration
type MySQLConfig struct {
	ServerID              uint32        `json:"server_id"`
	StartPosition         string        `json:"start_position,omitempty"`
	Flavor                string        `json:"flavor"` // mysql or mariadb
	GTIDEnabled           bool          `json:"gtid_enabled"`
	HeartbeatPeriod       time.Duration `json:"heartbeat_period"`
	ReadTimeout           time.Duration `json:"read_timeout"`
	UseDecimal            bool          `json:"use_decimal"`
	IgnoreJSONDecodeError bool          `json:"ignore_json_decode_error"`
}

// NewMySQLConnector creates a new MySQL CDC connector
func NewMySQLConnector(logger *zap.Logger) *MySQLConnector {
	return &MySQLConnector{
		logger:      logger.With(zap.String("connector", "mysql")),
		eventCh:     make(chan ChangeEvent, 1000),
		errorCh:     make(chan error, 100),
		stopCh:      make(chan struct{}),
		schemaCache: make(map[string]*schema.Table),
		serverID:    1001, // Default server ID
		health: HealthStatus{
			Status:    "disconnected",
			Message:   "Not connected",
			LastEvent: time.Time{},
		},
	}
}

// Connect establishes connection to MySQL and sets up binary log replication
func (c *MySQLConnector) Connect(config CDCConfig) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if err := config.Validate(); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "invalid config")
	}

	c.config = config

	// Parse MySQL-specific options
	mysqlConfig := MySQLConfig{
		ServerID:              1001,
		Flavor:                "mysql",
		GTIDEnabled:           false,
		HeartbeatPeriod:       60 * time.Second,
		ReadTimeout:           90 * time.Second,
		UseDecimal:            true,
		IgnoreJSONDecodeError: false,
	}

	if opts, ok := config.Options["mysql"]; ok {
		if optsMap, ok := opts.(map[string]interface{}); ok {
			if val, ok := optsMap["server_id"]; ok {
				if serverID, ok := val.(float64); ok {
					mysqlConfig.ServerID = uint32(serverID)
				}
			}
			if val, ok := optsMap["start_position"]; ok {
				mysqlConfig.StartPosition = val.(string)
			}
			if val, ok := optsMap["flavor"]; ok {
				mysqlConfig.Flavor = val.(string)
			}
			if val, ok := optsMap["gtid_enabled"]; ok {
				mysqlConfig.GTIDEnabled = val.(bool)
			}
		}
	}

	c.serverID = mysqlConfig.ServerID

	// Establish regular MySQL connection
	var err error
	c.db, err = sql.Open("mysql", config.ConnectionStr)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to connect to MySQL")
	}

	// Test connection
	var version string
	err = c.db.QueryRow("SELECT VERSION()").Scan(&version)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeQuery, "failed to query MySQL version")
	}

	c.logger.Info("connected to MySQL", zap.String("version", version))

	// Setup canal for schema tracking
	cfg := canal.NewDefaultConfig()
	cfg.Addr = c.extractHostPort(config.ConnectionStr)
	cfg.User, cfg.Password = c.extractCredentials(config.ConnectionStr)
	cfg.Dump.ExecutionPath = "" // Disable mysqldump
	cfg.Dump.DiscardErr = true

	c.canal, err = canal.NewCanal(cfg)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "failed to create canal")
	}

	// Setup binlog syncer
	syncerConfig := replication.BinlogSyncerConfig{
		ServerID:        c.serverID,
		Flavor:          mysqlConfig.Flavor,
		Host:            c.extractHost(config.ConnectionStr),
		Port:            c.extractPort(config.ConnectionStr),
		User:            c.extractUser(config.ConnectionStr),
		Password:        c.extractPassword(config.ConnectionStr),
		Charset:         "utf8mb4",
		HeartbeatPeriod: mysqlConfig.HeartbeatPeriod,
		ReadTimeout:     mysqlConfig.ReadTimeout,
		UseDecimal:      mysqlConfig.UseDecimal,
	}

	c.syncer = replication.NewBinlogSyncer(syncerConfig)

	// Get current binlog position if not specified
	if mysqlConfig.StartPosition == "" {
		if err := c.getCurrentPosition(); err != nil {
			return errors.Wrap(err, errors.ErrorTypeQuery, "failed to get current position")
		}
	} else {
		if err := c.parsePosition(mysqlConfig.StartPosition); err != nil {
			return errors.Wrap(err, errors.ErrorTypeConfig, "failed to parse start position")
		}
	}

	// Load table schemas
	if err := c.loadTableSchemas(); err != nil {
		return errors.Wrap(err, errors.ErrorTypeQuery, "failed to load table schemas")
	}

	c.updateHealth("connected", "Connected to MySQL", nil)

	c.logger.Info("MySQL CDC connector initialized successfully",
		zap.String("position", c.position.String()),
		zap.Uint32("server_id", c.serverID))

	return nil
}

// Subscribe starts listening to changes on specified tables
func (c *MySQLConnector) Subscribe(tables []string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.running {
		return errors.New(errors.ErrorTypeConfig, "connector is already running")
	}

	// Filter tables to monitor
	c.config.Tables = tables

	c.running = true

	// Start binary log streaming in a separate goroutine
	go c.startBinlogStreaming()

	c.updateHealth("running", "Subscribed to tables", nil)

	c.logger.Info("subscribed to tables", zap.Strings("tables", tables))

	return nil
}

// ReadChanges returns a channel of change events
func (c *MySQLConnector) ReadChanges(ctx context.Context) (<-chan ChangeEvent, error) {
	if !c.running {
		return nil, errors.New(errors.ErrorTypeConfig, "connector is not running")
	}

	return c.eventCh, nil
}

// GetPosition returns the current replication position
func (c *MySQLConnector) GetPosition() Position {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return Position{
		Type:  "mysql_binlog",
		Value: c.position.String(),
		Metadata: map[string]interface{}{
			"name": c.position.Name,
			"pos":  c.position.Pos,
		},
	}
}

// Acknowledge confirms processing of events up to the given position
func (c *MySQLConnector) Acknowledge(position Position) error {
	if position.Type != "mysql_binlog" {
		return errors.New(errors.ErrorTypeData, stringpool.Sprintf("invalid position type: %s", position.Type))
	}

	posStr, ok := position.Value.(string)
	if !ok {
		return errors.New(errors.ErrorTypeData, stringpool.Sprintf("invalid position value type: %T", position.Value))
	}

	newPos, err := c.parsePositionString(posStr)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to parse position")
	}

	c.mutex.Lock()
	c.position = newPos
	c.mutex.Unlock()

	c.logger.Debug("acknowledged position", zap.String("position", posStr))

	return nil
}

// Stop gracefully shuts down the connector
func (c *MySQLConnector) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.running {
		return nil
	}

	c.running = false
	close(c.stopCh)

	// Close syncer
	if c.syncer != nil {
		c.syncer.Close()
	}

	// Close canal
	if c.canal != nil {
		c.canal.Close()
	}

	// Close database connection
	if c.db != nil {
		_ = c.db.Close() // Best effort close
	}

	c.updateHealth("stopped", "Connector stopped", nil)

	c.logger.Info("MySQL CDC connector stopped")

	return nil
}

// Health returns the health status of the connector
func (c *MySQLConnector) Health() HealthStatus {
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

// getCurrentPosition gets the current binlog position
func (c *MySQLConnector) getCurrentPosition() error {
	var file string
	var pos uint32

	err := c.db.QueryRow("SHOW MASTER STATUS").Scan(&file, &pos, nil, nil, nil)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeQuery, "failed to get master status")
	}

	c.position = mysql.Position{
		Name: file,
		Pos:  pos,
	}

	return nil
}

// parsePosition parses a position string
func (c *MySQLConnector) parsePosition(posStr string) error {
	pos, err := c.parsePositionString(posStr)
	if err != nil {
		return err
	}

	c.position = pos
	return nil
}

// parsePositionString parses a position string into mysql.Position
func (c *MySQLConnector) parsePositionString(posStr string) (mysql.Position, error) {
	parts := strings.Split(posStr, ":")
	if len(parts) != 2 {
		return mysql.Position{}, errors.New(errors.ErrorTypeData, stringpool.Sprintf("invalid position format: %s", posStr))
	}

	pos, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return mysql.Position{}, errors.Wrap(err, errors.ErrorTypeData, stringpool.Sprintf("invalid position number: %s", parts[1]))
	}

	return mysql.Position{
		Name: parts[0],
		Pos:  uint32(pos),
	}, nil
}

// loadTableSchemas loads schema information for specified tables
func (c *MySQLConnector) loadTableSchemas() error {
	c.schemaMutex.Lock()
	defer c.schemaMutex.Unlock()

	// Load schemas for each table
	for _, table := range c.config.Tables {
		parts := strings.Split(table, ".")
		var dbName, tableName string

		if len(parts) == 2 {
			dbName = parts[0]
			tableName = parts[1]
		} else {
			dbName = c.config.Database
			tableName = table
		}

		tableSchema, err := c.canal.GetTable(dbName, tableName)
		if err != nil {
			c.logger.Warn("failed to load table schema",
				zap.String("database", dbName),
				zap.String("table", tableName),
				zap.Error(err))
			continue
		}

		fullTableName := stringpool.Sprintf("%s.%s", dbName, tableName)
		c.schemaCache[fullTableName] = tableSchema

		c.logger.Debug("loaded table schema",
			zap.String("table", fullTableName),
			zap.Int("columns", len(tableSchema.Columns)))
	}

	c.logger.Info("loaded table schemas", zap.Int("table_count", len(c.schemaCache)))

	return nil
}

// startBinlogStreaming starts the binary log streaming process
func (c *MySQLConnector) startBinlogStreaming() {
	defer c.updateHealth("stopped", "Binlog streaming stopped", nil)

	// Start syncing from the current position
	streamer, err := c.syncer.StartSync(c.position)
	if err != nil {
		c.logger.Error("failed to start sync", zap.Error(err))
		return
	}

	c.logger.Info("started binary log streaming",
		zap.String("position", c.position.String()))

	c.updateHealth("running", "Binlog streaming active", nil)

	for {
		select {
		case <-c.stopCh:
			return

		default:
			// Read events with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			ev, err := streamer.GetEvent(ctx)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					continue // Timeout is expected
				}
				c.logger.Error("failed to get binlog event", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			if err := c.processBinlogEvent(ev); err != nil {
				c.logger.Error("failed to process binlog event", zap.Error(err))
				c.updateMetrics(func(m *EventMetrics) {
					m.EventsErrored++
				})
			}
		}
	}
}

// processBinlogEvent processes a single binlog event
func (c *MySQLConnector) processBinlogEvent(ev *replication.BinlogEvent) error {
	// Update position
	c.mutex.Lock()
	c.position.Pos = ev.Header.LogPos
	c.mutex.Unlock()

	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		return c.processRotateEvent(e)
	case *replication.RowsEvent:
		return c.processRowsEvent(ev.Header, e)
	case *replication.TableMapEvent:
		return c.processTableMapEvent(e)
	case *replication.QueryEvent:
		return c.processQueryEvent(e)
	}

	return nil
}

// processRotateEvent processes binlog rotation events
func (c *MySQLConnector) processRotateEvent(e *replication.RotateEvent) error {
	c.mutex.Lock()
	c.position.Name = string(e.NextLogName)
	c.position.Pos = uint32(e.Position)
	c.mutex.Unlock()

	c.logger.Debug("binlog rotated",
		zap.String("new_file", string(e.NextLogName)),
		zap.Uint64("position", e.Position))

	return nil
}

// processRowsEvent processes row change events (INSERT, UPDATE, DELETE)
func (c *MySQLConnector) processRowsEvent(header *replication.EventHeader, e *replication.RowsEvent) error {
	// Get table schema
	tableName := stringpool.Sprintf("%s.%s", string(e.Table.Schema), string(e.Table.Table))

	// Check if we're monitoring this table
	if !c.isTableMonitored(tableName) {
		return nil
	}

	c.schemaMutex.RLock()
	tableSchema, exists := c.schemaCache[tableName]
	c.schemaMutex.RUnlock()

	if !exists {
		c.logger.Warn("table schema not found", zap.String("table", tableName))
		return nil
	}

	// Determine operation type
	var operation OperationType
	switch header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		operation = OperationInsert
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		operation = OperationUpdate
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		operation = OperationDelete
	default:
		return nil // Unsupported event type
	}

	// Process each row
	for i := 0; i < len(e.Rows); i++ {
		var before, after map[string]interface{}
		var err error

		switch operation {
		case OperationInsert:
			after, err = c.rowToMap(e.Rows[i], tableSchema)
			if err != nil {
				return errors.Wrap(err, errors.ErrorTypeData, "failed to convert insert row")
			}

		case OperationUpdate:
			if i+1 >= len(e.Rows) {
				return errors.New(errors.ErrorTypeData, "incomplete update row data")
			}

			before, err = c.rowToMap(e.Rows[i], tableSchema)
			if err != nil {
				return errors.Wrap(err, errors.ErrorTypeData, "failed to convert update before row")
			}

			i++ // Move to next row (after image)
			after, err = c.rowToMap(e.Rows[i], tableSchema)
			if err != nil {
				return errors.Wrap(err, errors.ErrorTypeData, "failed to convert update after row")
			}

		case OperationDelete:
			before, err = c.rowToMap(e.Rows[i], tableSchema)
			if err != nil {
				return errors.Wrap(err, errors.ErrorTypeData, "failed to convert delete row")
			}
		}

		event := ChangeEvent{
			ID:        c.generateEventID(),
			Operation: operation,
			Database:  string(e.Table.Schema),
			Table:     tableName,
			Before:    before,
			After:     after,
			Timestamp: time.Unix(int64(header.Timestamp), 0),
			Position:  c.GetPosition(),
			Source: SourceInfo{
				Name:          stringpool.Sprintf("mysql_%s", string(e.Table.Schema)),
				Database:      string(e.Table.Schema),
				Table:         tableName,
				ConnectorType: ConnectorMySQL,
				Timestamp:     time.Now(),
			},
		}

		if err := c.sendEvent(event); err != nil {
			return errors.Wrap(err, errors.ErrorTypeConnection, "failed to send event")
		}
	}

	return nil
}

// processTableMapEvent processes table map events
func (c *MySQLConnector) processTableMapEvent(e *replication.TableMapEvent) error {
	tableName := stringpool.Sprintf("%s.%s", string(e.Schema), string(e.Table))

	c.logger.Debug("table map event",
		zap.String("table", tableName),
		zap.Uint64("table_id", e.TableID))

	return nil
}

// processQueryEvent processes query events (DDL statements)
func (c *MySQLConnector) processQueryEvent(e *replication.QueryEvent) error {
	query := string(e.Query)
	schema := string(e.Schema)

	c.logger.Debug("query event",
		zap.String("schema", schema),
		zap.String("query", query))

	// Handle DDL statements that might affect monitored tables
	if c.isDDLStatement(query) {
		return c.handleDDLStatement(schema, query)
	}

	return nil
}

// rowToMap converts a MySQL row to a map
func (c *MySQLConnector) rowToMap(row []interface{}, table *schema.Table) (map[string]interface{}, error) {
	if len(row) != len(table.Columns) {
		return nil, errors.New(errors.ErrorTypeData, stringpool.Sprintf("row column count mismatch: got %d, expected %d",
			len(row), len(table.Columns)))
	}

	result := pool.GetMap()

	defer pool.PutMap(result)

	for i, col := range table.Columns {
		value := row[i]

		// Handle different MySQL data types
		switch col.Type {
		case schema.TYPE_JSON:
			if value != nil {
				if jsonStr, ok := value.(string); ok {
					var jsonValue interface{}
					if err := jsonpool.Unmarshal([]byte(jsonStr), &jsonValue); err == nil {
						value = jsonValue
					}
				}
			}
		case schema.TYPE_TIMESTAMP, schema.TYPE_DATETIME:
			if value != nil {
				if timeStr, ok := value.(string); ok {
					if t, err := time.Parse("2006-01-02 15:04:05", timeStr); err == nil {
						value = t
					}
				}
			}
		case schema.TYPE_DATE:
			if value != nil {
				if dateStr, ok := value.(string); ok {
					if t, err := time.Parse("2006-01-02", dateStr); err == nil {
						value = t
					}
				}
			}
		}

		result[col.Name] = value
	}

	return result, nil
}

// isTableMonitored checks if a table is being monitored
func (c *MySQLConnector) isTableMonitored(tableName string) bool {
	for _, table := range c.config.Tables {
		if strings.Contains(table, ".") {
			if table == tableName {
				return true
			}
		} else {
			// Check if table name matches (ignoring schema)
			parts := strings.Split(tableName, ".")
			if len(parts) == 2 && parts[1] == table {
				return true
			}
		}
	}
	return false
}

// isDDLStatement checks if a query is a DDL statement
func (c *MySQLConnector) isDDLStatement(query string) bool {
	upperQuery := strings.ToUpper(strings.TrimSpace(query))
	ddlKeywords := []string{"CREATE", "ALTER", "DROP", "RENAME", "TRUNCATE"}

	for _, keyword := range ddlKeywords {
		if strings.HasPrefix(upperQuery, keyword) {
			return true
		}
	}

	return false
}

// handleDDLStatement handles DDL statements that might affect schema
func (c *MySQLConnector) handleDDLStatement(schema, query string) error {
	c.logger.Info("DDL statement detected",
		zap.String("schema", schema),
		zap.String("query", query))

	// Reload schemas for affected tables
	return c.loadTableSchemas()
}

// sendEvent sends a change event to the event channel
func (c *MySQLConnector) sendEvent(event ChangeEvent) error {
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
		return errors.New(errors.ErrorTypeCapability, "event channel is full")
	}
}

// generateEventID generates a unique event ID
func (c *MySQLConnector) generateEventID() string {
	return stringpool.Sprintf("mysql_%d_%s", time.Now().UnixNano(), c.position.String())
}

// updateHealth updates the health status
func (c *MySQLConnector) updateHealth(status, message string, details map[string]interface{}) {
	c.healthMutex.Lock()
	defer c.healthMutex.Unlock()

	c.health.Status = status
	c.health.Message = message
	if details != nil {
		c.health.Details = details
	}
}

// updateMetrics updates the metrics
func (c *MySQLConnector) updateMetrics(updateFn func(*EventMetrics)) {
	c.metricsMutex.Lock()
	defer c.metricsMutex.Unlock()

	updateFn(&c.metrics)
}

// Helper functions to extract connection parameters

func (c *MySQLConnector) extractHostPort(connStr string) string {
	// Parse connection string to extract host:port
	// Example: user:pass@tcp(host:port)/db
	start := strings.Index(connStr, "@tcp(")
	if start == -1 {
		return "localhost:3306"
	}
	start += 5

	end := strings.Index(connStr[start:], ")")
	if end == -1 {
		return "localhost:3306"
	}

	return connStr[start : start+end]
}

func (c *MySQLConnector) extractHost(connStr string) string {
	hostPort := c.extractHostPort(connStr)
	parts := strings.Split(hostPort, ":")
	if len(parts) > 0 {
		return parts[0]
	}
	return "localhost"
}

func (c *MySQLConnector) extractPort(connStr string) uint16 {
	hostPort := c.extractHostPort(connStr)
	parts := strings.Split(hostPort, ":")
	if len(parts) > 1 {
		if port, err := strconv.ParseUint(parts[1], 10, 16); err == nil {
			return uint16(port)
		}
	}
	return 3306
}

func (c *MySQLConnector) extractCredentials(connStr string) (string, string) {
	// Extract user:pass from connection string
	at := strings.Index(connStr, "@")
	if at == -1 {
		return "", ""
	}

	userPass := connStr[:at]
	colon := strings.Index(userPass, ":")
	if colon == -1 {
		return userPass, ""
	}

	return userPass[:colon], userPass[colon+1:]
}

func (c *MySQLConnector) extractUser(connStr string) string {
	user, _ := c.extractCredentials(connStr)
	return user
}

func (c *MySQLConnector) extractPassword(connStr string) string {
	_, password := c.extractCredentials(connStr)
	return password
}
