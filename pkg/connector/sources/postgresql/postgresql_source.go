package postgresql

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/baseconnector"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
)

// PostgreSQLSource implements the core.Source interface for PostgreSQL
type PostgreSQLSource struct {
	*baseconnector.BaseConnector

	// Configuration
	connectionStr string
	tableName     string
	query         string
	batchSize     int

	// Connection pooling
	pool       *pgxpool.Pool
	poolConfig *pgxpool.Config

	// Reading state
	currentBatch  []*models.Record //nolint:unused // Reserved for batch processing
	recordsRead   int64
	bytesRead     int64
	isInitialized bool
	position      int64

	// Schema and metadata
	schema      *core.Schema
	columns     []string
	columnTypes []string

	// Synchronization
	mu sync.RWMutex

	// State management
	state        core.State
	lastPosition core.Position //nolint:unused // Reserved for position tracking
}

// PostgreSQLPosition implements core.Position for PostgreSQL
type PostgreSQLPosition struct {
	Offset int64 `json:"offset"`
}

func (p *PostgreSQLPosition) String() string {
	return stringpool.Sprintf("postgresql_offset_%d", p.Offset)
}

func (p *PostgreSQLPosition) Compare(other core.Position) int {
	if otherPG, ok := other.(*PostgreSQLPosition); ok {
		if p.Offset < otherPG.Offset {
			return -1
		} else if p.Offset > otherPG.Offset {
			return 1
		}
		return 0
	}
	return 0
}

// NewPostgreSQLSource creates a new PostgreSQL source connector
func NewPostgreSQLSource(config *config.BaseConfig) (core.Source, error) {
	base := baseconnector.NewBaseConnector("postgresql", core.ConnectorTypeSource, "1.0.0")

	source := &PostgreSQLSource{
		BaseConnector: base,
		batchSize:     1000,
		state:         make(core.State),
	}

	return source, nil
}

// Initialize sets up the PostgreSQL connection and prepares for reading
func (s *PostgreSQLSource) Initialize(ctx context.Context, config *config.BaseConfig) error {
	// Initialize base connector first
	if err := s.BaseConnector.Initialize(ctx, config); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig, "failed to initialize base connector")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isInitialized {
		return nebulaerrors.New(nebulaerrors.ErrorTypeValidation, "source already initialized")
	}

	// Parse configuration
	if err := s.parseConfig(config); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig, "failed to parse config")
	}

	// Setup connection pool with circuit breaker protection
	if err := s.ExecuteWithCircuitBreaker(func() error {
		return s.setupConnectionPool(ctx)
	}); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to setup connection pool")
	}

	// Discover schema
	if err := s.discoverSchema(ctx); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to discover schema")
	}

	s.isInitialized = true

	s.UpdateHealth(true, map[string]interface{}{
		"table":           s.tableName,
		"batch_size":      s.batchSize,
		"max_connections": s.poolConfig.MaxConns,
		"connected":       true,
	})

	s.GetLogger().Info("PostgreSQL source initialized",
		zap.String("table", s.tableName),
		zap.Int("batch_size", s.batchSize),
		zap.Int32("max_connections", s.poolConfig.MaxConns))

	return nil
}

// parseConfig extracts configuration from the config object
func (s *PostgreSQLSource) parseConfig(config *config.BaseConfig) error {
	// Connection string (required)
	if config.Security.Credentials == nil || config.Security.Credentials["connection_string"] == "" {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "connection_string is required in security.credentials")
	}
	s.connectionStr = config.Security.Credentials["connection_string"]

	// Table name or query (one is required)
	if table, ok := config.Security.Credentials["table"]; ok && table != "" {
		s.tableName = table
	}

	if query, ok := config.Security.Credentials["query"]; ok && query != "" {
		s.query = query
	}

	if s.tableName == "" && s.query == "" {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "either table name or query is required")
	}

	// Batch size (optional)
	if config.Performance.BatchSize > 0 {
		s.batchSize = config.Performance.BatchSize
	}

	return nil
}

// setupConnectionPool configures and creates the PostgreSQL connection pool
func (s *PostgreSQLSource) setupConnectionPool(ctx context.Context) error {
	var err error

	// Parse connection config
	s.poolConfig, err = pgxpool.ParseConfig(s.connectionStr)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig, "failed to parse connection string")
	}

	// Configure connection pool settings using BaseConfig
	// Use MaxConcurrency for max connections (database connections are a form of concurrency)
	s.poolConfig.MaxConns = int32(s.GetConfig().Performance.MaxConcurrency)
	if s.poolConfig.MaxConns <= 0 {
		s.poolConfig.MaxConns = 10 // Default
	}

	// Use Workers/4 for min connections (typically want fewer idle connections than workers)
	s.poolConfig.MinConns = int32(s.GetConfig().Performance.Workers / 4)
	if s.poolConfig.MinConns <= 0 {
		s.poolConfig.MinConns = 2 // Default minimum
	}
	if s.poolConfig.MinConns > s.poolConfig.MaxConns {
		s.poolConfig.MinConns = s.poolConfig.MaxConns / 2
	}

	// Use timeout configurations from BaseConfig
	s.poolConfig.MaxConnLifetime = s.GetConfig().Timeouts.Connection
	if s.poolConfig.MaxConnLifetime <= 0 {
		s.poolConfig.MaxConnLifetime = time.Hour // Default
	}

	s.poolConfig.MaxConnIdleTime = s.GetConfig().Timeouts.Idle
	if s.poolConfig.MaxConnIdleTime <= 0 {
		s.poolConfig.MaxConnIdleTime = 30 * time.Minute // Default
	}

	// Set health check interval based on keep alive timeout
	s.poolConfig.HealthCheckPeriod = s.GetConfig().Timeouts.KeepAlive
	if s.poolConfig.HealthCheckPeriod <= 0 {
		s.poolConfig.HealthCheckPeriod = 30 * time.Second // Default
	}

	// Create connection pool
	s.pool, err = pgxpool.NewWithConfig(ctx, s.poolConfig)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to create connection pool")
	}

	// Warm up the connection pool by establishing minimum connections
	if err := s.warmUpConnectionPool(ctx); err != nil {
		s.GetLogger().Warn("Failed to warm up connection pool", zap.Error(err))
		// Don't fail initialization, pool will create connections on demand
	}

	// Test connection and get version
	var version string
	if err := s.validateConnection(ctx, &version); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to validate connection")
	}

	s.GetLogger().Info("Connected to PostgreSQL",
		zap.String("version", version),
		zap.Int32("max_connections", s.poolConfig.MaxConns),
		zap.Int32("min_connections", s.poolConfig.MinConns),
		zap.Duration("idle_timeout", s.poolConfig.MaxConnIdleTime),
		zap.Duration("health_check_period", s.poolConfig.HealthCheckPeriod))

	return nil
}

// discoverSchema discovers the table schema and column information
func (s *PostgreSQLSource) discoverSchema(ctx context.Context) error {
	if s.query != "" {
		// For custom queries, we'll discover schema by executing the query with LIMIT 0
		return s.discoverSchemaFromQuery(ctx)
	}

	// For table queries, use information_schema
	return s.discoverSchemaFromTable(ctx)
}

// discoverSchemaFromTable discovers schema using information_schema
func (s *PostgreSQLSource) discoverSchemaFromTable(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to acquire connection")
	}
	defer conn.Release()

	// Parse schema and table name
	var schemaName, tableName string
	if parts := strings.Split(s.tableName, "."); len(parts) == 2 {
		schemaName = parts[0]
		tableName = parts[1]
	} else {
		schemaName = "public"
		tableName = s.tableName
	}

	query := `
		SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns 
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := conn.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeQuery, "failed to query table schema")
	}
	defer rows.Close() // Ignore close error

	var columns []string
	var columnTypes []string
	fields := make([]core.Field, 0)

	for rows.Next() {
		var columnName, dataType, isNullable string
		var columnDefault *string

		if err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault); err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to scan schema row")
		}

		columns = append(columns, columnName)
		columnTypes = append(columnTypes, dataType)

		field := core.Field{
			Name:     columnName,
			Type:     mapPostgreSQLType(dataType),
			Nullable: isNullable == "YES",
		}

		if columnDefault != nil {
			field.Default = *columnDefault
		}

		fields = append(fields, field)
	}

	if err := rows.Err(); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "error iterating schema rows")
	}

	if len(columns) == 0 {
		return nebulaerrors.New(nebulaerrors.ErrorTypeData, stringpool.Sprintf("table %s not found or has no columns", s.tableName))
	}

	s.columns = columns
	s.columnTypes = columnTypes
	s.schema = &core.Schema{
		Name:      s.tableName,
		Fields:    fields,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	s.GetLogger().Info("Discovered table schema",
		zap.String("table", s.tableName),
		zap.Int("columns", len(columns)))

	return nil
}

// discoverSchemaFromQuery discovers schema by executing query with LIMIT 0
func (s *PostgreSQLSource) discoverSchemaFromQuery(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to acquire connection")
	}
	defer conn.Release()

	// Execute query with LIMIT 0 to get column info without data
	sqlBuilder := stringpool.NewSQLBuilder(256)
	defer sqlBuilder.Close() // Ignore close error
	limitQuery := sqlBuilder.WriteQuery("SELECT * FROM (").
		WriteQuery(s.query).
		WriteQuery(") AS q LIMIT 0").
		String()

	rows, err := conn.Query(ctx, limitQuery)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeQuery, "failed to execute schema discovery query")
	}
	defer rows.Close() // Ignore close error

	fieldDescriptions := rows.FieldDescriptions()

	columns := make([]string, 0, len(fieldDescriptions))
	columnTypes := make([]string, 0, len(fieldDescriptions))
	fields := make([]core.Field, len(fieldDescriptions))

	for i, fd := range fieldDescriptions {
		columnName := fd.Name

		columns = append(columns, columnName)
		columnTypes = append(columnTypes, stringpool.Sprintf("oid_%d", fd.DataTypeOID))

		fields[i] = core.Field{
			Name:     columnName,
			Type:     mapPostgreSQLOIDType(fd.DataTypeOID),
			Nullable: true, // We can't determine nullability from query result
		}
	}

	s.columns = columns
	s.columnTypes = columnTypes
	s.schema = &core.Schema{
		Name:      "custom_query",
		Fields:    fields,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	s.GetLogger().Info("Discovered query schema", zap.Int("columns", len(columns)))

	return nil
}

// Discover returns the schema of the PostgreSQL table/query
func (s *PostgreSQLSource) Discover(ctx context.Context) (*core.Schema, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.schema == nil {
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeData, "schema not discovered yet")
	}

	return s.schema, nil
}

// Read returns a stream of records from PostgreSQL
func (s *PostgreSQLSource) Read(ctx context.Context) (*core.RecordStream, error) {
	s.mu.RLock()
	if !s.isInitialized {
		s.mu.RUnlock()
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeValidation, "source not initialized")
	}
	s.mu.RUnlock()

	recordChan := make(chan *models.Record, 100)
	errorChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errorChan)

		// Build query
		query := s.query
		if query == "" {
			sqlBuilder := stringpool.NewSQLBuilder(128)
			defer sqlBuilder.Close() // Ignore close error
			query = sqlBuilder.WriteQuery("SELECT * FROM ").
				WriteIdentifier(s.tableName).
				String()
		}

		// Apply rate limiting
		if err := s.RateLimit(ctx); err != nil {
			errorChan <- err
			return
		}

		// Execute with circuit breaker
		if err := s.ExecuteWithCircuitBreaker(func() error {
			return s.streamRecords(ctx, query, recordChan, errorChan)
		}); err != nil {
			errorChan <- err
		}
	}()

	return &core.RecordStream{
		Records: recordChan,
		Errors:  errorChan,
	}, nil
}

// ReadBatch returns a stream of record batches from PostgreSQL
func (s *PostgreSQLSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	s.mu.RLock()
	if !s.isInitialized {
		s.mu.RUnlock()
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeValidation, "source not initialized")
	}
	s.mu.RUnlock()

	batchChan := make(chan []*models.Record, 10)
	errorChan := make(chan error, 1)

	go func() {
		defer close(batchChan)
		defer close(errorChan)

		// Build query
		query := s.query
		if query == "" {
			sqlBuilder := stringpool.NewSQLBuilder(128)
			defer sqlBuilder.Close() // Ignore close error
			query = sqlBuilder.WriteQuery("SELECT * FROM ").
				WriteIdentifier(s.tableName).
				String()
		}

		// Apply rate limiting
		if err := s.RateLimit(ctx); err != nil {
			errorChan <- err
			return
		}

		// Execute with circuit breaker
		if err := s.ExecuteWithCircuitBreaker(func() error {
			return s.streamBatches(ctx, query, batchSize, batchChan, errorChan)
		}); err != nil {
			errorChan <- err
		}
	}()

	return &core.BatchStream{
		Batches: batchChan,
		Errors:  errorChan,
	}, nil
}

// streamRecords streams individual records
func (s *PostgreSQLSource) streamRecords(ctx context.Context, query string, recordChan chan<- *models.Record, errorChan chan<- error) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to acquire connection")
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeQuery, "failed to execute query")
	}
	defer rows.Close() // Ignore close error

	for rows.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			record, err := s.scanRowToRecord(rows)
			if err != nil {
				return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to scan row")
			}

			s.mu.Lock()
			s.recordsRead++
			s.position++
			s.mu.Unlock()

			select {
			case recordChan <- record:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return rows.Err()
}

// streamBatches streams record batches
func (s *PostgreSQLSource) streamBatches(ctx context.Context, query string, batchSize int, batchChan chan<- []*models.Record, errorChan chan<- error) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to acquire connection")
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeQuery, "failed to execute query")
	}
	defer rows.Close() // Ignore close error

	batch := pool.GetBatchSlice(batchSize)

	defer pool.PutBatchSlice(batch)

	for rows.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			record, err := s.scanRowToRecord(rows)
			if err != nil {
				return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to scan row")
			}

			batch = append(batch, record)

			s.mu.Lock()
			s.recordsRead++
			s.position++
			s.mu.Unlock()

			if len(batch) >= batchSize {
				select {
				case batchChan <- batch:
					batch = pool.GetBatchSlice(batchSize)
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}

	// Send final partial batch
	if len(batch) > 0 {
		select {
		case batchChan <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return rows.Err()
}

// scanRowToRecord converts a PostgreSQL row to a Record
func (s *PostgreSQLSource) scanRowToRecord(rows pgx.Rows) (*models.Record, error) {
	values, err := rows.Values()
	if err != nil {
		return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to get row values")
	}

	record := models.NewRecordFromPool("postgresql")

	for i, value := range values {
		if i < len(s.columns) {
			record.SetData(s.columns[i], convertPostgreSQLValue(value))
		}
	}
	record.SetTimestamp(time.Now())

	// Estimate record size (rough calculation)
	s.mu.Lock()
	s.bytesRead += int64(len(stringpool.Sprintf("%v", record.Data)))
	s.mu.Unlock()

	return record, nil
}

// Close closes the PostgreSQL connection and cleans up resources
func (s *PostgreSQLSource) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pool != nil {
		s.pool.Close()
		s.pool = nil
	}

	s.isInitialized = false

	s.GetLogger().Info("PostgreSQL source closed",
		zap.Int64("records_read", s.recordsRead),
		zap.Int64("bytes_read", s.bytesRead))

	return nil
}

// State management methods
func (s *PostgreSQLSource) GetPosition() core.Position {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &PostgreSQLPosition{Offset: s.position}
}

func (s *PostgreSQLSource) SetPosition(position core.Position) error {
	if pgPos, ok := position.(*PostgreSQLPosition); ok {
		s.mu.Lock()
		s.position = pgPos.Offset
		s.mu.Unlock()
		return nil
	}
	return nebulaerrors.New(nebulaerrors.ErrorTypeValidation, "invalid position type")
}

func (s *PostgreSQLSource) GetState() core.State {
	s.mu.RLock()
	defer s.mu.RUnlock()

	state := make(core.State)
	for k, v := range s.state {
		state[k] = v
	}
	state["position"] = s.position
	state["records_read"] = s.recordsRead

	return state
}

func (s *PostgreSQLSource) SetState(state core.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = state
	if pos, ok := state["position"].(int64); ok {
		s.position = pos
	}
	if records, ok := state["records_read"].(int64); ok {
		s.recordsRead = records
	}

	return nil
}

// Capability methods
func (s *PostgreSQLSource) SupportsIncremental() bool {
	return false // For now, full table reads only
}

func (s *PostgreSQLSource) SupportsRealtime() bool {
	return false // Use PostgreSQL CDC connector for real-time
}

func (s *PostgreSQLSource) SupportsBatch() bool {
	return true
}

// Real-time methods (not supported)
func (s *PostgreSQLSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	return nil, nebulaerrors.New(nebulaerrors.ErrorTypeValidation, "real-time subscription not supported - use postgresql_cdc connector")
}

// Health returns the health status
func (s *PostgreSQLSource) Health(ctx context.Context) error {
	if s.pool == nil {
		return nebulaerrors.New(nebulaerrors.ErrorTypeConnection, "connection pool not initialized")
	}

	// Test connection
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to acquire connection")
	}
	defer conn.Release()

	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeQuery, "health check query failed")
	}

	return nil
}

// Metrics returns performance metrics
func (s *PostgreSQLSource) Metrics() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	metrics := map[string]interface{}{
		"records_read": s.recordsRead,
		"bytes_read":   s.bytesRead,
		"table":        s.tableName,
		"batch_size":   s.batchSize,
	}

	if s.pool != nil {
		stat := s.pool.Stat()
		metrics["pool_stats"] = map[string]interface{}{
			"total_conns":         stat.TotalConns(),
			"acquired_conns":      stat.AcquiredConns(),
			"idle_conns":          stat.IdleConns(),
			"constructing_conns":  stat.ConstructingConns(),
			"empty_acquire":       stat.EmptyAcquireCount(),
			"acquire_count":       stat.AcquireCount(),
			"acquired_duration":   stat.AcquireDuration().Milliseconds(),
			"canceled_acquire":    stat.CanceledAcquireCount(),
			"max_conns":           s.poolConfig.MaxConns,
			"min_conns":           s.poolConfig.MinConns,
			"conn_utilization":    float64(stat.AcquiredConns()) / float64(s.poolConfig.MaxConns) * 100,
			"health_check_period": s.poolConfig.HealthCheckPeriod.Seconds(),
		}
	}

	return metrics
}

// Helper functions

// mapPostgreSQLType maps PostgreSQL data types to core types
func mapPostgreSQLType(pgType string) core.FieldType {
	switch pgType {
	case "integer", "bigint", "smallint", "serial", "bigserial":
		return core.FieldTypeInt
	case "numeric", "decimal", "real", "double precision":
		return core.FieldTypeFloat
	case "boolean":
		return core.FieldTypeBool
	case "timestamp", "timestamptz", "date", "time", "timetz":
		return core.FieldTypeTimestamp
	case "json", "jsonb":
		return core.FieldTypeJSON
	case "bytea":
		return core.FieldTypeBinary
	default:
		return core.FieldTypeString
	}
}

// mapPostgreSQLOIDType maps PostgreSQL OID types to core types
func mapPostgreSQLOIDType(oid uint32) core.FieldType {
	// Common PostgreSQL OIDs
	switch oid {
	case 20, 21, 23: // bigint, smallint, integer
		return core.FieldTypeInt
	case 700, 701, 1700: // real, double precision, numeric
		return core.FieldTypeFloat
	case 16: // boolean
		return core.FieldTypeBool
	case 1082, 1083, 1114, 1184: // date, time, timestamp, timestamptz
		return core.FieldTypeTimestamp
	case 114, 3802: // json, jsonb
		return core.FieldTypeJSON
	case 17: // bytea
		return core.FieldTypeBinary
	default:
		return core.FieldTypeString
	}
}

// convertPostgreSQLValue converts PostgreSQL values to Go types
func convertPostgreSQLValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case []byte:
		return string(v)
	case time.Time:
		return v.Unix()
	default:
		return v
	}
}

// warmUpConnectionPool establishes minimum connections to warm up the pool
func (s *PostgreSQLSource) warmUpConnectionPool(ctx context.Context) error {
	// Acquire minimum number of connections to warm up the pool
	conns := make([]pgxpool.Conn, 0, s.poolConfig.MinConns)

	for i := int32(0); i < s.poolConfig.MinConns; i++ {
		conn, err := s.pool.Acquire(ctx)
		if err != nil {
			// Release any connections we've acquired so far
			for _, c := range conns {
				c.Release()
			}
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection,
				stringpool.Sprintf("failed to warm up connection %d/%d", i+1, s.poolConfig.MinConns))
		}
		conns = append(conns, *conn)
	}

	// Release all connections back to the pool
	for _, conn := range conns {
		conn.Release()
	}

	s.GetLogger().Debug("Connection pool warmed up",
		zap.Int32("connections", s.poolConfig.MinConns))

	return nil
}

// validateConnection validates a connection and optionally retrieves server version
func (s *PostgreSQLSource) validateConnection(ctx context.Context, version *string) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to acquire connection for validation")
	}
	defer conn.Release()

	// Simple validation query
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeQuery, "validation query failed")
	}

	// Get version if requested
	if version != nil {
		err = conn.QueryRow(ctx, "SELECT version()").Scan(version)
		if err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeQuery, "failed to get server version")
		}
	}

	return nil
}
