package sdk

import (
	"context"
	"database/sql"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
)

// ConnectionBuilder provides utilities for building database connections
type ConnectionBuilder struct {
	logger *zap.Logger
}

// NewConnectionBuilder creates a new connection builder
func NewConnectionBuilder() *ConnectionBuilder {
	return &ConnectionBuilder{
		logger: GetGlobalSDK().logger.With(zap.String("component", "connection_builder")),
	}
}

// DatabaseConnectionBuilder builds database connections with common patterns
type DatabaseConnectionBuilder struct {
	connectionString   string
	maxConnections     int
	maxIdleConnections int
	maxLifetime        time.Duration
	healthQuery        string
	connectTimeout     time.Duration
	queryTimeout       time.Duration
	logger             *zap.Logger
}

// NewDatabaseConnectionBuilder creates a new database connection builder
func NewDatabaseConnectionBuilder() *DatabaseConnectionBuilder {
	return &DatabaseConnectionBuilder{
		maxConnections:     10,
		maxIdleConnections: 5,
		maxLifetime:        time.Hour,
		healthQuery:        "SELECT 1",
		connectTimeout:     30 * time.Second,
		queryTimeout:       10 * time.Second,
		logger:             GetGlobalSDK().logger.With(zap.String("component", "db_connection_builder")),
	}
}

// WithConnectionString sets the database connection string
func (dcb *DatabaseConnectionBuilder) WithConnectionString(connStr string) *DatabaseConnectionBuilder {
	dcb.connectionString = connStr
	return dcb
}

// WithMaxConnections sets the maximum number of connections
func (dcb *DatabaseConnectionBuilder) WithMaxConnections(max int) *DatabaseConnectionBuilder {
	dcb.maxConnections = max
	return dcb
}

// WithMaxIdleConnections sets the maximum number of idle connections
func (dcb *DatabaseConnectionBuilder) WithMaxIdleConnections(maxIdle int) *DatabaseConnectionBuilder {
	dcb.maxIdleConnections = maxIdle
	return dcb
}

// WithMaxLifetime sets the maximum lifetime for connections
func (dcb *DatabaseConnectionBuilder) WithMaxLifetime(lifetime time.Duration) *DatabaseConnectionBuilder {
	dcb.maxLifetime = lifetime
	return dcb
}

// WithHealthQuery sets the health check query
func (dcb *DatabaseConnectionBuilder) WithHealthQuery(query string) *DatabaseConnectionBuilder {
	dcb.healthQuery = query
	return dcb
}

// WithConnectTimeout sets the connection timeout
func (dcb *DatabaseConnectionBuilder) WithConnectTimeout(timeout time.Duration) *DatabaseConnectionBuilder {
	dcb.connectTimeout = timeout
	return dcb
}

// WithQueryTimeout sets the query timeout
func (dcb *DatabaseConnectionBuilder) WithQueryTimeout(timeout time.Duration) *DatabaseConnectionBuilder {
	dcb.queryTimeout = timeout
	return dcb
}

// PostgreSQLConnectionBuilder specialized builder for PostgreSQL connections
type PostgreSQLConnectionBuilder struct {
	*DatabaseConnectionBuilder
	poolConfig *pgxpool.Config
}

// NewPostgreSQLConnectionBuilder creates a PostgreSQL-specific connection builder
func NewPostgreSQLConnectionBuilder() *PostgreSQLConnectionBuilder {
	return &PostgreSQLConnectionBuilder{
		DatabaseConnectionBuilder: NewDatabaseConnectionBuilder(),
	}
}

// WithMaxConnections sets the maximum number of connections
func (pcb *PostgreSQLConnectionBuilder) WithMaxConnections(max int) *PostgreSQLConnectionBuilder {
	pcb.DatabaseConnectionBuilder.WithMaxConnections(max)
	return pcb
}

// WithMaxIdleConnections sets the maximum number of idle connections
func (pcb *PostgreSQLConnectionBuilder) WithMaxIdleConnections(maxIdle int) *PostgreSQLConnectionBuilder {
	pcb.DatabaseConnectionBuilder.WithMaxIdleConnections(maxIdle)
	return pcb
}

// WithMaxLifetime sets the maximum lifetime for connections
func (pcb *PostgreSQLConnectionBuilder) WithMaxLifetime(lifetime time.Duration) *PostgreSQLConnectionBuilder {
	pcb.DatabaseConnectionBuilder.WithMaxLifetime(lifetime)
	return pcb
}

// WithConnectTimeout sets the connection timeout
func (pcb *PostgreSQLConnectionBuilder) WithConnectTimeout(timeout time.Duration) *PostgreSQLConnectionBuilder {
	pcb.DatabaseConnectionBuilder.WithConnectTimeout(timeout)
	return pcb
}

// WithQueryTimeout sets the query timeout
func (pcb *PostgreSQLConnectionBuilder) WithQueryTimeout(timeout time.Duration) *PostgreSQLConnectionBuilder {
	pcb.DatabaseConnectionBuilder.WithQueryTimeout(timeout)
	return pcb
}

// WithHealthQuery sets the health check query
func (pcb *PostgreSQLConnectionBuilder) WithHealthQuery(query string) *PostgreSQLConnectionBuilder {
	pcb.DatabaseConnectionBuilder.WithHealthQuery(query)
	return pcb
}

// WithSSLMode sets the SSL mode for PostgreSQL
func (pcb *PostgreSQLConnectionBuilder) WithSSLMode(sslMode string) *PostgreSQLConnectionBuilder {
	// This will be applied when building the connection string
	return pcb
}

// WithApplicationName sets the application name for PostgreSQL
func (pcb *PostgreSQLConnectionBuilder) WithApplicationName(appName string) *PostgreSQLConnectionBuilder {
	// This will be applied when building the connection string
	return pcb
}

// BuildPostgreSQLPool builds a PostgreSQL connection pool
func (pcb *PostgreSQLConnectionBuilder) BuildPostgreSQLPool(ctx context.Context) (*pgxpool.Pool, error) {
	if pcb.connectionString == "" {
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "connection string is required")
	}

	// Parse connection string
	config, err := pgxpool.ParseConfig(pcb.connectionString)
	if err != nil {
		return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig, "failed to parse PostgreSQL connection string")
	}

	// Apply connection pool settings
	config.MaxConns = int32(pcb.maxConnections)
	config.MinConns = int32(pcb.maxIdleConnections)
	config.MaxConnLifetime = pcb.maxLifetime
	config.ConnConfig.ConnectTimeout = pcb.connectTimeout

	// Store config for later use
	pcb.poolConfig = config

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to create PostgreSQL connection pool")
	}

	// Test connection
	if err := pcb.testConnection(ctx, pool); err != nil {
		pool.Close() // Close the pool (no return value)
		return nil, err
	}

	pcb.logger.Info("PostgreSQL connection pool created successfully",
		zap.String("connection_string", obfuscateConnectionString(pcb.connectionString)),
		zap.Int("max_connections", pcb.maxConnections),
		zap.Int("min_connections", pcb.maxIdleConnections),
		zap.Duration("max_lifetime", pcb.maxLifetime))

	return pool, nil
}

// testConnection tests the PostgreSQL connection
func (pcb *PostgreSQLConnectionBuilder) testConnection(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to acquire connection for testing")
	}
	defer conn.Release()

	var result int
	err = conn.QueryRow(ctx, pcb.healthQuery).Scan(&result)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "health check query failed")
	}

	pcb.logger.Info("PostgreSQL connection test successful", zap.String("health_query", pcb.healthQuery))
	return nil
}

// BuildSQLDB builds a standard sql.DB connection
func (dcb *DatabaseConnectionBuilder) BuildSQLDB(driverName string) (*sql.DB, error) {
	if dcb.connectionString == "" {
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "connection string is required")
	}

	// Open database connection
	db, err := sql.Open(driverName, dcb.connectionString)
	if err != nil {
		return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to open database connection")
	}

	// Configure connection pool
	db.SetMaxOpenConns(dcb.maxConnections)
	db.SetMaxIdleConns(dcb.maxIdleConnections)
	db.SetConnMaxLifetime(dcb.maxLifetime)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), dcb.connectTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close() // Ignore close error when connection already failed
		return nil, nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "database ping failed")
	}

	dcb.logger.Info("SQL database connection created successfully",
		zap.String("driver", driverName),
		zap.String("connection_string", obfuscateConnectionString(dcb.connectionString)),
		zap.Int("max_connections", dcb.maxConnections),
		zap.Int("max_idle_connections", dcb.maxIdleConnections))

	return db, nil
}

// ConnectionHealthChecker provides health checking for database connections
type ConnectionHealthChecker struct {
	healthQuery   string
	checkInterval time.Duration
	logger        *zap.Logger
}

// NewConnectionHealthChecker creates a new connection health checker
func NewConnectionHealthChecker(healthQuery string, checkInterval time.Duration) *ConnectionHealthChecker {
	return &ConnectionHealthChecker{
		healthQuery:   healthQuery,
		checkInterval: checkInterval,
		logger:        GetGlobalSDK().logger.With(zap.String("component", "connection_health_checker")),
	}
}

// CheckPostgreSQLHealth checks PostgreSQL connection health
func (chc *ConnectionHealthChecker) CheckPostgreSQLHealth(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to acquire connection for health check")
	}
	defer conn.Release()

	var result int
	err = conn.QueryRow(ctx, chc.healthQuery).Scan(&result)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "health check query failed")
	}

	return nil
}

// CheckSQLDBHealth checks standard sql.DB health
func (chc *ConnectionHealthChecker) CheckSQLDBHealth(ctx context.Context, db *sql.DB) error {
	if err := db.PingContext(ctx); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "database ping failed")
	}

	// Execute health query if specified
	if chc.healthQuery != "" {
		var result int
		err := db.QueryRowContext(ctx, chc.healthQuery).Scan(&result)
		if err != nil {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "health check query failed")
		}
	}

	return nil
}

// ConnectionTemplate provides pre-configured connection templates
type ConnectionTemplate struct{}

// NewConnectionTemplate creates a new connection template
func NewConnectionTemplate() *ConnectionTemplate {
	return &ConnectionTemplate{}
}

// PostgreSQLDefault returns a default PostgreSQL connection builder
func (ct *ConnectionTemplate) PostgreSQLDefault() *PostgreSQLConnectionBuilder {
	return NewPostgreSQLConnectionBuilder().
		WithMaxConnections(10).
		WithMaxIdleConnections(5).
		WithMaxLifetime(time.Hour).
		WithConnectTimeout(30 * time.Second).
		WithQueryTimeout(10 * time.Second).
		WithHealthQuery("SELECT 1")
}

// PostgreSQLHighPerformance returns a high-performance PostgreSQL connection builder
func (ct *ConnectionTemplate) PostgreSQLHighPerformance() *PostgreSQLConnectionBuilder {
	return NewPostgreSQLConnectionBuilder().
		WithMaxConnections(50).
		WithMaxIdleConnections(25).
		WithMaxLifetime(30 * time.Minute).
		WithConnectTimeout(10 * time.Second).
		WithQueryTimeout(5 * time.Second).
		WithHealthQuery("SELECT 1")
}

// PostgreSQLLowLatency returns a low-latency PostgreSQL connection builder
func (ct *ConnectionTemplate) PostgreSQLLowLatency() *PostgreSQLConnectionBuilder {
	return NewPostgreSQLConnectionBuilder().
		WithMaxConnections(20).
		WithMaxIdleConnections(15).
		WithMaxLifetime(15 * time.Minute).
		WithConnectTimeout(5 * time.Second).
		WithQueryTimeout(3 * time.Second).
		WithHealthQuery("SELECT 1")
}

// Helper function to obfuscate sensitive information in connection strings
func obfuscateConnectionString(connStr string) string {
	// Simple obfuscation - replace password with ***
	// This is a basic implementation, could be enhanced for more security
	return "***connection_string_obfuscated***"
}

// ConnectionMetrics provides metrics for database connections
type ConnectionMetrics struct {
	ActiveConnections int64 `json:"active_connections"`
	IdleConnections   int64 `json:"idle_connections"`
	TotalConnections  int64 `json:"total_connections"`
	ConnectionErrors  int64 `json:"connection_errors"`
	HealthCheckPassed int64 `json:"health_check_passed"`
	HealthCheckFailed int64 `json:"health_check_failed"`
}

// GetPostgreSQLMetrics gets connection metrics for PostgreSQL pool
func GetPostgreSQLMetrics(pool *pgxpool.Pool) *ConnectionMetrics {
	stat := pool.Stat()
	return &ConnectionMetrics{
		ActiveConnections: int64(stat.AcquiredConns()),
		IdleConnections:   int64(stat.IdleConns()),
		TotalConnections:  int64(stat.TotalConns()),
	}
}

// GetSQLDBMetrics gets connection metrics for standard sql.DB
func GetSQLDBMetrics(db *sql.DB) *ConnectionMetrics {
	stats := db.Stats()
	return &ConnectionMetrics{
		ActiveConnections: int64(stats.InUse),
		IdleConnections:   int64(stats.Idle),
		TotalConnections:  int64(stats.OpenConnections),
	}
}
