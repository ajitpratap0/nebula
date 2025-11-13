package iceberg

import (
	"context"
	"fmt"
	"sync"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
	// iceberg "github.com/shubham-tomar/iceberg-go"
	// "github.com/shubham-tomar/iceberg-go/table"
	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"go.uber.org/zap"
)

// IcebergSource implements the core.Source interface for reading from Iceberg tables
type IcebergSource struct {
	// Configuration
	catalogProvider CatalogProvider
	catalogType     string
	catalogURI      string
	catalogName     string
	warehouse       string
	database        string
	tableName       string
	branch          string // For Nessie

	// Storage configuration (S3/MinIO)
	region     string
	s3Endpoint string
	accessKey  string
	secretKey  string
	properties map[string]string

	// Iceberg table metadata
	table         *table.Table
	schema        *core.Schema
	icebergSchema *iceberg.Schema

	// Snapshot management
	snapshotManager *SnapshotManager
	currentSnapshot *table.Snapshot

	// Data reading
	manifestReader *ManifestReader
	dataFileReader *DataFileReader

	// State tracking
	position      *IcebergPosition
	recordsRead   int64
	bytesRead     int64
	filesRead     int64
	isInitialized bool

	// Synchronization
	mu sync.RWMutex

	// Configuration
	config        *config.BaseConfig
	readBatchSize int

	// Logger
	logger *zap.Logger
}

// NewIcebergSource creates a new Iceberg source connector
func NewIcebergSource(config *config.BaseConfig) (core.Source, error) {
	if config == nil {
		return nil, fmt.Errorf("configuration cannot be nil")
	}

	logger, _ := zap.NewProduction()

	return &IcebergSource{
		config:     config,
		logger:     logger,
		properties: make(map[string]string),
		position: &IcebergPosition{
			Metadata: make(map[string]interface{}),
		},
	}, nil
}

// Initialize initializes the Iceberg source connector
func (s *IcebergSource) Initialize(ctx context.Context, config *config.BaseConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.config = config

	// Parse Iceberg-specific configuration
	if err := s.parseConfig(config); err != nil {
		return fmt.Errorf("failed to parse Iceberg config: %w", err)
	}

	// Create catalog provider
	catalogProvider, err := s.createCatalogProvider()
	if err != nil {
		return fmt.Errorf("failed to create catalog provider: %w", err)
	}
	s.catalogProvider = catalogProvider

	// Connect to catalog
	catalogConfig := CatalogConfig{
		Name:              s.catalogName,
		URI:               s.catalogURI,
		WarehouseLocation: s.warehouse,
		Branch:            s.branch,
		Region:            s.region,
		S3Endpoint:        s.s3Endpoint,
		AccessKey:         s.accessKey,
		SecretKey:         s.secretKey,
		Properties:        s.properties,
	}

	if err := s.catalogProvider.Connect(ctx, catalogConfig); err != nil {
		return fmt.Errorf("failed to connect to catalog: %w", err)
	}

	// Load table metadata
	if err := s.loadTable(ctx); err != nil {
		return fmt.Errorf("failed to load table: %w", err)
	}

	// Initialize snapshot manager
	s.snapshotManager = NewSnapshotManager(s.table, s.logger)

	// Get current snapshot
	snapshot, err := s.snapshotManager.GetCurrentSnapshot()
	if err != nil {
		return fmt.Errorf("failed to get current snapshot: %w", err)
	}
	s.currentSnapshot = snapshot

	// Discover schema
	if err := s.discoverSchema(ctx); err != nil {
		return fmt.Errorf("failed to discover schema: %w", err)
	}

	// Initialize manifest reader
	s.manifestReader = NewManifestReader(s.currentSnapshot, s.table, s.logger)

	// Initialize data file reader with the table (table has IO configured from catalog)
	s.dataFileReader = NewDataFileReader(s.table, s.readBatchSize, s.logger)

	s.isInitialized = true
	s.logger.Info("Iceberg source initialized",
		zap.String("table", fmt.Sprintf("%s.%s", s.database, s.tableName)),
		zap.Int64("snapshot_id", s.currentSnapshot.SnapshotID),
		zap.String("catalog_type", s.catalogType))

	return nil
}

// parseConfig parses Iceberg-specific configuration from BaseConfig
func (s *IcebergSource) parseConfig(config *config.BaseConfig) error {
	creds := config.Security.Credentials
	if creds == nil {
		return fmt.Errorf("missing security credentials")
	}

	// Required fields
	requiredFields := map[string]*string{
		"catalog_type": &s.catalogType,
		"catalog_uri":  &s.catalogURI,
		"catalog_name": &s.catalogName,
		"warehouse":    &s.warehouse,
		"database":     &s.database,
		"table":        &s.tableName,
	}

	for field, target := range requiredFields {
		if value, ok := creds[field]; ok && value != "" {
			*target = value
		} else {
			return fmt.Errorf("missing required field: %s", field)
		}
	}

	// Optional fields
	if branch, ok := creds["branch"]; ok {
		s.branch = branch
	} else {
		s.branch = "main" // default branch for Nessie
	}

	// S3 configuration - support both direct and prop_ prefixed fields
	s.region = getCredValue(creds, "region", "prop_s3.region")
	s.s3Endpoint = getCredValue(creds, "s3_endpoint", "prop_s3.endpoint")
	s.accessKey = getCredValue(creds, "access_key", "prop_s3.access-key-id")
	s.secretKey = getCredValue(creds, "secret_key", "prop_s3.secret-access-key")

	// Collect all properties with prop_ prefix
	for key, value := range creds {
		if len(key) > 5 && key[:5] == "prop_" {
			// Remove "prop_" prefix and add to properties
			propKey := key[5:]
			s.properties[propKey] = value
		}
	}

	// Performance configuration
	s.readBatchSize = config.Performance.BatchSize
	if s.readBatchSize <= 0 {
		s.readBatchSize = 10000 // default batch size
	}

	return nil
}

// getCredValue gets a credential value, trying multiple keys
func getCredValue(creds map[string]string, keys ...string) string {
	for _, key := range keys {
		if value, ok := creds[key]; ok && value != "" {
			return value
		}
	}
	return ""
}

// createCatalogProvider creates the appropriate catalog provider based on type
func (s *IcebergSource) createCatalogProvider() (CatalogProvider, error) {
	switch s.catalogType {
	case "nessie":
		return NewNessieCatalog(s.logger), nil
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", s.catalogType)
	}
}

// loadTable loads the Iceberg table from the catalog
func (s *IcebergSource) loadTable(ctx context.Context) error {
	table, err := s.catalogProvider.LoadTable(ctx, s.database, s.tableName)
	if err != nil {
		return fmt.Errorf("failed to load table: %w", err)
	}
	s.table = table
	return nil
}

// discoverSchema discovers the schema from the Iceberg table
func (s *IcebergSource) discoverSchema(ctx context.Context) error {
	// Get Iceberg schema from table
	s.icebergSchema = s.table.Schema()

	// Get schema from catalog provider
	schema, err := s.catalogProvider.GetSchema(ctx, s.database, s.tableName)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	s.schema = schema
	s.logger.Info("Discovered Iceberg table schema",
		zap.String("table", s.tableName),
		zap.Int("field_count", len(schema.Fields)))

	return nil
}

// Discover returns the discovered schema
func (s *IcebergSource) Discover(ctx context.Context) (*core.Schema, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.schema == nil {
		return nil, fmt.Errorf("schema not discovered yet")
	}

	return s.schema, nil
}

// Read streams individual records from the Iceberg table
func (s *IcebergSource) Read(ctx context.Context) (*core.RecordStream, error) {
	s.mu.RLock()
	if !s.isInitialized {
		s.mu.RUnlock()
		return nil, fmt.Errorf("source not initialized")
	}
	s.mu.RUnlock()

	recordChan := pool.GetRecordChannel(s.config.Performance.BufferSize)
	errorChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errorChan)
		defer pool.PutRecordChannel(recordChan)

		if err := s.streamRecords(ctx, recordChan, errorChan); err != nil {
			errorChan <- err
		}
	}()

	return &core.RecordStream{
		Records: recordChan,
		Errors:  errorChan,
	}, nil
}

// ReadBatch reads records in batches from the Iceberg table
func (s *IcebergSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	s.mu.RLock()
	if !s.isInitialized {
		s.mu.RUnlock()
		return nil, fmt.Errorf("source not initialized")
	}
	s.mu.RUnlock()

	batchChan := pool.GetBatchChannel()
	errorChan := make(chan error, 1)

	// Use configured batch size if not specified
	if batchSize <= 0 {
		batchSize = s.readBatchSize
	}

	go func() {
		defer close(batchChan)
		defer close(errorChan)
		defer pool.PutBatchChannel(batchChan)

		if err := s.streamBatches(ctx, batchSize, batchChan, errorChan); err != nil {
			errorChan <- err
		}
	}()

	return &core.BatchStream{
		Batches: batchChan,
		Errors:  errorChan,
	}, nil
}

// streamRecords streams individual records using table scan
func (s *IcebergSource) streamRecords(ctx context.Context, recordChan chan<- *pool.Record, errorChan chan<- error) error {
	s.logger.Info("Starting to read all records using table scan")

	// Read all records using table scan (more efficient than manual file iteration)
	allRecords, err := s.dataFileReader.ReadAllRecords(ctx)
	if err != nil {
		return fmt.Errorf("failed to read records: %w", err)
	}

	s.logger.Info("Read records from table scan",
		zap.Int("total_records", len(allRecords)))

	// Send records to channel
	for idx, record := range allRecords {
		select {
		case recordChan <- record:
			s.mu.Lock()
			s.recordsRead++
			s.position.RowOffset = int64(idx)
			s.mu.Unlock()
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// streamBatches streams batches of records using table scan
func (s *IcebergSource) streamBatches(ctx context.Context, batchSize int, batchChan chan<- []*pool.Record, errorChan chan<- error) error {
	s.logger.Info("Starting to read records in batches using table scan",
		zap.Int("batch_size", batchSize))

	// Read all records using table scan
	allRecords, err := s.dataFileReader.ReadAllRecords(ctx)
	if err != nil {
		return fmt.Errorf("failed to read records: %w", err)
	}

	s.logger.Info("Read records from table scan",
		zap.Int("total_records", len(allRecords)))

	// Send records in batches
	batch := pool.GetBatchSlice(batchSize)
	defer pool.PutBatchSlice(batch)

	for idx, record := range allRecords {
		batch = append(batch, record)

		s.mu.Lock()
		s.recordsRead++
		s.position.RowOffset = int64(idx)
		s.mu.Unlock()

		// Send batch when full
		if len(batch) >= batchSize {
			select {
			case batchChan <- batch:
				batch = pool.GetBatchSlice(batchSize)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// Send remaining batch
	if len(batch) > 0 {
		select {
		case batchChan <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// Close closes the Iceberg source connector
func (s *IcebergSource) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.catalogProvider != nil {
		if err := s.catalogProvider.Close(ctx); err != nil {
			return fmt.Errorf("failed to close catalog: %w", err)
		}
	}

	s.isInitialized = false
	s.logger.Info("Iceberg source closed")

	return nil
}

// GetPosition returns the current read position
func (s *IcebergSource) GetPosition() core.Position {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return &IcebergPosition{
		SnapshotID:    s.currentSnapshot.SnapshotID,
		ManifestIndex: s.manifestReader.currentIndex,
		DataFileIndex: s.position.DataFileIndex,
		RowOffset:     s.position.RowOffset,
		Metadata: map[string]interface{}{
			"records_read": s.recordsRead,
			"files_read":   s.filesRead,
		},
	}
}

// SetPosition sets the read position for incremental sync
func (s *IcebergSource) SetPosition(position core.Position) error {
	icebergPos, ok := position.(*IcebergPosition)
	if !ok {
		return fmt.Errorf("invalid position type")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.position = icebergPos
	return nil
}

// GetState returns the full connector state
func (s *IcebergSource) GetState() core.State {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return core.State{
		"snapshot_id":     s.currentSnapshot.SnapshotID,
		"manifest_index":  s.manifestReader.currentIndex,
		"data_file_index": s.position.DataFileIndex,
		"row_offset":      s.position.RowOffset,
		"records_read":    s.recordsRead,
		"files_read":      s.filesRead,
		"bytes_read":      s.bytesRead,
	}
}

// SetState restores connector state from a previous run
func (s *IcebergSource) SetState(state core.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Restore state fields if present
	if snapshotID, ok := state["snapshot_id"].(int64); ok {
		s.position.SnapshotID = snapshotID
	}

	if manifestIdx, ok := state["manifest_index"].(int); ok {
		s.manifestReader.currentIndex = manifestIdx
	}

	if dataFileIdx, ok := state["data_file_index"].(int); ok {
		s.position.DataFileIndex = dataFileIdx
	}

	if rowOffset, ok := state["row_offset"].(int64); ok {
		s.position.RowOffset = rowOffset
	}

	if recordsRead, ok := state["records_read"].(int64); ok {
		s.recordsRead = recordsRead
	}

	if filesRead, ok := state["files_read"].(int64); ok {
		s.filesRead = filesRead
	}

	if bytesRead, ok := state["bytes_read"].(int64); ok {
		s.bytesRead = bytesRead
	}

	return nil
}

// SupportsIncremental indicates if incremental sync is available
func (s *IcebergSource) SupportsIncremental() bool {
	return true // Iceberg supports incremental reads via snapshots
}

// SupportsRealtime indicates if real-time streaming is available
func (s *IcebergSource) SupportsRealtime() bool {
	return false // Not real-time, but supports incremental
}

// SupportsBatch indicates if batch reading is available
func (s *IcebergSource) SupportsBatch() bool {
	return true
}

// Subscribe starts CDC streaming for specified tables
func (s *IcebergSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	return nil, fmt.Errorf("iceberg source does not support real-time subscriptions")
}

// Health checks if the source is operational
func (s *IcebergSource) Health(ctx context.Context) error {
	if s.catalogProvider == nil {
		return fmt.Errorf("catalog provider not initialized")
	}

	return s.catalogProvider.Health(ctx)
}

// Metrics returns performance and operational metrics
func (s *IcebergSource) Metrics() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"records_read":        s.recordsRead,
		"bytes_read":          s.bytesRead,
		"files_read":          s.filesRead,
		"current_snapshot_id": s.currentSnapshot.SnapshotID,
		"table":               fmt.Sprintf("%s.%s", s.database, s.tableName),
		"catalog_type":        s.catalogType,
	}
}
