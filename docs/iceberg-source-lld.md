# Low-Level Design: Iceberg Source Connector

## 1. Overview

### 1.1 Purpose
This document describes the low-level design for implementing an Apache Iceberg source connector in Nebula. The connector will enable reading data from Iceberg tables through various catalog implementations (Nessie, REST) with high performance and memory efficiency.

### 1.2 Goals
- Read data from Iceberg tables efficiently with support for batch and streaming modes
- Support multiple catalog types (Nessie, REST, AWS Glue, Hive)
- Implement incremental reads using Iceberg snapshots and manifests
- Maintain Nebula's performance characteristics (1.7M-3.6M records/sec)
- Support schema evolution and partition pruning
- Enable predicate pushdown for efficient data filtering
- Provide CDC-like capabilities using Iceberg's snapshot isolation

### 1.3 Non-Goals
- Writing to Iceberg tables (already implemented as destination)
- Schema migration or DDL operations
- Custom file format support beyond Parquet/ORC/Avro

## 2. Architecture

### 2.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    IcebergSource                                │
│  (implements core.Source, embeds BaseConnector)                 │
├─────────────────────────────────────────────────────────────────┤
│  - Configuration & Initialization                               │
│  - Schema Discovery                                             │
│  - Read Operations (Stream/Batch)                               │
│  - State Management (Snapshot tracking)                         │
│  - Health & Metrics                                             │
└──────────────┬──────────────────────────────────────────────────┘
               │
               ├─────────────► CatalogProvider Interface
               │                  │
               │                  ├─► NessieCatalog
               │                  ├─► RestCatalog
               │                  ├─► GlueCatalog (future)
               │                  └─► HiveCatalog (future)
               │
               ├─────────────► SnapshotManager
               │                  - Snapshot discovery
               │                  - Incremental reads
               │                  - Position tracking
               │
               ├─────────────► ManifestReader
               │                  - Manifest file reading
               │                  - Data file discovery
               │                  - Partition pruning
               │
               └─────────────► DataFileReader
                                  - Parquet file reading
                                  - ORC file reading (future)
                                  - Avro file reading (future)
                                  - Arrow conversion
```

### 2.2 Package Structure

```
pkg/connector/sources/iceberg/
├── iceberg_source.go          # Main source implementation
├── catalog_provider.go         # Catalog interface (shared with destination)
├── snapshot_manager.go         # Snapshot and incremental read logic
├── manifest_reader.go          # Manifest file processing
├── data_file_reader.go         # Data file reading and conversion
├── schema_converter.go         # Iceberg to Core schema conversion
├── predicate_pushdown.go       # Filter predicate optimization
├── partition_filter.go         # Partition pruning logic
├── arrow_converter.go          # Arrow to Record conversion
├── types.go                    # Type definitions
├── config.go                   # Configuration structures
└── init.go                     # Registration
```

## 3. Detailed Design

### 3.1 Core Data Structures

#### 3.1.1 IcebergSource

```go
type IcebergSource struct {
    *baseconnector.BaseConnector

    // Configuration
    catalogProvider CatalogProvider
    catalogType     string
    catalogURI      string
    database        string
    tableName       string
    branch          string // For Nessie

    // Storage configuration (S3/MinIO)
    region          string
    s3Endpoint      string
    accessKey       string
    secretKey       string
    properties      map[string]string

    // Iceberg table metadata
    table           icebergGo.Table
    schema          *core.Schema
    icebergSchema   *icebergGo.Schema

    // Snapshot management
    snapshotManager *SnapshotManager
    currentSnapshot *icebergGo.Snapshot
    startSnapshot   int64  // For incremental reads
    endSnapshot     int64  // For incremental reads

    // Data reading
    manifestReader  *ManifestReader
    dataFileReader  *DataFileReader

    // State tracking
    position        *IcebergPosition
    recordsRead     int64
    bytesRead       int64
    filesRead       int64
    isInitialized   bool

    // Filtering
    predicates      []Predicate
    partitionFilter *PartitionFilter

    // Synchronization
    mu              sync.RWMutex

    // Performance
    readBatchSize   int
    prefetchFiles   int
    useArrowReader  bool
}

type IcebergPosition struct {
    SnapshotID    int64                    `json:"snapshot_id"`
    ManifestIndex int                      `json:"manifest_index"`
    DataFileIndex int                      `json:"data_file_index"`
    RowOffset     int64                    `json:"row_offset"`
    Metadata      map[string]interface{}   `json:"metadata"`
}

func (p *IcebergPosition) String() string {
    return fmt.Sprintf("iceberg_snapshot_%d_manifest_%d_file_%d_offset_%d",
        p.SnapshotID, p.ManifestIndex, p.DataFileIndex, p.RowOffset)
}

func (p *IcebergPosition) Compare(other core.Position) int {
    // Implementation for position comparison
}
```

#### 3.1.2 SnapshotManager

```go
type SnapshotManager struct {
    table           icebergGo.Table
    currentSnapshot *icebergGo.Snapshot
    snapshots       []*icebergGo.Snapshot
    logger          *zap.Logger
}

// GetCurrentSnapshot returns the current snapshot
func (sm *SnapshotManager) GetCurrentSnapshot() (*icebergGo.Snapshot, error)

// GetSnapshotsSince returns snapshots after a given snapshot ID
func (sm *SnapshotManager) GetSnapshotsSince(snapshotID int64) ([]*icebergGo.Snapshot, error)

// GetSnapshotsRange returns snapshots between start and end
func (sm *SnapshotManager) GetSnapshotsRange(start, end int64) ([]*icebergGo.Snapshot, error)

// ListSnapshots returns all available snapshots
func (sm *SnapshotManager) ListSnapshots() ([]*icebergGo.Snapshot, error)
```

#### 3.1.3 ManifestReader

```go
type ManifestReader struct {
    snapshot        *icebergGo.Snapshot
    manifests       []icebergGo.ManifestFile
    currentIndex    int
    dataFiles       []icebergGo.DataFile
    partitionFilter *PartitionFilter
    logger          *zap.Logger
}

// GetDataFiles returns all data files from manifests
func (mr *ManifestReader) GetDataFiles(ctx context.Context) ([]icebergGo.DataFile, error)

// GetDataFilesFiltered returns filtered data files
func (mr *ManifestReader) GetDataFilesFiltered(
    ctx context.Context,
    filter *PartitionFilter,
) ([]icebergGo.DataFile, error)

// GetManifestCount returns the number of manifests
func (mr *ManifestReader) GetManifestCount() int
```

#### 3.1.4 DataFileReader

```go
type DataFileReader struct {
    dataFiles       []icebergGo.DataFile
    currentIndex    int
    schema          *arrow.Schema
    batchSize       int
    s3Config        S3Config
    useArrowReader  bool
    recordPool      *RecordPool
    logger          *zap.Logger
}

type S3Config struct {
    Region     string
    Endpoint   string
    AccessKey  string
    SecretKey  string
    Properties map[string]string
}

// ReadFile reads a single data file and returns records
func (dfr *DataFileReader) ReadFile(
    ctx context.Context,
    file icebergGo.DataFile,
) ([]*pool.Record, error)

// ReadFileStream streams records from a file
func (dfr *DataFileReader) ReadFileStream(
    ctx context.Context,
    file icebergGo.DataFile,
    recordChan chan<- *pool.Record,
    errorChan chan<- error,
)

// ReadParquet reads a Parquet file
func (dfr *DataFileReader) ReadParquet(
    ctx context.Context,
    filePath string,
) ([]*pool.Record, error)

// ConvertArrowToRecords converts Arrow records to Nebula records
func (dfr *DataFileReader) ConvertArrowToRecords(
    arrowRecord arrow.Record,
) ([]*pool.Record, error)
```

#### 3.1.5 Configuration

```go
type IcebergSourceConfig struct {
    // Catalog configuration
    CatalogType       string            `json:"catalog_type"`        // nessie, rest, glue, hive
    CatalogURI        string            `json:"catalog_uri"`
    CatalogName       string            `json:"catalog_name"`
    Warehouse         string            `json:"warehouse"`
    Database          string            `json:"database"`
    Table             string            `json:"table"`
    Branch            string            `json:"branch"`              // For Nessie

    // S3/MinIO configuration
    Region            string            `json:"region"`
    S3Endpoint        string            `json:"s3_endpoint"`
    AccessKey         string            `json:"access_key"`
    SecretKey         string            `json:"secret_key"`

    // Read options
    SnapshotID        *int64            `json:"snapshot_id"`         // Specific snapshot
    StartSnapshot     *int64            `json:"start_snapshot"`      // For incremental
    EndSnapshot       *int64            `json:"end_snapshot"`        // For incremental
    AsOfTimestamp     *time.Time        `json:"as_of_timestamp"`     // Time travel

    // Filtering
    Predicates        []string          `json:"predicates"`          // SQL-like filters
    PartitionFilters  map[string]string `json:"partition_filters"`   // Partition pruning

    // Performance tuning
    BatchSize         int               `json:"batch_size"`
    PrefetchFiles     int               `json:"prefetch_files"`
    UseArrowReader    bool              `json:"use_arrow_reader"`
    MaxConcurrency    int               `json:"max_concurrency"`

    // Additional properties
    Properties        map[string]string `json:"properties"`
}
```

### 3.2 Core Operations

#### 3.2.1 Initialize

```go
func (s *IcebergSource) Initialize(ctx context.Context, config *config.BaseConfig) error {
    // 1. Initialize base connector
    if err := s.BaseConnector.Initialize(ctx, config); err != nil {
        return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig,
            "failed to initialize base connector")
    }

    // 2. Parse Iceberg-specific configuration
    if err := s.parseConfig(config); err != nil {
        return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig,
            "failed to parse Iceberg config")
    }

    // 3. Create catalog provider
    catalogProvider, err := NewCatalogProvider(s.catalogType, s.GetLogger())
    if err != nil {
        return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig,
            "failed to create catalog provider")
    }
    s.catalogProvider = catalogProvider

    // 4. Connect to catalog with circuit breaker
    if err := s.ExecuteWithCircuitBreaker(func() error {
        return s.connectToCatalog(ctx)
    }); err != nil {
        return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection,
            "failed to connect to catalog")
    }

    // 5. Load table metadata
    if err := s.loadTable(ctx); err != nil {
        return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData,
            "failed to load table")
    }

    // 6. Initialize snapshot manager
    s.snapshotManager = NewSnapshotManager(s.table, s.GetLogger())

    // 7. Discover schema
    if err := s.discoverSchema(ctx); err != nil {
        return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData,
            "failed to discover schema")
    }

    // 8. Initialize manifest and data file readers
    if err := s.initializeReaders(ctx); err != nil {
        return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData,
            "failed to initialize readers")
    }

    // 9. Update health status
    s.UpdateHealth(true, map[string]interface{}{
        "table":           s.tableName,
        "database":        s.database,
        "snapshot_id":     s.currentSnapshot.SnapshotID,
        "schema_version":  s.icebergSchema.ID,
    })

    s.isInitialized = true
    s.GetLogger().Info("Iceberg source initialized",
        zap.String("table", fmt.Sprintf("%s.%s", s.database, s.tableName)),
        zap.Int64("snapshot_id", s.currentSnapshot.SnapshotID))

    return nil
}
```

#### 3.2.2 Discover

```go
func (s *IcebergSource) Discover(ctx context.Context) (*core.Schema, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    if s.schema == nil {
        return nil, nebulaerrors.New(nebulaerrors.ErrorTypeData,
            "schema not discovered yet")
    }

    return s.schema, nil
}

func (s *IcebergSource) discoverSchema(ctx context.Context) error {
    // Get Iceberg schema from table
    s.icebergSchema = s.table.Schema()

    // Convert to core schema
    fields := make([]core.Field, 0, len(s.icebergSchema.Fields()))
    for _, icebergField := range s.icebergSchema.Fields() {
        coreField := s.convertIcebergFieldToCore(icebergField)
        fields = append(fields, coreField)
    }

    s.schema = &core.Schema{
        Name:        s.tableName,
        Description: fmt.Sprintf("Iceberg table %s.%s", s.database, s.tableName),
        Fields:      fields,
        Version:     s.icebergSchema.ID,
        CreatedAt:   time.Now(),
        UpdatedAt:   time.Now(),
    }

    s.GetLogger().Info("Discovered Iceberg table schema",
        zap.String("table", s.tableName),
        zap.Int("schema_id", s.icebergSchema.ID),
        zap.Int("field_count", len(fields)))

    return nil
}
```

#### 3.2.3 Read Operations

##### ReadBatch (Primary Implementation)

```go
func (s *IcebergSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
    s.mu.RLock()
    if !s.isInitialized {
        s.mu.RUnlock()
        return nil, nebulaerrors.New(nebulaerrors.ErrorTypeValidation,
            "source not initialized")
    }
    s.mu.RUnlock()

    batchChan := pool.GetBatchChannel()
    errorChan := make(chan error, 1)

    // Use optimized batch size
    optimizedBatchSize := s.OptimizeBatchSize()
    if batchSize > 0 && batchSize < optimizedBatchSize {
        optimizedBatchSize = batchSize
    }

    go func() {
        defer close(batchChan)
        defer close(errorChan)
        defer pool.PutBatchChannel(batchChan)

        if err := s.streamBatches(ctx, optimizedBatchSize, batchChan, errorChan); err != nil {
            errorChan <- err
        }
    }()

    return &core.BatchStream{
        Batches: batchChan,
        Errors:  errorChan,
    }, nil
}

func (s *IcebergSource) streamBatches(
    ctx context.Context,
    batchSize int,
    batchChan chan<- []*pool.Record,
    errorChan chan<- error,
) error {
    // Get data files from manifests
    dataFiles, err := s.manifestReader.GetDataFilesFiltered(ctx, s.partitionFilter)
    if err != nil {
        return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData,
            "failed to get data files")
    }

    s.GetLogger().Info("Starting to read data files",
        zap.Int("file_count", len(dataFiles)))

    batch := pool.GetBatchSlice(batchSize)
    defer pool.PutBatchSlice(batch)

    // Process each data file
    for fileIdx, dataFile := range dataFiles {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }

        // Update position
        s.mu.Lock()
        s.position.DataFileIndex = fileIdx
        s.mu.Unlock()

        // Apply rate limiting
        if err := s.RateLimit(ctx); err != nil {
            return err
        }

        // Read file with circuit breaker
        var fileRecords []*pool.Record
        if err := s.ExecuteWithCircuitBreaker(func() error {
            var readErr error
            fileRecords, readErr = s.dataFileReader.ReadFile(ctx, dataFile)
            return readErr
        }); err != nil {
            if handleErr := s.HandleError(ctx, err, nil); handleErr != nil {
                return handleErr
            }
            continue
        }

        // Add records to batch
        for _, record := range fileRecords {
            batch = append(batch, record)

            s.mu.Lock()
            s.recordsRead++
            s.position.RowOffset++
            s.mu.Unlock()

            // Send batch when full
            if len(batch) >= batchSize {
                s.RecordMetric("batches_read", 1, core.MetricTypeCounter)
                s.RecordMetric("records_in_batch", len(batch), core.MetricTypeGauge)

                select {
                case batchChan <- batch:
                    batch = pool.GetBatchSlice(batchSize)
                case <-ctx.Done():
                    return ctx.Err()
                }
            }
        }

        s.mu.Lock()
        s.filesRead++
        s.mu.Unlock()

        s.RecordMetric("files_read", 1, core.MetricTypeCounter)
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
```

##### Read (Streaming)

```go
func (s *IcebergSource) Read(ctx context.Context) (*core.RecordStream, error) {
    s.mu.RLock()
    if !s.isInitialized {
        s.mu.RUnlock()
        return nil, nebulaerrors.New(nebulaerrors.ErrorTypeValidation,
            "source not initialized")
    }
    s.mu.RUnlock()

    recordChan := pool.GetRecordChannel(s.GetConfig().Performance.BufferSize)
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
```

#### 3.2.4 State Management

```go
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

func (s *IcebergSource) SetPosition(position core.Position) error {
    icebergPos, ok := position.(*IcebergPosition)
    if !ok {
        return nebulaerrors.New(nebulaerrors.ErrorTypeValidation,
            "invalid position type")
    }

    s.mu.Lock()
    defer s.mu.Unlock()

    // Update current position
    s.position = icebergPos

    // Seek to snapshot if different
    if s.currentSnapshot.SnapshotID != icebergPos.SnapshotID {
        snapshot, err := s.snapshotManager.GetSnapshotByID(icebergPos.SnapshotID)
        if err != nil {
            return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData,
                "failed to find snapshot")
        }
        s.currentSnapshot = snapshot
    }

    return nil
}

func (s *IcebergSource) GetState() core.State {
    s.mu.RLock()
    defer s.mu.RUnlock()

    return core.State{
        "snapshot_id":    s.currentSnapshot.SnapshotID,
        "manifest_index": s.manifestReader.currentIndex,
        "data_file_index": s.position.DataFileIndex,
        "row_offset":     s.position.RowOffset,
        "records_read":   s.recordsRead,
        "files_read":     s.filesRead,
        "bytes_read":     s.bytesRead,
    }
}

func (s *IcebergSource) SetState(state core.State) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    if snapshotID, ok := state["snapshot_id"].(int64); ok {
        snapshot, err := s.snapshotManager.GetSnapshotByID(snapshotID)
        if err != nil {
            return err
        }
        s.currentSnapshot = snapshot
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
```

#### 3.2.5 Capabilities

```go
func (s *IcebergSource) SupportsIncremental() bool {
    return true  // Iceberg supports incremental reads via snapshots
}

func (s *IcebergSource) SupportsRealtime() bool {
    return false  // Not real-time, but supports incremental
}

func (s *IcebergSource) SupportsBatch() bool {
    return true
}

func (s *IcebergSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
    return nil, nebulaerrors.New(nebulaerrors.ErrorTypeCapability,
        "Iceberg source does not support real-time subscriptions")
}
```

### 3.3 Advanced Features

#### 3.3.1 Incremental Reads

```go
type IncrementalReadConfig struct {
    StartSnapshotID int64
    EndSnapshotID   int64
    Mode            IncrementalMode
}

type IncrementalMode string

const (
    IncrementalModeAppend    IncrementalMode = "append"     // Only new data
    IncrementalModeSnapshot  IncrementalMode = "snapshot"   // Full snapshot diff
    IncrementalModeChangelog IncrementalMode = "changelog"  // With delete detection
)

func (s *IcebergSource) ReadIncremental(
    ctx context.Context,
    config IncrementalReadConfig,
) (*core.BatchStream, error) {
    // Get snapshots between start and end
    snapshots, err := s.snapshotManager.GetSnapshotsRange(
        config.StartSnapshotID,
        config.EndSnapshotID,
    )
    if err != nil {
        return nil, err
    }

    // Process each snapshot incrementally
    // ... implementation
}
```

#### 3.3.2 Partition Pruning

```go
type PartitionFilter struct {
    filters map[string]interface{}
}

func (pf *PartitionFilter) ApplyToDataFiles(
    dataFiles []icebergGo.DataFile,
) []icebergGo.DataFile {
    filtered := make([]icebergGo.DataFile, 0)

    for _, file := range dataFiles {
        if pf.matchesPartition(file.Partition()) {
            filtered = append(filtered, file)
        }
    }

    return filtered
}

func (pf *PartitionFilter) matchesPartition(
    partition map[string]interface{},
) bool {
    for key, filterValue := range pf.filters {
        partValue, exists := partition[key]
        if !exists || partValue != filterValue {
            return false
        }
    }
    return true
}
```

#### 3.3.3 Predicate Pushdown

```go
type Predicate struct {
    Field    string
    Operator PredicateOp
    Value    interface{}
}

type PredicateOp string

const (
    OpEqual              PredicateOp = "="
    OpNotEqual           PredicateOp = "!="
    OpGreaterThan        PredicateOp = ">"
    OpGreaterThanOrEqual PredicateOp = ">="
    OpLessThan           PredicateOp = "<"
    OpLessThanOrEqual    PredicateOp = "<="
    OpIn                 PredicateOp = "IN"
    OpNotIn              PredicateOp = "NOT IN"
)

func (s *IcebergSource) ApplyPredicates(
    predicates []Predicate,
) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    s.predicates = predicates

    // Convert to Iceberg expressions for file-level filtering
    // ... implementation

    return nil
}
```

## 4. Configuration

### 4.1 JSON Configuration Example

```json
{
  "connector_id": "iceberg_source_01",
  "source_type": "iceberg",
  "name": "iceberg_source",
  "storage_mode": "columnar",
  "security": {
    "credentials": {
      "catalog_type": "nessie",
      "catalog_uri": "http://localhost:19120/api/v1",
      "catalog_name": "nessie",
      "warehouse": "s3://warehouse/",
      "database": "analytics",
      "table": "events",
      "branch": "main",
      "region": "us-east-1",
      "s3_endpoint": "http://localhost:9000",
      "access_key": "minioadmin",
      "secret_key": "minioadmin",
      "snapshot_id": "7893247829347823",
      "start_snapshot": "7893247829347820",
      "partition_filters": {
        "year": "2024",
        "month": "11"
      }
    }
  },
  "performance": {
    "batch_size": 10000,
    "workers": 4,
    "buffer_size": 1000,
    "max_concurrency": 8,
    "prefetch_files": 2
  },
  "timeouts": {
    "connection": "30s",
    "read": "5m",
    "idle": "10m"
  }
}
```

### 4.2 CLI Usage Example

```bash
# Full table read
go run cmd/nebula/main.go run \
  --source examples/configs/iceberg-source.json \
  --destination examples/configs/bigquery-destination.json \
  --batch-size 10000 \
  --log-level info

# Incremental read from specific snapshot
go run cmd/nebula/main.go run \
  --source examples/configs/iceberg-source-incremental.json \
  --destination examples/configs/snowflake-destination.json \
  --batch-size 10000
```

## 5. Performance Considerations

### 5.1 Memory Optimization

- **Arrow Integration**: Use Arrow for zero-copy data transfer from Parquet files
- **Record Pooling**: Leverage Nebula's record pool for memory efficiency
- **Streaming**: Stream data files instead of loading all into memory
- **Batch Processing**: Process files in configurable batches

### 5.2 I/O Optimization

- **File Prefetching**: Prefetch next N data files while processing current file
- **Parallel File Reading**: Read multiple data files concurrently
- **S3 Optimizations**: Use byte-range requests for Parquet footer reading
- **Connection Pooling**: Reuse S3 connections across file reads

### 5.3 Partition Pruning

- **Manifest-Level Filtering**: Filter manifests based on partition bounds
- **File-Level Filtering**: Skip data files that don't match partition filters
- **Statistics-Based Filtering**: Use Iceberg column statistics for pruning

### 5.4 Expected Performance

Based on Nebula's architecture:
- **Target Throughput**: 1M-2M records/sec for Parquet files
- **Memory Usage**: <100 bytes per record
- **Latency**: <100ms to first record
- **Scalability**: Linear with number of data files

## 6. Testing Strategy

### 6.1 Unit Tests

```go
// Test files in pkg/connector/sources/iceberg/

// iceberg_source_test.go
func TestIcebergSource_Initialize(t *testing.T)
func TestIcebergSource_Discover(t *testing.T)
func TestIcebergSource_ReadBatch(t *testing.T)
func TestIcebergSource_Read(t *testing.T)
func TestIcebergSource_StateManagement(t *testing.T)

// snapshot_manager_test.go
func TestSnapshotManager_GetCurrentSnapshot(t *testing.T)
func TestSnapshotManager_GetSnapshotsSince(t *testing.T)

// manifest_reader_test.go
func TestManifestReader_GetDataFiles(t *testing.T)
func TestManifestReader_PartitionPruning(t *testing.T)

// data_file_reader_test.go
func TestDataFileReader_ReadParquet(t *testing.T)
func TestDataFileReader_ArrowConversion(t *testing.T)
```

### 6.2 Integration Tests

```go
// tests/integration/iceberg_source_test.go

func TestIcebergSource_E2E_Nessie(t *testing.T)
func TestIcebergSource_E2E_IncrementalRead(t *testing.T)
func TestIcebergSource_E2E_PartitionedTable(t *testing.T)
func TestIcebergSource_E2E_SchemaEvolution(t *testing.T)
func TestIcebergSource_E2E_LargeTable(t *testing.T)
```

### 6.3 Benchmark Tests

```go
// tests/benchmarks/iceberg_source_bench_test.go

func BenchmarkIcebergSource_ReadBatch(b *testing.B)
func BenchmarkIcebergSource_ParquetConversion(b *testing.B)
func BenchmarkIcebergSource_PartitionPruning(b *testing.B)
func BenchmarkIcebergSource_SnapshotIteration(b *testing.B)
```

## 7. Error Handling

### 7.1 Error Types

```go
const (
    ErrorCatalogConnection    = "catalog_connection_error"
    ErrorTableNotFound        = "table_not_found"
    ErrorSnapshotNotFound     = "snapshot_not_found"
    ErrorManifestRead         = "manifest_read_error"
    ErrorDataFileRead         = "data_file_read_error"
    ErrorSchemaConversion     = "schema_conversion_error"
    ErrorPartitionFilter      = "partition_filter_error"
)
```

### 7.2 Recovery Strategies

- **Connection Errors**: Retry with exponential backoff (via BaseConnector)
- **File Read Errors**: Skip corrupted files and log error
- **Schema Mismatch**: Attempt schema evolution handling
- **Snapshot Errors**: Fall back to latest snapshot

## 8. Monitoring and Metrics

### 8.1 Key Metrics

```go
func (s *IcebergSource) Metrics() map[string]interface{} {
    return map[string]interface{}{
        "records_read":        s.recordsRead,
        "bytes_read":          s.bytesRead,
        "files_read":          s.filesRead,
        "manifests_processed": s.manifestReader.currentIndex,
        "current_snapshot_id": s.currentSnapshot.SnapshotID,
        "table":               fmt.Sprintf("%s.%s", s.database, s.tableName),
        "catalog_type":        s.catalogType,
        "read_throughput":     s.calculateThroughput(),
        "avg_file_size":       s.bytesRead / s.filesRead,
    }
}
```

### 8.2 Health Checks

```go
func (s *IcebergSource) Health(ctx context.Context) error {
    // Check catalog connection
    if err := s.catalogProvider.Health(ctx); err != nil {
        return err
    }

    // Check table accessibility
    if _, err := s.table.CurrentSnapshot(); err != nil {
        return err
    }

    return nil
}
```

## 9. Dependencies

### 9.1 New Dependencies

```go
require (
    github.com/apache/arrow-go/v18 v18.0.0
    github.com/shubham-tomar/iceberg-go v0.1.0
    github.com/apache/parquet-go v0.4.0
)
```

### 9.2 Shared with Destination

- Catalog providers (Nessie, REST)
- Schema conversion utilities
- Arrow conversion helpers

## 10. Implementation Phases

### Phase 1: Core Reading
- [ ] Basic IcebergSource structure
- [ ] Nessie catalog integration
- [ ] Single snapshot reading
- [ ] Parquet file reading
- [ ] Schema discovery
- [ ] Batch reading

### Phase 2: State Management
- [ ] Position tracking
- [ ] State persistence

- [ ] Resume from position
- [ ] Progress reporting

### Phase 3: Advanced Features
- [ ] Incremental reads
- [ ] Partition pruning
- [ ] Predicate pushdown
- [ ] Schema evolution handling

### Phase 4: Optimization
- [ ] Parallel file reading
- [ ] File prefetching
- [ ] Arrow optimization
- [ ] Memory pooling

### Phase 5: Additional Catalogs
- [ ] REST catalog
- [ ] AWS Glue catalog
- [ ] Hive catalog

## 11. Migration Path

### 11.1 From Existing Destination

Leverage existing code from `pkg/connector/destinations/iceberg/`:
- Catalog provider interface and implementations
- Schema conversion utilities
- Arrow integration patterns
- Configuration structures

### 11.2 Code Reuse

Create shared package:
```
pkg/connector/iceberg/
├── catalog/          # Shared catalog providers
├── schema/           # Schema conversion utilities
└── arrow/            # Arrow conversion helpers
```

Both source and destination import from shared package.

## 12. Documentation

### 12.1 User Documentation
- Configuration guide
- Usage examples
- Best practices
- Troubleshooting guide

### 12.2 Developer Documentation
- Architecture overview
- Adding new catalog types
- Performance tuning
- Testing guidelines

## 13. Open Questions

1. **ORC/Avro Support**: Should we support ORC and Avro formats in Phase 1 or defer?
   - **Recommendation**: Defer to Phase 4, focus on Parquet (most common)

2. **Delete Files**: How to handle Iceberg delete files for CDC-like behavior?
   - **Recommendation**: Implement in Phase 3 as part of incremental reads

3. **Time Travel**: Should we support arbitrary timestamp-based queries?
   - **Recommendation**: Yes, add in Phase 1 as it's a key Iceberg feature

4. **Catalog Caching**: Should we cache catalog metadata?
   - **Recommendation**: Yes, implement simple LRU cache in Phase 2

5. **Multi-Table Support**: Should we support reading multiple tables in one source?
   - **Recommendation**: Defer to future, keep single-table for now

## 14. Success Criteria

- ✅ Successfully read Iceberg tables from Nessie catalog
- ✅ Achieve >1M records/sec throughput on typical tables
- ✅ Support partition pruning with significant performance improvement
- ✅ Handle schema evolution gracefully
- ✅ Maintain <100 bytes memory per record
- ✅ Support incremental reads via snapshots
- ✅ Pass all unit and integration tests
- ✅ Complete documentation and examples

## 15. References

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Iceberg Table Format](https://iceberg.apache.org/docs/latest/spec/)
- [iceberg-go Library](https://github.com/apache/iceberg-go)
- [Apache Arrow Go](https://github.com/apache/arrow-go)
- [Nessie Documentation](https://projectnessie.org/)
