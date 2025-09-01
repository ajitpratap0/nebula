package sdk

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
	"go.uber.org/zap"
)

// TestSuite provides testing utilities for connectors
type TestSuite struct {
	t              *testing.T
	logger         *zap.Logger
	timeout        time.Duration
	expectedErrors map[string]bool
}

// NewTestSuite creates a new test suite
func NewTestSuite(t *testing.T) *TestSuite {
	logger, _ := zap.NewDevelopment()
	return &TestSuite{
		t:              t,
		logger:         logger,
		timeout:        30 * time.Second,
		expectedErrors: make(map[string]bool),
	}
}

// WithTimeout sets the test timeout
func (ts *TestSuite) WithTimeout(timeout time.Duration) *TestSuite {
	ts.timeout = timeout
	return ts
}

// ExpectError marks an error as expected (for negative testing)
func (ts *TestSuite) ExpectError(errorMessage string) *TestSuite {
	ts.expectedErrors[errorMessage] = true
	return ts
}

// TestSourceConnector tests a source connector comprehensively
func (ts *TestSuite) TestSourceConnector(source core.Source, config *config.BaseConfig) {
	ctx, cancel := context.WithTimeout(context.Background(), ts.timeout)
	defer cancel()

	ts.t.Run("Initialize", func(t *testing.T) {
		err := source.Initialize(ctx, config)
		if err != nil {
			if !ts.isExpectedError(err.Error()) {
				t.Fatalf("Initialize failed: %v", err)
			}
		}
	})

	ts.t.Run("Health", func(t *testing.T) {
		err := source.Health(ctx)
		if err != nil {
			if !ts.isExpectedError(err.Error()) {
				t.Fatalf("Health check failed: %v", err)
			}
		}
	})

	ts.t.Run("Discover", func(t *testing.T) {
		schema, err := source.Discover(ctx)
		if err != nil {
			if !ts.isExpectedError(err.Error()) {
				t.Fatalf("Schema discovery failed: %v", err)
			}
		} else {
			ts.validateSchema(schema)
		}
	})

	ts.t.Run("Capabilities", func(t *testing.T) {
		ts.testSourceCapabilities(source)
	})

	ts.t.Run("Read", func(t *testing.T) {
		if source.SupportsBatch() {
			ts.testSourceRead(ctx, source)
		}
	})

	ts.t.Run("ReadBatch", func(t *testing.T) {
		if source.SupportsBatch() {
			ts.testSourceReadBatch(ctx, source)
		}
	})

	ts.t.Run("StateManagement", func(t *testing.T) {
		if source.SupportsIncremental() {
			ts.testSourceStateManagement(source)
		}
	})

	ts.t.Run("Close", func(t *testing.T) {
		err := source.Close(ctx)
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	})
}

// TestDestinationConnector tests a destination connector comprehensively
func (ts *TestSuite) TestDestinationConnector(destination core.Destination, config *config.BaseConfig) {
	ctx, cancel := context.WithTimeout(context.Background(), ts.timeout)
	defer cancel()

	ts.t.Run("Initialize", func(t *testing.T) {
		err := destination.Initialize(ctx, config)
		if err != nil {
			if !ts.isExpectedError(err.Error()) {
				t.Fatalf("Initialize failed: %v", err)
			}
		}
	})

	ts.t.Run("Health", func(t *testing.T) {
		err := destination.Health(ctx)
		if err != nil {
			if !ts.isExpectedError(err.Error()) {
				t.Fatalf("Health check failed: %v", err)
			}
		}
	})

	ts.t.Run("Capabilities", func(t *testing.T) {
		ts.testDestinationCapabilities(destination)
	})

	ts.t.Run("SchemaOperations", func(t *testing.T) {
		ts.testDestinationSchemaOperations(ctx, destination)
	})

	ts.t.Run("Write", func(t *testing.T) {
		if destination.SupportsStreaming() {
			ts.testDestinationWrite(ctx, destination)
		}
	})

	ts.t.Run("WriteBatch", func(t *testing.T) {
		if destination.SupportsBatch() {
			ts.testDestinationWriteBatch(ctx, destination)
		}
	})

	ts.t.Run("BulkLoad", func(t *testing.T) {
		if destination.SupportsBulkLoad() {
			ts.testDestinationBulkLoad(ctx, destination)
		}
	})

	ts.t.Run("Transactions", func(t *testing.T) {
		if destination.SupportsTransactions() {
			ts.testDestinationTransactions(ctx, destination)
		}
	})

	ts.t.Run("Upsert", func(t *testing.T) {
		if destination.SupportsUpsert() {
			ts.testDestinationUpsert(ctx, destination)
		}
	})

	ts.t.Run("Close", func(t *testing.T) {
		err := destination.Close(ctx)
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	})
}

// testSourceCapabilities tests source capabilities
func (ts *TestSuite) testSourceCapabilities(source core.Source) {
	capabilities := []struct {
		name  string
		check func() bool
	}{
		{"Incremental", source.SupportsIncremental},
		{"Realtime", source.SupportsRealtime},
		{"Batch", source.SupportsBatch},
	}

	for _, cap := range capabilities {
		ts.logger.Info("Testing capability",
			zap.String("capability", cap.name),
			zap.Bool("supported", cap.check()))
	}
}

// testDestinationCapabilities tests destination capabilities
func (ts *TestSuite) testDestinationCapabilities(destination core.Destination) {
	capabilities := []struct {
		name  string
		check func() bool
	}{
		{"BulkLoad", destination.SupportsBulkLoad},
		{"Transactions", destination.SupportsTransactions},
		{"Upsert", destination.SupportsUpsert},
		{"Batch", destination.SupportsBatch},
		{"Streaming", destination.SupportsStreaming},
	}

	for _, cap := range capabilities {
		ts.logger.Info("Testing capability",
			zap.String("capability", cap.name),
			zap.Bool("supported", cap.check()))
	}
}

// testSourceRead tests source reading
func (ts *TestSuite) testSourceRead(ctx context.Context, source core.Source) {
	stream, err := source.Read(ctx)
	if err != nil {
		if !ts.isExpectedError(err.Error()) {
			ts.t.Fatalf("Read failed: %v", err)
		}
		return
	}

	recordCount := 0
	timeout := time.After(5 * time.Second)

	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				ts.logger.Info("Read test completed", zap.Int("records", recordCount))
				return
			}
			ts.validateRecord(record)
			recordCount++
			if recordCount >= 10 { // Test first 10 records
				return
			}
		case err := <-stream.Errors:
			if err != nil && !ts.isExpectedError(err.Error()) {
				ts.t.Fatalf("Read error: %v", err)
			}
		case <-timeout:
			ts.logger.Info("Read test timeout", zap.Int("records", recordCount))
			return
		case <-ctx.Done():
			return
		}
	}
}

// testSourceReadBatch tests source batch reading
func (ts *TestSuite) testSourceReadBatch(ctx context.Context, source core.Source) {
	stream, err := source.ReadBatch(ctx, 5)
	if err != nil {
		if !ts.isExpectedError(err.Error()) {
			ts.t.Fatalf("ReadBatch failed: %v", err)
		}
		return
	}

	batchCount := 0
	recordCount := 0
	timeout := time.After(5 * time.Second)

	for {
		select {
		case batch, ok := <-stream.Batches:
			if !ok {
				ts.logger.Info("ReadBatch test completed",
					zap.Int("batches", batchCount),
					zap.Int("records", recordCount))
				return
			}
			for _, record := range batch {
				ts.validateRecord(record)
				recordCount++
			}
			batchCount++
			if batchCount >= 3 { // Test first 3 batches
				return
			}
		case err := <-stream.Errors:
			if err != nil && !ts.isExpectedError(err.Error()) {
				ts.t.Fatalf("ReadBatch error: %v", err)
			}
		case <-timeout:
			ts.logger.Info("ReadBatch test timeout",
				zap.Int("batches", batchCount),
				zap.Int("records", recordCount))
			return
		case <-ctx.Done():
			return
		}
	}
}

// testSourceStateManagement tests source state management
func (ts *TestSuite) testSourceStateManagement(source core.Source) {
	// Test state operations
	originalState := source.GetState()
	testState := core.State{"test": "value"}

	err := source.SetState(testState)
	if err != nil {
		ts.t.Fatalf("SetState failed: %v", err)
	}

	retrievedState := source.GetState()
	if retrievedState["test"] != "value" {
		ts.t.Fatalf("State not persisted correctly")
	}

	// Restore original state
	if err := source.SetState(originalState); err != nil {
		ts.logger.Error("Failed to restore original state", zap.Error(err))
	}

	ts.logger.Info("State management test completed")
}

// testDestinationSchemaOperations tests destination schema operations
func (ts *TestSuite) testDestinationSchemaOperations(ctx context.Context, destination core.Destination) {
	testSchema := &core.Schema{
		Name:    "test_schema",
		Version: 1,
		Fields: []core.Field{
			{
				Name: "id",
				Type: core.FieldTypeString,
			},
			{
				Name: "value",
				Type: core.FieldTypeInt,
			},
		},
	}

	err := destination.CreateSchema(ctx, testSchema)
	if err != nil && !ts.isExpectedError(err.Error()) {
		ts.t.Fatalf("CreateSchema failed: %v", err)
	}
}

// testDestinationWrite tests destination writing
func (ts *TestSuite) testDestinationWrite(ctx context.Context, destination core.Destination) {
	records := make(chan *models.Record, 5)
	errors := make(chan error, 1)

	// Create test records
	for i := 0; i < 5; i++ {
		record := &models.Record{
			ID: fmt.Sprintf("test_%d", i),
			Data: map[string]interface{}{
				"id":    fmt.Sprintf("test_%d", i),
				"value": i,
			},
			Metadata: models.RecordMetadata{
				Source:    "test",
				Timestamp: time.Now(),
			},
		}
		records <- record
	}
	close(records)

	stream := &core.RecordStream{
		Records: records,
		Errors:  errors,
	}

	err := destination.Write(ctx, stream)
	if err != nil && !ts.isExpectedError(err.Error()) {
		ts.t.Fatalf("Write failed: %v", err)
	}
}

// testDestinationWriteBatch tests destination batch writing
func (ts *TestSuite) testDestinationWriteBatch(ctx context.Context, destination core.Destination) {
	batches := make(chan []*models.Record, 2)
	errors := make(chan error, 1)

	// Create test batches
	for i := 0; i < 2; i++ {
		batch := make([]*models.Record, 3)
		for j := 0; j < 3; j++ {
			batch[j] = &models.Record{
				ID: fmt.Sprintf("batch_%d_record_%d", i, j),
				Data: map[string]interface{}{
					"id":    fmt.Sprintf("batch_%d_record_%d", i, j),
					"value": i*10 + j,
				},
				Metadata: models.RecordMetadata{
					Source:    "test",
					Timestamp: time.Now(),
				},
			}
		}
		batches <- batch
	}
	close(batches)

	stream := &core.BatchStream{
		Batches: batches,
		Errors:  errors,
	}

	err := destination.WriteBatch(ctx, stream)
	if err != nil && !ts.isExpectedError(err.Error()) {
		ts.t.Fatalf("WriteBatch failed: %v", err)
	}
}

// testDestinationBulkLoad tests destination bulk loading
func (ts *TestSuite) testDestinationBulkLoad(ctx context.Context, destination core.Destination) {
	// Mock bulk data
	testData := "test bulk data"

	err := destination.BulkLoad(ctx, testData, "text")
	if err != nil && !ts.isExpectedError(err.Error()) {
		ts.t.Fatalf("BulkLoad failed: %v", err)
	}
}

// testDestinationTransactions tests destination transactions
func (ts *TestSuite) testDestinationTransactions(ctx context.Context, destination core.Destination) {
	txn, err := destination.BeginTransaction(ctx)
	if err != nil {
		if !ts.isExpectedError(err.Error()) {
			ts.t.Fatalf("BeginTransaction failed: %v", err)
		}
		return
	}

	// Test commit
	err = txn.Commit(ctx)
	if err != nil {
		ts.t.Fatalf("Transaction commit failed: %v", err)
	}
}

// testDestinationUpsert tests destination upsert
func (ts *TestSuite) testDestinationUpsert(ctx context.Context, destination core.Destination) {
	records := []*models.Record{
		{
			ID: "upsert_test",
			Data: map[string]interface{}{
				"id":    "upsert_test",
				"value": 42,
			},
			Metadata: models.RecordMetadata{
				Source:    "test",
				Timestamp: time.Now(),
			},
		},
	}

	keys := []string{"id"}

	err := destination.Upsert(ctx, records, keys)
	if err != nil && !ts.isExpectedError(err.Error()) {
		ts.t.Fatalf("Upsert failed: %v", err)
	}
}

// validateSchema validates a schema
func (ts *TestSuite) validateSchema(schema *core.Schema) {
	if schema == nil {
		ts.t.Fatal("Schema is nil")
	}

	if schema.Name == "" {
		ts.t.Fatal("Schema name is empty")
	}

	if len(schema.Fields) == 0 {
		ts.t.Fatal("Schema has no fields")
	}

	for _, field := range schema.Fields {
		if field.Name == "" {
			ts.t.Fatal("Field name is empty")
		}
		if field.Type == "" {
			ts.t.Fatal("Field type is empty")
		}
	}
}

// validateRecord validates a record
func (ts *TestSuite) validateRecord(record *models.Record) {
	if record == nil {
		ts.t.Fatal("Record is nil")
	}

	if record.Metadata.Source == "" {
		ts.t.Fatal("Record source is empty")
	}

	if record.Data == nil {
		ts.t.Fatal("Record data is nil")
	}

	if record.Metadata.Timestamp.IsZero() {
		ts.t.Fatal("Record timestamp is zero")
	}
}

// isExpectedError checks if an error is expected
func (ts *TestSuite) isExpectedError(errorMessage string) bool {
	return ts.expectedErrors[errorMessage]
}

// BenchmarkSuite provides benchmarking utilities for connectors
type BenchmarkSuite struct {
	b           *testing.B
	logger      *zap.Logger
	recordCount int
	batchSize   int
}

// NewBenchmarkSuite creates a new benchmark suite
func NewBenchmarkSuite(b *testing.B) *BenchmarkSuite {
	logger, _ := zap.NewDevelopment()
	return &BenchmarkSuite{
		b:           b,
		logger:      logger,
		recordCount: 10000,
		batchSize:   1000,
	}
}

// WithRecordCount sets the number of records for benchmarks
func (bs *BenchmarkSuite) WithRecordCount(count int) *BenchmarkSuite {
	bs.recordCount = count
	return bs
}

// WithBatchSize sets the batch size for benchmarks
func (bs *BenchmarkSuite) WithBatchSize(size int) *BenchmarkSuite {
	bs.batchSize = size
	return bs
}

// BenchmarkSourceThroughput benchmarks source throughput
func (bs *BenchmarkSuite) BenchmarkSourceThroughput(source core.Source, config *config.BaseConfig) {
	ctx := context.Background()

	// Initialize source
	if err := source.Initialize(ctx, config); err != nil {
		bs.b.Fatal(err)
	}
	defer source.Close(ctx) //nolint:errcheck // Benchmark cleanup, error not critical

	bs.b.ResetTimer()
	bs.b.ReportAllocs()

	for i := 0; i < bs.b.N; i++ {
		stream, err := source.Read(ctx)
		if err != nil {
			bs.b.Fatal(err)
		}

		recordCount := 0
		for record := range stream.Records {
			_ = record // Just consume the record
			recordCount++
			if recordCount >= bs.recordCount {
				break
			}
		}
	}
}

// BenchmarkDestinationThroughput benchmarks destination throughput
func (bs *BenchmarkSuite) BenchmarkDestinationThroughput(destination core.Destination, config *config.BaseConfig) {
	ctx := context.Background()

	// Initialize destination
	if err := destination.Initialize(ctx, config); err != nil {
		bs.b.Fatal(err)
	}
	defer destination.Close(ctx) //nolint:errcheck // Benchmark cleanup, error not critical

	// Create test records
	records := make(chan *models.Record, bs.recordCount)
	for i := 0; i < bs.recordCount; i++ {
		record := &models.Record{
			ID: fmt.Sprintf("bench_%d", i),
			Data: map[string]interface{}{
				"id":    fmt.Sprintf("bench_%d", i),
				"value": i,
			},
			Metadata: models.RecordMetadata{
				Source:    "benchmark",
				Timestamp: time.Now(),
			},
		}
		records <- record
	}
	close(records)

	stream := &core.RecordStream{
		Records: records,
		Errors:  make(chan error, 1),
	}

	bs.b.ResetTimer()
	bs.b.ReportAllocs()

	for i := 0; i < bs.b.N; i++ {
		if err := destination.Write(ctx, stream); err != nil {
			bs.b.Fatal(err)
		}
	}
}
