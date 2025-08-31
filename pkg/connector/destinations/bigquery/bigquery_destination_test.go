package bigquery

import (
	"context"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/base"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBigQueryDestination_NewBigQueryDestination(t *testing.T) {
	config := &config.BaseConfig{
		Name: "test-bigquery",
		Type: "destination",
		Performance: config.PerformanceConfig{
			BatchSize:       10000,
			MaxConcurrency:  10,
			EnableStreaming: true,
		},
	}

	dest, err := NewBigQueryDestination("test", config)
	require.NoError(t, err)
	assert.NotNil(t, dest)

	bqDest := dest.(*BigQueryDestination)
	assert.Equal(t, 10000, bqDest.batchSize)
	assert.Equal(t, 5*time.Second, bqDest.batchTimeout)
	assert.True(t, bqDest.enableStreaming)
}

func TestBigQueryDestination_ValidateConfig(t *testing.T) {
	tests := []struct {
		name      string
		config    *config.BaseConfig
		wantError bool
	}{
		{
			name: "valid config",
			config: &config.BaseConfig{
				Name: "test-bigquery",
			},
			wantError: false,
		},
		{
			name: "missing name",
			config: &config.BaseConfig{
				Name: "",
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := &BigQueryDestination{}
			err := dest.validateConfig(tt.config)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBigQueryDestination_ExtractConfig(t *testing.T) {
	config := &config.BaseConfig{
		Name: "test-bigquery",
		Performance: config.PerformanceConfig{
			BatchSize:       5000,
			Workers:         8,
			MaxConcurrency:  20,
			EnableStreaming: false,
		},
	}

	dest := &BigQueryDestination{
		microBatch: pool.GetBatchSlice(10000),
	}
	dest.extractBigQueryConfig(config)

	// Test simplified config extraction
	assert.Equal(t, "default-project", dest.projectID)
	assert.Equal(t, "default-dataset", dest.datasetID)
	assert.Equal(t, "default-table", dest.tableID)
	assert.Equal(t, "US", dest.location)
	assert.Equal(t, 5000, dest.batchSize)
	assert.Equal(t, 8, dest.workerCount)
	assert.Equal(t, 20, dest.maxConcurrentBatches)
	assert.False(t, dest.enableStreaming)
}

func TestBigQueryDestination_ConvertFieldTypes(t *testing.T) {
	dest := &BigQueryDestination{}

	tests := []struct {
		coreType core.FieldType
		bqType   string
	}{
		{core.FieldTypeString, "STRING"},
		{core.FieldTypeInt, "INTEGER"},
		{core.FieldTypeFloat, "FLOAT"},
		{core.FieldTypeBool, "BOOLEAN"},
		{core.FieldTypeTimestamp, "TIMESTAMP"},
		{core.FieldTypeDate, "DATE"},
		{core.FieldTypeTime, "TIME"},
		{core.FieldTypeBinary, "BYTES"},
		{core.FieldTypeJSON, "JSON"},
		{"unknown", "STRING"}, // Default case
	}

	for _, tt := range tests {
		t.Run(string(tt.coreType), func(t *testing.T) {
			bqType := dest.mapFieldTypeToBigQuery(tt.coreType)
			assert.Equal(t, tt.bqType, string(bqType))
		})
	}
}

func TestBigQueryDestination_Capabilities(t *testing.T) {
	dest := &BigQueryDestination{
		enableStreaming: true,
	}

	assert.True(t, dest.SupportsBulkLoad())
	assert.False(t, dest.SupportsTransactions())
	assert.True(t, dest.SupportsUpsert())
	assert.True(t, dest.SupportsBatch())
	assert.True(t, dest.SupportsStreaming())

	dest.enableStreaming = false
	assert.False(t, dest.SupportsStreaming())
}

func TestBigQueryDestination_GetStats(t *testing.T) {
	dest := &BigQueryDestination{
		stats:           &BigQueryStats{},
		enableStreaming: true,
	}

	// Set some values
	dest.recordsWritten = 1000
	dest.bytesWritten = 50000
	dest.batchesProcessed = 10
	dest.errorsEncountered = 2

	stats := dest.GetStats()
	assert.Equal(t, int64(1000), stats.RecordsWritten)
	assert.Equal(t, int64(50000), stats.BytesWritten)
	assert.Equal(t, int64(10), stats.BatchesProcessed)
	assert.Equal(t, int64(2), stats.ErrorsEncountered)
	assert.True(t, stats.StreamingEnabled)
}

func TestBigQueryDestination_MicroBatching(t *testing.T) {
	ctx := context.Background()
	
	// Create base connector and initialize it
	baseConn := base.NewBaseConnector("test-bigquery", core.ConnectorTypeDestination, "1.0.0")
	testConfig := &config.BaseConfig{
		Name: "test-bigquery",
		Reliability: config.ReliabilityConfig{
			RetryAttempts: 3,
			RetryDelay:   1,
		},
	}
	err := baseConn.Initialize(ctx, testConfig)
	require.NoError(t, err)
	
	dest := &BigQueryDestination{
		BaseConnector: baseConn,
		batchSize:    3,
		batchTimeout: 100 * time.Millisecond,
		microBatch:   pool.GetBatchSlice(3),
		batchChan:    make(chan *RecordBatch, 10),
	}

	// Add records that don't fill the batch
	for i := 0; i < 2; i++ {
		record := pool.GetRecord()
		defer record.Release()
		record.ID = string(rune('a' + i))
		record.SetData("value", i)
		err := dest.addToMicroBatch(ctx, record)
		assert.NoError(t, err)
	}

	// Verify batch not flushed yet
	assert.Equal(t, 2, len(dest.microBatch))

	// Add one more to trigger flush
	record := pool.GetRecord()
	defer record.Release()
	record.ID = "c"
	record.SetData("value", 2)
	err = dest.addToMicroBatch(ctx, record)
	assert.NoError(t, err)

	// Verify batch was flushed
	assert.Equal(t, 0, len(dest.microBatch))

	// Check that batch was sent to channel
	select {
	case batch := <-dest.batchChan:
		assert.Equal(t, 3, len(batch.Records))
		assert.Equal(t, "a", batch.Records[0].ID)
		assert.Equal(t, "b", batch.Records[1].ID)
		assert.Equal(t, "c", batch.Records[2].ID)
	case <-time.After(1 * time.Second):
		t.Fatal("batch not received in channel")
	}
}

func TestBigQueryDestination_ProcessBatch(t *testing.T) {
	ctx := context.Background()
	dest := &BigQueryDestination{
		batchChan: make(chan *RecordBatch, 10),
	}

	// Create test batch
	batch := make([]*pool.Record, 3)
	for i := 0; i < 3; i++ {
		record := pool.GetRecord()
		defer record.Release()
		record.ID = string(rune('1' + i))
		record.SetData("value", i+1)
		batch[i] = record
	}

	// Process batch
	err := dest.processBatch(ctx, batch)
	assert.NoError(t, err)

	// Verify batch was sent
	select {
	case receivedBatch := <-dest.batchChan:
		assert.Equal(t, 3, len(receivedBatch.Records))
		assert.Equal(t, batch, receivedBatch.Records)
		assert.NotEmpty(t, receivedBatch.BatchID)
	case <-time.After(1 * time.Second):
		t.Fatal("batch not received in channel")
	}

	// Verify metrics updated
	assert.Equal(t, int64(3), dest.recordsWritten)
}
