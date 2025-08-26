// Package integration provides integration tests for the optimized Snowflake connector
package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/destinations"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// SnowflakeOptimizedTestSuite tests the optimized Snowflake connector
type SnowflakeOptimizedTestSuite struct {
	suite.Suite
	connector *destinations.SnowflakeOptimizedDestination
	config    *config.BaseConfig
	ctx       context.Context
	cancel    context.CancelFunc
}

func (suite *SnowflakeOptimizedTestSuite) SetupSuite() {
	// Skip if no Snowflake credentials
	if os.Getenv("SNOWFLAKE_ACCOUNT") == "" {
		suite.T().Skip("Skipping Snowflake integration tests - no credentials provided")
	}

	suite.ctx, suite.cancel = context.WithTimeout(context.Background(), 5*time.Minute)

	// Create configuration
	suite.config = config.NewBaseConfig("test_optimized", "snowflake_optimized")
	// Set Snowflake specific configurations in the security section
	suite.config.Security.Credentials = map[string]string{
		"account":   os.Getenv("SNOWFLAKE_ACCOUNT"),
		"user":      os.Getenv("SNOWFLAKE_USER"),
		"password":  os.Getenv("SNOWFLAKE_PASSWORD"),
		"database":  os.Getenv("SNOWFLAKE_DATABASE"),
		"schema":    os.Getenv("SNOWFLAKE_SCHEMA"),
		"warehouse": os.Getenv("SNOWFLAKE_WAREHOUSE"),
	}
	// Set performance configuration
	suite.config.Performance.BatchSize = 10000
	suite.config.Performance.Workers = 8

	// Create optimized connector
	conn, err := destinations.NewSnowflakeOptimizedDestination("test_optimized", suite.config)
	require.NoError(suite.T(), err)

	var ok bool
	suite.connector, ok = conn.(*destinations.SnowflakeOptimizedDestination)
	require.True(suite.T(), ok)

	// Initialize connector
	err = suite.connector.Initialize(suite.ctx, suite.config)
	require.NoError(suite.T(), err)
}

func (suite *SnowflakeOptimizedTestSuite) TearDownSuite() {
	if suite.connector != nil {
		suite.connector.Close()
	}
	if suite.cancel != nil {
		suite.cancel()
	}
}

func (suite *SnowflakeOptimizedTestSuite) TestHighThroughputWrite() {
	// Create schema
	schema := &core.Schema{
		Name: "test_schema",
		Fields: []core.Field{
			{Name: "id", Type: core.FieldTypeInt},
			{Name: "name", Type: core.FieldTypeString},
			{Name: "amount", Type: core.FieldTypeFloat},
			{Name: "created_at", Type: core.FieldTypeTimestamp},
			{Name: "metadata", Type: core.FieldTypeJSON},
		},
	}

	// Create schema in Snowflake
	err := suite.connector.CreateSchema(suite.ctx, schema)
	require.NoError(suite.T(), err)

	// Create record stream
	stream := core.NewRecordStream()
	recordCount := 100000 // 100K records

	// Start record producer
	go func() {
		defer stream.Close()

		batch := make([]*pool.Record, 0, 1000)
		for i := 0; i < recordCount; i++ {
			record := pool.GetRecord()
			record.SetData("id", i)
			record.SetData("name", fmt.Sprintf("Record_%d", i))
			record.SetData("amount", float64(i)*1.23)
			record.SetData("created_at", time.Now())
			record.SetData("metadata", map[string]interface{}{"index": i, "type": "test"})
			batch = append(batch, record)

			// Send batch when full
			if len(batch) >= 1000 {
				stream.Records <- batch
				batch = make([]*pool.Record, 0, 1000)
			}
		}

		// Send remaining records
		if len(batch) > 0 {
			stream.Records <- batch
		}
	}()

	// Measure performance
	start := time.Now()

	// Write records
	err = suite.connector.Write(suite.ctx, stream)
	require.NoError(suite.T(), err)

	duration := time.Since(start)
	throughput := float64(recordCount) / duration.Seconds()

	// Get statistics
	stats := suite.connector.GetStats()

	suite.T().Logf("Performance Results:")
	suite.T().Logf("- Records written: %d", stats.RecordsWritten)
	suite.T().Logf("- Bytes uploaded: %d", stats.BytesUploaded)
	suite.T().Logf("- Files uploaded: %d", stats.FilesUploaded)
	suite.T().Logf("- COPY operations: %d", stats.CopyOperations)
	suite.T().Logf("- Duration: %v", duration)
	suite.T().Logf("- Throughput: %.0f records/sec", throughput)

	// Verify high throughput
	require.Greater(suite.T(), throughput, 50000.0, "Should achieve at least 50K records/sec")
}

func (suite *SnowflakeOptimizedTestSuite) TestParquetFormat() {
	// Update config for Parquet
	suite.config.Properties["file_format"] = "PARQUET"
	suite.config.Properties["table"] = "NEBULA_TEST_PARQUET"

	// Re-initialize connector
	err := suite.connector.Initialize(suite.ctx, suite.config)
	require.NoError(suite.T(), err)

	// Create schema
	schema := &core.Schema{
		Name: "parquet_schema",
		Fields: []core.Field{
			{Name: "id", Type: core.FieldTypeInt},
			{Name: "value", Type: core.FieldTypeFloat},
			{Name: "timestamp", Type: core.FieldTypeTimestamp},
		},
	}

	err = suite.connector.CreateSchema(suite.ctx, schema)
	require.NoError(suite.T(), err)

	// Create batch stream
	batchStream := core.NewBatchStream()

	// Send test batch
	go func() {
		defer batchStream.Close()

		records := make([]*pool.Record, 10000)
		for i := 0; i < 10000; i++ {
			record := pool.GetRecord()
			record.SetData("id", i)
			record.SetData("value", float64(i)*0.5)
			record.SetData("timestamp", time.Now())
			records[i] = record
		}

		batch := &pool.RecordBatch{
			Records: records,
		}

		batchStream.Batches <- batch
	}()

	// Write batch
	err = suite.connector.WriteBatch(suite.ctx, batchStream)
	require.NoError(suite.T(), err)

	stats := suite.connector.GetStats()
	suite.T().Logf("Parquet write - Records: %d, Bytes: %d",
		stats.RecordsWritten, stats.BytesUploaded)
}

func (suite *SnowflakeOptimizedTestSuite) TestExternalStageS3() {
	// Skip if no S3 configuration
	if os.Getenv("SNOWFLAKE_S3_STAGE_URL") == "" {
		suite.T().Skip("Skipping S3 external stage test - no S3 URL provided")
	}

	// Update config for S3 external stage
	suite.config.Properties["use_external_stage"] = true
	suite.config.Properties["external_stage_type"] = "S3"
	suite.config.Properties["external_stage_url"] = os.Getenv("SNOWFLAKE_S3_STAGE_URL")
	suite.config.Properties["table"] = "NEBULA_TEST_S3"

	// Re-initialize connector
	err := suite.connector.Initialize(suite.ctx, suite.config)
	require.NoError(suite.T(), err)

	// Create simple test schema
	schema := &core.Schema{
		Name: "s3_test_schema",
		Fields: []core.Field{
			{Name: "id", Type: core.FieldTypeInt},
			{Name: "message", Type: core.FieldTypeString},
		},
	}

	err = suite.connector.CreateSchema(suite.ctx, schema)
	require.NoError(suite.T(), err)

	// Write test records
	stream := core.NewRecordStream()

	go func() {
		defer stream.Close()

		records := make([]*pool.Record, 1000)
		for i := 0; i < 1000; i++ {
			record := pool.GetRecord()
			record.SetData("id", i)
			record.SetData("message", fmt.Sprintf("S3 test message %d", i))
			records[i] = record
		}

		stream.Records <- records
	}()

	err = suite.connector.Write(suite.ctx, stream)
	require.NoError(suite.T(), err)

	stats := suite.connector.GetStats()
	suite.T().Logf("S3 external stage - Records: %d, Files: %d",
		stats.RecordsWritten, stats.FilesUploaded)
}

func (suite *SnowflakeOptimizedTestSuite) TestMicroBatching() {
	// Test micro-batching effectiveness
	recordCounts := []int{100, 1000, 10000, 50000}

	for _, count := range recordCounts {
		suite.Run(fmt.Sprintf("Records_%d", count), func() {
			// Create stream
			stream := core.NewRecordStream()

			// Send records one by one to test micro-batching
			go func() {
				defer stream.Close()

				for i := 0; i < count; i++ {
					record := pool.GetRecord()
					record.SetData("id", i)
					record.SetData("value", i*2)

					// Send single record
					stream.Records <- []*pool.Record{record}

					// Small delay to simulate streaming
					if i%100 == 0 {
						time.Sleep(10 * time.Millisecond)
					}
				}
			}()

			start := time.Now()
			err := suite.connector.Write(suite.ctx, stream)
			require.NoError(suite.T(), err)
			duration := time.Since(start)

			stats := suite.connector.GetStats()
			suite.T().Logf("Micro-batching %d records: Duration=%v, Files=%d, Batches=%d",
				count, duration, stats.FilesUploaded, stats.CopyOperations)
		})
	}
}

func TestSnowflakeOptimizedIntegration(t *testing.T) {
	suite.Run(t, new(SnowflakeOptimizedTestSuite))
}

// BenchmarkSnowflakeOptimizedThroughput benchmarks real Snowflake throughput
func BenchmarkSnowflakeOptimizedThroughput(b *testing.B) {
	if os.Getenv("SNOWFLAKE_ACCOUNT") == "" {
		b.Skip("Skipping Snowflake benchmark - no credentials provided")
	}

	config := config.NewBaseConfig("benchmark", "snowflake_optimized")
	config.Security.Credentials = map[string]string{
		"account":   os.Getenv("SNOWFLAKE_ACCOUNT"),
		"user":      os.Getenv("SNOWFLAKE_USER"),
		"password":  os.Getenv("SNOWFLAKE_PASSWORD"),
		"database":  os.Getenv("SNOWFLAKE_DATABASE"),
		"schema":    os.Getenv("SNOWFLAKE_SCHEMA"),
		"warehouse": os.Getenv("SNOWFLAKE_WAREHOUSE"),
	}
	config.Performance.BatchSize = 200000
	config.Performance.Workers = 16

	ctx := context.Background()

	conn, err := destinations.NewSnowflakeOptimizedDestination("benchmark", config)
	require.NoError(b, err)

	dest := conn.(*destinations.SnowflakeOptimizedDestination)
	err = dest.Initialize(ctx, config)
	require.NoError(b, err)
	defer dest.Close()

	// Prepare test data
	recordCount := 1000000 // 1M records

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		stream := core.NewRecordStream()

		// Producer goroutine
		go func() {
			defer stream.Close()

			batchSize := 10000
			for j := 0; j < recordCount; j += batchSize {
				batch := make([]*pool.Record, batchSize)
				for k := 0; k < batchSize; k++ {
					idx := j + k
					record := pool.GetRecord()
					record.SetData("id", idx)
					record.SetData("name", fmt.Sprintf("Record_%d", idx))
					record.SetData("amount", float64(idx)*1.23)
					record.SetData("timestamp", time.Now())
					batch[k] = record
				}
				stream.Records <- batch
			}
		}()

		// Write records
		start := time.Now()
		err := dest.Write(ctx, stream)
		require.NoError(b, err)
		duration := time.Since(start)

		// Report metrics
		throughput := float64(recordCount) / duration.Seconds()
		b.ReportMetric(throughput, "records/sec")
		b.ReportMetric(float64(dest.GetStats().BytesUploaded)/duration.Seconds()/(1024*1024), "MB/sec")
		b.ReportMetric(float64(dest.GetStats().FilesUploaded), "files")
		b.ReportMetric(float64(dest.GetStats().CopyOperations), "copy_ops")
	}
}
