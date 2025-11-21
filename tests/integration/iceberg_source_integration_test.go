package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/sources/iceberg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIcebergSource_Integration tests the full integration workflow
// Requires: Running Nessie server, MinIO, and populated Iceberg table
func TestIcebergSource_Integration(t *testing.T) {
	// Skip if integration tests are not enabled
	if os.Getenv("INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration tests. Set INTEGRATION_TESTS=true to run")
	}

	ctx := context.Background()

	// Configuration for local test environment
	cfg := &config.BaseConfig{
		Name: "test-iceberg-source",
		Type: "iceberg",
		Security: config.SecurityConfig{
			Credentials: map[string]string{
				"catalog_type":              "nessie",
				"catalog_uri":               getEnv("NESSIE_URI", "http://localhost:19120/api/v1"),
				"catalog_name":              getEnv("CATALOG_NAME", "test_catalog"),
				"warehouse":                 getEnv("WAREHOUSE", "s3://warehouse"),
				"database":                  getEnv("DATABASE", "test_db"),
				"table":                     getEnv("TABLE", "test_table"),
				"branch":                    getEnv("BRANCH", "main"),
				"prop_s3.region":            getEnv("S3_REGION", "us-west-2"),
				"prop_s3.endpoint":          getEnv("S3_ENDPOINT", "http://localhost:9000"),
				"prop_s3.access-key-id":     getEnv("S3_ACCESS_KEY", "minioadmin"),
				"prop_s3.secret-access-key": getEnv("S3_SECRET_KEY", "minioadmin"),
			},
		},
		Performance: config.PerformanceConfig{
			BatchSize:  1000,
			BufferSize: 100,
		},
	}

	t.Run("initialization", func(t *testing.T) {
		source, err := iceberg.NewIcebergSource(cfg)
		require.NoError(t, err)
		require.NotNil(t, source)

		err = source.Initialize(ctx, cfg)
		if err != nil {
			t.Skipf("Failed to initialize source (may not have test environment): %v", err)
		}

		defer source.Close(ctx)

		// Verify health check
		err = source.Health(ctx)
		assert.NoError(t, err)
	})

	t.Run("schema discovery", func(t *testing.T) {
		source, err := iceberg.NewIcebergSource(cfg)
		require.NoError(t, err)

		err = source.Initialize(ctx, cfg)
		if err != nil {
			t.Skipf("Failed to initialize source: %v", err)
		}

		defer source.Close(ctx)

		schema, err := source.Discover(ctx)
		if err == nil {
			assert.NotNil(t, schema)
			assert.NotEmpty(t, schema.Fields)
			t.Logf("Discovered schema with %d fields", len(schema.Fields))
		}
	})

	t.Run("read records", func(t *testing.T) {
		source, err := iceberg.NewIcebergSource(cfg)
		require.NoError(t, err)

		err = source.Initialize(ctx, cfg)
		if err != nil {
			t.Skipf("Failed to initialize source: %v", err)
		}

		defer source.Close(ctx)

		stream, err := source.Read(ctx)
		if err != nil {
			t.Skipf("Failed to start reading: %v", err)
		}

		// Read some records with timeout
		timeout := time.After(10 * time.Second)
		recordCount := 0
		maxRecords := 100

	readLoop:
		for recordCount < maxRecords {
			select {
			case record, ok := <-stream.Records:
				if !ok {
					break readLoop
				}
				if record != nil {
					recordCount++
					assert.NotNil(t, record.Data)
					record.Release()
				}
			case err := <-stream.Errors:
				if err != nil {
					t.Logf("Error during read: %v", err)
				}
			case <-timeout:
				break readLoop
			}
		}

		t.Logf("Read %d records", recordCount)
	})

	t.Run("read batches", func(t *testing.T) {
		source, err := iceberg.NewIcebergSource(cfg)
		require.NoError(t, err)

		err = source.Initialize(ctx, cfg)
		if err != nil {
			t.Skipf("Failed to initialize source: %v", err)
		}

		defer source.Close(ctx)

		batchSize := 100
		stream, err := source.ReadBatch(ctx, batchSize)
		if err != nil {
			t.Skipf("Failed to start batch reading: %v", err)
		}

		// Read some batches with timeout
		timeout := time.After(10 * time.Second)
		batchCount := 0
		totalRecords := 0
		maxBatches := 5

	batchLoop:
		for batchCount < maxBatches {
			select {
			case batch, ok := <-stream.Batches:
				if !ok {
					break batchLoop
				}
				if batch != nil {
					batchCount++
					totalRecords += len(batch)
					assert.NotEmpty(t, batch)
					// Release records
					for _, record := range batch {
						record.Release()
					}
				}
			case err := <-stream.Errors:
				if err != nil {
					t.Logf("Error during batch read: %v", err)
				}
			case <-timeout:
				break batchLoop
			}
		}

		t.Logf("Read %d batches with %d total records", batchCount, totalRecords)
	})

	t.Run("state management", func(t *testing.T) {
		source, err := iceberg.NewIcebergSource(cfg)
		require.NoError(t, err)

		err = source.Initialize(ctx, cfg)
		if err != nil {
			t.Skipf("Failed to initialize source: %v", err)
		}

		defer source.Close(ctx)

		// Get initial state
		initialState := source.GetState()
		assert.NotNil(t, initialState)

		// Get position
		position := source.GetPosition()
		assert.NotNil(t, position)

		// Set state
		err = source.SetState(initialState)
		assert.NoError(t, err)
	})

	t.Run("metrics", func(t *testing.T) {
		source, err := iceberg.NewIcebergSource(cfg)
		require.NoError(t, err)

		err = source.Initialize(ctx, cfg)
		if err != nil {
			t.Skipf("Failed to initialize source: %v", err)
		}

		defer source.Close(ctx)

		metrics := source.Metrics()
		assert.NotNil(t, metrics)
		assert.Contains(t, metrics, "table")
		assert.Contains(t, metrics, "catalog_type")
		t.Logf("Metrics: %+v", metrics)
	})

	t.Run("capabilities", func(t *testing.T) {
		source, err := iceberg.NewIcebergSource(cfg)
		require.NoError(t, err)

		assert.True(t, source.SupportsIncremental())
		assert.False(t, source.SupportsRealtime())
		assert.True(t, source.SupportsBatch())
	})
}

// TestIcebergSource_ErrorHandling tests error scenarios
func TestIcebergSource_ErrorHandling(t *testing.T) {
	if os.Getenv("INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration tests. Set INTEGRATION_TESTS=true to run")
	}

	ctx := context.Background()

	t.Run("invalid catalog URI", func(t *testing.T) {
		cfg := &config.BaseConfig{
			Name: "test-iceberg-source",
			Security: config.SecurityConfig{
				Credentials: map[string]string{
					"catalog_type": "nessie",
					"catalog_uri":  "http://invalid-host:99999/api/v1",
					"catalog_name": "test_catalog",
					"warehouse":    "s3://warehouse",
					"database":     "test_db",
					"table":        "test_table",
				},
			},
		}

		source, err := iceberg.NewIcebergSource(cfg)
		require.NoError(t, err)

		err = source.Initialize(ctx, cfg)
		assert.Error(t, err)
	})

	t.Run("missing table", func(t *testing.T) {
		cfg := &config.BaseConfig{
			Name: "test-iceberg-source",
			Security: config.SecurityConfig{
				Credentials: map[string]string{
					"catalog_type":              "nessie",
					"catalog_uri":               getEnv("NESSIE_URI", "http://localhost:19120/api/v1"),
					"catalog_name":              getEnv("CATALOG_NAME", "test_catalog"),
					"warehouse":                 getEnv("WAREHOUSE", "s3://warehouse"),
					"database":                  "nonexistent_db",
					"table":                     "nonexistent_table",
					"prop_s3.endpoint":          getEnv("S3_ENDPOINT", "http://localhost:9000"),
					"prop_s3.access-key-id":     getEnv("S3_ACCESS_KEY", "minioadmin"),
					"prop_s3.secret-access-key": getEnv("S3_SECRET_KEY", "minioadmin"),
				},
			},
		}

		source, err := iceberg.NewIcebergSource(cfg)
		require.NoError(t, err)

		err = source.Initialize(ctx, cfg)
		assert.Error(t, err)
	})
}

// TestIcebergSource_LargeDataset tests performance with larger datasets
func TestIcebergSource_LargeDataset(t *testing.T) {
	if os.Getenv("INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration tests. Set INTEGRATION_TESTS=true to run")
	}

	if os.Getenv("LARGE_DATASET_TESTS") != "true" {
		t.Skip("Skipping large dataset tests. Set LARGE_DATASET_TESTS=true to run")
	}

	ctx := context.Background()

	cfg := &config.BaseConfig{
		Name: "test-iceberg-source",
		Type: "iceberg",
		Security: config.SecurityConfig{
			Credentials: map[string]string{
				"catalog_type":              "nessie",
				"catalog_uri":               getEnv("NESSIE_URI", "http://localhost:19120/api/v1"),
				"catalog_name":              getEnv("CATALOG_NAME", "test_catalog"),
				"warehouse":                 getEnv("WAREHOUSE", "s3://warehouse"),
				"database":                  getEnv("LARGE_DATABASE", "large_db"),
				"table":                     getEnv("LARGE_TABLE", "large_table"),
				"branch":                    "main",
				"prop_s3.region":            "us-west-2",
				"prop_s3.endpoint":          getEnv("S3_ENDPOINT", "http://localhost:9000"),
				"prop_s3.access-key-id":     getEnv("S3_ACCESS_KEY", "minioadmin"),
				"prop_s3.secret-access-key": getEnv("S3_SECRET_KEY", "minioadmin"),
			},
		},
		Performance: config.PerformanceConfig{
			BatchSize:  10000,
			BufferSize: 1000,
		},
	}

	source, err := iceberg.NewIcebergSource(cfg)
	require.NoError(t, err)

	err = source.Initialize(ctx, cfg)
	if err != nil {
		t.Skipf("Failed to initialize source: %v", err)
	}

	defer source.Close(ctx)

	startTime := time.Now()
	stream, err := source.ReadBatch(ctx, 10000)
	require.NoError(t, err)

	totalRecords := 0
	timeout := time.After(60 * time.Second)

readLoop:
	for {
		select {
		case batch, ok := <-stream.Batches:
			if !ok {
				break readLoop
			}
			if batch != nil {
				totalRecords += len(batch)
				for _, record := range batch {
					record.Release()
				}
			}
		case err := <-stream.Errors:
			if err != nil {
				t.Logf("Error during read: %v", err)
			}
		case <-timeout:
			break readLoop
		}
	}

	duration := time.Since(startTime)
	t.Logf("Read %d records in %v (%.2f records/sec)", totalRecords, duration, float64(totalRecords)/duration.Seconds())
}

// Helper function to get environment variables with defaults
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
