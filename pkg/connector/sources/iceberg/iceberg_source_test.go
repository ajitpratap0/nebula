package iceberg

import (
	"context"
	"testing"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIcebergSource(t *testing.T) {
	t.Run("valid configuration", func(t *testing.T) {
		cfg := &config.BaseConfig{
			Name: "test-iceberg-source",
			Type: "iceberg",
			Performance: config.PerformanceConfig{
				BatchSize:  10000,
				BufferSize: 1000,
			},
		}

		source, err := NewIcebergSource(cfg)
		require.NoError(t, err)
		assert.NotNil(t, source)

		iceSource, ok := source.(*IcebergSource)
		require.True(t, ok)
		assert.Equal(t, cfg, iceSource.config)
		assert.NotNil(t, iceSource.logger)
		assert.NotNil(t, iceSource.properties)
		assert.NotNil(t, iceSource.position)
	})

	t.Run("nil configuration", func(t *testing.T) {
		source, err := NewIcebergSource(nil)
		assert.Error(t, err)
		assert.Nil(t, source)
		assert.Contains(t, err.Error(), "configuration cannot be nil")
	})
}

func TestIcebergSource_ParseConfig(t *testing.T) {
	tests := []struct {
		name      string
		config    *config.BaseConfig
		wantError bool
		errorMsg  string
	}{
		{
			name: "valid complete configuration",
			config: &config.BaseConfig{
				Name: "test-iceberg",
				Type: "iceberg",
				Security: config.SecurityConfig{
					Credentials: map[string]string{
						"catalog_type": "nessie",
						"catalog_uri":  "http://localhost:19120/api/v1",
						"catalog_name": "test_catalog",
						"warehouse":    "s3://test-warehouse",
						"database":     "test_db",
						"table":        "test_table",
						"branch":       "main",
						"region":       "us-west-2",
						"s3_endpoint":  "http://localhost:9000",
						"access_key":   "minioadmin",
						"secret_key":   "minioadmin",
					},
				},
				Performance: config.PerformanceConfig{
					BatchSize: 5000,
				},
			},
			wantError: false,
		},
		{
			name: "missing catalog_type",
			config: &config.BaseConfig{
				Security: config.SecurityConfig{
					Credentials: map[string]string{
						"catalog_uri":  "http://localhost:19120/api/v1",
						"catalog_name": "test_catalog",
						"warehouse":    "s3://test-warehouse",
						"database":     "test_db",
						"table":        "test_table",
					},
				},
			},
			wantError: true,
			errorMsg:  "missing required field: catalog_type",
		},
		{
			name: "missing database",
			config: &config.BaseConfig{
				Security: config.SecurityConfig{
					Credentials: map[string]string{
						"catalog_type": "nessie",
						"catalog_uri":  "http://localhost:19120/api/v1",
						"catalog_name": "test_catalog",
						"warehouse":    "s3://test-warehouse",
						"table":        "test_table",
					},
				},
			},
			wantError: true,
			errorMsg:  "missing required field: database",
		},
		{
			name: "missing table",
			config: &config.BaseConfig{
				Security: config.SecurityConfig{
					Credentials: map[string]string{
						"catalog_type": "nessie",
						"catalog_uri":  "http://localhost:19120/api/v1",
						"catalog_name": "test_catalog",
						"warehouse":    "s3://test-warehouse",
						"database":     "test_db",
					},
				},
			},
			wantError: true,
			errorMsg:  "missing required field: table",
		},
		{
			name: "mismatched S3 credentials - only access_key",
			config: &config.BaseConfig{
				Security: config.SecurityConfig{
					Credentials: map[string]string{
						"catalog_type": "nessie",
						"catalog_uri":  "http://localhost:19120/api/v1",
						"catalog_name": "test_catalog",
						"warehouse":    "s3://test-warehouse",
						"database":     "test_db",
						"table":        "test_table",
						"access_key":   "minioadmin",
					},
				},
			},
			wantError: true,
			errorMsg:  "both S3 access_key and secret_key must be provided together",
		},
		{
			name: "default branch when not specified",
			config: &config.BaseConfig{
				Security: config.SecurityConfig{
					Credentials: map[string]string{
						"catalog_type": "nessie",
						"catalog_uri":  "http://localhost:19120/api/v1",
						"catalog_name": "test_catalog",
						"warehouse":    "s3://test-warehouse",
						"database":     "test_db",
						"table":        "test_table",
					},
				},
			},
			wantError: false,
		},
		{
			name: "empty database name",
			config: &config.BaseConfig{
				Security: config.SecurityConfig{
					Credentials: map[string]string{
						"catalog_type": "nessie",
						"catalog_uri":  "http://localhost:19120/api/v1",
						"catalog_name": "test_catalog",
						"warehouse":    "s3://test-warehouse",
						"database":     "  ",
						"table":        "test_table",
					},
				},
			},
			wantError: true,
			errorMsg:  "database name cannot be empty",
		},
		{
			name: "properties with prop_ prefix",
			config: &config.BaseConfig{
				Security: config.SecurityConfig{
					Credentials: map[string]string{
						"catalog_type":              "nessie",
						"catalog_uri":               "http://localhost:19120/api/v1",
						"catalog_name":              "test_catalog",
						"warehouse":                 "s3://test-warehouse",
						"database":                  "test_db",
						"table":                     "test_table",
						"prop_s3.region":            "us-west-2",
						"prop_s3.path-style-access": "true",
						"prop_custom.property":      "value",
					},
				},
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &IcebergSource{
				properties: make(map[string]string),
			}
			err := source.parseConfig(tt.config)

			if tt.wantError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)

				// Verify default values
				if tt.config.Security.Credentials["branch"] == "" {
					assert.Equal(t, DefaultBranch, source.branch)
				}

				// Verify batch size
				if tt.config.Performance.BatchSize > 0 {
					assert.Equal(t, tt.config.Performance.BatchSize, source.readBatchSize)
				} else {
					assert.Equal(t, DefaultBatchSize, source.readBatchSize)
				}
			}
		})
	}
}

func TestIcebergSource_GetCredValue(t *testing.T) {
	tests := []struct {
		name     string
		creds    map[string]string
		keys     []string
		expected string
	}{
		{
			name: "first key found",
			creds: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			keys:     []string{"key1", "key2"},
			expected: "value1",
		},
		{
			name: "second key found",
			creds: map[string]string{
				"key2": "value2",
			},
			keys:     []string{"key1", "key2"},
			expected: "value2",
		},
		{
			name:     "no key found",
			creds:    map[string]string{},
			keys:     []string{"key1", "key2"},
			expected: "",
		},
		{
			name: "empty value",
			creds: map[string]string{
				"key1": "",
			},
			keys:     []string{"key1", "key2"},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getCredValue(tt.creds, tt.keys...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIcebergSource_CreateCatalogProvider(t *testing.T) {
	tests := []struct {
		name        string
		catalogType string
		wantError   bool
		errorMsg    string
	}{
		{
			name:        "nessie catalog",
			catalogType: "nessie",
			wantError:   false,
		},
		{
			name:        "unsupported catalog",
			catalogType: "unsupported",
			wantError:   true,
			errorMsg:    "unsupported catalog type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Need to create logger first
			cfg := &config.BaseConfig{Name: "test"}
			src, _ := NewIcebergSource(cfg)
			source, ok := src.(*IcebergSource)
			require.True(t, ok)
			source.catalogType = tt.catalogType

			provider, err := source.createCatalogProvider()

			if tt.wantError {
				assert.Error(t, err)
				assert.Nil(t, provider)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, provider)
			}
		})
	}
}

func TestIcebergSource_GetPosition(t *testing.T) {
	source := &IcebergSource{
		position: &IcebergPosition{
			DataFileIndex: 5,
			RowOffset:     100,
		},
		recordsRead: 1000,
		filesRead:   10,
	}

	position := source.GetPosition()
	assert.NotNil(t, position)

	icebergPos, ok := position.(*IcebergPosition)
	assert.True(t, ok)
	assert.Equal(t, 5, icebergPos.DataFileIndex)
	assert.Equal(t, int64(100), icebergPos.RowOffset)
	assert.Equal(t, int64(1000), icebergPos.Metadata["records_read"])
	assert.Equal(t, int64(10), icebergPos.Metadata["files_read"])
}

func TestIcebergSource_SetPosition(t *testing.T) {
	source := &IcebergSource{
		position: &IcebergPosition{},
	}

	newPos := &IcebergPosition{
		SnapshotID:    12345,
		ManifestIndex: 2,
		DataFileIndex: 5,
		RowOffset:     100,
	}

	err := source.SetPosition(newPos)
	assert.NoError(t, err)
	assert.Equal(t, newPos, source.position)

	// Test with invalid position type
	err = source.SetPosition(&mockPosition{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid position type")
}

func TestIcebergSource_GetState(t *testing.T) {
	source := &IcebergSource{
		position: &IcebergPosition{
			DataFileIndex: 5,
			RowOffset:     100,
		},
		recordsRead: 1000,
		filesRead:   10,
		bytesRead:   50000,
		manifestReader: &ManifestReader{
			currentIndex: 2,
		},
	}

	// Create a mock snapshot
	snapshotID := int64(12345)
	source.currentSnapshot = &table.Snapshot{
		SnapshotID: snapshotID,
	}

	state := source.GetState()
	assert.NotNil(t, state)
	assert.Equal(t, snapshotID, state["snapshot_id"])
	assert.Equal(t, 2, state["manifest_index"])
	assert.Equal(t, 5, state["data_file_index"])
	assert.Equal(t, int64(100), state["row_offset"])
	assert.Equal(t, int64(1000), state["records_read"])
	assert.Equal(t, int64(10), state["files_read"])
	assert.Equal(t, int64(50000), state["bytes_read"])
}

func TestIcebergSource_SetState(t *testing.T) {
	source := &IcebergSource{
		position: &IcebergPosition{
			Metadata: make(map[string]interface{}),
		},
		manifestReader: &ManifestReader{},
	}

	state := core.State{
		"snapshot_id":     int64(12345),
		"manifest_index":  3,
		"data_file_index": 7,
		"row_offset":      int64(200),
		"records_read":    int64(2000),
		"files_read":      int64(20),
		"bytes_read":      int64(100000),
	}

	err := source.SetState(state)
	assert.NoError(t, err)
	assert.Equal(t, int64(12345), source.position.SnapshotID)
	assert.Equal(t, 3, source.manifestReader.currentIndex)
	assert.Equal(t, 7, source.position.DataFileIndex)
	assert.Equal(t, int64(200), source.position.RowOffset)
	assert.Equal(t, int64(2000), source.recordsRead)
	assert.Equal(t, int64(20), source.filesRead)
	assert.Equal(t, int64(100000), source.bytesRead)
}

func TestIcebergSource_Capabilities(t *testing.T) {
	source := &IcebergSource{}

	assert.True(t, source.SupportsIncremental())
	assert.False(t, source.SupportsRealtime())
	assert.True(t, source.SupportsBatch())
}

func TestIcebergSource_Subscribe(t *testing.T) {
	source := &IcebergSource{}
	ctx := context.Background()

	stream, err := source.Subscribe(ctx, []string{"table1", "table2"})
	assert.Error(t, err)
	assert.Nil(t, stream)
	assert.Contains(t, err.Error(), "does not support real-time subscriptions")
}

func TestIcebergSource_Metrics(t *testing.T) {
	source := &IcebergSource{
		recordsRead: 5000,
		bytesRead:   250000,
		filesRead:   25,
		database:    "test_db",
		tableName:   "test_table",
		catalogType: "nessie",
	}

	metrics := source.Metrics()
	assert.NotNil(t, metrics)
	assert.Equal(t, int64(5000), metrics["records_read"])
	assert.Equal(t, int64(250000), metrics["bytes_read"])
	assert.Equal(t, int64(25), metrics["files_read"])
	assert.Equal(t, "test_db.test_table", metrics["table"])
	assert.Equal(t, "nessie", metrics["catalog_type"])

	// Test with snapshot
	snapshotID := int64(67890)
	source.currentSnapshot = &table.Snapshot{
		SnapshotID: snapshotID,
	}
	metrics = source.Metrics()
	assert.Equal(t, snapshotID, metrics["current_snapshot_id"])
}

func TestIcebergSource_Health(t *testing.T) {
	t.Run("catalog provider not initialized", func(t *testing.T) {
		source := &IcebergSource{}
		ctx := context.Background()

		err := source.Health(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "catalog provider not initialized")
	})

	t.Run("catalog provider initialized", func(t *testing.T) {
		source := &IcebergSource{
			catalogProvider: &mockCatalogProvider{},
		}
		ctx := context.Background()

		err := source.Health(ctx)
		assert.NoError(t, err)
	})
}

func TestIcebergSource_ReadNotInitialized(t *testing.T) {
	source := &IcebergSource{
		isInitialized: false,
	}
	ctx := context.Background()

	stream, err := source.Read(ctx)
	assert.Error(t, err)
	assert.Nil(t, stream)
	assert.Contains(t, err.Error(), "source not initialized")
}

func TestIcebergSource_ReadBatchNotInitialized(t *testing.T) {
	source := &IcebergSource{
		isInitialized: false,
	}
	ctx := context.Background()

	stream, err := source.ReadBatch(ctx, 1000)
	assert.Error(t, err)
	assert.Nil(t, stream)
	assert.Contains(t, err.Error(), "source not initialized")
}

func TestIcebergSource_DiscoverNotDiscovered(t *testing.T) {
	source := &IcebergSource{}
	ctx := context.Background()

	schema, err := source.Discover(ctx)
	assert.Error(t, err)
	assert.Nil(t, schema)
	assert.Contains(t, err.Error(), "schema not discovered yet")
}

// Mock types for testing
type mockPosition struct{}

func (m *mockPosition) String() string {
	return "mock"
}

func (m *mockPosition) Compare(other core.Position) int {
	return 0
}

type mockCatalogProvider struct{}

func (m *mockCatalogProvider) Connect(ctx context.Context, config CatalogConfig) error {
	return nil
}

func (m *mockCatalogProvider) LoadTable(ctx context.Context, database, tableName string) (*table.Table, error) {
	return nil, nil
}

func (m *mockCatalogProvider) GetSchema(ctx context.Context, database, tableName string) (*core.Schema, error) {
	return &core.Schema{}, nil
}

func (m *mockCatalogProvider) Close(ctx context.Context) error {
	return nil
}

func (m *mockCatalogProvider) Health(ctx context.Context) error {
	return nil
}

func (m *mockCatalogProvider) Type() string {
	return "mock"
}
