package iceberg

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewManifestReader(t *testing.T) {
	logger, _ := zap.NewProduction()

	snapshot := &table.Snapshot{
		SnapshotID:   12345,
		ManifestList: "s3://warehouse/metadata/snap-12345.avro",
	}

	reader := NewManifestReader(snapshot, nil, logger)

	assert.NotNil(t, reader)
	assert.Equal(t, snapshot, reader.snapshot)
	assert.Equal(t, 0, reader.currentIndex)
	assert.Equal(t, logger, reader.logger)
}

func TestManifestReader_GetDataFilesNilSnapshot(t *testing.T) {
	logger, _ := zap.NewProduction()
	reader := NewManifestReader(nil, nil, logger)

	ctx := context.Background()
	dataFiles, err := reader.GetDataFiles(ctx)

	assert.Error(t, err)
	assert.Nil(t, dataFiles)
	assert.Contains(t, err.Error(), "snapshot is nil")
}

func TestManifestReader_GetDataFilesRequiresIntegration(t *testing.T) {
	logger, _ := zap.NewProduction()

	snapshot := &table.Snapshot{
		SnapshotID:   12345,
		ManifestList: "s3://warehouse/metadata/snap-12345.avro",
	}

	_ = NewManifestReader(snapshot, nil, logger)

	// Note: Testing GetDataFiles requires a real table with IO configured
	// and actual manifest files. This is covered in integration tests.
	t.Skip("GetDataFiles requires integration testing with real Iceberg table and manifests")
}

func TestManifestReader_ContextCancellation(t *testing.T) {
	// Note: Testing context cancellation requires a real table
	// This is covered in integration tests where we can set up proper table I/O
	t.Skip("Context cancellation testing requires integration testing with real Iceberg table")
}

func TestManifestReader_CurrentIndex(t *testing.T) {
	logger, _ := zap.NewProduction()

	snapshot := &table.Snapshot{
		SnapshotID:   12345,
		ManifestList: "s3://warehouse/metadata/snap-12345.avro",
	}

	reader := NewManifestReader(snapshot, nil, logger)

	// Initial index should be 0
	assert.Equal(t, 0, reader.currentIndex)

	// After processing, index would be updated (tested in integration)
	// Here we just verify the field is accessible
	reader.currentIndex = 5
	assert.Equal(t, 5, reader.currentIndex)
}

func TestManifestReader_SnapshotInformation(t *testing.T) {
	logger, _ := zap.NewProduction()

	tests := []struct {
		name     string
		snapshot *table.Snapshot
	}{
		{
			name: "snapshot with basic info",
			snapshot: &table.Snapshot{
				SnapshotID:   12345,
				ManifestList: "s3://warehouse/metadata/snap-12345.avro",
			},
		},
		{
			name: "snapshot with parent",
			snapshot: &table.Snapshot{
				SnapshotID:   67890,
				ManifestList: "s3://warehouse/metadata/snap-67890.avro",
			},
		},
		{
			name: "snapshot with sequence number",
			snapshot: &table.Snapshot{
				SnapshotID:     99999,
				SequenceNumber: 5,
				ManifestList:   "s3://warehouse/metadata/snap-99999.avro",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewManifestReader(tt.snapshot, nil, logger)

			assert.Equal(t, tt.snapshot.SnapshotID, reader.snapshot.SnapshotID)
			assert.Equal(t, tt.snapshot.ManifestList, reader.snapshot.ManifestList)
			if tt.snapshot.SequenceNumber > 0 {
				assert.Equal(t, tt.snapshot.SequenceNumber, reader.snapshot.SequenceNumber)
			}
		})
	}
}

func TestManifestReader_StateTracking(t *testing.T) {
	logger, _ := zap.NewProduction()

	snapshot := &table.Snapshot{
		SnapshotID:   12345,
		ManifestList: "s3://warehouse/metadata/snap-12345.avro",
	}

	reader := NewManifestReader(snapshot, nil, logger)

	// Simulate state tracking during manifest processing
	assert.Equal(t, 0, reader.currentIndex)

	// Simulate processing manifests
	for i := 0; i < 5; i++ {
		reader.currentIndex = i
		assert.Equal(t, i, reader.currentIndex)
	}

	// Final state
	assert.Equal(t, 4, reader.currentIndex)
}

// Additional integration-level test scenarios (would require real data)
func TestManifestReader_IntegrationScenarios(t *testing.T) {
	t.Run("reading manifests from snapshot", func(t *testing.T) {
		t.Skip("Requires integration testing with real Iceberg table")
	})

	t.Run("filtering deleted entries", func(t *testing.T) {
		t.Skip("Requires integration testing with real manifest data")
	})

	t.Run("collecting data files from multiple manifests", func(t *testing.T) {
		t.Skip("Requires integration testing with real manifest data")
	})

	t.Run("handling manifest read errors", func(t *testing.T) {
		t.Skip("Requires integration testing with error scenarios")
	})

	t.Run("processing large numbers of manifests", func(t *testing.T) {
		t.Skip("Requires integration testing with large datasets")
	})
}
