package iceberg

import (
	"context"
	"fmt"

	// iceberg "github.com/shubham-tomar/iceberg-go"
	// "github.com/shubham-tomar/iceberg-go/table"
	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"go.uber.org/zap"
)

// ManifestReader reads manifest files from an Iceberg snapshot
type ManifestReader struct {
	snapshot     *table.Snapshot
	table        *table.Table
	currentIndex int
	logger       *zap.Logger
}

// NewManifestReader creates a new manifest reader
func NewManifestReader(snapshot *table.Snapshot, tbl *table.Table, logger *zap.Logger) *ManifestReader {
	return &ManifestReader{
		snapshot:     snapshot,
		table:        tbl,
		currentIndex: 0,
		logger:       logger,
	}
}

// GetDataFiles returns all data files from the snapshot's manifests
func (mr *ManifestReader) GetDataFiles(ctx context.Context) ([]iceberg.DataFile, error) {
	if mr.snapshot == nil {
		return nil, fmt.Errorf("snapshot is nil")
	}

	mr.logger.Info("Reading data files from snapshot",
		zap.Int64("snapshot_id", mr.snapshot.SnapshotID),
		zap.String("manifest_list", mr.snapshot.ManifestList))

	// Get manifests from snapshot using table's IO
	fio, err := mr.table.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get table filesystem: %w", err)
	}

	manifests, err := mr.snapshot.Manifests(fio)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifests: %w", err)
	}

	mr.logger.Info("Found manifests",
		zap.Int("manifest_count", len(manifests)))

	// Collect all data files from manifests
	var allDataFiles []iceberg.DataFile

	for i, manifest := range manifests {
		mr.currentIndex = i

		// Get entries from manifest
		fio, err := mr.table.FS(ctx)
		if err != nil {
			mr.logger.Error("Failed to get filesystem",
				zap.Int("manifest_index", i),
				zap.Error(err))
			continue
		}
		entries, err := manifest.FetchEntries(fio, false)
		if err != nil {
			mr.logger.Error("Failed to fetch manifest entries",
				zap.Int("manifest_index", i),
				zap.Error(err))
			continue
		}

		mr.logger.Debug("Processing manifest",
			zap.Int("manifest_index", i),
			zap.Int("entry_count", len(entries)))

		// Add data files from entries
		for _, entry := range entries {
			// Only include added and existing files (not deleted)
			if entry.Status() != iceberg.EntryStatusDELETED {
				allDataFiles = append(allDataFiles, entry.DataFile())
			}
		}
	}

	mr.logger.Info("Collected data files from manifests",
		zap.Int("data_file_count", len(allDataFiles)))

	return allDataFiles, nil
}
