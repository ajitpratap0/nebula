package iceberg

import (
	"fmt"

	"github.com/shubham-tomar/iceberg-go/table"
	"go.uber.org/zap"
)

// SnapshotManager manages Iceberg table snapshots for reading
type SnapshotManager struct {
	table           *table.Table
	currentSnapshot *table.Snapshot
	logger          *zap.Logger
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(tbl *table.Table, logger *zap.Logger) *SnapshotManager {
	return &SnapshotManager{
		table:  tbl,
		logger: logger,
	}
}

// GetCurrentSnapshot returns the current snapshot of the table
func (sm *SnapshotManager) GetCurrentSnapshot() (*table.Snapshot, error) {
	snapshot := sm.table.CurrentSnapshot()
	if snapshot == nil {
		return nil, fmt.Errorf("no current snapshot found for table")
	}

	sm.currentSnapshot = snapshot

	parentID := int64(0)
	if snapshot.ParentSnapshotID != nil {
		parentID = *snapshot.ParentSnapshotID
	}

	sm.logger.Info("Got current snapshot",
		zap.Int64("snapshot_id", snapshot.SnapshotID),
		zap.Int64("parent_id", parentID),
		zap.Int64("sequence_number", snapshot.SequenceNumber))

	return snapshot, nil
}

// GetSnapshotByID returns a specific snapshot by ID
func (sm *SnapshotManager) GetSnapshotByID(snapshotID int64) (*table.Snapshot, error) {
	snapshot := sm.table.SnapshotByID(snapshotID)
	if snapshot == nil {
		return nil, fmt.Errorf("snapshot not found: %d", snapshotID)
	}

	sm.logger.Info("Found snapshot",
		zap.Int64("snapshot_id", snapshotID))

	return snapshot, nil
}
