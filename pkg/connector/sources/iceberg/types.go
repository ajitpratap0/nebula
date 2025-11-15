package iceberg

import (
	"fmt"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	sharedIceberg "github.com/ajitpratap0/nebula/pkg/connector/shared/iceberg"
)

// Re-export shared types for backward compatibility
type (
	CatalogProvider = sharedIceberg.CatalogProvider
	CatalogConfig   = sharedIceberg.CatalogConfig
	S3Config        = sharedIceberg.S3Config
)

// IcebergPosition represents a position in the Iceberg table for incremental reads
type IcebergPosition struct {
	SnapshotID    int64                  `json:"snapshot_id"`
	ManifestIndex int                    `json:"manifest_index"`
	DataFileIndex int                    `json:"data_file_index"`
	RowOffset     int64                  `json:"row_offset"`
	Metadata      map[string]interface{} `json:"metadata"`
}

// String returns a string representation of the position
func (p *IcebergPosition) String() string {
	return fmt.Sprintf("iceberg_snapshot_%d_manifest_%d_file_%d_offset_%d",
		p.SnapshotID, p.ManifestIndex, p.DataFileIndex, p.RowOffset)
}

// Compare compares this position with another position
func (p *IcebergPosition) Compare(other core.Position) int {
	otherPos, ok := other.(*IcebergPosition)
	if !ok {
		return -1
	}

	if p.SnapshotID != otherPos.SnapshotID {
		if p.SnapshotID < otherPos.SnapshotID {
			return -1
		}
		return 1
	}

	if p.ManifestIndex != otherPos.ManifestIndex {
		if p.ManifestIndex < otherPos.ManifestIndex {
			return -1
		}
		return 1
	}

	if p.DataFileIndex != otherPos.DataFileIndex {
		if p.DataFileIndex < otherPos.DataFileIndex {
			return -1
		}
		return 1
	}

	if p.RowOffset != otherPos.RowOffset {
		if p.RowOffset < otherPos.RowOffset {
			return -1
		}
		return 1
	}

	return 0
}
