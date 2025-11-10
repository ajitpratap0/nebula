package iceberg

import (
	"context"
	"fmt"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/shubham-tomar/iceberg-go/table"
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

// CatalogProvider defines the interface for Iceberg catalog operations
type CatalogProvider interface {
	// Connect establishes connection to the catalog
	Connect(ctx context.Context, config CatalogConfig) error

	// LoadTable loads an Iceberg table
	LoadTable(ctx context.Context, database, tableName string) (*table.Table, error)

	// GetSchema retrieves the schema from the catalog
	GetSchema(ctx context.Context, database, tableName string) (*core.Schema, error)

	// Close closes the catalog connection
	Close(ctx context.Context) error

	// Health checks the health of the catalog
	Health(ctx context.Context) error

	// Type returns the catalog type
	Type() string
}

// CatalogConfig contains configuration for catalog connection
type CatalogConfig struct {
	Name              string
	URI               string
	WarehouseLocation string
	Branch            string
	Region            string
	S3Endpoint        string
	AccessKey         string
	SecretKey         string
	Properties        map[string]string
}

// S3Config contains configuration for S3/MinIO access
type S3Config struct {
	Region     string
	Endpoint   string
	AccessKey  string
	SecretKey  string
	Properties map[string]string
}
