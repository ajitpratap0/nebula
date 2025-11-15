package iceberg

import (
	"context"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/apache/iceberg-go/table"
)

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
