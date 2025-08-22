package iceberg

import (
	"context"
	"fmt"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

// CatalogProvider defines the interface for Iceberg catalog operations
type CatalogProvider interface {
	Connect(ctx context.Context, config CatalogConfig) error
	GetSchema(ctx context.Context, database, table string) (*core.Schema, error)
	WriteData(ctx context.Context, database, table string, batch []*pool.Record) error
	Close(ctx context.Context) error
	Health(ctx context.Context) error
	Type() string
}

// NessieCatalogProvider extends CatalogProvider with Nessie-specific operations
type NessieCatalogProvider interface {
	CatalogProvider
	CreateBranch(ctx context.Context, branchName, fromRef string) error
	MergeBranch(ctx context.Context, branchName, targetBranch string) error
	ListBranches(ctx context.Context) ([]string, error)
}

// NewCatalogProvider creates a new catalog provider based on catalog type
func NewCatalogProvider(catalogType string, logger *zap.Logger) (CatalogProvider, error) {
	switch catalogType {
	case "nessie":
		return NewNessieCatalog(logger), nil
	case "rest":
		return NewRestCatalog(logger), nil
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", catalogType)
	}
}
