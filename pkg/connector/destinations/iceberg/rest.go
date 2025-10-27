package iceberg

import (
	"context"
	"fmt"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

type RestCatalog struct {
	config CatalogConfig
	logger *zap.Logger
}

func NewRestCatalog(logger *zap.Logger) *RestCatalog {
	return &RestCatalog{
		logger: logger,
	}
}

func (r *RestCatalog) Connect(ctx context.Context, config CatalogConfig) error {
	r.config = config
	r.logger.Warn("REST catalog implementation not yet available",
		zap.String("catalog_name", config.Name))
	return fmt.Errorf("REST catalog not implemented yet")
}

func (r *RestCatalog) GetSchema(ctx context.Context, database, table string) (*core.Schema, error) {
	return nil, fmt.Errorf("REST catalog GetSchema not implemented yet")
}

func (r *RestCatalog) WriteData(ctx context.Context, database, table string, batch []*pool.Record) error {
	return fmt.Errorf("REST catalog WriteData not implemented yet")
}

func (r *RestCatalog) Close(ctx context.Context) error {
	return nil
}

func (r *RestCatalog) Health(ctx context.Context) error {
	return fmt.Errorf("REST catalog not initialized")
}

func (r *RestCatalog) Type() string {
	return "rest"
}
