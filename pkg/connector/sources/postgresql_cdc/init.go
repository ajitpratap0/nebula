package postgresql_cdc

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register PostgreSQL CDC source connector
	registry.GetRegistry().RegisterSource("postgresql-cdc", func(cfg *config.BaseConfig) (core.Source, error) {
		return NewPostgreSQLCDCSource(cfg)
	})
}
