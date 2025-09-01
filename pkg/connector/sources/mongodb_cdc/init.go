package mongodbcdc

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register MongoDB CDC source with the registry
	_ = registry.GetRegistry().RegisterSource("mongodb-cdc", func(cfg *config.BaseConfig) (core.Source, error) {
		return NewMongoDBCDCSource(cfg)
	}) // Ignore registration error - will fail later if needed
}
