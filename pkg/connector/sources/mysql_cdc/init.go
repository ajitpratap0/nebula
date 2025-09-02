package mysqlcdc

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register MySQL CDC source connector
	_ = registry.GetRegistry().RegisterSource("mysql-cdc", func(cfg *config.BaseConfig) (core.Source, error) {
		return NewMySQLCDCSource(cfg)
	}) // Ignore registration error - will fail later if needed
}
