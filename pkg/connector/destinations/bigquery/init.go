package bigquery

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register BigQuery destination connector in the global registry
	_ = registry.RegisterDestination("bigquery", func(config *config.BaseConfig) (core.Destination, error) {
		return NewBigQueryDestination("bigquery", config)
	})

	// Also register as "bq" for convenience
	_ = registry.RegisterDestination("bq", func(config *config.BaseConfig) (core.Destination, error) {
		return NewBigQueryDestination("bq", config)
	})
}
