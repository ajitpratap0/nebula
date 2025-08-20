package iceberg

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register the Iceberg destination connector
	registry.RegisterDestination("iceberg", func(config *config.BaseConfig) (core.Destination, error) {
		return NewIcebergDestination(config)
	})
}
