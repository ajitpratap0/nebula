package snowflake

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register Snowflake optimized destination connector in the global registry
	_ = registry.RegisterDestination("snowflake_optimized", func(config *config.BaseConfig) (core.Destination, error) {
		return NewSnowflakeOptimizedDestination("snowflake_optimized", config)
	})

	// Also register as "snowflake" for backwards compatibility
	_ = registry.RegisterDestination("snowflake", func(config *config.BaseConfig) (core.Destination, error) {
		return NewSnowflakeOptimizedDestination("snowflake", config)
	})
}
