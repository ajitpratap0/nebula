// Package destinations provides factory functions for all destination connectors
package destinations

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"

	// Import all destination connectors to trigger init() registration
	_ "github.com/ajitpratap0/nebula/pkg/connector/destinations/bigquery"
	_ "github.com/ajitpratap0/nebula/pkg/connector/destinations/csv"
	_ "github.com/ajitpratap0/nebula/pkg/connector/destinations/json"
	_ "github.com/ajitpratap0/nebula/pkg/connector/destinations/s3"
	_ "github.com/ajitpratap0/nebula/pkg/connector/destinations/snowflake"

	"github.com/ajitpratap0/nebula/pkg/connector/destinations/bigquery"
	"github.com/ajitpratap0/nebula/pkg/connector/destinations/snowflake"
)

// NewSnowflakeOptimizedDestination creates a new Snowflake optimized destination connector
// This function is expected by the benchmark and integration tests
func NewSnowflakeOptimizedDestination(name string, config *config.BaseConfig) (core.Destination, error) {
	return snowflake.NewSnowflakeOptimizedDestination(name, config)
}

// NewBigQueryDestination creates a new BigQuery destination connector
// This function is expected by the benchmark and integration tests
func NewBigQueryDestination(name string, config *config.BaseConfig) (core.Destination, error) {
	return bigquery.NewBigQueryDestination(name, config)
}