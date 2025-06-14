package postgresql

import (
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register the PostgreSQL source connector
	registry.RegisterSource("postgresql", NewPostgreSQLSource)

	// Register connector metadata
	registry.RegisterConnectorInfo(&registry.ConnectorInfo{
		Name:        "postgresql",
		Type:        "source",
		Description: "PostgreSQL source connector with connection pooling",
		Version:     "1.0.0",
		Author:      "Nebula Team",
		Capabilities: []string{
			"streaming",
			"batch",
			"schema_discovery",
			"custom_queries",
			"connection_pooling",
			"circuit_breaker",
			"rate_limiting",
			"health_checks",
			"metrics",
		},
		ConfigSchema: map[string]interface{}{
			"connection_string": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "PostgreSQL connection string",
			},
			"table": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"description": "Table name to read from",
			},
			"query": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"description": "Custom SQL query",
			},
			"batch_size": map[string]interface{}{
				"type":        "integer",
				"required":    false,
				"default":     1000,
				"description": "Records per batch",
			},
		},
	})
}
