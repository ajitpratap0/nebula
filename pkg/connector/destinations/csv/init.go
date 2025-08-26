package csv

import (
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register the V2 CSV destination connector as primary
	_ = registry.RegisterDestination("csv", NewCSVDestination)

	// Register the simple CSV destination connector as legacy (deprecated)
	_ = registry.RegisterDestination("csv-legacy", NewSimpleCSVDestination)

	// Register primary connector info (V2 promoted)
	_ = registry.RegisterConnectorInfo(&registry.ConnectorInfo{
		Name:        "csv",
		Type:        "destination",
		Description: "Production-ready CSV file destination connector with BaseConnector features",
		Version:     "2.0.0",
		Author:      "Nebula Team",
		Capabilities: []string{
			"streaming",
			"batch",
			"circuit_breaker",
			"rate_limiting",
			"health_checks",
			"metrics",
			"error_handling",
			"progress_reporting",
		},
		ConfigSchema: map[string]interface{}{
			"path": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "Path to the CSV file",
			},
			"overwrite": map[string]interface{}{
				"type":        "bool",
				"required":    false,
				"default":     true,
				"description": "Whether to overwrite existing files",
			},
			"buffer_size": map[string]interface{}{
				"type":        "integer",
				"required":    false,
				"default":     10000,
				"description": "Buffer size for file operations",
			},
			"flush_interval": map[string]interface{}{
				"type":        "integer",
				"required":    false,
				"default":     1000,
				"description": "Number of records between flushes",
			},
		},
	})

	// Register legacy connector info (deprecated)
	_ = registry.RegisterConnectorInfo(&registry.ConnectorInfo{
		Name:        "csv-legacy",
		Type:        "destination",
		Description: "DEPRECATED: CSV file destination connector (use 'csv' instead)",
		Version:     "1.0.0",
		Author:      "Nebula Team",
		Capabilities: []string{
			"streaming",
			"batch",
		},
		ConfigSchema: map[string]interface{}{
			"path": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "Path to the CSV file",
			},
		},
	})
}
