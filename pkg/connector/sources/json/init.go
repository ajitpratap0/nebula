package json

import (
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register JSON source factory
	registry.RegisterSource("json", NewJSONSource)

	// Register connector info
	registry.RegisterConnectorInfo(&registry.ConnectorInfo{
		Name:        "json",
		Type:        "source",
		Description: "JSON file source connector supporting array and line-delimited formats",
		Version:     "1.0.0",
		Author:      "Nebula Team",
		Capabilities: []string{
			"streaming",
			"batch",
			"nested_objects",
			"json_array",
			"json_lines",
		},
		ConfigSchema: map[string]interface{}{
			"path": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "Path to the JSON file",
			},
			"format": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"default":     "lines",
				"description": "JSON format: 'array' for JSON array, 'lines' for line-delimited JSON",
				"enum":        []string{"array", "lines"},
			},
			"buffer_size": map[string]interface{}{
				"type":        "integer",
				"required":    false,
				"default":     65536,
				"description": "Buffer size for reading (bytes)",
			},
		},
	})
}
