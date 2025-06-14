package json

import (
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register JSON destination factory
	registry.RegisterDestination("json", NewJSONDestination)

	// Register connector info
	registry.RegisterConnectorInfo(&registry.ConnectorInfo{
		Name:        "json",
		Type:        "destination",
		Description: "JSON file destination connector supporting array and line-delimited formats",
		Version:     "1.0.0",
		Author:      "Nebula Team",
		Capabilities: []string{
			"streaming",
			"batch",
			"nested_objects",
			"json_array",
			"json_lines",
			"pretty_print",
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
			"pretty": map[string]interface{}{
				"type":        "boolean",
				"required":    false,
				"default":     false,
				"description": "Pretty print JSON with indentation",
			},
			"indent": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"default":     "  ",
				"description": "Indentation string for pretty printing",
			},
			"buffer_size": map[string]interface{}{
				"type":        "integer",
				"required":    false,
				"default":     65536,
				"description": "Buffer size for writing (bytes)",
			},
		},
	})
}
