package iceberg

import (
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register Iceberg destination factory
	registry.RegisterDestination("iceberg", NewIcebergDestination)

	// Register connector info
	registry.RegisterConnectorInfo(&registry.ConnectorInfo{
		Name:        "iceberg",
		Type:        "destination",
		Description: "Apache Iceberg destination connector supporting multiple catalog types and ACID transactions",
		Version:     "1.0.0",
		Author:      "Nebula Team",
		Capabilities: []string{
			"streaming",
			"batch",
			"transactions",
			"schema_evolution",
			"time_travel",
			"partitioning",
			"bulk_load",
			"parquet",
			"multiple_catalogs",
		},
		ConfigSchema: map[string]interface{}{
			"catalog": map[string]interface{}{
				"type":        "object",
				"required":    true,
				"description": "Catalog configuration",
				"properties": map[string]interface{}{
					"type": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Catalog type (nessie, rest, hive, glue, hadoop)",
						"enum":        []string{"nessie", "rest", "hive", "glue", "hadoop"},
					},
					"uri": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Catalog URI",
					},
					"warehouse": map[string]interface{}{
						"type":        "string",
						"required":    false,
						"description": "Warehouse location (S3, HDFS, etc.)",
					},
					"name": map[string]interface{}{
						"type":        "string",
						"required":    false,
						"description": "Catalog name",
					},
					"properties": map[string]interface{}{
						"type":        "object",
						"required":    false,
						"description": "Additional catalog properties",
					},
				},
			},
			"database": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "Database/namespace name",
			},
			"table": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "Table name",
			},
			"branch": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"default":     "main",
				"description": "Branch name (for Nessie catalog)",
			},
			"write": map[string]interface{}{
				"type":        "object",
				"required":    false,
				"description": "Write configuration",
				"properties": map[string]interface{}{
					"format": map[string]interface{}{
						"type":        "string",
						"required":    false,
						"default":     "parquet",
						"description": "File format",
						"enum":        []string{"parquet"},
					},
					"compression": map[string]interface{}{
						"type":        "string",
						"required":    false,
						"default":     "snappy",
						"description": "Compression codec",
						"enum":        []string{"none", "snappy", "gzip", "lz4", "zstd"},
					},
					"batch_size": map[string]interface{}{
						"type":        "integer",
						"required":    false,
						"default":     10000,
						"description": "Batch size for writing",
					},
					"target_file_size_mb": map[string]interface{}{
						"type":        "integer",
						"required":    false,
						"default":     128,
						"description": "Target file size in MB",
					},
				},
			},
		},
	})
}
