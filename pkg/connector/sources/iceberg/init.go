package iceberg

import (
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register the Iceberg source connector
	_ = registry.RegisterSource("iceberg", NewIcebergSource)

	// Register connector information
	_ = registry.RegisterConnectorInfo(&registry.ConnectorInfo{
		Name:        "iceberg",
		Type:        "source",
		Description: "Apache Iceberg table source connector with Nessie catalog support",
		Version:     "1.0.0",
		Author:      "Nebula Team",
		Capabilities: []string{
			"streaming",
			"batch",
			"incremental",
			"schema_discovery",
			"snapshot_isolation",
			"partition_pruning",
		},
		ConfigSchema: map[string]interface{}{
			"catalog_type": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "Type of Iceberg catalog (nessie, rest)",
			},
			"catalog_uri": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "URI of the catalog service",
			},
			"catalog_name": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "Name of the catalog",
			},
			"warehouse": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "Warehouse location (e.g., s3://warehouse/)",
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
			"region": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"description": "AWS region for S3",
			},
			"s3_endpoint": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"description": "S3/MinIO endpoint URL",
			},
			"access_key": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"description": "S3 access key",
			},
			"secret_key": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"description": "S3 secret key",
			},
		},
	})
}
