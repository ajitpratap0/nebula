package gcs

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register the GCS destination connector
	registry.RegisterDestination("gcs", func(config *config.BaseConfig) (core.Destination, error) {
		return NewGCSDestination("gcs", config)
	})

	// Register connector metadata
	registry.RegisterConnectorInfo(&registry.ConnectorInfo{
		Name:        "gcs",
		Type:        "destination",
		Description: "Google Cloud Storage destination connector with Parquet support",
		Version:     "1.0.0",
		Author:      "Nebula Team",
		Capabilities: []string{
			"streaming",
			"batch",
			"bulk_load",
			"compression",
			"partitioning",
			"circuit_breaker",
			"rate_limiting",
			"health_checks",
			"metrics",
			"large_files",
			"parquet",
			"avro",
			"orc",
		},
		ConfigSchema: map[string]interface{}{
			"bucket": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "GCS bucket name",
			},
			"project_id": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "Google Cloud Project ID",
			},
			"prefix": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"description": "Object key prefix for uploaded files",
			},
			"credentials_file": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"description": "Path to Google Cloud service account credentials JSON file",
			},
			"file_format": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"default":     "parquet",
				"enum":        []string{"csv", "json", "jsonl", "parquet", "avro", "orc"},
				"description": "Output file format",
			},
			"compression": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"default":     "snappy",
				"enum":        []string{"none", "gzip", "snappy", "lz4", "zstd"},
				"description": "Compression type",
			},
			"partition_strategy": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"default":     "daily",
				"enum":        []string{"none", "hourly", "daily", "monthly", "yearly"},
				"description": "Data partitioning strategy",
			},
			"batch_size": map[string]interface{}{
				"type":        "integer",
				"required":    false,
				"default":     10000,
				"description": "Number of records per file",
			},
			"upload_timeout": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"default":     "5m",
				"description": "Upload timeout duration (e.g., 5m, 1h)",
			},
			"max_concurrency": map[string]interface{}{
				"type":        "integer",
				"required":    false,
				"default":     10,
				"description": "Maximum concurrent uploads",
			},
		},
	})
}
