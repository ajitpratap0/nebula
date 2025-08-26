package s3

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register the S3 destination connector
	_ = registry.RegisterDestination("s3", func(config *config.BaseConfig) (core.Destination, error) {
		return NewS3Destination("s3", config)
	})

	// Register connector metadata
	_ = registry.RegisterConnectorInfo(&registry.ConnectorInfo{
		Name:        "s3",
		Type:        "destination",
		Description: "Amazon S3 destination connector with batching and compression support",
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
		},
		ConfigSchema: map[string]interface{}{
			"bucket": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "S3 bucket name",
			},
			"region": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "AWS region (e.g., us-east-1)",
			},
			"prefix": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"description": "Object key prefix for uploaded files",
			},
			"aws_access_key_id": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"description": "AWS access key ID (optional if using IAM roles)",
			},
			"aws_secret_access_key": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"description": "AWS secret access key (optional if using IAM roles)",
			},
			"format": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"default":     "csv",
				"enum":        []string{"csv", "json", "jsonl", "parquet"},
				"description": "Output file format",
			},
			"compression": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"default":     "none",
				"enum":        []string{"none", "gzip"},
				"description": "Compression type",
			},
			"batch_size": map[string]interface{}{
				"type":        "integer",
				"required":    false,
				"default":     1000,
				"description": "Number of records per file",
			},
			"max_file_size": map[string]interface{}{
				"type":        "integer",
				"required":    false,
				"default":     104857600, // 100MB
				"description": "Maximum file size in bytes",
			},
			"flush_interval": map[string]interface{}{
				"type":        "string",
				"required":    false,
				"default":     "5m",
				"description": "Time interval to flush files (e.g., 5m, 1h)",
			},
		},
	})
}
