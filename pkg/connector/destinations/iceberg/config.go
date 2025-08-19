package iceberg

import (
	"fmt"
)

// IcebergConfig represents the configuration for Iceberg destination connector
type IcebergConfig struct {
	// Catalog configuration
	Catalog CatalogConfig `json:"catalog" mapstructure:"catalog"`
	
	// Database/Namespace
	Database string `json:"database" mapstructure:"database"`
	
	// Table name
	Table string `json:"table" mapstructure:"table"`
	
	// Branch name (for Nessie catalog)
	Branch string `json:"branch,omitempty" mapstructure:"branch"`
	
	// Write configuration
	Write WriteConfig `json:"write,omitempty" mapstructure:"write"`
}

// CatalogConfig represents catalog-specific configuration
type CatalogConfig struct {
	// Type of catalog (nessie, hive, glue, hadoop)
	Type string `json:"type" mapstructure:"type"`
	
	// Catalog URI
	URI string `json:"uri" mapstructure:"uri"`
	
	// Warehouse location (S3, HDFS, etc.)
	Warehouse string `json:"warehouse" mapstructure:"warehouse"`
	
	// Catalog name
	Name string `json:"name,omitempty" mapstructure:"name"`
	
	// Additional properties
	Properties map[string]string `json:"properties,omitempty" mapstructure:"properties"`
}

// WriteConfig represents write-specific configuration
type WriteConfig struct {
	// File format (parquet is default for Iceberg)
	Format string `json:"format,omitempty" mapstructure:"format"`
	
	// Compression codec
	Compression string `json:"compression,omitempty" mapstructure:"compression"`
	
	// Batch size for writing
	BatchSize int `json:"batch_size,omitempty" mapstructure:"batch_size"`
	
	// Target file size in MB
	TargetFileSizeMB int `json:"target_file_size_mb,omitempty" mapstructure:"target_file_size_mb"`
}

// Validate validates the Iceberg configuration
func (c *IcebergConfig) Validate() error {
	if c.Catalog.Type == "" {
		return fmt.Errorf("catalog type is required")
	}
	
	if c.Catalog.URI == "" {
		return fmt.Errorf("catalog URI is required")
	}
	
	if c.Database == "" {
		return fmt.Errorf("database/namespace is required")
	}
	
	if c.Table == "" {
		return fmt.Errorf("table name is required")
	}
	
	// Set defaults
	if c.Write.Format == "" {
		c.Write.Format = "parquet"
	}
	
	if c.Write.Compression == "" {
		c.Write.Compression = "snappy"
	}
	
	if c.Write.BatchSize == 0 {
		c.Write.BatchSize = 10000
	}
	
	if c.Write.TargetFileSizeMB == 0 {
		c.Write.TargetFileSizeMB = 128
	}
	
	// For Nessie, set default branch if not specified
	if c.Catalog.Type == "nessie" && c.Branch == "" {
		c.Branch = "main"
	}
	
	return nil
}
