package iceberg

import (
	"context"
	"fmt"
	"net/url"

	"github.com/shubham-tomar/iceberg-go"
	"github.com/shubham-tomar/iceberg-go/catalog"
	"github.com/shubham-tomar/iceberg-go/catalog/rest"
)

// CatalogManager handles different catalog types and connections
type CatalogManager struct {
	config  *CatalogConfig
	catalog catalog.Catalog
}

// NewCatalogManager creates a new catalog manager
func NewCatalogManager(config *CatalogConfig) *CatalogManager {
	return &CatalogManager{
		config: config,
	}
}

// Initialize initializes the catalog connection based on type
func (cm *CatalogManager) Initialize(ctx context.Context, branch string) error {
	switch cm.config.Type {
	case "nessie":
		return cm.initializeNessie(ctx, branch)
	case "rest":
		return cm.initializeRest(ctx)
	case "hive":
		return cm.initializeHive(ctx)
	case "glue":
		return cm.initializeGlue(ctx)
	case "hadoop":
		return cm.initializeHadoop(ctx)
	default:
		return fmt.Errorf("unsupported catalog type: %s", cm.config.Type)
	}
}

// GetCatalog returns the initialized catalog
func (cm *CatalogManager) GetCatalog() catalog.Catalog {
	return cm.catalog
}

// Close closes the catalog connection
func (cm *CatalogManager) Close() error {
	if cm.catalog != nil {
		// Note: catalog interface doesn't have Close method in iceberg-go
		// but we can add cleanup logic here if needed
	}
	return nil
}

// initializeNessie initializes Nessie catalog (similar to your icebridge pattern)
func (cm *CatalogManager) initializeNessie(ctx context.Context, branch string) error {
	// Build URI with branch path for Nessie
	uri := cm.config.URI
	if branch != "" {
		var err error
		uri, err = url.JoinPath(cm.config.URI, "iceberg", branch)
		if err != nil {
			return fmt.Errorf("failed to build Nessie URI: %w", err)
		}
	}
	
	// Build properties
	props := iceberg.Properties{
		"uri": uri,
	}
	
	// Add warehouse location if specified
	if cm.config.Warehouse != "" {
		props["warehouse"] = cm.config.Warehouse
	}
	
	// Add custom properties
	for k, v := range cm.config.Properties {
		props[k] = v
	}
	
	// Set default S3 region if not specified
	if _, exists := props["s3.region"]; !exists {
		props["s3.region"] = "us-east-1"
	}
	
	catalogName := cm.config.Name
	if catalogName == "" {
		catalogName = "nessie"
	}
	
	cat, err := catalog.Load(ctx, catalogName, props)
	if err != nil {
		return fmt.Errorf("failed to load Nessie catalog: %w", err)
	}
	
	cm.catalog = cat
	return nil
}

// initializeRest initializes REST catalog
func (cm *CatalogManager) initializeRest(ctx context.Context) error {
	opts := []rest.Option{}
	
	if cm.config.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(cm.config.Warehouse))
	}
	
	// Add credential if specified
	if credential, exists := cm.config.Properties["credential"]; exists {
		opts = append(opts, rest.WithCredential(credential))
	}
	
	catalogName := cm.config.Name
	if catalogName == "" {
		catalogName = "rest"
	}
	
	cat, err := rest.NewCatalog(ctx, catalogName, cm.config.URI, opts...)
	if err != nil {
		return fmt.Errorf("failed to create REST catalog: %w", err)
	}
	
	cm.catalog = cat
	return nil
}

// initializeHive initializes Hive metastore catalog
func (cm *CatalogManager) initializeHive(ctx context.Context) error {
	// Build properties for Hive
	props := iceberg.Properties{
		"uri": cm.config.URI,
	}
	
	if cm.config.Warehouse != "" {
		props["warehouse"] = cm.config.Warehouse
	}
	
	// Add custom properties
	for k, v := range cm.config.Properties {
		props[k] = v
	}
	
	catalogName := cm.config.Name
	if catalogName == "" {
		catalogName = "hive"
	}
	
	cat, err := catalog.Load(ctx, catalogName, props)
	if err != nil {
		return fmt.Errorf("failed to load Hive catalog: %w", err)
	}
	
	cm.catalog = cat
	return nil
}

// initializeGlue initializes AWS Glue catalog
func (cm *CatalogManager) initializeGlue(ctx context.Context) error {
	// Build properties for Glue
	props := iceberg.Properties{
		"catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
	}
	
	if cm.config.Warehouse != "" {
		props["warehouse"] = cm.config.Warehouse
	}
	
	// Add AWS region if specified
	if region, exists := cm.config.Properties["aws.region"]; exists {
		props["aws.region"] = region
	}
	
	// Add custom properties
	for k, v := range cm.config.Properties {
		props[k] = v
	}
	
	catalogName := cm.config.Name
	if catalogName == "" {
		catalogName = "glue"
	}
	
	cat, err := catalog.Load(ctx, catalogName, props)
	if err != nil {
		return fmt.Errorf("failed to load Glue catalog: %w", err)
	}
	
	cm.catalog = cat
	return nil
}

// initializeHadoop initializes Hadoop catalog
func (cm *CatalogManager) initializeHadoop(ctx context.Context) error {
	// Build properties for Hadoop
	props := iceberg.Properties{
		"catalog-impl": "org.apache.iceberg.hadoop.HadoopCatalog",
	}
	
	if cm.config.Warehouse != "" {
		props["warehouse"] = cm.config.Warehouse
	}
	
	// Add custom properties
	for k, v := range cm.config.Properties {
		props[k] = v
	}
	
	catalogName := cm.config.Name
	if catalogName == "" {
		catalogName = "hadoop"
	}
	
	cat, err := catalog.Load(ctx, catalogName, props)
	if err != nil {
		return fmt.Errorf("failed to load Hadoop catalog: %w", err)
	}
	
	cm.catalog = cat
	return nil
}
