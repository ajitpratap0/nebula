package iceberg

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"go.uber.org/zap"
)

// NessieCatalog implements CatalogProvider for Nessie catalog
type NessieCatalog struct {
	catalog catalog.Catalog
	config  CatalogConfig
	logger  *zap.Logger
}

// NewNessieCatalog creates a new Nessie catalog provider
func NewNessieCatalog(logger *zap.Logger) *NessieCatalog {
	return &NessieCatalog{
		logger: logger,
	}
}

// Connect establishes connection to the Nessie catalog
func (n *NessieCatalog) Connect(ctx context.Context, config CatalogConfig) error {
	n.config = config
	n.logger.Debug("Connecting to Nessie catalog", zap.String("uri", config.URI))

	// Ensure URI has proper scheme
	if !strings.HasPrefix(config.URI, "http://") && !strings.HasPrefix(config.URI, "https://") {
		config.URI = "http://" + config.URI
	}

	// Build catalog URI for Nessie
	baseURI := strings.TrimSuffix(config.URI, "/api/v1")
	catalogURI, err := url.JoinPath(baseURI, "iceberg", config.Branch)
	if err != nil {
		return fmt.Errorf("failed to build catalog URI: %w", err)
	}

	n.logger.Debug("Loading Nessie Iceberg catalog",
		zap.String("catalog_uri", catalogURI),
		zap.String("catalog_name", config.Name),
		zap.String("warehouse", config.WarehouseLocation))

	// Build properties for catalog
	props := iceberg.Properties{
		"uri": catalogURI,
	}

	// Add S3 configuration
	if config.Region != "" {
		props["s3.region"] = config.Region
	}
	if config.S3Endpoint != "" {
		props["s3.endpoint"] = config.S3Endpoint
	}
	if config.AccessKey != "" {
		props["s3.access-key-id"] = config.AccessKey
	}
	if config.SecretKey != "" {
		props["s3.secret-access-key"] = config.SecretKey
	}

	// Add custom properties first
	for key, value := range config.Properties {
		props[key] = value
	}

	// Set default for path-style access if not explicitly configured
	// This is needed for MinIO and other S3-compatible storage
	if _, exists := props["s3.path-style-access"]; !exists && config.S3Endpoint != "" {
		// Default to true for custom S3 endpoints (like MinIO)
		props["s3.path-style-access"] = "true"
	}

	n.logger.Debug("Attempting Nessie catalog.Load with properties",
		zap.String("uri", catalogURI),
		zap.String("catalog_name", config.Name),
		zap.Any("properties", SanitizeProperties(props)))

	// Load catalog
	iceCatalog, err := catalog.Load(ctx, config.Name, props)
	if err != nil {
		n.logger.Error("Nessie catalog.Load failed",
			zap.String("uri", catalogURI),
			zap.String("catalog_name", config.Name),
			zap.Error(err))
		return fmt.Errorf("failed to load Nessie catalog: %w", err)
	}

	// Type assert to REST catalog (Nessie uses REST catalog interface)
	restCatalog, ok := iceCatalog.(*rest.Catalog)
	if !ok {
		return fmt.Errorf("expected *rest.Catalog for Nessie, got %T", iceCatalog)
	}

	// Store catalog reference
	n.catalog = restCatalog

	n.logger.Info("Nessie catalog loaded successfully",
		zap.String("catalog_uri", catalogURI),
		zap.String("catalog_name", config.Name))

	return nil
}

// LoadTable loads an Iceberg table from the catalog
func (n *NessieCatalog) LoadTable(ctx context.Context, database, tableName string) (*table.Table, error) {
	if n.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	identifier := catalog.ToIdentifier(fmt.Sprintf("%s.%s", database, tableName))
	tbl, err := n.catalog.LoadTable(ctx, identifier)
	if err != nil {
		return nil, fmt.Errorf("failed to load table: %w", err)
	}

	n.logger.Debug("Successfully loaded table via catalog",
		zap.String("table", tableName),
		zap.String("location", tbl.Location()),
		zap.String("identifier", fmt.Sprintf("%s.%s", database, tableName)))

	return tbl, nil
}

// GetSchema retrieves the schema from the catalog
func (n *NessieCatalog) GetSchema(ctx context.Context, database, table string) (*core.Schema, error) {
	if n.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	identifier := catalog.ToIdentifier(fmt.Sprintf("%s.%s", database, table))
	tbl, err := n.catalog.LoadTable(ctx, identifier)
	if err != nil {
		return nil, fmt.Errorf("failed to load table: %w", err)
	}

	n.logger.Debug("Successfully loaded table via catalog",
		zap.String("table", table),
		zap.String("location", tbl.Location()),
		zap.String("identifier", fmt.Sprintf("%s.%s", database, table)))

	// Get Iceberg schema
	iceSchema := tbl.Schema()
	schemaFields := iceSchema.Fields()
	fields := make([]core.Field, 0, len(schemaFields))

	n.logger.Debug("Table schema details",
		zap.String("table", table),
		zap.Int("schema_id", iceSchema.ID),
		zap.Int("field_count", len(schemaFields)))

	// Convert Iceberg fields to core fields
	for _, field := range schemaFields {
		coreField := ConvertIcebergFieldToCore(field)
		fields = append(fields, coreField)

		n.logger.Debug("Schema field details",
			zap.String("table", table),
			zap.Int("field_id", field.ID),
			zap.String("field_name", field.Name),
			zap.String("field_type", field.Type.String()),
			zap.Bool("required", field.Required),
			zap.String("doc", field.Doc))
	}

	return &core.Schema{
		Name:   table,
		Fields: fields,
	}, nil
}

// Close closes the catalog connection
func (n *NessieCatalog) Close(ctx context.Context) error {
	// Nessie catalog doesn't require explicit close
	return nil
}

// Health checks the health of the catalog
func (n *NessieCatalog) Health(ctx context.Context) error {
	if n.catalog == nil {
		return fmt.Errorf("catalog not initialized")
	}
	return nil
}

// Type returns the catalog type
func (n *NessieCatalog) Type() string {
	return "nessie"
}
