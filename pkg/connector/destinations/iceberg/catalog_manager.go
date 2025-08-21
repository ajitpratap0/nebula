package iceberg

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	icebergGo "github.com/shubham-tomar/iceberg-go"
	"github.com/shubham-tomar/iceberg-go/catalog"
	"github.com/shubham-tomar/iceberg-go/catalog/rest"
	"go.uber.org/zap"
)

// loadCatalog loads the appropriate catalog based on catalog type in config
func (d *IcebergDestination) loadCatalog(ctx context.Context) error {
	switch strings.ToLower(d.catalogName) {
	case "nessie":
		return d.loadNessieCatalog(ctx)
	case "rest":
		return d.loadRestCatalog(ctx)
	case "glue":
		return d.loadGlueCatalog(ctx)
	default:
		return fmt.Errorf("unsupported catalog type: %s", d.catalogName)
	}
}

// loadNessieCatalog loads Nessie catalog with proper configuration
func (d *IcebergDestination) loadNessieCatalog(ctx context.Context) error {
	// Build catalog URI with branch path (following icebridge pattern)
	// Remove /api/v1 suffix and use base URI like icebridge
	baseURI := strings.TrimSuffix(d.catalogURI, "/api/v1")
	catalogURI, err := url.JoinPath(baseURI, "iceberg", d.branch)
	if err != nil {
		return fmt.Errorf("failed to build catalog URI: %w", err)
	}

	d.logger.Info("Loading Nessie Iceberg catalog",
		zap.String("catalog_uri", catalogURI),
		zap.String("catalog_name", d.catalogName),
		zap.String("warehouse", d.warehouse))

	// Build catalog properties with all S3/MinIO configuration for Nessie
	props := icebergGo.Properties{
		"uri":                      catalogURI,
		"s3.region":               d.region,
		"s3.endpoint":             d.s3Endpoint,
		"s3.access-key-id":        d.accessKey,
		"s3.secret-access-key":    d.secretKey,
		"s3.path-style-access":    "true",
	}
	
	d.logger.Info("Attempting Nessie catalog.Load with properties",
		zap.String("uri", catalogURI),
		zap.String("catalog_name", d.catalogName),
		zap.Any("properties", props))
	
	iceCatalog, err := catalog.Load(ctx, d.catalogName, props)
	if err != nil {
		d.logger.Error("Nessie catalog.Load failed",
			zap.String("uri", catalogURI),
			zap.String("catalog_name", d.catalogName),
			zap.Error(err))
		return fmt.Errorf("failed to load Nessie catalog: %w", err)
	}

	// Type assert to REST catalog
	restCatalog, ok := iceCatalog.(*rest.Catalog)
	if !ok {
		return fmt.Errorf("expected *rest.Catalog for Nessie, got %T", iceCatalog)
	}

	d.catalog = restCatalog
	d.logger.Info("Nessie catalog loaded successfully",
		zap.String("catalog_uri", catalogURI),
		zap.String("catalog_name", d.catalogName))

	return nil
}

// loadRestCatalog loads REST catalog (placeholder implementation)
func (d *IcebergDestination) loadRestCatalog(ctx context.Context) error {
	d.logger.Warn("REST catalog implementation not yet available",
		zap.String("catalog_name", d.catalogName))
	return fmt.Errorf("REST catalog not implemented yet")
}

// loadGlueCatalog loads AWS Glue catalog (placeholder implementation)
func (d *IcebergDestination) loadGlueCatalog(ctx context.Context) error {
	d.logger.Warn("Glue catalog implementation not yet available",
		zap.String("catalog_name", d.catalogName))
	return fmt.Errorf("Glue catalog not implemented yet")
}

// getTableSchemaViaCatalog retrieves detailed table schema using catalog.LoadTable
func (d *IcebergDestination) getTableSchemaViaCatalog(ctx context.Context) (*core.Schema, error) {
	if d.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Create table identifier
	identifier := catalog.ToIdentifier(fmt.Sprintf("%s.%s", d.database, d.tableName))
	
	// Load the table using catalog
	tbl, err := d.catalog.LoadTable(ctx, identifier, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load table: %w", err)
	}

	d.logger.Info("Successfully loaded table via catalog",
		zap.String("table", d.tableName),
		zap.String("location", tbl.Location()),
		zap.String("identifier", fmt.Sprintf("%s.%s", d.database, d.tableName)))

	// Get schema information
	iceSchema := tbl.Schema()
	var fields []core.Field

	d.logger.Info("Table schema details",
		zap.String("table", d.tableName),
		zap.Int("schema_id", iceSchema.ID),
		zap.Int("field_count", len(iceSchema.Fields())))

	// Process schema fields to get detailed column info
	for _, field := range iceSchema.Fields() {
		coreField := convertIcebergFieldToCore(field)
		fields = append(fields, coreField)
		
		// Log detailed field information
		d.logger.Info("Schema field details",
			zap.String("table", d.tableName),
			zap.Int("field_id", field.ID),
			zap.String("field_name", field.Name),
			zap.String("field_type", field.Type.String()),
			zap.Bool("required", field.Required),
			zap.String("doc", field.Doc))
	}

	// Get partition information if available
	spec := tbl.Spec()
	partitionCount := 0
	for field := range spec.Fields() {
		partitionCount++
		d.logger.Info("Partition field details",
			zap.String("table", d.tableName),
			zap.Int("source_id", field.SourceID),
			zap.Int("field_id", field.FieldID),
			zap.String("name", field.Name),
			zap.String("transform", field.Transform.String()))
	}
	
	if partitionCount > 0 {
		d.logger.Info("Table partition information",
			zap.String("table", d.tableName),
			zap.Int("partition_count", partitionCount))
	}

	// Get table metrics if available
	if snap := tbl.CurrentSnapshot(); snap != nil {
		d.logger.Info("Table snapshot information",
			zap.String("table", d.tableName),
			zap.Int64("snapshot_id", snap.SnapshotID),
			zap.Int64("timestamp_ms", snap.TimestampMs),
			zap.Int("schema_id", *snap.SchemaID))
		
		if snap.ParentSnapshotID != nil {
			d.logger.Info("Parent snapshot information",
				zap.String("table", d.tableName),
				zap.Int64("parent_snapshot_id", *snap.ParentSnapshotID))
		}
	}

	// Get table properties
	props := tbl.Properties()
	if len(props) > 0 {
		d.logger.Info("Table properties",
			zap.String("table", d.tableName),
			zap.Any("properties", props))
	}

	return &core.Schema{
		Name:    d.tableName,
		Fields:  fields,
		Version: iceSchema.ID,
	}, nil
}
