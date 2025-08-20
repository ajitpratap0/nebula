package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
	groot "github.com/pixisai/go-nessie/api"
	icebergGo "github.com/shubham-tomar/iceberg-go"
	"github.com/shubham-tomar/iceberg-go/catalog"
	"github.com/shubham-tomar/iceberg-go/catalog/rest"
	"go.uber.org/zap"
)

// IcebergDestination is a minimal Iceberg destination connector
type IcebergDestination struct {
	catalog      *rest.Catalog
	nessieClient *groot.Client
	
	// Configuration
	catalogURI   string
	catalogName  string
	warehouse    string
	branch       string
	database     string
	tableName    string
	
	// S3/MinIO configuration
	region       string
	s3Endpoint   string
	accessKey    string
	secretKey    string
	properties   map[string]string
	
	logger *zap.Logger
}

// CatalogConfig represents the catalog configuration
type CatalogConfig struct {
	Name              string
	URI               string
	WarehouseLocation string
	Credential        string
}

// NewIcebergDestination creates a new Iceberg destination connector
func NewIcebergDestination(config *config.BaseConfig) (core.Destination, error) {
	logger, _ := zap.NewProduction()
	
	return &IcebergDestination{
		properties: make(map[string]string),
		logger:     logger,
	}, nil
}

// Initialize initializes the Iceberg destination connector
func (d *IcebergDestination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	if err := d.extractConfig(config); err != nil {
		return err
	}

	d.logger.Info("Initializing Iceberg destination",
		zap.String("catalog_uri", d.catalogURI),
		zap.String("catalog_name", d.catalogName),
		zap.String("warehouse", d.warehouse),
		zap.String("branch", d.branch),
		zap.String("database", d.database),
		zap.String("table", d.tableName))

	// Initialize Nessie client
	d.nessieClient = groot.NewClient(d.catalogURI)

	// Validate connection to Nessie server
	if err := d.validateConnection(ctx, d.catalogURI); err != nil {
		return fmt.Errorf("failed to connect to Nessie server: %w", err)
	}

	// Load catalog for data writing operations (optional for now)
	if err := d.loadCatalog(ctx); err != nil {
		d.logger.Warn("Failed to load catalog, continuing without it (schema fetching via HTTP still works)",
			zap.Error(err))
		// Don't fail initialization - HTTP-based operations still work
	}

	// Test table accessibility via HTTP (skip catalog loading)
	_, err := d.FetchTableSchema(ctx)
	if err != nil {
		d.logger.Warn("Table schema fetch failed during initialization", zap.Error(err))
		return fmt.Errorf("failed to access table schema: %w", err)
	}

	// If catalog is loaded, also test catalog-based schema fetching with detailed logging
	if d.catalog != nil {
		d.logger.Info("Testing catalog-based schema fetching with detailed information")
		_, err := d.getTableSchemaViaCatalog(ctx)
		if err != nil {
			d.logger.Warn("Catalog-based schema fetch failed", zap.Error(err))
		}
	}

	d.logger.Info("Iceberg destination initialized successfully",
		zap.String("catalog_uri", d.catalogURI),
		zap.String("branch", d.branch),
		zap.String("database", d.database),
		zap.String("table", d.tableName))

	return nil
}

// validateConnection checks if the Nessie server is accessible
func (d *IcebergDestination) validateConnection(ctx context.Context, baseURI string) error {
	client := &http.Client{Timeout: 10 * time.Second}
	
	// Test basic connectivity to Nessie API
	req, err := http.NewRequestWithContext(ctx, "GET", baseURI+"/config", nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to Nessie server at '%s': %w. Please ensure Nessie server is running", baseURI, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("Nessie server returned error status %d for '%s'", resp.StatusCode, baseURI)
	}

	d.logger.Info("Successfully validated connection to Nessie server", zap.String("uri", baseURI))
	return nil
}

// LoadCatalog loads the Iceberg catalog with Nessie integration (based on icebridge implementation)
func (d *IcebergDestination) LoadCatalog(ctx context.Context, workingBranch string, catalogName string, config CatalogConfig) (*rest.Catalog, error) {
	// First validate connection to Nessie server
	if err := d.validateConnection(ctx, config.URI); err != nil {
		return nil, err
	}

	// Use correct Nessie Iceberg REST API path: /iceberg/v1/{branch} instead of /api/v1/iceberg/{branch}
	baseURL := strings.TrimSuffix(config.URI, "/api/v1")
	uri, err := url.JoinPath(baseURL, "iceberg", "v1", workingBranch)
	if err != nil {
		return nil, fmt.Errorf("failed to construct catalog URI: %w", err)
	}

	d.logger.Info("Loading Iceberg catalog",
		zap.String("catalog_name", catalogName),
		zap.String("base_uri", config.URI),
		zap.String("branch", workingBranch),
		zap.String("full_uri", uri))

	// Build properties from configuration
	properties := icebergGo.Properties{
		"uri":       uri,
		"s3.region": "us-east-1",
	}

	// Add S3 properties from config
	for key, value := range d.properties {
		properties[key] = value
		d.logger.Debug("Added catalog property", zap.String("key", key), zap.String("value", value))
	}

	d.logger.Info("Attempting to load catalog with properties", zap.Any("properties", properties))

	iceCatalog, err := catalog.Load(ctx, catalogName, properties)
	if err != nil {
		d.logger.Error("Failed to load catalog",
			zap.String("catalog_name", catalogName),
			zap.String("uri", uri),
			zap.Error(err))
		return nil, fmt.Errorf("failed to load catalog '%s' from '%s': %w", catalogName, uri, err)
	}

	restCatalog, ok := iceCatalog.(*rest.Catalog)
	if !ok {
		return nil, fmt.Errorf("expected *rest.Catalog, got %T", iceCatalog)
	}

	d.logger.Info("Successfully loaded Iceberg catalog",
		zap.String("catalog_name", catalogName),
		zap.String("uri", uri))

	return restCatalog, nil
}

// TableResponse represents the Nessie table response structure
type TableResponse struct {
	Metadata struct {
		Schemas []struct {
			SchemaID int `json:"schema-id"`
			Fields   []struct {
				ID       int    `json:"id"`
				Name     string `json:"name"`
				Required bool   `json:"required"`
				Type     string `json:"type"`
			} `json:"fields"`
		} `json:"schemas"`
		CurrentSchemaID int `json:"current-schema-id"`
	} `json:"metadata"`
}

// FetchTableSchema retrieves the current schema of the Iceberg table using direct HTTP call
func (d *IcebergDestination) FetchTableSchema(ctx context.Context) (*core.Schema, error) {
	// Build the direct API URL
	baseURL := strings.TrimSuffix(d.catalogURI, "/api/v1")
	tableURL := fmt.Sprintf("%s/iceberg/v1/%s/namespaces/%s/tables/%s", 
		baseURL, d.branch, d.database, d.tableName)

	d.logger.Info("Fetching table schema via HTTP",
		zap.String("url", tableURL),
		zap.String("database", d.database),
		zap.String("table", d.tableName))

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "GET", tableURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Execute request
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch table schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch table schema, status: %d", resp.StatusCode)
	}

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse JSON response
	var tableResp TableResponse
	if err := json.Unmarshal(body, &tableResp); err != nil {
		return nil, fmt.Errorf("failed to parse table response: %w", err)
	}

	// Find current schema
	var currentSchema *struct {
		SchemaID int `json:"schema-id"`
		Fields   []struct {
			ID       int    `json:"id"`
			Name     string `json:"name"`
			Required bool   `json:"required"`
			Type     string `json:"type"`
		} `json:"fields"`
	}

	for _, schema := range tableResp.Metadata.Schemas {
		if schema.SchemaID == tableResp.Metadata.CurrentSchemaID {
			currentSchema = &schema
			break
		}
	}

	if currentSchema == nil {
		return nil, fmt.Errorf("current schema not found")
	}

	// Convert to Nebula schema format
	fields := make([]core.Field, len(currentSchema.Fields))
	for i, field := range currentSchema.Fields {
		// Map Iceberg types to Nebula FieldType
		var fieldType core.FieldType
		switch field.Type {
		case "string":
			fieldType = core.FieldTypeString
		case "int", "long":
			fieldType = core.FieldTypeInt
		case "float", "double":
			fieldType = core.FieldTypeFloat
		case "boolean":
			fieldType = core.FieldTypeBool
		case "timestamp":
			fieldType = core.FieldTypeTimestamp
		case "date":
			fieldType = core.FieldTypeDate
		case "time":
			fieldType = core.FieldTypeTime
		default:
			fieldType = core.FieldTypeString // Default fallback
		}

		fields[i] = core.Field{
			Name:     field.Name,
			Type:     fieldType,
			Nullable: !field.Required,
		}
	}

	d.logger.Info("Successfully fetched table schema",
		zap.String("table", d.tableName),
		zap.Int("field_count", len(fields)),
		zap.Int("schema_id", currentSchema.SchemaID))

	return &core.Schema{
		Name:    d.tableName,
		Fields:  fields,
		Version: currentSchema.SchemaID,
	}, nil
}

// extractConfig extracts configuration from BaseConfig
func (d *IcebergDestination) extractConfig(config *config.BaseConfig) error {
	creds := config.Security.Credentials
	if creds == nil {
		return fmt.Errorf("missing security credentials")
	}

	// Required fields
	requiredFields := map[string]*string{
		"catalog_uri":  &d.catalogURI,
		"warehouse":    &d.warehouse,
		"catalog_name": &d.catalogName,
		"database":     &d.database,
		"table":        &d.tableName,
		"branch":       &d.branch,
	}

	for field, target := range requiredFields {
		if value, ok := creds[field]; ok && value != "" {
			*target = value
		} else {
			return fmt.Errorf("missing required field: %s", field)
		}
	}

	// Initialize properties map
	if d.properties == nil {
		d.properties = make(map[string]string)
	}

	// Extract S3 configuration from credentials
	d.region = creds["prop_s3.region"]
	d.s3Endpoint = creds["prop_s3.endpoint"]
	d.accessKey = creds["prop_s3.access-key-id"]
	d.secretKey = creds["prop_s3.secret-access-key"]

	// Extract S3 properties
	for key, value := range creds {
		if strings.HasPrefix(key, "prop_") {
			propKey := strings.TrimPrefix(key, "prop_")
			d.properties[propKey] = value
		}
	}

	return nil
}

// CreateSchema validates that the table exists and schema is accessible
func (d *IcebergDestination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	d.logger.Info("Validating existing table schema", zap.String("table", d.tableName))
	
	// Since table already exists, just validate we can access it
	existingSchema, err := d.FetchTableSchema(ctx)
	if err != nil {
		return fmt.Errorf("failed to validate existing table schema: %w", err)
	}
	
	d.logger.Info("Table schema validated successfully",
		zap.String("table", existingSchema.Name),
		zap.Int("field_count", len(existingSchema.Fields)))
	
	return nil
}

func (d *IcebergDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	return fmt.Errorf("Write not implemented")
}

func (d *IcebergDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	if d.catalog == nil {
		d.logger.Warn("Catalog not available, WriteBatch will process but not write data")
		// Continue processing to avoid pipeline errors
	}

	// Only attempt table loading if catalog is available
	if d.catalog != nil {
		// Create table identifier
		identifier := catalog.ToIdentifier(fmt.Sprintf("%s.%s", d.database, d.tableName))
		
		// Load the table using catalog
		_, err := d.catalog.LoadTable(ctx, identifier, nil)
		if err != nil {
			return fmt.Errorf("failed to load table: %w", err)
		}

		d.logger.Info("WriteBatch loaded table successfully",
			zap.String("table", d.tableName),
			zap.String("identifier", fmt.Sprintf("%s.%s", d.database, d.tableName)))
	}

	// Process batches from stream channels
	batchCount := 0
	for {
		select {
		case batch, ok := <-stream.Batches:
			if !ok {
				// Batches channel closed
				d.logger.Info("WriteBatch completed",
					zap.String("table", d.tableName),
					zap.Int("batches_processed", batchCount))
				return nil
			}
			
			batchCount++
			d.logger.Info("Processing batch",
				zap.Int("batch_number", batchCount),
				zap.Int("record_count", len(batch)))
			
			// Write batch data to Iceberg table
			err := d.insertData(ctx, batch)
			if err != nil {
				d.logger.Error("Failed to write batch data",
					zap.Int("batch_number", batchCount),
					zap.Error(err))
				return fmt.Errorf("failed to write batch %d: %w", batchCount, err)
			}
			
		case err := <-stream.Errors:
			if err != nil {
				return fmt.Errorf("batch stream error: %w", err)
			}
			
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// loadCatalog loads the Iceberg catalog for data writing operations
func (d *IcebergDestination) loadCatalog(ctx context.Context) error {
	// Build catalog URI with branch path (following icebridge pattern)
	// Remove /api/v1 suffix and use base URI like icebridge
	baseURI := strings.TrimSuffix(d.catalogURI, "/api/v1")
	catalogURI, err := url.JoinPath(baseURI, "iceberg", d.branch)
	if err != nil {
		return fmt.Errorf("failed to build catalog URI: %w", err)
	}

	d.logger.Info("Loading Iceberg catalog",
		zap.String("catalog_uri", catalogURI),
		zap.String("catalog_name", d.catalogName),
		zap.String("warehouse", d.warehouse))

	// Load catalog using iceberg-go catalog.Load (following icebridge pattern)
	// Build catalog properties with all S3/MinIO configuration
	props := icebergGo.Properties{
		"uri":                      catalogURI,
		"s3.region":               d.region,
		"s3.endpoint":             d.s3Endpoint,
		"s3.access-key-id":        d.accessKey,
		"s3.secret-access-key":    d.secretKey,
		"s3.path-style-access":    "true",
	}
	
	d.logger.Info("Attempting catalog.Load with properties",
		zap.String("uri", catalogURI),
		zap.String("catalog_name", d.catalogName),
		zap.Any("properties", props))
	
	iceCatalog, err := catalog.Load(ctx, d.catalogName, props)
	if err != nil {
		d.logger.Error("catalog.Load failed",
			zap.String("uri", catalogURI),
			zap.String("catalog_name", d.catalogName),
			zap.Error(err))
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	// Type assert to REST catalog
	restCatalog, ok := iceCatalog.(*rest.Catalog)
	if !ok {
		return fmt.Errorf("expected *rest.Catalog, got %T", iceCatalog)
	}

	d.catalog = restCatalog
	d.logger.Info("Catalog loaded successfully",
		zap.String("catalog_uri", catalogURI),
		zap.String("catalog_name", d.catalogName))

	return nil
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
		coreField := core.Field{
			Name:     field.Name,
			Type:     core.FieldTypeString, // Default to string, can be enhanced later
			Nullable: !field.Required,
		}
		
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

// insertData writes batch data to Iceberg table using catalog
func (d *IcebergDestination) insertData(ctx context.Context, batch []*pool.Record) error {
	if d.catalog == nil {
		return fmt.Errorf("catalog not initialized for data writing")
	}

	// Load the table using catalog
	identifier := catalog.ToIdentifier(fmt.Sprintf("%s.%s", d.database, d.tableName))
	tbl, err := d.catalog.LoadTable(ctx, identifier, nil)
	if err != nil {
		d.logger.Error("Failed to load table for data writing", 
			zap.String("identifier", fmt.Sprintf("%s.%s", d.database, d.tableName)),
			zap.Error(err))
		return fmt.Errorf("failed to load table: %w", err)
	}

	d.logger.Debug("Loaded table for data writing",
		zap.String("table", d.tableName),
		zap.String("database", d.database))

	// Convert Iceberg schema to Arrow schema
	icebergSchema := tbl.Schema()
	arrowSchema, err := d.icebergToArrowSchema(icebergSchema)
	if err != nil {
		return fmt.Errorf("failed to convert schema: %w", err)
	}

	// Convert batch records to Arrow format
	d.logger.Debug("Converting batch to Arrow record",
		zap.Int("batch_size", len(batch)),
		zap.Int("schema_fields", len(arrowSchema.Fields())))
	
	arrowRecord, err := d.batchToArrowRecord(arrowSchema, batch)
	if err != nil {
		return fmt.Errorf("failed to convert batch to Arrow: %w", err)
	}
	defer arrowRecord.Release()
	
	d.logger.Debug("Arrow record created",
		zap.Int64("num_rows", arrowRecord.NumRows()),
		zap.Int64("num_cols", arrowRecord.NumCols()))

	// Create a RecordReader from the Arrow record
	reader, err := array.NewRecordReader(arrowSchema, []arrow.Record{arrowRecord})
	if err != nil {
		return fmt.Errorf("failed to create record reader: %w", err)
	}
	defer reader.Release()
	
	// Write data to table using Append
	d.logger.Debug("Starting table append operation",
		zap.String("table", d.tableName),
		zap.Int64("records", arrowRecord.NumRows()))
	
	newTable, err := tbl.Append(ctx, reader, nil)
	if err != nil {
		d.logger.Error("Failed to append data to table",
			zap.String("table", d.tableName),
			zap.Int64("records", arrowRecord.NumRows()),
			zap.Error(err))
		return fmt.Errorf("failed to append data: %w", err)
	}

	d.logger.Info("Successfully wrote batch to Iceberg table",
		zap.String("table", d.tableName),
		zap.Int64("records", arrowRecord.NumRows()),
		zap.String("new_table_location", newTable.Location()),
		zap.Int("new_schema_id", newTable.Schema().ID))

	// For Nessie catalogs, we may need to ensure the commit is visible
	// The table.Append() should handle this automatically, but let's verify
	d.logger.Debug("Verifying table state after append",
		zap.String("table_location", newTable.Location()),
		zap.Any("current_snapshot", newTable.CurrentSnapshot()))

	return nil
}

// icebergToArrowSchema converts Iceberg schema to Arrow schema
func (d *IcebergDestination) icebergToArrowSchema(icebergSchema *icebergGo.Schema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(icebergSchema.Fields()))
	
	for _, field := range icebergSchema.Fields() {
		arrowType, err := d.icebergTypeToArrowType(field.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", field.Name, err)
		}
		
		arrowField := arrow.Field{
			Name:     field.Name,
			Type:     arrowType,
			Nullable: !field.Required,
		}
		fields = append(fields, arrowField)
	}
	
	return arrow.NewSchema(fields, nil), nil
}

// icebergTypeToArrowType converts Iceberg type to Arrow type
func (d *IcebergDestination) icebergTypeToArrowType(icebergType icebergGo.Type) (arrow.DataType, error) {
	switch t := icebergType.(type) {
	case icebergGo.BooleanType:
		return arrow.FixedWidthTypes.Boolean, nil
	case icebergGo.Int32Type:
		return arrow.PrimitiveTypes.Int32, nil
	case icebergGo.Int64Type:
		return arrow.PrimitiveTypes.Int64, nil
	case icebergGo.Float32Type:
		return arrow.PrimitiveTypes.Float32, nil
	case icebergGo.Float64Type:
		return arrow.PrimitiveTypes.Float64, nil
	case icebergGo.StringType:
		return arrow.BinaryTypes.String, nil
	case icebergGo.TimestampType:
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case icebergGo.DateType:
		return arrow.FixedWidthTypes.Date32, nil
	default:
		// Default to string for unsupported types
		d.logger.Warn("Unsupported Iceberg type, defaulting to string",
			zap.String("type", t.String()))
		return arrow.BinaryTypes.String, nil
	}
}

// batchToArrowRecord converts Nebula batch to Arrow record
func (d *IcebergDestination) batchToArrowRecord(schema *arrow.Schema, batch []*pool.Record) (arrow.Record, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("no records to convert")
	}

	d.logger.Debug("Starting batch to Arrow conversion",
		zap.Int("num_records", len(batch)),
		zap.Int("num_fields", len(schema.Fields())))

	// Log sample record data for debugging
	if len(batch) > 0 {
		d.logger.Debug("Sample record data", zap.Any("record_data", batch[0].Data))
	}

	pool := memory.NewGoAllocator()
	recBuilder := array.NewRecordBuilder(pool, schema)
	defer recBuilder.Release()

	// Build arrays for each field
	for i, field := range schema.Fields() {
		d.logger.Debug("Processing field", 
			zap.String("field_name", field.Name),
			zap.String("field_type", field.Type.String()))
		
		fieldBuilder := recBuilder.Field(i)
		for recordIdx, record := range batch {
			val, exists := record.GetData(field.Name)
			d.logger.Debug("Field value", 
				zap.String("field", field.Name),
				zap.Int("record_idx", recordIdx),
				zap.Bool("exists", exists),
				zap.Any("value", val))
			d.appendToBuilder(field.Type, fieldBuilder, val)
		}
	}

	rec := recBuilder.NewRecord()
	rec.Retain() // retain to return after releasing builder
	
	d.logger.Debug("Arrow record built successfully",
		zap.Int64("rows", rec.NumRows()),
		zap.Int64("cols", rec.NumCols()))
	
	return rec, nil
}

// appendToBuilder appends value to Arrow array builder based on type
func (d *IcebergDestination) appendToBuilder(dt arrow.DataType, b array.Builder, val interface{}) {
	if val == nil {
		b.AppendNull()
		return
	}

	switch builder := b.(type) {
	case *array.BooleanBuilder:
		if v, ok := val.(bool); ok {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	case *array.Int32Builder:
		if v, ok := val.(int); ok {
			builder.Append(int32(v))
		} else if v, ok := val.(int32); ok {
			builder.Append(v)
		} else if v, ok := val.(float64); ok {
			// Handle numbers from JSON
			builder.Append(int32(v))
		} else {
			builder.AppendNull()
		}
	case *array.Int64Builder:
		if v, ok := val.(int64); ok {
			builder.Append(v)
		} else if v, ok := val.(int); ok {
			builder.Append(int64(v))
		} else if v, ok := val.(float64); ok {
			builder.Append(int64(v))
		} else {
			builder.AppendNull()
		}
	case *array.Float32Builder:
		if v, ok := val.(float32); ok {
			builder.Append(v)
		} else if v, ok := val.(float64); ok {
			builder.Append(float32(v))
		} else {
			builder.AppendNull()
		}
	case *array.Float64Builder:
		if v, ok := val.(float64); ok {
			builder.Append(v)
		} else if v, ok := val.(float32); ok {
			builder.Append(float64(v))
		} else {
			builder.AppendNull()
		}
	case *array.StringBuilder:
		if v, ok := val.(string); ok {
			builder.Append(v)
		} else {
			// Convert other types to string
			builder.Append(fmt.Sprintf("%v", val))
		}
	case *array.TimestampBuilder:
		if v, ok := val.(time.Time); ok {
			builder.Append(arrow.Timestamp(v.UnixMicro()))
		} else if v, ok := val.(string); ok {
			// Try to parse string as timestamp
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				builder.Append(arrow.Timestamp(t.UnixMicro()))
			} else {
				builder.AppendNull()
			}
		} else {
			builder.AppendNull()
		}
	case *array.Date32Builder:
		if v, ok := val.(time.Time); ok {
			days := int32(v.Unix() / 86400) // Convert to days since epoch
			builder.Append(arrow.Date32(days))
		} else if v, ok := val.(string); ok {
			if t, err := time.Parse("2006-01-02", v); err == nil {
				days := int32(t.Unix() / 86400)
				builder.Append(arrow.Date32(days))
			} else {
				builder.AppendNull()
			}
		} else {
			builder.AppendNull()
		}
	default:
		// Default case - append null for unsupported types
		d.logger.Warn("Unsupported Arrow builder type",
			zap.String("type", fmt.Sprintf("%T", builder)))
		b.AppendNull()
	}
}

func (d *IcebergDestination) Close(ctx context.Context) error {
	return nil
}

func (d *IcebergDestination) Health(ctx context.Context) error {
	if d.catalog == nil {
		return fmt.Errorf("catalog not initialized")
	}
	return nil
}

// Capability methods
func (d *IcebergDestination) SupportsBulkLoad() bool     { return false }
func (d *IcebergDestination) SupportsTransactions() bool { return false }
func (d *IcebergDestination) SupportsUpsert() bool       { return false }
func (d *IcebergDestination) SupportsBatch() bool        { return false }
func (d *IcebergDestination) SupportsStreaming() bool    { return false }

// Unimplemented methods
func (d *IcebergDestination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	return fmt.Errorf("BulkLoad not implemented")
}

func (d *IcebergDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return nil, fmt.Errorf("BeginTransaction not implemented")
}

func (d *IcebergDestination) Upsert(ctx context.Context, records []*pool.Record, keys []string) error {
	return fmt.Errorf("Upsert not implemented")
}

func (d *IcebergDestination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	return fmt.Errorf("AlterSchema not implemented")
}

func (d *IcebergDestination) DropSchema(ctx context.Context, schema *core.Schema) error {
	return fmt.Errorf("DropSchema not implemented")
}

func (d *IcebergDestination) Metrics() map[string]interface{} {
	return map[string]interface{}{
		"connector_type": "iceberg",
		"catalog_uri":    d.catalogURI,
		"branch":         d.branch,
		"database":       d.database,
		"table":          d.tableName,
		"initialized":    d.catalog != nil,
	}
}
