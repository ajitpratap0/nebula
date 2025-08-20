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
	database     string
	tableName    string
	branch       string
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

	// Test table accessibility via HTTP (skip catalog loading)
	_, err := d.FetchTableSchema(ctx)
	if err != nil {
		d.logger.Warn("Table schema fetch failed during initialization", zap.Error(err))
		return fmt.Errorf("failed to access table schema: %w", err)
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
	return fmt.Errorf("WriteBatch not implemented")
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
