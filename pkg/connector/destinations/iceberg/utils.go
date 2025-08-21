package iceberg

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	icebergGo "github.com/shubham-tomar/iceberg-go"
	"github.com/shubham-tomar/iceberg-go/catalog"
	"github.com/shubham-tomar/iceberg-go/catalog/rest"
	"go.uber.org/zap"
)

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

