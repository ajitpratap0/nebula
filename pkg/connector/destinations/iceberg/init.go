package iceberg

import (
	"context"
	"fmt"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

func init() {
	// Register the Iceberg destination connector
	registry.RegisterDestination("iceberg", func(config *config.BaseConfig) (core.Destination, error) {
		return NewIcebergDestination(config)
	})
}

func (d *IcebergDestination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	if err := d.extractConfig(config); err != nil {
		return err
	}

	d.logger.Debug("Initializing Iceberg destination",
		zap.String("table", fmt.Sprintf("%s.%s", d.database, d.tableName)))

	// Create catalog provider using factory
	catalogProvider, err := NewCatalogProvider(d.catalogName, d.logger)
	if err != nil {
		return fmt.Errorf("failed to create catalog provider: %w", err)
	}
	d.catalogProvider = catalogProvider

	// Create catalog config
	catalogConfig := CatalogConfig{
		Name:              d.catalogName,
		URI:               d.catalogURI,
		WarehouseLocation: d.warehouse,
		Branch:            d.branch,
		Region:            d.region,
		S3Endpoint:        d.s3Endpoint,
		AccessKey:         d.accessKey,
		SecretKey:         d.secretKey,
		Properties:        d.properties,
	}

	// Connect to catalog
	if err := d.catalogProvider.Connect(ctx, catalogConfig); err != nil {
		return fmt.Errorf("failed to connect to catalog: %w", err)
	}

	// Test table accessibility
	if _, err := d.catalogProvider.GetSchema(ctx, d.database, d.tableName); err != nil {
		return fmt.Errorf("failed to access table schema: %w", err)
	}

	d.logger.Debug("Iceberg destination initialized successfully")
	return nil
}

func (d *IcebergDestination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	existingSchema, err := d.catalogProvider.GetSchema(ctx, d.database, d.tableName)
	if err != nil {
		return fmt.Errorf("failed to validate table schema: %w", err)
	}
	d.logger.Debug("Table schema validated", zap.Int("fields", len(existingSchema.Fields)))
	return nil
}

func (d *IcebergDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	return fmt.Errorf("Write not implemented")
}

func (d *IcebergDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	batchCount := 0
	for {
		select {
		case batch, ok := <-stream.Batches:
			if !ok {
				d.logger.Debug("WriteBatch completed", zap.Int("batches", batchCount))
				return nil
			}
			batchCount++
			d.logger.Debug("Processing batch", zap.Int("records", len(batch)))
			if err := d.catalogProvider.WriteData(ctx, d.database, d.tableName, batch); err != nil {
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

func (d *IcebergDestination) Close(ctx context.Context) error {
	if d.catalogProvider != nil {
		return d.catalogProvider.Close(ctx)
	}
	return nil
}

func (d *IcebergDestination) Health(ctx context.Context) error {
	if d.catalogProvider == nil {
		return fmt.Errorf("catalog provider not initialized")
	}
	return d.catalogProvider.Health(ctx)
}

// Capability methods - Iceberg connector currently supports batch writing only
func (d *IcebergDestination) SupportsBulkLoad() bool     { return false }
func (d *IcebergDestination) SupportsTransactions() bool { return false }
func (d *IcebergDestination) SupportsUpsert() bool       { return false }
func (d *IcebergDestination) SupportsBatch() bool        { return true }
func (d *IcebergDestination) SupportsStreaming() bool    { return false }

func (d *IcebergDestination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	return fmt.Errorf("BulkLoad not implemented")
}

func (d *IcebergDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return nil, fmt.Errorf("BeginTransaction not implemented")
}

func (d *IcebergDestination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	return fmt.Errorf("AlterSchema not implemented")
}

func (d *IcebergDestination) DropSchema(ctx context.Context, schema *core.Schema) error {
	return fmt.Errorf("DropSchema not implemented")
}

func (d *IcebergDestination) Upsert(ctx context.Context, records []*pool.Record, keys []string) error {
	return fmt.Errorf("Upsert not implemented")
}

func (d *IcebergDestination) Metrics() map[string]interface{} {
	return map[string]interface{}{
		"connector_type": "iceberg",
		"table":          fmt.Sprintf("%s.%s", d.database, d.tableName),
		"initialized":    d.catalogProvider != nil,
	}
}
