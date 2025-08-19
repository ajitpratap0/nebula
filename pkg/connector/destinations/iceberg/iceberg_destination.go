package iceberg

import (
	"context"
	"fmt"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/base"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/shubham-tomar/iceberg-go/table"
)

// IcebergDestination implements the Destination interface for Iceberg tables
type IcebergDestination struct {
	*base.BaseConnector
	config         *IcebergConfig
	catalogManager *CatalogManager
	table          *table.Table
	writer         *IcebergWriter
	schemaMapper   *SchemaMapper
}

// NewIcebergDestination creates a new Iceberg destination connector
func NewIcebergDestination(name string, config *config.BaseConfig) (core.Destination, error) {
	baseConnector := base.NewBaseConnector(name, core.ConnectorTypeDestination, "1.0.0")
	
	return &IcebergDestination{
		BaseConnector: baseConnector,
		schemaMapper:  NewSchemaMapper(),
	}, nil
}

// Initialize prepares the Iceberg destination with configuration
func (d *IcebergDestination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	// Parse Iceberg-specific configuration
	icebergConfig := &IcebergConfig{}
	if err := config.UnmarshalConnectorConfig(icebergConfig); err != nil {
		return fmt.Errorf("failed to unmarshal Iceberg config: %w", err)
	}
	
	// Validate configuration
	if err := icebergConfig.Validate(); err != nil {
		return fmt.Errorf("invalid Iceberg config: %w", err)
	}
	
	d.config = icebergConfig
	
	// Initialize catalog manager
	d.catalogManager = NewCatalogManager(&icebergConfig.Catalog)
	if err := d.catalogManager.Initialize(ctx, icebergConfig.Branch); err != nil {
		return fmt.Errorf("failed to initialize catalog: %w", err)
	}
	
	d.Logger().Info("Iceberg destination initialized",
		"catalog_type", icebergConfig.Catalog.Type,
		"database", icebergConfig.Database,
		"table", icebergConfig.Table,
		"branch", icebergConfig.Branch)
	
	return nil
}

// CreateSchema creates the target schema in the Iceberg table
func (d *IcebergDestination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	if d.catalogManager == nil {
		return fmt.Errorf("catalog not initialized")
	}
	
	catalog := d.catalogManager.GetCatalog()
	tableIdentifier := d.schemaMapper.GetTableIdentifier(d.config.Database, d.config.Table)
	
	// Convert Nebula schema to Iceberg schema
	icebergSchema, err := d.schemaMapper.ToIcebergSchema(schema)
	if err != nil {
		return fmt.Errorf("failed to convert schema: %w", err)
	}
	
	// Check if table already exists
	exists, err := catalog.TableExists(ctx, tableIdentifier)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}
	
	if exists {
		// Load existing table
		tbl, err := catalog.LoadTable(ctx, tableIdentifier, nil)
		if err != nil {
			return fmt.Errorf("failed to load existing table: %w", err)
		}
		d.table = tbl
		
		d.Logger().Info("Loaded existing Iceberg table",
			"table", d.config.Table,
			"schema_id", tbl.Schema().ID())
	} else {
		// Create new table
		tbl, err := catalog.CreateTable(ctx, tableIdentifier, icebergSchema)
		if err != nil {
			return fmt.Errorf("failed to create Iceberg table: %w", err)
		}
		d.table = tbl
		
		d.Logger().Info("Created new Iceberg table",
			"table", d.config.Table,
			"schema_id", tbl.Schema().ID())
	}
	
	// Initialize writer
	d.writer = NewIcebergWriter(d.table, &d.config.Write)
	
	return nil
}

// Write processes a stream of individual records
func (d *IcebergDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	if d.writer == nil {
		return fmt.Errorf("writer not initialized - call CreateSchema first")
	}
	
	return d.writer.WriteStream(ctx, stream)
}

// WriteBatch processes batches of records for improved efficiency
func (d *IcebergDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	if d.writer == nil {
		return fmt.Errorf("writer not initialized - call CreateSchema first")
	}
	
	return d.writer.WriteBatchStream(ctx, stream)
}

// Close cleanly shuts down the connector and releases resources
func (d *IcebergDestination) Close(ctx context.Context) error {
	var errs []error
	
	// Close writer
	if d.writer != nil {
		if err := d.writer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close writer: %w", err))
		}
	}
	
	// Close catalog manager
	if d.catalogManager != nil {
		if err := d.catalogManager.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close catalog: %w", err))
		}
	}
	
	// Close base connector
	if err := d.BaseConnector.Close(ctx); err != nil {
		errs = append(errs, err)
	}
	
	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}
	
	d.Logger().Info("Iceberg destination closed")
	return nil
}

// Health checks if the destination is operational
func (d *IcebergDestination) Health(ctx context.Context) error {
	if d.catalogManager == nil {
		return fmt.Errorf("catalog not initialized")
	}
	
	// TODO: Add more comprehensive health checks
	// - Check catalog connectivity
	// - Check table accessibility
	// - Check storage permissions
	
	return nil
}

// Metrics returns performance and operational metrics
func (d *IcebergDestination) Metrics() map[string]interface{} {
	metrics := d.BaseConnector.Metrics()
	
	// Add Iceberg-specific metrics
	if d.table != nil {
		metrics["iceberg_table_location"] = d.table.Location()
		metrics["iceberg_schema_id"] = d.table.Schema().ID()
	}
	
	if d.config != nil {
		metrics["catalog_type"] = d.config.Catalog.Type
		metrics["database"] = d.config.Database
		metrics["table_name"] = d.config.Table
	}
	
	return metrics
}

// Placeholder implementations for remaining Destination interface methods
// These will be implemented in future phases

func (d *IcebergDestination) SupportsBatch() bool {
	return true
}

func (d *IcebergDestination) SupportsTransactions() bool {
	return true
}

func (d *IcebergDestination) SupportsBulkLoad() bool {
	return true
}

func (d *IcebergDestination) SupportsUpsert() bool {
	return false // TODO: Implement upsert support
}

func (d *IcebergDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return nil, fmt.Errorf("transactions not yet implemented")
}

func (d *IcebergDestination) BulkLoad(ctx context.Context, source string, options map[string]interface{}) error {
	return fmt.Errorf("bulk load not yet implemented")
}

func (d *IcebergDestination) Upsert(ctx context.Context, records []*pool.Record, keys []string) error {
	return fmt.Errorf("upsert not yet implemented")
}

func (d *IcebergDestination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	return fmt.Errorf("schema evolution not yet implemented")
}

func (d *IcebergDestination) DropSchema(ctx context.Context, schema *core.Schema) error {
	return fmt.Errorf("drop schema not yet implemented")
}
