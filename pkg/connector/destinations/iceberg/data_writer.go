package iceberg

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/shubham-tomar/iceberg-go/catalog"
	"go.uber.org/zap"
)

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
