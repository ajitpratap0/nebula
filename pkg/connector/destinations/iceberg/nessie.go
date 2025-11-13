package iceberg

import (
	"context"
	"fmt"

	sharedIceberg "github.com/ajitpratap0/nebula/pkg/connector/shared/iceberg"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	icebergGo "github.com/apache/iceberg-go"
	"go.uber.org/zap"
)

// NessieCatalog wraps the shared Nessie catalog and adds destination-specific methods
type NessieCatalog struct {
	*sharedIceberg.NessieCatalog
	logger *zap.Logger
}

func NewNessieCatalog(logger *zap.Logger) *NessieCatalog {
	return &NessieCatalog{
		NessieCatalog: sharedIceberg.NewNessieCatalog(logger),
		logger:        logger,
	}
}

// Utility methods for schema and batch conversion
func (n *NessieCatalog) convertSchema(icebergSchema *icebergGo.Schema) (*arrow.Schema, error) {
	tempDest := &IcebergDestination{
		logger:          n.logger,
		schemaValidator: &SimpleSchemaValidator{},
		bufferConfig: BufferConfig{
			StringDataMultiplier:  32,
			ListElementMultiplier: 5,
		},
	}

	// Ensure proper initialization without builder pool (not needed for schema conversion)
	return tempDest.icebergToArrowSchema(icebergSchema)
}

func (n *NessieCatalog) convertBatch(arrowSchema *arrow.Schema, batch []*pool.Record) (arrow.Record, error) {
	tempDest := &IcebergDestination{
		logger:          n.logger,
		schemaValidator: &SimpleSchemaValidator{},
		bufferConfig: BufferConfig{
			StringDataMultiplier:  32,
			ListElementMultiplier: 5,
		},
	}

	// Initialize builder pool with proper cleanup
	allocator := memory.NewGoAllocator()
	tempDest.builderPool = NewArrowBuilderPool(allocator, n.logger)

	// Ensure cleanup of temporary builder pool resources
	defer func() {
		if tempDest.builderPool != nil {
			tempDest.builderPool.Clear()
		}
	}()

	return tempDest.batchToArrowRecord(arrowSchema, batch)
}

func (n *NessieCatalog) WriteData(ctx context.Context, database, table string, batch []*pool.Record) error {
	// Use the embedded NessieCatalog's LoadTable method
	tbl, err := n.NessieCatalog.LoadTable(ctx, database, table)
	if err != nil {
		n.logger.Error("Failed to load table for data writing",
			zap.String("identifier", fmt.Sprintf("%s.%s", database, table)),
			zap.Error(err))
		return fmt.Errorf("failed to load table: %w", err)
	}

	n.logger.Debug("Loaded table for data writing",
		zap.String("table", table),
		zap.String("database", database))

	icebergSchema := tbl.Schema()
	arrowSchema, err := n.convertSchema(icebergSchema)
	if err != nil {
		return fmt.Errorf("failed to convert schema: %w", err)
	}

	n.logger.Debug("Converting batch to Arrow record",
		zap.Int("batch_size", len(batch)),
		zap.Int("schema_fields", len(arrowSchema.Fields())))

	arrowRecord, err := n.convertBatch(arrowSchema, batch)
	if err != nil {
		return fmt.Errorf("failed to convert batch to Arrow: %w", err)
	}
	defer arrowRecord.Release()

	n.logger.Debug("Arrow record created",
		zap.Int64("num_rows", arrowRecord.NumRows()),
		zap.Int64("num_cols", arrowRecord.NumCols()))

	reader, err := array.NewRecordReader(arrowSchema, []arrow.Record{arrowRecord})
	if err != nil {
		return fmt.Errorf("failed to create record reader: %w", err)
	}
	defer reader.Release()

	n.logger.Debug("Starting table append operation",
		zap.String("table", table),
		zap.Int64("records", arrowRecord.NumRows()))

	newTable, err := tbl.Append(ctx, reader, nil)
	if err != nil {
		n.logger.Error("Failed to append data to table",
			zap.String("table", table),
			zap.Int64("records", arrowRecord.NumRows()),
			zap.Error(err))
		return fmt.Errorf("failed to append data: %w", err)
	}

	n.logger.Info("Successfully wrote batch to Iceberg table",
		zap.String("table", table),
		zap.Int64("records", arrowRecord.NumRows()),
		zap.String("new_table_location", newTable.Location()),
		zap.Int("new_schema_id", newTable.Schema().ID))

	n.logger.Debug("Verifying table state after append",
		zap.String("table_location", newTable.Location()),
		zap.Any("current_snapshot", newTable.CurrentSnapshot()))

	return nil
}
