package iceberg

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	icebergGo "github.com/shubham-tomar/iceberg-go"
	"github.com/shubham-tomar/iceberg-go/catalog"
	"github.com/shubham-tomar/iceberg-go/catalog/rest"
	"go.uber.org/zap"
)

type NessieCatalog struct {
	catalog catalog.Catalog
	config  CatalogConfig
	logger  *zap.Logger
}

type CatalogConfig struct {
	Name              string
	URI               string
	WarehouseLocation string
	Branch            string
	Region            string
	S3Endpoint        string
	AccessKey         string
	SecretKey         string
	Properties        map[string]string
}

func NewNessieCatalog(logger *zap.Logger) *NessieCatalog {
	return &NessieCatalog{
		logger: logger,
	}
}

func (n *NessieCatalog) Connect(ctx context.Context, config CatalogConfig) error {
	n.config = config
	n.logger.Debug("Connecting to Nessie catalog", zap.String("uri", config.URI))

	if !strings.HasPrefix(config.URI, "http://") && !strings.HasPrefix(config.URI, "https://") {
		config.URI = "http://" + config.URI
	}

	baseURI := strings.TrimSuffix(config.URI, "/api/v1")
	catalogURI, err := url.JoinPath(baseURI, "iceberg", config.Branch)
	if err != nil {
		return fmt.Errorf("failed to build catalog URI: %w", err)
	}

	n.logger.Debug("Loading Nessie Iceberg catalog",
		zap.String("catalog_uri", catalogURI),
		zap.String("catalog_name", config.Name),
		zap.String("warehouse", config.WarehouseLocation))

	props := icebergGo.Properties{
		"uri": catalogURI,
	}

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
	if config.Properties != nil {
		props["s3.path-style-access"] = "true"
	}

	for key, value := range config.Properties {
		props[key] = value
	}

	n.logger.Debug("Attempting Nessie catalog.Load with properties",
		zap.String("uri", catalogURI),
		zap.String("catalog_name", config.Name),
		zap.Any("properties", props))

	iceCatalog, err := catalog.Load(ctx, config.Name, props)
	if err != nil {
		n.logger.Error("Nessie catalog.Load failed",
			zap.String("uri", catalogURI),
			zap.String("catalog_name", config.Name),
			zap.Error(err))
		return fmt.Errorf("failed to load Nessie catalog: %w", err)
	}

	// Type assert to REST catalog
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

func (n *NessieCatalog) GetSchema(ctx context.Context, database, table string) (*core.Schema, error) {
	if n.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	identifier := catalog.ToIdentifier(fmt.Sprintf("%s.%s", database, table))
	tbl, err := n.catalog.LoadTable(ctx, identifier, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load table: %w", err)
	}

	n.logger.Debug("Successfully loaded table via catalog",
		zap.String("table", table),
		zap.String("location", tbl.Location()),
		zap.String("identifier", fmt.Sprintf("%s.%s", database, table)))

	iceSchema := tbl.Schema()
	var fields []core.Field

	n.logger.Debug("Table schema details",
		zap.String("table", table),
		zap.Int("schema_id", iceSchema.ID),
		zap.Int("field_count", len(iceSchema.Fields())))

	for _, field := range iceSchema.Fields() {
		coreField := convertIcebergFieldToCore(field)
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

func (n *NessieCatalog) WriteBulkBatches(ctx context.Context, database, table string, batches [][]*pool.Record) error {
	return fmt.Errorf("Nessie catalog WriteBulkBatches not implemented yet")
}

func (n *NessieCatalog) WriteData(ctx context.Context, database, table string, batch []*pool.Record) error {
	if n.catalog == nil {
		return fmt.Errorf("catalog not initialized for data writing")
	}

	identifier := catalog.ToIdentifier(fmt.Sprintf("%s.%s", database, table))
	tbl, err := n.catalog.LoadTable(ctx, identifier, nil)
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
	tempDest := &IcebergDestination{logger: n.logger}
	arrowSchema, err := tempDest.icebergToArrowSchema(icebergSchema)
	if err != nil {
		return fmt.Errorf("failed to convert schema: %w", err)
	}

	n.logger.Debug("Converting batch to Arrow record",
		zap.Int("batch_size", len(batch)),
		zap.Int("schema_fields", len(arrowSchema.Fields())))

	arrowRecord, err := tempDest.batchToArrowRecord(arrowSchema, batch)
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

func (n *NessieCatalog) Close(ctx context.Context) error {
	return nil
}

func (n *NessieCatalog) Health(ctx context.Context) error {
	if n.catalog == nil {
		return fmt.Errorf("catalog not initialized")
	}
	return nil
}

func (n *NessieCatalog) Type() string {
	return "nessie"
}

func (n *NessieCatalog) DeleteBranch(ctx context.Context, branchName string) error {
	return fmt.Errorf("DeleteBranch not implemented")
}

func (n *NessieCatalog) CreateBranch(ctx context.Context, branchName, fromRef string) error {
	return fmt.Errorf("CreateBranch not implemented yet")
}

func (n *NessieCatalog) MergeBranch(ctx context.Context, branchName, targetBranch string) error {
	return fmt.Errorf("MergeBranch not implemented yet")
}
func (n *NessieCatalog) ListBranches(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("ListBranches not implemented yet")
}
