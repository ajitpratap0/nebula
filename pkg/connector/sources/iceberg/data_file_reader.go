package iceberg

import (
	"context"
	"fmt"
	"time"

	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/table"
	"go.uber.org/zap"
)

// DataFileReader reads Parquet data files from Iceberg tables using table scan
type DataFileReader struct {
	table     *table.Table
	batchSize int
	logger    *zap.Logger
}

// NewDataFileReader creates a new data file reader
func NewDataFileReader(tbl *table.Table, batchSize int, logger *zap.Logger) *DataFileReader {
	logger.Info("Created data file reader using table scan")
	return &DataFileReader{
		table:     tbl,
		batchSize: batchSize,
		logger:    logger,
	}
}

// StreamRecords streams records from the table using scan with chunked processing
// This avoids loading all records into memory at once
func (dfr *DataFileReader) StreamRecords(ctx context.Context, recordChan chan<- *pool.Record) error {
	dfr.logger.Info("Starting table scan to stream records")

	// Use table scan - no limit to read all records
	scanOptions := []table.ScanOption{}

	// Perform scan and get Arrow records
	schema, recordsIter, err := dfr.table.Scan(scanOptions...).ToArrowRecords(ctx)
	if err != nil {
		return fmt.Errorf("failed to scan table: %w", err)
	}

	dfr.logger.Info("Table scan started", zap.Int("schema_fields", len(schema.Fields())))

	// Process records from iterator in streaming fashion
	recordCount := 0
	totalRows := int64(0)

	for record, err := range recordsIter {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			dfr.logger.Error("Error reading record from iterator", zap.Error(err))
			continue
		}
		if record == nil {
			continue
		}

		recordCount++
		totalRows += record.NumRows()
		dfr.logger.Debug("Processing Arrow record",
			zap.Int("record_num", recordCount),
			zap.Int64("num_rows", record.NumRows()))

		// Convert Arrow record to Nebula records and stream them
		if err := dfr.streamArrowRecordToChannel(ctx, record, schema, recordChan); err != nil {
			record.Release()
			return fmt.Errorf("failed to stream Arrow record: %w", err)
		}

		record.Release()
	}

	dfr.logger.Info("Table scan completed",
		zap.Int64("total_rows", totalRows),
		zap.Int("arrow_batches", recordCount))

	return nil
}

// StreamBatches streams batches of records from the table using scan
// This avoids loading all records into memory at once
func (dfr *DataFileReader) StreamBatches(ctx context.Context, batchSize int, batchChan chan<- []*pool.Record) error {
	dfr.logger.Info("Starting table scan to stream batches",
		zap.Int("batch_size", batchSize))

	// Use table scan - no limit to read all records
	scanOptions := []table.ScanOption{}

	// Perform scan and get Arrow records
	schema, recordsIter, err := dfr.table.Scan(scanOptions...).ToArrowRecords(ctx)
	if err != nil {
		return fmt.Errorf("failed to scan table: %w", err)
	}

	dfr.logger.Info("Table scan started", zap.Int("schema_fields", len(schema.Fields())))

	// Process records from iterator in batches
	recordCount := 0
	totalRows := int64(0)
	currentBatch := make([]*pool.Record, 0, batchSize)

	for record, err := range recordsIter {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			dfr.logger.Error("Error reading record from iterator", zap.Error(err))
			continue
		}
		if record == nil {
			continue
		}

		recordCount++
		totalRows += record.NumRows()
		dfr.logger.Debug("Processing Arrow record",
			zap.Int("record_num", recordCount),
			zap.Int64("num_rows", record.NumRows()))

		// Convert Arrow record to Nebula records
		nebRecords, err := dfr.convertArrowRecordToRecords(record, schema)
		if err != nil {
			dfr.logger.Error("Failed to convert Arrow record", zap.Error(err))
			record.Release()
			continue
		}
		record.Release()

		// Add to current batch
		for _, nebRecord := range nebRecords {
			currentBatch = append(currentBatch, nebRecord)

			// Send batch when full
			if len(currentBatch) >= batchSize {
				select {
				case batchChan <- currentBatch:
					currentBatch = make([]*pool.Record, 0, batchSize)
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}

	// Send remaining batch
	if len(currentBatch) > 0 {
		select {
		case batchChan <- currentBatch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	dfr.logger.Info("Table scan completed",
		zap.Int64("total_rows", totalRows),
		zap.Int("arrow_batches", recordCount))

	return nil
}

// streamArrowRecordToChannel converts Arrow record to Nebula records and sends them to channel
func (dfr *DataFileReader) streamArrowRecordToChannel(ctx context.Context, record arrow.Record, schema *arrow.Schema, recordChan chan<- *pool.Record) error {
	numRows := int(record.NumRows())

	// Convert each row to a Nebula record and stream it
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		nebRecord := models.NewRecordFromPool("iceberg")
		nebRecord.SetTimestamp(time.Now())

		// Extract values for each column
		for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
			field := schema.Field(colIdx)
			col := record.Column(colIdx)

			value := dfr.extractValue(col, rowIdx)
			if value != nil {
				nebRecord.SetData(field.Name, value)
			}
		}

		// Send record to channel
		select {
		case recordChan <- nebRecord:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// convertArrowRecordToRecords converts an Arrow record to Nebula records
func (dfr *DataFileReader) convertArrowRecordToRecords(record arrow.Record, schema *arrow.Schema) ([]*pool.Record, error) {
	numRows := int(record.NumRows())
	records := make([]*pool.Record, 0, numRows)

	// Convert each row to a Nebula record
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		nebRecord := models.NewRecordFromPool("iceberg")
		nebRecord.SetTimestamp(time.Now())

		// Extract values for each column
		for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
			field := schema.Field(colIdx)
			col := record.Column(colIdx)

			value := dfr.extractValue(col, rowIdx)
			if value != nil {
				nebRecord.SetData(field.Name, value)
			}
		}

		records = append(records, nebRecord)
	}

	return records, nil
}

// convertArrowTableToRecords converts an Arrow table to Nebula records (legacy, not used)
func (dfr *DataFileReader) convertArrowTableToRecords(table arrow.Table) ([]*pool.Record, error) {
	numRows := int(table.NumRows())
	records := make([]*pool.Record, 0, numRows)

	schema := table.Schema()
	columns := make([]arrow.Array, table.NumCols())

	// Get all columns
	for i := int64(0); i < table.NumCols(); i++ {
		col := table.Column(int(i))
		if col.Len() > 0 {
			// Concatenate all chunks into a single array
			chunks := make([]arrow.Array, col.Len())
			for j := 0; j < col.Len(); j++ {
				chunks[j] = col.Data().Chunk(j)
			}
			concatenated, err := array.Concatenate(chunks, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to concatenate arrays: %w", err)
			}
			columns[i] = concatenated
		}
	}

	// Release all columns after use
	defer func() {
		for _, col := range columns {
			if col != nil {
				col.Release()
			}
		}
	}()

	// Convert each row to a record
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		record := models.NewRecordFromPool("iceberg")
		record.SetTimestamp(time.Now())

		// Extract values for each column
		for colIdx, field := range schema.Fields() {
			if columns[colIdx] == nil {
				continue
			}

			value := dfr.extractValue(columns[colIdx], rowIdx)
			if value != nil {
				record.SetData(field.Name, value)
			}
		}

		records = append(records, record)
	}

	return records, nil
}

// extractValue extracts a value from an Arrow array at a specific index
func (dfr *DataFileReader) extractValue(arr arrow.Array, index int) interface{} {
	if arr.IsNull(index) {
		return nil
	}

	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(index)
	case *array.Int32:
		return a.Value(index)
	case *array.Int64:
		return a.Value(index)
	case *array.Float32:
		return a.Value(index)
	case *array.Float64:
		return a.Value(index)
	case *array.String:
		return a.Value(index)
	case *array.Binary:
		return a.Value(index)
	case *array.Date32:
		// Convert Date32 to time.Time
		days := a.Value(index)
		return time.Unix(int64(days)*86400, 0).UTC()
	case *array.Timestamp:
		// Convert timestamp to time.Time
		ts := a.Value(index)
		tsType := a.DataType().(*arrow.TimestampType)
		switch tsType.Unit {
		case arrow.Second:
			return time.Unix(int64(ts), 0).UTC()
		case arrow.Millisecond:
			return time.Unix(0, int64(ts)*1e6).UTC()
		case arrow.Microsecond:
			return time.Unix(0, int64(ts)*1e3).UTC()
		case arrow.Nanosecond:
			return time.Unix(0, int64(ts)).UTC()
		}
	case *array.List:
		// Convert list to slice
		start, end := a.ValueOffsets(index)
		valueArr := a.ListValues()
		values := make([]interface{}, end-start)
		for i := start; i < end; i++ {
			values[i-start] = dfr.extractValue(valueArr, int(i))
		}
		return values
	case *array.Struct:
		// Convert struct to map
		structType := a.DataType().(*arrow.StructType)
		result := make(map[string]interface{})
		for i, field := range structType.Fields() {
			fieldArr := a.Field(i)
			result[field.Name] = dfr.extractValue(fieldArr, index)
		}
		return result
	default:
		dfr.logger.Warn("Unsupported Arrow type",
			zap.String("type", fmt.Sprintf("%T", arr)))
		return nil
	}

	return nil
}
