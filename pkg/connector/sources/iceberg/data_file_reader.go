package iceberg

import (
	"context"
	"fmt"
	"time"

	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/shubham-tomar/iceberg-go/table"
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

// ReadAllRecords reads all records from the table using scan
func (dfr *DataFileReader) ReadAllRecords(ctx context.Context) ([]*pool.Record, error) {
	dfr.logger.Info("Starting table scan to read all records")

	// Use table scan with limit if batch size is set
	scanOptions := []table.ScanOption{}
	if dfr.batchSize > 0 {
		scanOptions = append(scanOptions, table.WithLimit(int64(dfr.batchSize)))
	}

	// Perform scan and get Arrow records
	schema, recordsIter, err := dfr.table.Scan(scanOptions...).ToArrowRecords(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to scan table: %w", err)
	}

	dfr.logger.Info("Table scan started", zap.Int("schema_fields", len(schema.Fields())))

	// Process all records from iterator
	var allRecords []*pool.Record
	recordCount := 0

	for record, err := range recordsIter {
		if err != nil {
			dfr.logger.Error("Error reading record from iterator", zap.Error(err))
			continue
		}
		if record == nil {
			continue
		}

		recordCount++
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

		allRecords = append(allRecords, nebRecords...)
		record.Release()
	}

	dfr.logger.Info("Table scan completed",
		zap.Int("total_records", len(allRecords)),
		zap.Int("arrow_batches", recordCount))

	return allRecords, nil
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
			defer columns[i].Release()
		}
	}

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
