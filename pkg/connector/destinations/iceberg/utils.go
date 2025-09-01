package iceberg

import (
	"fmt"
	"strings"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	icebergGo "github.com/shubham-tomar/iceberg-go"
	"go.uber.org/zap"
)

func (d *IcebergDestination) extractConfig(config *config.BaseConfig) error {
	creds := config.Security.Credentials
	if creds == nil {
		return fmt.Errorf("missing security credentials")
	}

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

func (d *IcebergDestination) icebergToArrowSchema(icebergSchema *icebergGo.Schema) (*arrow.Schema, error) {
	// Check if already validated and converted
	if d.schemaValidator != nil && d.schemaValidator.validated && d.schemaValidator.arrowSchema != nil {
		return d.schemaValidator.arrowSchema, nil
	}

	d.logger.Debug("Converting Iceberg to Arrow schema",
		zap.Int("schema_id", icebergSchema.ID),
		zap.Int("field_count", len(icebergSchema.Fields())))

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

	arrowSchema := arrow.NewSchema(fields, nil)

	// Store for reuse during this CLI run
	if d.schemaValidator != nil {
		d.schemaValidator.icebergSchema = icebergSchema
		d.schemaValidator.arrowSchema = arrowSchema
		d.schemaValidator.validated = true
	}

	return arrowSchema, nil
}

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
		d.logger.Debug("Converting Iceberg timestamp type",
			zap.String("type", t.String()))
		// Create timestamp type without timezone to match Iceberg requirements
		return &arrow.TimestampType{
			Unit:     arrow.Microsecond,
			TimeZone: "", // must be empty to indicate *no time zone*
		}, nil
	case icebergGo.DateType:
		return arrow.FixedWidthTypes.Date32, nil
	case *icebergGo.ListType:
		// Handle list types recursively
		elemArrowType, err := d.icebergTypeToArrowType(t.Element)
		if err != nil {
			return nil, fmt.Errorf("failed to get list element type: %w", err)
		}
		return arrow.ListOf(elemArrowType), nil
	case *icebergGo.StructType:
		// Handle struct types recursively
		var fields []arrow.Field
		for _, f := range t.Fields() {
			fieldArrowType, err := d.icebergTypeToArrowType(f.Type)
			if err != nil {
				return nil, fmt.Errorf("failed to convert struct field '%s': %w", f.Name, err)
			}
			fields = append(fields, arrow.Field{
				Name:     f.Name,
				Type:     fieldArrowType,
				Nullable: !f.Required,
			})
		}
		return arrow.StructOf(fields...), nil
	default:
		d.logger.Warn("Unsupported Iceberg type, defaulting to string",
			zap.String("type", t.String()))
		return arrow.BinaryTypes.String, nil
	}
}

func (d *IcebergDestination) batchToArrowRecord(schema *arrow.Schema, batch []*pool.Record) (arrow.Record, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("no records to convert")
	}

	d.logger.Debug("Starting optimized Arrow conversion",
		zap.Int("num_records", len(batch)),
		zap.Int("num_fields", len(schema.Fields())))

	// Check if we can use TypedRecords for better performance
	if typedBatch := d.extractTypedRecords(batch); typedBatch != nil {
		return d.batchToArrowRecordTyped(schema, typedBatch)
	}

	// Fallback to regular record processing
	return d.batchToArrowRecordRegular(schema, batch)
}

// extractTypedRecords attempts to extract TypedRecords from regular Records
func (d *IcebergDestination) extractTypedRecords(batch []*pool.Record) []*pool.TypedRecord {
	typedBatch := make([]*pool.TypedRecord, len(batch))

	for i, record := range batch {
		// Try to convert regular record to TypedRecord for better performance
		typedRecord := pool.GetTypedRecord()

		// Copy data to typed fields based on type inference
		for key, value := range record.Data {
			switch v := value.(type) {
			case string:
				typedRecord.SetString(key, v)
			case int64:
				typedRecord.SetInt(key, v)
			case int:
				typedRecord.SetInt(key, int64(v))
			case int32:
				typedRecord.SetInt(key, int64(v))
			case float64:
				typedRecord.SetFloat(key, v)
			case float32:
				typedRecord.SetFloat(key, float64(v))
			case bool:
				typedRecord.SetBool(key, v)
			case time.Time:
				typedRecord.SetTime(key, v)
			case []byte:
				typedRecord.SetBytes(key, v)
			default:
				// Keep in generic map for unsupported types
				typedRecord.SetData(key, value)
			}
		}

		// Copy metadata
		typedRecord.Metadata = record.Metadata
		typedRecord.ID = record.ID
		typedRecord.Schema = record.Schema

		typedBatch[i] = typedRecord
	}

	return typedBatch
}

// batchToArrowRecordTyped handles TypedRecord conversion for optimal performance
func (d *IcebergDestination) batchToArrowRecordTyped(schema *arrow.Schema, batch []*pool.TypedRecord) (arrow.Record, error) {
	d.logger.Debug("Using TypedRecord optimization", zap.Int("batch_size", len(batch)))

	// Convert TypedRecords to unified extractors
	extractors := make([]ValueExtractor, len(batch))
	for i, record := range batch {
		extractors[i] = &TypedRecordExtractor{record: record}
	}

	// Clean up TypedRecords after conversion
	defer func() {
		for _, tr := range batch {
			tr.Release()
		}
	}()

	return d.buildArrowRecordUnified(schema, extractors)
}

// batchToArrowRecordRegular handles regular Record conversion
func (d *IcebergDestination) batchToArrowRecordRegular(schema *arrow.Schema, batch []*pool.Record) (arrow.Record, error) {
	// Convert Records to unified extractors
	extractors := make([]ValueExtractor, len(batch))
	for i, record := range batch {
		extractors[i] = &RegularRecordExtractor{record: record}
	}

	return d.buildArrowRecordUnified(schema, extractors)
}

// buildArrowRecordUnified creates Arrow records using the unified extractor approach with builder pooling
func (d *IcebergDestination) buildArrowRecordUnified(schema *arrow.Schema, extractors []ValueExtractor) (arrow.Record, error) {
	// Initialize builder pool if not already done (defensive programming)
	if d.builderPool == nil {
		d.logger.Warn("Builder pool was nil, initializing it now")
		allocator := memory.NewGoAllocator()
		d.builderPool = NewArrowBuilderPool(allocator, d.logger)
	}

	// Get pooled builder to eliminate allocation overhead
	pooledBuilder := d.builderPool.Get(schema)
	if pooledBuilder == nil {
		return nil, fmt.Errorf("failed to get pooled builder")
	}

	recBuilder := pooledBuilder.Builder()

	// Pre-allocate builders and populate using unified approach
	for i, field := range schema.Fields() {
		builder := recBuilder.Field(i)
		d.preallocateBuilder(builder, len(extractors))

		// Use unified field appenders for all types
		switch field.Type.ID() {
		case arrow.STRING:
			appendStringField(builder.(*array.StringBuilder), field.Name, extractors)
		case arrow.INT64:
			appendInt64Field(builder.(*array.Int64Builder), field.Name, extractors)
		case arrow.INT32:
			appendInt32Field(builder.(*array.Int32Builder), field.Name, extractors)
		case arrow.FLOAT64:
			appendFloat64Field(builder.(*array.Float64Builder), field.Name, extractors)
		case arrow.FLOAT32:
			appendFloat32Field(builder.(*array.Float32Builder), field.Name, extractors)
		case arrow.BOOL:
			appendBoolField(builder.(*array.BooleanBuilder), field.Name, extractors)
		case arrow.TIMESTAMP:
			appendTimestampField(builder.(*array.TimestampBuilder), field.Name, extractors, field.Type.(*arrow.TimestampType))
		case arrow.DATE32:
			appendDate32Field(builder.(*array.Date32Builder), field.Name, extractors)
		case arrow.LIST:
			d.appendListFieldUnified(builder.(*array.ListBuilder), field.Name, extractors, field.Type.(*arrow.ListType))
		case arrow.STRUCT:
			d.appendStructFieldUnified(builder.(*array.StructBuilder), field.Name, extractors, field.Type.(*arrow.StructType))
		default:
			d.logger.Warn("Unsupported Arrow type, using generic fallback",
				zap.String("type", field.Type.String()))
			d.appendFieldGenericUnified(builder, field.Name, field.Type, extractors)
		}
	}

	// Use the pooled builder's NewRecord method which automatically returns builder to pool
	rec := pooledBuilder.NewRecord()

	d.logger.Debug("Unified Arrow record built with pooled builder",
		zap.Int64("rows", rec.NumRows()),
		zap.Int64("cols", rec.NumCols()))

	return rec, nil
}

// Unified complex type handlers
func (d *IcebergDestination) appendListFieldUnified(builder *array.ListBuilder, fieldName string, extractors []ValueExtractor, listType *arrow.ListType) {
	elemType := listType.Elem()

	for _, extractor := range extractors {
		if val, ok := extractor.GetValue(fieldName); ok {
			// Handle both []any and []interface{} for compatibility
			var values []any
			if vals, ok := val.([]any); ok {
				values = vals
			} else if interfaceVals, ok := val.([]interface{}); ok {
				values = make([]any, len(interfaceVals))
				for i, v := range interfaceVals {
					values[i] = v
				}
			} else {
				builder.AppendNull()
				continue
			}

			builder.Append(true)
			elemBuilder := builder.ValueBuilder()
			for _, v := range values {
				d.appendToBuilder(elemType, elemBuilder, v)
			}
		} else {
			builder.AppendNull()
		}
	}
}

func (d *IcebergDestination) appendStructFieldUnified(builder *array.StructBuilder, fieldName string, extractors []ValueExtractor, structType *arrow.StructType) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetValue(fieldName); ok {
			// Handle both map[string]any and map[string]interface{} for compatibility
			var valueMap map[string]any
			if valMap, ok := val.(map[string]any); ok {
				valueMap = valMap
			} else if interfaceMap, ok := val.(map[string]interface{}); ok {
				valueMap = make(map[string]any, len(interfaceMap))
				for k, v := range interfaceMap {
					valueMap[k] = v
				}
			} else {
				builder.AppendNull()
				continue
			}

			builder.Append(true)
			for i, field := range structType.Fields() {
				d.appendToBuilder(field.Type, builder.FieldBuilder(i), valueMap[field.Name])
			}
		} else {
			builder.AppendNull()
		}
	}
}

func (d *IcebergDestination) appendFieldGenericUnified(builder array.Builder, fieldName string, fieldType arrow.DataType, extractors []ValueExtractor) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetValue(fieldName); ok {
			d.appendToBuilder(fieldType, builder, val)
		} else {
			builder.AppendNull()
		}
	}
}

func (d *IcebergDestination) appendToBuilder(dt arrow.DataType, b array.Builder, val any) {
	if val == nil {
		b.AppendNull()
		return
	}

	switch builder := b.(type) {
	case *array.BooleanBuilder:
		if v, ok := convertToBool(val); ok {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	case *array.Int32Builder:
		if v, ok := convertToNumeric[int32](val); ok {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	case *array.Int64Builder:
		if v, ok := convertToNumeric[int64](val); ok {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	case *array.Float32Builder:
		if v, ok := convertToNumeric[float32](val); ok {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	case *array.Float64Builder:
		if v, ok := convertToNumeric[float64](val); ok {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	case *array.StringBuilder:
		builder.Append(convertToString(val))
	case *array.TimestampBuilder:
		tsType := dt.(*arrow.TimestampType)
		if ts, ok := convertToTimestamp(val, tsType.Unit); ok {
			builder.Append(ts)
		} else {
			d.logger.Error("Failed to parse timestamp - data may be lost",
				zap.Any("value", val))
			builder.AppendNull()
		}
	case *array.Date32Builder:
		if date, ok := convertToDate32(val); ok {
			builder.Append(date)
		} else {
			builder.AppendNull()
		}
	case *array.ListBuilder:
		// Handle list/array values
		values, ok := val.([]any)
		if !ok {
			// Try to convert from []interface{} for backward compatibility
			if interfaceValues, ok := val.([]interface{}); ok {
				values = make([]any, len(interfaceValues))
				for i, v := range interfaceValues {
					values[i] = v
				}
			} else {
				builder.AppendNull()
				return
			}
		}

		builder.Append(true)
		elemBuilder := builder.ValueBuilder()
		elemType := dt.(*arrow.ListType).Elem()

		for _, v := range values {
			d.appendToBuilder(elemType, elemBuilder, v)
		}
	case *array.StructBuilder:
		// Handle struct/object values
		valueMap, ok := val.(map[string]any)
		if !ok {
			// Try to convert from map[string]interface{} for backward compatibility
			if interfaceMap, ok := val.(map[string]interface{}); ok {
				valueMap = make(map[string]any, len(interfaceMap))
				for k, v := range interfaceMap {
					valueMap[k] = v
				}
			} else {
				builder.AppendNull()
				return
			}
		}

		builder.Append(true)
		structType := dt.(*arrow.StructType)
		for i, field := range structType.Fields() {
			d.appendToBuilder(field.Type, builder.FieldBuilder(i), valueMap[field.Name])
		}
	default:
		d.logger.Warn("Unsupported Arrow builder type",
			zap.String("type", fmt.Sprintf("%T", builder)))
		b.AppendNull()
	}
}

// Type conversion utilities for zero-copy performance
type numeric interface {
	int | int32 | int64 | float32 | float64
}

// convertToTimestamp converts various input types to Arrow timestamp with proper unit handling
func convertToTimestamp(val interface{}, unit arrow.TimeUnit) (arrow.Timestamp, bool) {
	getTimestampValue := func(t time.Time) arrow.Timestamp {
		switch unit {
		case arrow.Nanosecond:
			return arrow.Timestamp(t.UnixNano())
		case arrow.Microsecond:
			return arrow.Timestamp(t.UnixNano() / 1000)
		case arrow.Millisecond:
			return arrow.Timestamp(t.UnixNano() / 1000000)
		case arrow.Second:
			return arrow.Timestamp(t.Unix())
		default:
			return arrow.Timestamp(t.UnixMicro())
		}
	}

	switch v := val.(type) {
	case time.Time:
		return getTimestampValue(v), true
	case string:
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			time.RFC3339,
			"2006-01-02 15:04:05.999999",
			time.RFC3339Nano,
			"2006-01-02",
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return getTimestampValue(t), true
			}
		}
	}
	return 0, false
}

// convertToDate32 converts various input types to Arrow Date32
func convertToDate32(val interface{}) (arrow.Date32, bool) {
	switch v := val.(type) {
	case time.Time:
		days := int32(v.Unix() / 86400)
		return arrow.Date32(days), true
	case string:
		if t, err := time.Parse("2006-01-02", v); err == nil {
			days := int32(t.Unix() / 86400)
			return arrow.Date32(days), true
		}
	}
	return 0, false
}

// convertToNumeric converts various input types to specific numeric type with zero-copy performance
func convertToNumeric[T numeric](val interface{}) (T, bool) {
	switch v := val.(type) {
	case T:
		return v, true
	case int:
		return T(v), true
	case int32:
		return T(v), true
	case int64:
		return T(v), true
	case float32:
		return T(v), true
	case float64:
		return T(v), true
	}
	var zero T
	return zero, false
}

// convertToBool converts various input types to boolean
func convertToBool(val interface{}) (bool, bool) {
	if b, ok := val.(bool); ok {
		return b, true
	}
	return false, false
}

// convertToString converts any value to string representation
func convertToString(val any) string {
	if str, ok := val.(string); ok {
		return str
	}
	return fmt.Sprintf("%v", val)
}

// ValueExtractor provides unified interface for extracting values from different record types
type ValueExtractor interface {
	GetValue(fieldName string) (any, bool)
	GetString(fieldName string) (string, bool)
	GetInt(fieldName string) (int64, bool)
	GetFloat(fieldName string) (float64, bool)
	GetBool(fieldName string) (bool, bool)
	GetTime(fieldName string) (time.Time, bool)
}

// RegularRecordExtractor adapts pool.Record to ValueExtractor interface
type RegularRecordExtractor struct {
	record *pool.Record
}

func (r *RegularRecordExtractor) GetValue(fieldName string) (any, bool) {
	return r.record.GetData(fieldName)
}

func (r *RegularRecordExtractor) GetString(fieldName string) (string, bool) {
	if val, ok := r.record.GetData(fieldName); ok {
		return convertToString(val), true
	}
	return "", false
}

func (r *RegularRecordExtractor) GetInt(fieldName string) (int64, bool) {
	if val, ok := r.record.GetData(fieldName); ok {
		return convertToNumeric[int64](val)
	}
	return 0, false
}

func (r *RegularRecordExtractor) GetFloat(fieldName string) (float64, bool) {
	if val, ok := r.record.GetData(fieldName); ok {
		return convertToNumeric[float64](val)
	}
	return 0, false
}

func (r *RegularRecordExtractor) GetBool(fieldName string) (bool, bool) {
	if val, ok := r.record.GetData(fieldName); ok {
		return convertToBool(val)
	}
	return false, false
}

func (r *RegularRecordExtractor) GetTime(fieldName string) (time.Time, bool) {
	if val, ok := r.record.GetData(fieldName); ok {
		if t, ok := val.(time.Time); ok {
			return t, true
		}
	}
	return time.Time{}, false
}

// TypedRecordExtractor adapts pool.TypedRecord to ValueExtractor interface
type TypedRecordExtractor struct {
	record *pool.TypedRecord
}

func (t *TypedRecordExtractor) GetValue(fieldName string) (any, bool) {
	return t.record.GetData(fieldName)
}

func (t *TypedRecordExtractor) GetString(fieldName string) (string, bool) {
	if val, ok := t.record.GetString(fieldName); ok {
		return val, true
	}
	// Fallback to generic data map
	if val, ok := t.record.GetData(fieldName); ok {
		return convertToString(val), true
	}
	return "", false
}

func (t *TypedRecordExtractor) GetInt(fieldName string) (int64, bool) {
	if val, ok := t.record.GetInt(fieldName); ok {
		return val, true
	}
	// Fallback to generic data map
	if val, ok := t.record.GetData(fieldName); ok {
		return convertToNumeric[int64](val)
	}
	return 0, false
}

func (t *TypedRecordExtractor) GetFloat(fieldName string) (float64, bool) {
	if val, ok := t.record.GetFloat(fieldName); ok {
		return val, true
	}
	// Fallback to generic data map
	if val, ok := t.record.GetData(fieldName); ok {
		return convertToNumeric[float64](val)
	}
	return 0, false
}

func (t *TypedRecordExtractor) GetBool(fieldName string) (bool, bool) {
	if val, ok := t.record.GetBool(fieldName); ok {
		return val, true
	}
	// Fallback to generic data map
	if val, ok := t.record.GetData(fieldName); ok {
		return convertToBool(val)
	}
	return false, false
}

func (t *TypedRecordExtractor) GetTime(fieldName string) (time.Time, bool) {
	if val, ok := t.record.GetTime(fieldName); ok {
		return val, true
	}
	// Fallback to generic data map
	if val, ok := t.record.GetData(fieldName); ok {
		if time, ok := val.(time.Time); ok {
			return time, true
		}
	}
	return time.Time{}, false
}

// Generic field appender functions for zero-copy performance
func appendStringField(builder *array.StringBuilder, fieldName string, extractors []ValueExtractor) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetString(fieldName); ok {
			builder.Append(val)
		} else {
			builder.AppendNull()
		}
	}
}

func appendInt64Field(builder *array.Int64Builder, fieldName string, extractors []ValueExtractor) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetInt(fieldName); ok {
			builder.Append(val)
		} else {
			builder.AppendNull()
		}
	}
}

func appendInt32Field(builder *array.Int32Builder, fieldName string, extractors []ValueExtractor) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetInt(fieldName); ok {
			builder.Append(int32(val))
		} else {
			builder.AppendNull()
		}
	}
}

func appendFloat64Field(builder *array.Float64Builder, fieldName string, extractors []ValueExtractor) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetFloat(fieldName); ok {
			builder.Append(val)
		} else {
			builder.AppendNull()
		}
	}
}

func appendFloat32Field(builder *array.Float32Builder, fieldName string, extractors []ValueExtractor) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetFloat(fieldName); ok {
			builder.Append(float32(val))
		} else {
			builder.AppendNull()
		}
	}
}

func appendBoolField(builder *array.BooleanBuilder, fieldName string, extractors []ValueExtractor) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetBool(fieldName); ok {
			builder.Append(val)
		} else {
			builder.AppendNull()
		}
	}
}

func appendTimestampField(builder *array.TimestampBuilder, fieldName string, extractors []ValueExtractor, tsType *arrow.TimestampType) {
	unit := tsType.Unit

	for _, extractor := range extractors {
		if val, ok := extractor.GetTime(fieldName); ok {
			if ts, ok := convertToTimestamp(val, unit); ok {
				builder.Append(ts)
			} else {
				builder.AppendNull()
			}
		} else if val, ok := extractor.GetValue(fieldName); ok {
			if ts, ok := convertToTimestamp(val, unit); ok {
				builder.Append(ts)
			} else {
				builder.AppendNull()
			}
		} else {
			builder.AppendNull()
		}
	}
}

func appendDate32Field(builder *array.Date32Builder, fieldName string, extractors []ValueExtractor) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetTime(fieldName); ok {
			if date, ok := convertToDate32(val); ok {
				builder.Append(date)
			} else {
				builder.AppendNull()
			}
		} else if val, ok := extractor.GetValue(fieldName); ok {
			if date, ok := convertToDate32(val); ok {
				builder.Append(date)
			} else {
				builder.AppendNull()
			}
		} else {
			builder.AppendNull()
		}
	}
}

func convertIcebergFieldToCore(field icebergGo.NestedField) core.Field {
	var fieldType core.FieldType
	switch field.Type.String() {
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
		fieldType = core.FieldTypeString
	}

	return core.Field{
		Name:     field.Name,
		Type:     fieldType,
		Nullable: !field.Required,
	}
}

// Enhanced builder preallocation with better type support
func (d *IcebergDestination) preallocateBuilder(builder array.Builder, capacity int) {
	switch b := builder.(type) {
	case *array.StringBuilder:
		b.Reserve(capacity)
	case *array.Int64Builder:
		b.Reserve(capacity)
	case *array.Int32Builder:
		b.Reserve(capacity)
	case *array.Float64Builder:
		b.Reserve(capacity)
	case *array.Float32Builder:
		b.Reserve(capacity)
	case *array.BooleanBuilder:
		b.Reserve(capacity)
	case *array.TimestampBuilder:
		b.Reserve(capacity)
	case *array.Date32Builder:
		b.Reserve(capacity)
	case *array.ListBuilder:
		b.Reserve(capacity)
	case *array.StructBuilder:
		b.Reserve(capacity)
	}
}
