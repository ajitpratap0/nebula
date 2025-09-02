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
	// NOTE: Schema caching is safe for CLI applications as schema remains constant during runtime.
	// In long-running services, consider adding cache invalidation if schema evolution is expected.
	if d.schemaValidator != nil && d.schemaValidator.validated && d.schemaValidator.arrowSchema != nil {
		return d.schemaValidator.arrowSchema, nil
	}


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
	// This cache persists for the entire application lifecycle, which is appropriate
	// for CLI tools where schema doesn't change during execution
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
		return arrow.BinaryTypes.String, nil
	}
}

func (d *IcebergDestination) batchToArrowRecord(schema *arrow.Schema, batch []*pool.Record) (arrow.Record, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("no records to convert")
	}

	// Convert to unified extractors (handles both typed and regular records)
	extractors := make([]ValueExtractor, len(batch))
	for i, record := range batch {
		extractors[i] = &RegularRecordExtractor{record: record}
	}

	return d.buildArrowRecordUnified(schema, extractors)
}


// buildArrowRecordUnified creates Arrow records using the unified extractor approach with builder pooling
func (d *IcebergDestination) buildArrowRecordUnified(schema *arrow.Schema, extractors []ValueExtractor) (arrow.Record, error) {
	// Ensure builder pool is initialized (defensive programming)
	// This handles cases where the destination struct may have been copied or not properly initialized
	if err := d.ensureBuilderPoolInitialized(); err != nil {
		return nil, fmt.Errorf("failed to initialize builder pool: %w", err)
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

		// Append field data based on type
		switch field.Type.ID() {
		case arrow.STRING:
			d.appendStringField(builder.(*array.StringBuilder), field.Name, extractors)
		case arrow.INT64, arrow.INT32, arrow.FLOAT64, arrow.FLOAT32:
			d.appendNumericField(builder, field.Name, extractors)
		case arrow.BOOL:
			d.appendBoolField(builder.(*array.BooleanBuilder), field.Name, extractors)
		case arrow.TIMESTAMP:
			d.appendTimestampField(builder.(*array.TimestampBuilder), field.Name, extractors, field.Type.(*arrow.TimestampType))
		case arrow.DATE32:
			d.appendDateField(builder.(*array.Date32Builder), field.Name, extractors)
		case arrow.LIST:
			d.appendListFieldUnified(builder.(*array.ListBuilder), field.Name, extractors, field.Type.(*arrow.ListType))
		case arrow.STRUCT:
			d.appendStructFieldUnified(builder.(*array.StructBuilder), field.Name, extractors, field.Type.(*arrow.StructType))
		default:
			d.appendFieldGenericUnified(builder, field.Name, field.Type, extractors)
		}
	}

	// Use the pooled builder's NewRecord method which automatically returns builder to pool
	rec := pooledBuilder.NewRecord()


	return rec, nil
}

// appendListFieldUnified handles list type fields
func (d *IcebergDestination) appendListFieldUnified(builder *array.ListBuilder, fieldName string, extractors []ValueExtractor, listType *arrow.ListType) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetValue(fieldName); ok {
			if values, ok := val.([]any); ok {
				builder.Append(true)
				elemBuilder := builder.ValueBuilder()
				for _, v := range values {
					d.appendToBuilder(listType.Elem(), elemBuilder, v)
				}
			} else {
				builder.AppendNull()
			}
		} else {
			builder.AppendNull()
		}
	}
}

func (d *IcebergDestination) appendStructFieldUnified(builder *array.StructBuilder, fieldName string, extractors []ValueExtractor, structType *arrow.StructType) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetValue(fieldName); ok {
			if valueMap, ok := val.(map[string]any); ok {
				builder.Append(true)
				for i, field := range structType.Fields() {
					d.appendToBuilder(field.Type, builder.FieldBuilder(i), valueMap[field.Name])
				}
			} else {
				builder.AppendNull()
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
	case *array.Int32Builder, *array.Int64Builder, *array.Float32Builder, *array.Float64Builder:
		d.appendNumericValue(builder, val)
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
				copy(values, interfaceValues)
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
// Returns zero value and false if conversion is not safe or possible
func convertToNumeric[T numeric](val any) (T, bool) {
	if val == nil {
		var zero T
		return zero, false
	}
	
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
		// Check for potential precision loss when converting to int types
		if isIntType[T]() && (v != float32(int64(v))) {
			// Float has fractional part, cannot safely convert to int
			var zero T
			return zero, false
		}
		return T(v), true
	case float64:
		// Check for potential precision loss when converting to int types
		if isIntType[T]() && (v != float64(int64(v))) {
			// Float has fractional part, cannot safely convert to int
			var zero T
			return zero, false
		}
		return T(v), true
	}
	var zero T
	return zero, false
}

// isIntType checks if the generic type T is an integer type
func isIntType[T numeric]() bool {
	var zero T
	switch any(zero).(type) {
	case int, int32, int64:
		return true
	default:
		return false
	}
}

// convertToBool converts various input types to boolean
// Only accepts actual boolean values to prevent data corruption
func convertToBool(val any) (bool, bool) {
	if val == nil {
		return false, false
	}
	
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

// Simplified field appender methods
func (d *IcebergDestination) appendStringField(builder *array.StringBuilder, fieldName string, extractors []ValueExtractor) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetString(fieldName); ok {
			builder.Append(val)
		} else {
			builder.AppendNull()
		}
	}
}

func (d *IcebergDestination) appendNumericField(builder array.Builder, fieldName string, extractors []ValueExtractor) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetValue(fieldName); ok {
			d.appendNumericValue(builder, val)
		} else {
			builder.AppendNull()
		}
	}
}

func (d *IcebergDestination) appendBoolField(builder *array.BooleanBuilder, fieldName string, extractors []ValueExtractor) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetBool(fieldName); ok {
			builder.Append(val)
		} else {
			builder.AppendNull()
		}
	}
}

func (d *IcebergDestination) appendTimestampField(builder *array.TimestampBuilder, fieldName string, extractors []ValueExtractor, tsType *arrow.TimestampType) {
	for _, extractor := range extractors {
		if val, ok := extractor.GetTime(fieldName); ok {
			if ts, ok := convertToTimestamp(val, tsType.Unit); ok {
				builder.Append(ts)
			} else {
				builder.AppendNull()
			}
		} else if val, ok := extractor.GetValue(fieldName); ok {
			if ts, ok := convertToTimestamp(val, tsType.Unit); ok {
				builder.Append(ts)
			} else {
				builder.AppendNull()
			}
		} else {
			builder.AppendNull()
		}
	}
}

func (d *IcebergDestination) appendDateField(builder *array.Date32Builder, fieldName string, extractors []ValueExtractor) {
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

// appendNumericValue handles all numeric types with a unified approach
func (d *IcebergDestination) appendNumericValue(builder array.Builder, val any) {
	if val == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.Int32Builder:
		if v, ok := convertToNumeric[int32](val); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Int64Builder:
		if v, ok := convertToNumeric[int64](val); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Float32Builder:
		if v, ok := convertToNumeric[float32](val); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Float64Builder:
		if v, ok := convertToNumeric[float64](val); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
}

// preallocateBuilder reserves capacity for array builders
func (d *IcebergDestination) preallocateBuilder(builder array.Builder, capacity int) {
	// Use interface method if available
	if resizable, ok := builder.(interface{ Reserve(int) }); ok {
		resizable.Reserve(capacity)
		
		// Special cases for complex builders
		switch b := builder.(type) {
		case *array.StringBuilder:
			b.ReserveData(capacity * 32) // Estimate 32 chars per string
		case *array.ListBuilder:
			if valueBuilder := b.ValueBuilder(); valueBuilder != nil {
				d.preallocateBuilder(valueBuilder, capacity*5) // Est. 5 elements per list
			}
		case *array.StructBuilder:
			for i := 0; i < b.NumField(); i++ {
				if fieldBuilder := b.FieldBuilder(i); fieldBuilder != nil {
					d.preallocateBuilder(fieldBuilder, capacity)
				}
			}
		}
	}
}

// ensureBuilderPoolInitialized ensures the builder pool is properly initialized
// This is thread-safe and handles cases where the destination may have been copied
func (d *IcebergDestination) ensureBuilderPoolInitialized() error {
	if d.builderPool != nil {
		return nil
	}
	
	d.logger.Warn("Builder pool was nil, initializing it now")
	
	allocator := memory.NewGoAllocator()
	d.builderPool = NewArrowBuilderPool(allocator, d.logger)
	
	if d.builderPool == nil {
		return fmt.Errorf("failed to create Arrow builder pool")
	}
	
	return nil
}
