package iceberg

import (
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
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
	
	return arrow.NewSchema(fields, nil), nil
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

	d.logger.Debug("Starting batch to Arrow conversion",
		zap.Int("num_records", len(batch)),
		zap.Int("num_fields", len(schema.Fields())))

	// Log arrow schema details for debugging
	for i, field := range schema.Fields() {
		d.logger.Debug("Arrow schema field",
			zap.Int("index", i),
			zap.String("name", field.Name),
			zap.String("type", field.Type.String()),
			zap.Bool("nullable", field.Nullable))
	}

	if len(batch) > 0 {
		// Log sample record structure
		d.logger.Debug("Sample record data", zap.Any("record_data", batch[0].Data))
		
		// Log data types of each field in sample record
		for k, v := range batch[0].Data {
			d.logger.Debug("Record field type info",
				zap.String("field_name", k),
				zap.String("go_type", fmt.Sprintf("%T", v)),
				zap.Any("value", v))
		}
	}

	pool := memory.NewGoAllocator()
	recBuilder := array.NewRecordBuilder(pool, schema)
	defer recBuilder.Release()

	for i, field := range schema.Fields() {
		d.logger.Debug("Processing field", 
			zap.String("field_name", field.Name),
			zap.String("field_type", field.Type.String()))
		
		fieldBuilder := recBuilder.Field(i)
		for recordIdx, record := range batch {
			val, exists := record.GetData(field.Name)
			d.logger.Debug("Field value", 
				zap.String("field", field.Name),
				zap.Int("record_idx", recordIdx),
				zap.Bool("exists", exists),
				zap.Any("value", val))
			d.appendToBuilder(field.Type, fieldBuilder, val)
		}
	}

	rec := recBuilder.NewRecord()
	rec.Retain()
	
	d.logger.Debug("Arrow record built successfully",
		zap.Int64("rows", rec.NumRows()),
		zap.Int64("cols", rec.NumCols()))
	
	return rec, nil
}

func (d *IcebergDestination) appendToBuilder(dt arrow.DataType, b array.Builder, val interface{}) {
	if val == nil {
		b.AppendNull()
		return
	}

	switch builder := b.(type) {
	case *array.BooleanBuilder:
		if v, ok := val.(bool); ok {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	case *array.Int32Builder:
		if v, ok := val.(int); ok {
			builder.Append(int32(v))
		} else if v, ok := val.(int32); ok {
			builder.Append(v)
		} else if v, ok := val.(float64); ok {
			builder.Append(int32(v))
		} else {
			builder.AppendNull()
		}
	case *array.Int64Builder:
		if v, ok := val.(int64); ok {
			builder.Append(v)
		} else if v, ok := val.(int); ok {
			builder.Append(int64(v))
		} else if v, ok := val.(float64); ok {
			builder.Append(int64(v))
		} else {
			builder.AppendNull()
		}
	case *array.Float32Builder:
		if v, ok := val.(float32); ok {
			builder.Append(v)
		} else if v, ok := val.(float64); ok {
			builder.Append(float32(v))
		} else {
			builder.AppendNull()
		}
	case *array.Float64Builder:
		if v, ok := val.(float64); ok {
			builder.Append(v)
		} else if v, ok := val.(float32); ok {
			builder.Append(float64(v))
		} else {
			builder.AppendNull()
		}
	case *array.StringBuilder:
		if v, ok := val.(string); ok {
			builder.Append(v)
		} else {
			builder.Append(fmt.Sprintf("%v", val))
		}
	case *array.TimestampBuilder:
		tsType := dt.(*arrow.TimestampType)
		unit := tsType.Unit
		
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
				return arrow.Timestamp(t.UnixMicro()) // Default to microseconds
			}
		}

		if v, ok := val.(time.Time); ok {
			builder.Append(getTimestampValue(v))
		} else if v, ok := val.(string); ok {
			// Try multiple timestamp formats
			formats := []string{
				time.RFC3339,
				time.RFC3339Nano,
				"2006-01-02T15:04:05",
				"2006-01-02 15:04:05",
				"2006-01-02 15:04:05.999999",
				"2006-01-02",
			}

			var parsed bool
			for _, format := range formats {
				if t, err := time.Parse(format, v); err == nil {
					builder.Append(getTimestampValue(t))
					parsed = true
					break
				}
			}

			if !parsed {
				d.logger.Warn("Failed to parse timestamp string", zap.String("value", v))
				builder.AppendNull()
			}
		} else {
			builder.AppendNull()
		}
	case *array.Date32Builder:
		if v, ok := val.(time.Time); ok {
			days := int32(v.Unix() / 86400)
			builder.Append(arrow.Date32(days))
		} else if v, ok := val.(string); ok {
			if t, err := time.Parse("2006-01-02", v); err == nil {
				days := int32(t.Unix() / 86400)
				builder.Append(arrow.Date32(days))
			} else {
				builder.AppendNull()
			}
		} else {
			builder.AppendNull()
		}
	default:
		d.logger.Warn("Unsupported Arrow builder type",
			zap.String("type", fmt.Sprintf("%T", builder)))
		b.AppendNull()
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

