package iceberg

import (
	"fmt"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/shubham-tomar/iceberg-go"
)

// SchemaMapper handles conversion between Nebula and Iceberg schemas
type SchemaMapper struct{}

// NewSchemaMapper creates a new schema mapper
func NewSchemaMapper() *SchemaMapper {
	return &SchemaMapper{}
}

// ToIcebergSchema converts a Nebula schema to an Iceberg schema
func (sm *SchemaMapper) ToIcebergSchema(nebulaSchema *core.Schema) (*iceberg.Schema, error) {
	if nebulaSchema == nil {
		return nil, fmt.Errorf("nebula schema cannot be nil")
	}
	
	fields := make([]iceberg.NestedField, 0, len(nebulaSchema.Fields))
	
	for i, field := range nebulaSchema.Fields {
		icebergField, err := sm.convertField(field, i+1) // Iceberg field IDs start from 1
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", field.Name, err)
		}
		fields = append(fields, icebergField)
	}
	
	return iceberg.NewSchema(0, fields...), nil
}

// convertField converts a single Nebula field to an Iceberg field
func (sm *SchemaMapper) convertField(field *core.Field, fieldID int) (iceberg.NestedField, error) {
	icebergType, err := sm.convertType(field.Type)
	if err != nil {
		return iceberg.NestedField{}, fmt.Errorf("failed to convert type for field %s: %w", field.Name, err)
	}
	
	return iceberg.NestedField{
		ID:       fieldID,
		Name:     field.Name,
		Type:     icebergType,
		Required: !field.Nullable,
	}, nil
}

// convertType converts Nebula field types to Iceberg types
func (sm *SchemaMapper) convertType(fieldType core.FieldType) (iceberg.Type, error) {
	switch fieldType {
	case core.FieldTypeString:
		return iceberg.PrimitiveTypes.String, nil
	case core.FieldTypeInt32:
		return iceberg.PrimitiveTypes.Int32, nil
	case core.FieldTypeInt64:
		return iceberg.PrimitiveTypes.Int64, nil
	case core.FieldTypeFloat32:
		return iceberg.PrimitiveTypes.Float32, nil
	case core.FieldTypeFloat64:
		return iceberg.PrimitiveTypes.Float64, nil
	case core.FieldTypeBoolean:
		return iceberg.PrimitiveTypes.Bool, nil
	case core.FieldTypeBytes:
		return iceberg.PrimitiveTypes.Binary, nil
	case core.FieldTypeTimestamp:
		// Use timestamp with microsecond precision
		return iceberg.PrimitiveTypes.TimestampTz, nil
	case core.FieldTypeDate:
		return iceberg.PrimitiveTypes.Date, nil
	case core.FieldTypeTime:
		// Use time with microsecond precision
		return iceberg.PrimitiveTypes.Time, nil
	case core.FieldTypeDecimal:
		// Default decimal with precision 38, scale 9
		return iceberg.DecimalTypeOf(38, 9), nil
	case core.FieldTypeUUID:
		return iceberg.PrimitiveTypes.UUID, nil
	default:
		return nil, fmt.Errorf("unsupported field type: %v", fieldType)
	}
}

// FromIcebergSchema converts an Iceberg schema to a Nebula schema
func (sm *SchemaMapper) FromIcebergSchema(icebergSchema *iceberg.Schema) (*core.Schema, error) {
	if icebergSchema == nil {
		return nil, fmt.Errorf("iceberg schema cannot be nil")
	}
	
	fields := make([]*core.Field, 0, len(icebergSchema.Fields()))
	
	for _, field := range icebergSchema.Fields() {
		nebulaField, err := sm.convertIcebergField(field)
		if err != nil {
			return nil, fmt.Errorf("failed to convert Iceberg field %s: %w", field.Name, err)
		}
		fields = append(fields, nebulaField)
	}
	
	return &core.Schema{
		Fields: fields,
	}, nil
}

// convertIcebergField converts an Iceberg field to a Nebula field
func (sm *SchemaMapper) convertIcebergField(field iceberg.NestedField) (*core.Field, error) {
	nebulaType, err := sm.convertIcebergType(field.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to convert Iceberg type: %w", err)
	}
	
	return &core.Field{
		Name:     field.Name,
		Type:     nebulaType,
		Nullable: !field.Required,
	}, nil
}

// convertIcebergType converts Iceberg types to Nebula field types
func (sm *SchemaMapper) convertIcebergType(icebergType iceberg.Type) (core.FieldType, error) {
	switch t := icebergType.(type) {
	case iceberg.StringType:
		return core.FieldTypeString, nil
	case iceberg.Int32Type:
		return core.FieldTypeInt32, nil
	case iceberg.Int64Type:
		return core.FieldTypeInt64, nil
	case iceberg.Float32Type:
		return core.FieldTypeFloat32, nil
	case iceberg.Float64Type:
		return core.FieldTypeFloat64, nil
	case iceberg.BooleanType:
		return core.FieldTypeBoolean, nil
	case iceberg.BinaryType, iceberg.FixedType:
		return core.FieldTypeBytes, nil
	case iceberg.TimestampType, iceberg.TimestampTzType:
		return core.FieldTypeTimestamp, nil
	case iceberg.DateType:
		return core.FieldTypeDate, nil
	case iceberg.TimeType:
		return core.FieldTypeTime, nil
	case iceberg.DecimalType:
		return core.FieldTypeDecimal, nil
	case iceberg.UUIDType:
		return core.FieldTypeUUID, nil
	default:
		return "", fmt.Errorf("unsupported Iceberg type: %T", t)
	}
}

// GetTableIdentifier creates an Iceberg table identifier from database and table name
func (sm *SchemaMapper) GetTableIdentifier(database, table string) []string {
	if database == "" {
		return []string{table}
	}
	return []string{database, table}
}
