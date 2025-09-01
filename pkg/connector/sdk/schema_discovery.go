package sdk

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/errors"
)

// SchemaDiscovery provides automated schema discovery capabilities
type SchemaDiscovery struct {
	logger          *zap.Logger
	typeMapper      *TypeMapper
	inferenceConfig *TypeInferenceConfig
}

// NewSchemaDiscovery creates a new schema discovery instance
func NewSchemaDiscovery() *SchemaDiscovery {
	return &SchemaDiscovery{
		logger:          GetGlobalSDK().logger.With(zap.String("component", "schema_discovery")),
		typeMapper:      NewTypeMapper(),
		inferenceConfig: DefaultTypeInferenceConfig(),
	}
}

// TypeInferenceConfig configures type inference behavior
type TypeInferenceConfig struct {
	SampleSize          int      // Number of records to sample for inference
	ConfidenceThreshold float64  // Minimum confidence level for type inference
	EnableNullDetection bool     // Whether to detect nullable fields
	EnableTypePromotion bool     // Whether to promote types (int -> float -> string)
	DateTimeFormats     []string // Supported datetime formats for inference
	NumericPrecision    int      // Precision for numeric type detection
}

// DefaultTypeInferenceConfig returns default type inference configuration
func DefaultTypeInferenceConfig() *TypeInferenceConfig {
	return &TypeInferenceConfig{
		SampleSize:          1000,
		ConfidenceThreshold: 0.8,
		EnableNullDetection: true,
		EnableTypePromotion: true,
		DateTimeFormats: []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
			"15:04:05",
		},
		NumericPrecision: 10,
	}
}

// TypeMapper handles mapping between different type systems
type TypeMapper struct {
	logger *zap.Logger
}

// NewTypeMapper creates a new type mapper
func NewTypeMapper() *TypeMapper {
	return &TypeMapper{
		logger: GetGlobalSDK().logger.With(zap.String("component", "type_mapper")),
	}
}

// PostgreSQLSchemaDiscovery discovers schema from PostgreSQL database
type PostgreSQLSchemaDiscovery struct {
	*SchemaDiscovery
	pool *pgxpool.Pool
}

// NewPostgreSQLSchemaDiscovery creates PostgreSQL schema discovery
func NewPostgreSQLSchemaDiscovery(pool *pgxpool.Pool) *PostgreSQLSchemaDiscovery {
	return &PostgreSQLSchemaDiscovery{
		SchemaDiscovery: NewSchemaDiscovery(),
		pool:            pool,
	}
}

// DiscoverTableSchema discovers schema for a specific table
func (psd *PostgreSQLSchemaDiscovery) DiscoverTableSchema(ctx context.Context, tableName string) (*core.Schema, error) {
	// Query information_schema to get column metadata
	query := `
		SELECT 
			column_name,
			data_type,
			is_nullable,
			column_default,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			datetime_precision,
			udt_name
		FROM information_schema.columns 
		WHERE table_name = $1 
		ORDER BY ordinal_position`

	conn, err := psd.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConnection, "failed to acquire connection")
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, tableName)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeQuery, "failed to query table schema")
	}
	defer rows.Close() // Ignore close error

	var fields []core.Field
	for rows.Next() {
		var (
			columnName         string
			dataType           string
			isNullable         string
			columnDefault      sql.NullString
			characterMaxLength sql.NullInt64
			numericPrecision   sql.NullInt64
			numericScale       sql.NullInt64
			datetimePrecision  sql.NullInt64
			udtName            string
		)

		err := rows.Scan(
			&columnName,
			&dataType,
			&isNullable,
			&columnDefault,
			&characterMaxLength,
			&numericPrecision,
			&numericScale,
			&datetimePrecision,
			&udtName,
		)
		if err != nil {
			return nil, errors.Wrap(err, errors.ErrorTypeData, "failed to scan column metadata")
		}

		field := core.Field{
			Name:        columnName,
			Type:        psd.typeMapper.PostgreSQLToFieldType(dataType, udtName),
			Description: fmt.Sprintf("PostgreSQL %s column", dataType),
			Nullable:    isNullable == "YES",
		}

		// Set default value if exists
		if columnDefault.Valid {
			field.Default = columnDefault.String
		}

		// Add type information to description
		if characterMaxLength.Valid {
			field.Description = fmt.Sprintf("%s (max_length: %d)", field.Description, characterMaxLength.Int64)
		}
		if numericPrecision.Valid && numericScale.Valid {
			field.Description = fmt.Sprintf("%s (precision: %d, scale: %d)", field.Description, numericPrecision.Int64, numericScale.Int64)
		}

		fields = append(fields, field)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeData, "error reading schema rows")
	}

	schema := &core.Schema{
		Name:        tableName,
		Description: fmt.Sprintf("Auto-discovered schema for PostgreSQL table %s", tableName),
		Fields:      fields,
		Version:     1,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	psd.logger.Info("Discovered PostgreSQL table schema",
		zap.String("table", tableName),
		zap.Int("field_count", len(fields)))

	return schema, nil
}

// DiscoverDatabaseSchema discovers schemas for all tables in the database
func (psd *PostgreSQLSchemaDiscovery) DiscoverDatabaseSchema(ctx context.Context, schemaName string) (map[string]*core.Schema, error) {
	// Get list of tables
	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name`

	conn, err := psd.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConnection, "failed to acquire connection")
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, schemaName)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeQuery, "failed to query database tables")
	}
	defer rows.Close() // Ignore close error

	schemas := make(map[string]*core.Schema)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, errors.Wrap(err, errors.ErrorTypeData, "failed to scan table name")
		}

		tableSchema, err := psd.DiscoverTableSchema(ctx, tableName)
		if err != nil {
			psd.logger.Warn("Failed to discover schema for table",
				zap.String("table", tableName),
				zap.Error(err))
			continue
		}

		schemas[tableName] = tableSchema
	}

	psd.logger.Info("Discovered database schemas",
		zap.String("schema", schemaName),
		zap.Int("table_count", len(schemas)))

	return schemas, nil
}

// PostgreSQLToFieldType maps PostgreSQL types to core field types
func (tm *TypeMapper) PostgreSQLToFieldType(dataType, udtName string) core.FieldType {
	switch strings.ToLower(dataType) {
	case "smallint", "integer", "bigint", "serial", "bigserial":
		return core.FieldTypeInt
	case "real", "double precision", "numeric", "decimal":
		return core.FieldTypeFloat
	case "boolean":
		return core.FieldTypeBool
	case "timestamp", "timestamp with time zone", "timestamp without time zone", "date", "time":
		return core.FieldTypeTimestamp
	case "json", "jsonb":
		return core.FieldTypeJSON
	case "uuid":
		return core.FieldTypeString
	case "text", "character varying", "character", "varchar", "char":
		return core.FieldTypeString
	case "bytea":
		return core.FieldTypeString // Handle as base64 string
	default:
		tm.logger.Debug("Unknown PostgreSQL type, defaulting to string",
			zap.String("data_type", dataType),
			zap.String("udt_name", udtName))
		return core.FieldTypeString
	}
}

// DataSampleInference performs type inference from data samples
type DataSampleInference struct {
	*SchemaDiscovery
	samples []map[string]interface{}
}

// NewDataSampleInference creates data sample inference
func NewDataSampleInference(samples []map[string]interface{}) *DataSampleInference {
	return &DataSampleInference{
		SchemaDiscovery: NewSchemaDiscovery(),
		samples:         samples,
	}
}

// InferSchema infers schema from data samples
func (dsi *DataSampleInference) InferSchema(schemaName string) (*core.Schema, error) {
	if len(dsi.samples) == 0 {
		return nil, errors.New(errors.ErrorTypeData, "no data samples provided for inference")
	}

	// Collect all field names
	fieldNames := make(map[string]bool)
	for _, sample := range dsi.samples {
		for fieldName := range sample {
			fieldNames[fieldName] = true
		}
	}

	var fields []core.Field
	for fieldName := range fieldNames {
		field, err := dsi.inferFieldType(fieldName)
		if err != nil {
			dsi.logger.Warn("Failed to infer type for field",
				zap.String("field", fieldName),
				zap.Error(err))
			// Default to string type
			field = core.Field{
				Name:        fieldName,
				Type:        core.FieldTypeString,
				Description: "Inferred as string (fallback)",
				Nullable:    true,
			}
		}
		fields = append(fields, field)
	}

	schema := &core.Schema{
		Name:        schemaName,
		Description: fmt.Sprintf("Auto-inferred schema from %d samples", len(dsi.samples)),
		Fields:      fields,
		Version:     1,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	dsi.logger.Info("Inferred schema from data samples",
		zap.String("schema", schemaName),
		zap.Int("field_count", len(fields)),
		zap.Int("sample_count", len(dsi.samples)))

	return schema, nil
}

// inferFieldType infers the type of a specific field
func (dsi *DataSampleInference) inferFieldType(fieldName string) (core.Field, error) {
	var values []interface{}
	nullCount := 0

	// Collect all values for this field
	for _, sample := range dsi.samples {
		if value, exists := sample[fieldName]; exists {
			if value == nil {
				nullCount++
			} else {
				values = append(values, value)
			}
		} else {
			nullCount++
		}
	}

	if len(values) == 0 {
		return core.Field{}, errors.New(errors.ErrorTypeData, fmt.Sprintf("no non-null values found for field %s", fieldName))
	}

	// Determine type based on value analysis
	fieldType := dsi.analyzeFieldType(values)
	nullable := float64(nullCount)/float64(len(dsi.samples)) > (1.0 - dsi.inferenceConfig.ConfidenceThreshold)

	field := core.Field{
		Name: fieldName,
		Type: fieldType,
		Description: fmt.Sprintf("Inferred as %s from %d samples (confidence: %.2f%%, null: %.2f%%)",
			fieldType, len(values),
			float64(len(values))/float64(len(dsi.samples))*100,
			float64(nullCount)/float64(len(dsi.samples))*100),
		Nullable: nullable,
	}

	return field, nil
}

// analyzeFieldType analyzes a set of values to determine the most appropriate type
func (dsi *DataSampleInference) analyzeFieldType(values []interface{}) core.FieldType {
	typeScores := map[core.FieldType]float64{
		core.FieldTypeBool:      0,
		core.FieldTypeInt:       0,
		core.FieldTypeFloat:     0,
		core.FieldTypeTimestamp: 0,
		core.FieldTypeJSON:      0,
		core.FieldTypeString:    0,
	}

	for _, value := range values {
		switch v := value.(type) {
		case bool:
			typeScores[core.FieldTypeBool] += 1.0
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			typeScores[core.FieldTypeInt] += 1.0
		case float32, float64:
			typeScores[core.FieldTypeFloat] += 1.0
		case time.Time:
			typeScores[core.FieldTypeTimestamp] += 1.0
		case string:
			stringValue := v

			// Try to parse as different types
			if dsi.looksLikeBool(stringValue) {
				typeScores[core.FieldTypeBool] += 0.9
			} else if dsi.looksLikeInt(stringValue) {
				typeScores[core.FieldTypeInt] += 0.9
			} else if dsi.looksLikeFloat(stringValue) {
				typeScores[core.FieldTypeFloat] += 0.9
			} else if dsi.looksLikeTimestamp(stringValue) {
				typeScores[core.FieldTypeTimestamp] += 0.9
			} else if dsi.looksLikeJSON(stringValue) {
				typeScores[core.FieldTypeJSON] += 0.9
			} else {
				typeScores[core.FieldTypeString] += 1.0
			}
		default:
			// Try to handle as JSON if it's a complex type
			if reflect.TypeOf(value).Kind() == reflect.Map || reflect.TypeOf(value).Kind() == reflect.Slice {
				typeScores[core.FieldTypeJSON] += 1.0
			} else {
				typeScores[core.FieldTypeString] += 1.0
			}
		}
	}

	// Find type with highest score
	var bestType core.FieldType
	var bestScore float64
	for fieldType, score := range typeScores {
		confidence := score / float64(len(values))
		if confidence >= dsi.inferenceConfig.ConfidenceThreshold && score > bestScore {
			bestType = fieldType
			bestScore = score
		}
	}

	// If no type meets confidence threshold, default to string
	if bestScore == 0 {
		return core.FieldTypeString
	}

	return bestType
}

// Type checking helper functions
func (dsi *DataSampleInference) looksLikeBool(s string) bool {
	lower := strings.ToLower(strings.TrimSpace(s))
	return lower == "true" || lower == "false" || lower == "t" || lower == "f" ||
		lower == "yes" || lower == "no" || lower == "y" || lower == "n" ||
		lower == "1" || lower == "0"
}

func (dsi *DataSampleInference) looksLikeInt(s string) bool {
	_, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return err == nil
}

func (dsi *DataSampleInference) looksLikeFloat(s string) bool {
	_, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return err == nil
}

func (dsi *DataSampleInference) looksLikeTimestamp(s string) bool {
	trimmed := strings.TrimSpace(s)
	for _, format := range dsi.inferenceConfig.DateTimeFormats {
		if _, err := time.Parse(format, trimmed); err == nil {
			return true
		}
	}
	return false
}

func (dsi *DataSampleInference) looksLikeJSON(s string) bool {
	trimmed := strings.TrimSpace(s)
	return (strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}")) ||
		(strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]"))
}

// SchemaEvolution handles schema evolution and migration
type SchemaEvolution struct {
	logger *zap.Logger
}

// NewSchemaEvolution creates a new schema evolution handler
func NewSchemaEvolution() *SchemaEvolution {
	return &SchemaEvolution{
		logger: GetGlobalSDK().logger.With(zap.String("component", "schema_evolution")),
	}
}

// CompareSchemas compares two schemas and returns differences
func (se *SchemaEvolution) CompareSchemas(oldSchema, newSchema *core.Schema) *SchemaDiff {
	diff := &SchemaDiff{
		OldSchema: oldSchema,
		NewSchema: newSchema,
		Changes:   make([]SchemaChange, 0),
	}

	// Create field maps for easy lookup
	oldFields := make(map[string]core.Field)
	newFields := make(map[string]core.Field)

	for _, field := range oldSchema.Fields {
		oldFields[field.Name] = field
	}
	for _, field := range newSchema.Fields {
		newFields[field.Name] = field
	}

	// Find added fields
	for name, newField := range newFields {
		if _, exists := oldFields[name]; !exists {
			diff.Changes = append(diff.Changes, SchemaChange{
				Type:      ChangeTypeFieldAdded,
				FieldName: name,
				NewField:  &newField,
			})
		}
	}

	// Find removed and modified fields
	for name, oldField := range oldFields {
		if newField, exists := newFields[name]; exists {
			// Field exists in both, check for modifications
			if !se.fieldsEqual(oldField, newField) {
				diff.Changes = append(diff.Changes, SchemaChange{
					Type:      ChangeTypeFieldModified,
					FieldName: name,
					OldField:  &oldField,
					NewField:  &newField,
				})
			}
		} else {
			// Field removed
			diff.Changes = append(diff.Changes, SchemaChange{
				Type:      ChangeTypeFieldRemoved,
				FieldName: name,
				OldField:  &oldField,
			})
		}
	}

	se.logger.Info("Schema comparison completed",
		zap.String("old_schema", oldSchema.Name),
		zap.String("new_schema", newSchema.Name),
		zap.Int("changes", len(diff.Changes)))

	return diff
}

// fieldsEqual compares two fields for equality
func (se *SchemaEvolution) fieldsEqual(field1, field2 core.Field) bool {
	return field1.Name == field2.Name &&
		field1.Type == field2.Type &&
		field1.Nullable == field2.Nullable &&
		field1.Primary == field2.Primary &&
		field1.Unique == field2.Unique
}

// SchemaDiff represents differences between two schemas
type SchemaDiff struct {
	OldSchema *core.Schema   `json:"old_schema"`
	NewSchema *core.Schema   `json:"new_schema"`
	Changes   []SchemaChange `json:"changes"`
}

// SchemaChange represents a single schema change
type SchemaChange struct {
	Type      ChangeType  `json:"type"`
	FieldName string      `json:"field_name"`
	OldField  *core.Field `json:"old_field,omitempty"`
	NewField  *core.Field `json:"new_field,omitempty"`
}

// ChangeType represents the type of schema change
type ChangeType string

const (
	ChangeTypeFieldAdded    ChangeType = "field_added"
	ChangeTypeFieldRemoved  ChangeType = "field_removed"
	ChangeTypeFieldModified ChangeType = "field_modified"
)

// HasBreakingChanges checks if the schema diff contains breaking changes
func (sd *SchemaDiff) HasBreakingChanges() bool {
	for _, change := range sd.Changes {
		if change.Type == ChangeTypeFieldRemoved {
			return true
		}
		if change.Type == ChangeTypeFieldModified {
			// Type changes are generally breaking
			if change.OldField != nil && change.NewField != nil && change.OldField.Type != change.NewField.Type {
				return true
			}
		}
	}
	return false
}
