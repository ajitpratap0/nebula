package schema

import (
	"fmt"
	"strconv"
	"time"

	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"
)

// MigrationEngine handles data migration between schema versions
type MigrationEngine struct {
	logger   *zap.Logger
	registry *Registry

	// Type converters
	converters map[string]TypeConverter

	// Custom transformations
	transformations map[string]TransformationFunc
}

// TypeConverter converts values between types
type TypeConverter func(value interface{}, targetType string) (interface{}, error)

// TransformationFunc applies custom transformations
type TransformationFunc func(record map[string]interface{}, change *SchemaChange) error

// NewMigrationEngine creates a new migration engine
func NewMigrationEngine(logger *zap.Logger, registry *Registry) *MigrationEngine {
	engine := &MigrationEngine{
		logger:          logger,
		registry:        registry,
		converters:      make(map[string]TypeConverter),
		transformations: make(map[string]TransformationFunc),
	}

	// Register default type converters
	engine.registerDefaultConverters()

	return engine
}

// MigrateRecord migrates a single record according to a migration plan
func (me *MigrationEngine) MigrateRecord(record map[string]interface{}, plan *MigrationPlan) (map[string]interface{}, error) {
	// Create a copy to avoid modifying the original
	migrated := pool.GetMap()

	defer pool.PutMap(migrated)
	for k, v := range record {
		migrated[k] = v
	}

	// Apply each change in the migration plan
	for _, change := range plan.Changes {
		if err := me.applyChange(migrated, &change); err != nil {
			return nil, fmt.Errorf("failed to apply change %s: %w", change.Type, err)
		}
	}

	return migrated, nil
}

// MigrateBatch migrates a batch of records
func (me *MigrationEngine) MigrateBatch(records []map[string]interface{}, plan *MigrationPlan) ([]map[string]interface{}, error) {
	migrated := make([]map[string]interface{}, 0, len(records))

	for i, record := range records {
		migratedRecord, err := me.MigrateRecord(record, plan)
		if err != nil {
			return nil, fmt.Errorf("failed to migrate record %d: %w", i, err)
		}
		migrated = append(migrated, migratedRecord)
	}

	return migrated, nil
}

// ValidateRecord validates a record against a schema
func (me *MigrationEngine) ValidateRecord(record map[string]interface{}, schema *models.Schema) []ValidationError {
	errors := []ValidationError{}

	// Create field map for easy lookup
	fieldMap := make(map[string]*models.Field)
	for i := range schema.Fields {
		fieldMap[schema.Fields[i].Name] = &schema.Fields[i]
	}

	// Check required fields
	for _, field := range schema.Fields {
		if field.Required {
			value, exists := record[field.Name]
			if !exists || value == nil {
				errors = append(errors, ValidationError{
					Field:   field.Name,
					VType:   ValidationErrorRequired,
					Message: fmt.Sprintf("required field '%s' is missing", field.Name),
				})
			}
		}
	}

	// Validate field types and values
	for fieldName, value := range record {
		field, exists := fieldMap[fieldName]
		if !exists {
			// Unknown field - may be okay depending on schema strictness
			continue
		}

		if err := me.validateFieldValue(fieldName, value, field); err != nil {
			errors = append(errors, ValidationError{
				Field:   fieldName,
				VType:   ValidationErrorTypeError,
				Message: err.Error(),
			})
		}
	}

	return errors
}

// applyChange applies a single schema change to a record
func (me *MigrationEngine) applyChange(record map[string]interface{}, change *SchemaChange) error {
	switch change.Type {
	case ChangeTypeAddField:
		return me.applyAddField(record, change)

	case ChangeTypeRemoveField:
		return me.applyRemoveField(record, change)

	case ChangeTypeRenameField:
		return me.applyRenameField(record, change)

	case ChangeTypeModifyType:
		return me.applyModifyType(record, change)

	case ChangeTypeModifyRequired:
		// No data change needed, just validation rules change
		return nil

	default:
		return fmt.Errorf("unknown change type: %s", change.Type)
	}
}

// applyAddField adds a new field with default value
func (me *MigrationEngine) applyAddField(record map[string]interface{}, change *SchemaChange) error {
	if change.NewField == nil {
		return fmt.Errorf("new field definition missing")
	}

	// Don't overwrite if field already exists
	if _, exists := record[change.Field]; exists {
		return nil
	}

	// Apply migration rule
	switch change.Migration.Type {
	case "default":
		record[change.Field] = change.Migration.DefaultValue

	case "transform":
		if transformation, exists := me.transformations[change.Migration.Transformation]; exists {
			if err := transformation(record, change); err != nil {
				return fmt.Errorf("transformation failed: %w", err)
			}
		}

	case "custom":
		if change.Migration.CustomLogic != nil {
			record[change.Field] = change.Migration.CustomLogic(record)
		}

	default:
		// Set to zero value of the type
		record[change.Field] = me.getZeroValue(change.NewField.Type)
	}

	return nil
}

// applyRemoveField removes a field from the record
func (me *MigrationEngine) applyRemoveField(record map[string]interface{}, change *SchemaChange) error {
	delete(record, change.Field)
	return nil
}

// applyRenameField renames a field
func (me *MigrationEngine) applyRenameField(record map[string]interface{}, change *SchemaChange) error {
	if value, exists := record[change.Field]; exists {
		// Assuming new field name is in NewField.Name
		if change.NewField != nil {
			record[change.NewField.Name] = value
			delete(record, change.Field)
		}
	}
	return nil
}

// applyModifyType converts field to new type
func (me *MigrationEngine) applyModifyType(record map[string]interface{}, change *SchemaChange) error {
	value, exists := record[change.Field]
	if !exists {
		return nil // Field doesn't exist, nothing to convert
	}

	if change.OldField == nil || change.NewField == nil {
		return fmt.Errorf("type modification requires both old and new field definitions")
	}

	// Convert value to new type
	converterKey := fmt.Sprintf("%s_to_%s", change.OldField.Type, change.NewField.Type)
	converter, exists := me.converters[converterKey]
	if !exists {
		// Try generic converter
		converter = me.genericConverter
	}

	converted, err := converter(value, change.NewField.Type)
	if err != nil {
		return fmt.Errorf("failed to convert %s from %s to %s: %w",
			change.Field, change.OldField.Type, change.NewField.Type, err)
	}

	record[change.Field] = converted
	return nil
}

// validateFieldValue validates a field value against its schema
func (me *MigrationEngine) validateFieldValue(fieldName string, value interface{}, field *models.Field) error {
	if value == nil {
		if field.Required {
			return fmt.Errorf("required field '%s' is null", fieldName)
		}
		return nil
	}

	// Check type
	actualType := me.getValueType(value)
	if !me.isTypeCompatible(actualType, field.Type) {
		return fmt.Errorf("field '%s' has type %s but expected %s", fieldName, actualType, field.Type)
	}

	// Additional validation could be added here
	// For example, format validation, constraint validation, etc.

	return nil
}

// registerDefaultConverters registers built-in type converters
func (me *MigrationEngine) registerDefaultConverters() {
	// String to other types
	me.converters["string_to_integer"] = me.stringToInteger
	me.converters["string_to_float"] = me.stringToFloat
	me.converters["string_to_boolean"] = me.stringToBoolean
	me.converters["string_to_timestamp"] = me.stringToTimestamp

	// Integer conversions
	me.converters["integer_to_string"] = me.integerToString
	me.converters["integer_to_float"] = me.integerToFloat
	me.converters["integer_to_boolean"] = me.integerToBoolean

	// Float conversions
	me.converters["float_to_string"] = me.floatToString
	me.converters["float_to_integer"] = me.floatToInteger

	// Boolean conversions
	me.converters["boolean_to_string"] = me.booleanToString
	me.converters["boolean_to_integer"] = me.booleanToInteger
}

// Type conversion functions
func (me *MigrationEngine) stringToInteger(value interface{}, _ string) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	return strconv.ParseInt(str, 10, 64)
}

func (me *MigrationEngine) stringToFloat(value interface{}, _ string) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	return strconv.ParseFloat(str, 64)
}

func (me *MigrationEngine) stringToBoolean(value interface{}, _ string) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	return strconv.ParseBool(str)
}

func (me *MigrationEngine) stringToTimestamp(value interface{}, _ string) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	// Try common formats
	formats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, str); err == nil {
			return t.Unix(), nil
		}
	}

	return nil, fmt.Errorf("unable to parse timestamp: %s", str)
}

func (me *MigrationEngine) integerToString(value interface{}, _ string) (interface{}, error) {
	return stringpool.ValueToString(value), nil
}

func (me *MigrationEngine) integerToFloat(value interface{}, _ string) (interface{}, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	default:
		return nil, fmt.Errorf("expected integer, got %T", value)
	}
}

func (me *MigrationEngine) integerToBoolean(value interface{}, _ string) (interface{}, error) {
	switch v := value.(type) {
	case int:
		return v != 0, nil
	case int64:
		return v != 0, nil
	default:
		return nil, fmt.Errorf("expected integer, got %T", value)
	}
}

func (me *MigrationEngine) floatToString(value interface{}, _ string) (interface{}, error) {
	return stringpool.ValueToString(value), nil
}

func (me *MigrationEngine) floatToInteger(value interface{}, _ string) (interface{}, error) {
	switch v := value.(type) {
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	default:
		return nil, fmt.Errorf("expected float, got %T", value)
	}
}

func (me *MigrationEngine) booleanToString(value interface{}, _ string) (interface{}, error) {
	return stringpool.ValueToString(value), nil
}

func (me *MigrationEngine) booleanToInteger(value interface{}, _ string) (interface{}, error) {
	b, ok := value.(bool)
	if !ok {
		return nil, fmt.Errorf("expected boolean, got %T", value)
	}

	if b {
		return int64(1), nil
	}
	return int64(0), nil
}

// genericConverter attempts generic type conversion
func (me *MigrationEngine) genericConverter(value interface{}, targetType string) (interface{}, error) {
	// Try JSON marshaling/unmarshaling for complex types
	data, err := jsonpool.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value: %w", err)
	}

	switch targetType {
	case "string":
		return string(data), nil
	case "object":
		var obj map[string]interface{}
		if err := jsonpool.Unmarshal(data, &obj); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to object: %w", err)
		}
		return obj, nil
	case "array":
		var arr []interface{}
		if err := jsonpool.Unmarshal(data, &arr); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to array: %w", err)
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("cannot convert to %s", targetType)
	}
}

// Helper methods
func (me *MigrationEngine) getValueType(value interface{}) string {
	switch value.(type) {
	case string:
		return "string"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "integer"
	case float32, float64:
		return "float"
	case bool:
		return "boolean"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	case time.Time:
		return "timestamp"
	default:
		return "unknown"
	}
}

func (me *MigrationEngine) isTypeCompatible(actualType, expectedType string) bool {
	// Exact match
	if actualType == expectedType {
		return true
	}

	// Allow numeric compatibility
	if (actualType == "integer" || actualType == "float") &&
		(expectedType == "integer" || expectedType == "float") {
		return true
	}

	// String can hold anything
	if expectedType == "string" {
		return true
	}

	return false
}

func (me *MigrationEngine) getZeroValue(fieldType string) interface{} {
	switch fieldType {
	case "string":
		return ""
	case "integer":
		return int64(0)
	case "float":
		return 0.0
	case "boolean":
		return false
	case "array":
		return []interface{}{}
	case "object":
		return map[string]interface{}{}
	case "timestamp":
		return time.Time{}.Unix()
	default:
		return nil
	}
}

func (me *MigrationEngine) validateFormat(value interface{}, format string) error {
	// Implement format validation (email, url, uuid, etc.)
	return nil
}

func (me *MigrationEngine) validateConstraints(value interface{}, metadata map[string]interface{}) error {
	// Implement constraint validation (min, max, pattern, etc.)
	return nil
}

// ValidationError represents a validation error
type ValidationError struct {
	Field   string              `json:"field"`
	VType   ValidationErrorType `json:"type"`
	Message string              `json:"message"`
}

// ValidationErrorType represents the type of validation error
type ValidationErrorType string

const (
	ValidationErrorRequired   ValidationErrorType = "REQUIRED"
	ValidationErrorTypeError  ValidationErrorType = "TYPE"
	ValidationErrorFormat     ValidationErrorType = "FORMAT"
	ValidationErrorConstraint ValidationErrorType = "CONSTRAINT"
)
