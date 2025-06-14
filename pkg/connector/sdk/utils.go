package sdk

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
	"github.com/ajitpratap0/nebula/pkg/errors"
	"github.com/ajitpratap0/nebula/pkg/models"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
)

// ConfigValidator provides common validation functions for connector configurations
type ConfigValidator struct{}

// NewConfigValidator creates a new configuration validator
func NewConfigValidator() *ConfigValidator {
	return &ConfigValidator{}
}

// ValidateRequired validates that required properties are present
func (cv *ConfigValidator) ValidateRequired(config *config.BaseConfig, requiredProps ...string) error {
	if config.Security.Credentials == nil {
		return errors.New(errors.ErrorTypeConfig, "security credentials are required for connector-specific properties")
	}

	for _, prop := range requiredProps {
		if val, exists := config.Security.Credentials[prop]; !exists || val == "" {
			return errors.New(errors.ErrorTypeConfig, fmt.Sprintf("required property '%s' is missing", prop))
		}
	}

	return nil
}

// ValidateString validates string properties
func (cv *ConfigValidator) ValidateString(config *config.BaseConfig, propName string, minLength, maxLength int, pattern string) error {
	val, exists := config.Security.Credentials[propName]
	if !exists || val == "" {
		return nil // Optional property
	}

	str := val // credentials map values are strings

	if len(str) < minLength {
		return errors.New(errors.ErrorTypeConfig, fmt.Sprintf("property '%s' must be at least %d characters", propName, minLength))
	}

	if maxLength > 0 && len(str) > maxLength {
		return errors.New(errors.ErrorTypeConfig, fmt.Sprintf("property '%s' must be at most %d characters", propName, maxLength))
	}

	if pattern != "" {
		matched, err := regexp.MatchString(pattern, str)
		if err != nil {
			return errors.Wrap(err, errors.ErrorTypeConfig, fmt.Sprintf("invalid pattern for property '%s'", propName))
		}
		if !matched {
			return errors.New(errors.ErrorTypeConfig, fmt.Sprintf("property '%s' does not match required pattern", propName))
		}
	}

	return nil
}

// ValidateInt validates integer properties
func (cv *ConfigValidator) ValidateInt(config *config.BaseConfig, propName string, min, max int) error {
	val, exists := config.Security.Credentials[propName]
	if !exists || val == "" {
		return nil // Optional property
	}

	// Parse string to int (credentials map values are strings)
	intVal, err := strconv.Atoi(val)
	if err != nil {
		return errors.New(errors.ErrorTypeConfig, fmt.Sprintf("property '%s' must be an integer", propName))
	}

	if intVal < min {
		return errors.New(errors.ErrorTypeConfig, fmt.Sprintf("property '%s' must be at least %d", propName, min))
	}

	if max > 0 && intVal > max {
		return errors.New(errors.ErrorTypeConfig, fmt.Sprintf("property '%s' must be at most %d", propName, max))
	}

	return nil
}

// ValidateEnum validates enumerated properties
func (cv *ConfigValidator) ValidateEnum(config *config.BaseConfig, propName string, validValues ...string) error {
	val, exists := config.Security.Credentials[propName]
	if !exists || val == "" {
		return nil // Optional property
	}

	str := val // credentials map values are strings

	for _, validValue := range validValues {
		if str == validValue {
			return nil
		}
	}

	return errors.New(errors.ErrorTypeConfig, stringpool.Sprintf("property '%s' must be one of: %s", propName, stringpool.JoinPooled(validValues, ", ")))
}

// SchemaBuilder helps build schemas programmatically
type SchemaBuilder struct {
	name        string
	description string
	version     string
	fields      []core.Field
}

// NewSchemaBuilder creates a new schema builder
func NewSchemaBuilder(name string) *SchemaBuilder {
	return &SchemaBuilder{
		name:    name,
		version: "1.0.0",
		fields:  make([]core.Field, 0),
	}
}

// WithDescription sets the schema description
func (sb *SchemaBuilder) WithDescription(description string) *SchemaBuilder {
	sb.description = description
	return sb
}

// WithVersion sets the schema version
func (sb *SchemaBuilder) WithVersion(version string) *SchemaBuilder {
	sb.version = version
	return sb
}

// AddField adds a field to the schema
func (sb *SchemaBuilder) AddField(name string, fieldType core.FieldType) *SchemaBuilder {
	field := core.Field{
		Name: name,
		Type: fieldType,
	}
	sb.fields = append(sb.fields, field)
	return sb
}

// AddFieldWithOptions adds a field with additional options
func (sb *SchemaBuilder) AddFieldWithOptions(name string, fieldType core.FieldType, nullable, primary, unique bool, defaultValue interface{}, description string) *SchemaBuilder {
	field := core.Field{
		Name:        name,
		Type:        fieldType,
		Description: description,
		Nullable:    nullable,
		Primary:     primary,
		Unique:      unique,
		Default:     defaultValue,
	}
	sb.fields = append(sb.fields, field)
	return sb
}

// Build builds the schema
func (sb *SchemaBuilder) Build() *core.Schema {
	// Convert version string to int (extract major version)
	version := 1 // default version
	if sb.version != "" {
		if versionParts := strings.Split(sb.version, "."); len(versionParts) > 0 {
			if v, err := strconv.Atoi(versionParts[0]); err == nil {
				version = v
			}
		}
	}

	return &core.Schema{
		Name:        sb.name,
		Description: sb.description,
		Version:     version,
		Fields:      sb.fields,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
}

// RecordBuilder helps build records programmatically
type RecordBuilder struct {
	record *models.Record
}

// NewRecordBuilder creates a new record builder
func NewRecordBuilder(source string) *RecordBuilder {
	return &RecordBuilder{
		record: models.NewRecordFromPool(source),
	}
}

// WithID sets the record ID
func (rb *RecordBuilder) WithID(id string) *RecordBuilder {
	rb.record.ID = id
	return rb
}

// WithTimestamp sets the record timestamp
func (rb *RecordBuilder) WithTimestamp(timestamp time.Time) *RecordBuilder {
	rb.record.Metadata.Timestamp = timestamp
	return rb
}

// WithData sets record data
func (rb *RecordBuilder) WithData(data map[string]interface{}) *RecordBuilder {
	rb.record.Data = data
	return rb
}

// AddField adds a field to the record data
func (rb *RecordBuilder) AddField(name string, value interface{}) *RecordBuilder {
	if rb.record.Data == nil {
		rb.record.Data = pool.GetMap()
	}
	rb.record.Data[name] = value
	return rb
}

// AddMetadata adds metadata to the record
func (rb *RecordBuilder) AddMetadata(key string, value interface{}) *RecordBuilder {
	if rb.record.Metadata.Custom == nil {
		rb.record.Metadata.Custom = pool.GetMap()
	}
	rb.record.Metadata.Custom[key] = value
	return rb
}

// Build builds the record
func (rb *RecordBuilder) Build() *models.Record {
	return rb.record
}

// StreamBuilder helps build record streams for testing
type StreamBuilder struct {
	records    []*models.Record
	errors     []error
	bufferSize int
}

// NewStreamBuilder creates a new stream builder
func NewStreamBuilder() *StreamBuilder {
	return &StreamBuilder{
		records:    make([]*models.Record, 0),
		errors:     make([]error, 0),
		bufferSize: 100,
	}
}

// WithBufferSize sets the stream buffer size
func (sb *StreamBuilder) WithBufferSize(size int) *StreamBuilder {
	sb.bufferSize = size
	return sb
}

// AddRecord adds a record to the stream
func (sb *StreamBuilder) AddRecord(record *models.Record) *StreamBuilder {
	sb.records = append(sb.records, record)
	return sb
}

// AddRecords adds multiple records to the stream
func (sb *StreamBuilder) AddRecords(records ...*models.Record) *StreamBuilder {
	sb.records = append(sb.records, records...)
	return sb
}

// AddError adds an error to the stream
func (sb *StreamBuilder) AddError(err error) *StreamBuilder {
	sb.errors = append(sb.errors, err)
	return sb
}

// BuildRecordStream builds a record stream
func (sb *StreamBuilder) BuildRecordStream() *core.RecordStream {
	recordChan := make(chan *models.Record, sb.bufferSize)
	errorChan := make(chan error, len(sb.errors)+1)

	go func() {
		defer close(recordChan)
		defer close(errorChan)

		// Send records
		for _, record := range sb.records {
			recordChan <- record
		}

		// Send errors
		for _, err := range sb.errors {
			errorChan <- err
		}
	}()

	return &core.RecordStream{
		Records: recordChan,
		Errors:  errorChan,
	}
}

// BuildBatchStream builds a batch stream
func (sb *StreamBuilder) BuildBatchStream(batchSize int) *core.BatchStream {
	batchChan := make(chan []*models.Record, sb.bufferSize/batchSize+1)
	errorChan := make(chan error, len(sb.errors)+1)

	go func() {
		defer close(batchChan)
		defer close(errorChan)

		// Send records in batches
		for i := 0; i < len(sb.records); i += batchSize {
			end := i + batchSize
			if end > len(sb.records) {
				end = len(sb.records)
			}
			batch := sb.records[i:end]
			batchChan <- batch
		}

		// Send errors
		for _, err := range sb.errors {
			errorChan <- err
		}
	}()

	return &core.BatchStream{
		Batches: batchChan,
		Errors:  errorChan,
	}
}

// ConnectorRegistry provides utility functions for connector registration
type ConnectorRegistry struct{}

// NewConnectorRegistry creates a new connector registry
func NewConnectorRegistry() *ConnectorRegistry {
	return &ConnectorRegistry{}
}

// RegisterSourceBuilder registers a source builder as a connector factory
func (cr *ConnectorRegistry) RegisterSourceBuilder(name string, builder *SourceBuilder) error {
	factory := func(config *config.BaseConfig) (core.Source, error) {
		connector, err := NewSDKSourceConnector(builder)
		if err != nil {
			return nil, err
		}
		return connector, nil
	}

	err := registry.RegisterSource(name, factory)
	if err != nil {
		return err
	}

	// Register metadata
	metadata := builder.GetMetadata()
	connectorInfo := &registry.ConnectorInfo{
		Name:         metadata.Name,
		Type:         string(metadata.Type),
		Description:  metadata.Description,
		Version:      metadata.Version,
		Author:       metadata.Author,
		Capabilities: metadata.Capabilities,
		ConfigSchema: metadata.ConfigSchema,
	}

	return registry.RegisterConnectorInfo(connectorInfo)
}

// RegisterDestinationBuilder registers a destination builder as a connector factory
func (cr *ConnectorRegistry) RegisterDestinationBuilder(name string, builder *DestinationBuilder) error {
	factory := func(config *config.BaseConfig) (core.Destination, error) {
		connector, err := NewSDKDestinationConnector(builder)
		if err != nil {
			return nil, err
		}
		return connector, nil
	}

	err := registry.RegisterDestination(name, factory)
	if err != nil {
		return err
	}

	// Register metadata
	metadata := builder.GetMetadata()
	connectorInfo := &registry.ConnectorInfo{
		Name:         metadata.Name,
		Type:         string(metadata.Type),
		Description:  metadata.Description,
		Version:      metadata.Version,
		Author:       metadata.Author,
		Capabilities: metadata.Capabilities,
		ConfigSchema: metadata.ConfigSchema,
	}

	return registry.RegisterConnectorInfo(connectorInfo)
}

// TypeConverter provides utilities for converting between data types
type TypeConverter struct{}

// NewTypeConverter creates a new type converter
func NewTypeConverter() *TypeConverter {
	return &TypeConverter{}
}

// ConvertValue converts a value to the specified field type
func (tc *TypeConverter) ConvertValue(value interface{}, targetType core.FieldType) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch targetType {
	case core.FieldTypeString:
		return tc.toString(value), nil
	case core.FieldTypeInt:
		return tc.toInt(value)
	case core.FieldTypeFloat:
		return tc.toFloat(value)
	case core.FieldTypeBool:
		return tc.toBool(value)
	case core.FieldTypeTimestamp:
		return tc.toTimestamp(value)
	case core.FieldTypeJSON:
		return value, nil // Keep as-is for JSON
	default:
		return value, nil
	}
}

// toString converts a value to string
func (tc *TypeConverter) toString(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

// toInt converts a value to int64
func (tc *TypeConverter) toInt(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, errors.New(errors.ErrorTypeData, fmt.Sprintf("cannot convert %T to int", value))
	}
}

// toFloat converts a value to float64
func (tc *TypeConverter) toFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, errors.New(errors.ErrorTypeData, fmt.Sprintf("cannot convert %T to float", value))
	}
}

// toBool converts a value to bool
func (tc *TypeConverter) toBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int:
		return v != 0, nil
	case int32:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case float32:
		return v != 0, nil
	case float64:
		return v != 0, nil
	case string:
		return strconv.ParseBool(v)
	default:
		return false, errors.New(errors.ErrorTypeData, fmt.Sprintf("cannot convert %T to bool", value))
	}
}

// toTimestamp converts a value to timestamp
func (tc *TypeConverter) toTimestamp(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case int64:
		return time.Unix(0, v), nil
	case string:
		// Try parsing common timestamp formats
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}

		return time.Time{}, errors.New(errors.ErrorTypeData, fmt.Sprintf("cannot parse timestamp: %s", v))
	default:
		return time.Time{}, errors.New(errors.ErrorTypeData, fmt.Sprintf("cannot convert %T to timestamp", value))
	}
}

// ReflectionHelper provides utilities for reflection-based operations
type ReflectionHelper struct{}

// NewReflectionHelper creates a new reflection helper
func NewReflectionHelper() *ReflectionHelper {
	return &ReflectionHelper{}
}

// InferSchemaFromStruct infers a schema from a Go struct
func (rh *ReflectionHelper) InferSchemaFromStruct(structType interface{}, schemaName string) *core.Schema {
	t := reflect.TypeOf(structType)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil
	}

	fields := make([]core.Field, 0, t.NumField())

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		fieldName := field.Name
		if tag := field.Tag.Get("json"); tag != "" && tag != "-" {
			if idx := strings.Index(tag, ","); idx != -1 {
				fieldName = tag[:idx]
			} else {
				fieldName = tag
			}
		}

		fieldType := rh.mapGoTypeToFieldType(field.Type)

		coreField := core.Field{
			Name:        fieldName,
			Type:        fieldType,
			Description: field.Tag.Get("description"),
			Nullable:    rh.isNullable(field.Type),
		}

		fields = append(fields, coreField)
	}

	return &core.Schema{
		Name:      schemaName,
		Fields:    fields,
		Version:   1,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

// mapGoTypeToFieldType maps Go types to field types
func (rh *ReflectionHelper) mapGoTypeToFieldType(t reflect.Type) core.FieldType {
	// Handle pointers
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.String:
		return core.FieldTypeString
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return core.FieldTypeInt
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return core.FieldTypeInt
	case reflect.Float32, reflect.Float64:
		return core.FieldTypeFloat
	case reflect.Bool:
		return core.FieldTypeBool
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return core.FieldTypeTimestamp
		}
		return core.FieldTypeJSON
	case reflect.Map, reflect.Slice, reflect.Array:
		return core.FieldTypeJSON
	default:
		return core.FieldTypeString
	}
}

// isNullable checks if a type is nullable (pointer type)
func (rh *ReflectionHelper) isNullable(t reflect.Type) bool {
	return t.Kind() == reflect.Ptr || t.Kind() == reflect.Interface || t.Kind() == reflect.Map || t.Kind() == reflect.Slice
}
