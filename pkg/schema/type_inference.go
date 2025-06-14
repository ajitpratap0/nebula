package schema

import (
	"encoding/json" // For json.RawMessage type only
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"
)

// TypeInferenceEngine provides advanced type detection capabilities
type TypeInferenceEngine struct {
	logger *zap.Logger

	// Type detection patterns
	datePatterns      []*regexp.Regexp
	timestampPatterns []*regexp.Regexp
	emailPattern      *regexp.Regexp
	urlPattern        *regexp.Regexp
	uuidPattern       *regexp.Regexp
	jsonPattern       *regexp.Regexp

	// Configuration
	sampleSize          int
	confidenceThreshold float64
	detectComplexTypes  bool
}

// InferredType represents a type inference result with confidence
type InferredType struct {
	Type          string         `json:"type"`
	Format        string         `json:"format,omitempty"`
	Confidence    float64        `json:"confidence"`
	Nullable      bool           `json:"nullable"`
	Cardinality   int            `json:"cardinality"`
	Examples      []interface{}  `json:"examples,omitempty"`
	Distribution  map[string]int `json:"distribution,omitempty"`
	NumericStats  *NumericStats  `json:"numeric_stats,omitempty"`
	StringStats   *StringStats   `json:"string_stats,omitempty"`
	TemporalStats *TemporalStats `json:"temporal_stats,omitempty"`
}

// NumericStats holds statistics for numeric types
type NumericStats struct {
	Min       float64 `json:"min"`
	Max       float64 `json:"max"`
	Mean      float64 `json:"mean"`
	Median    float64 `json:"median"`
	StdDev    float64 `json:"std_dev"`
	Precision int     `json:"precision"`
	Scale     int     `json:"scale"`
}

// StringStats holds statistics for string types
type StringStats struct {
	MinLength    int      `json:"min_length"`
	MaxLength    int      `json:"max_length"`
	AvgLength    float64  `json:"avg_length"`
	Pattern      string   `json:"pattern,omitempty"`
	CommonValues []string `json:"common_values,omitempty"`
}

// TemporalStats holds statistics for temporal types
type TemporalStats struct {
	MinDate  time.Time `json:"min_date"`
	MaxDate  time.Time `json:"max_date"`
	Format   string    `json:"format"`
	Timezone string    `json:"timezone,omitempty"`
}

// NewTypeInferenceEngine creates a new type inference engine
func NewTypeInferenceEngine(logger *zap.Logger) *TypeInferenceEngine {
	engine := &TypeInferenceEngine{
		logger:              logger,
		sampleSize:          1000,
		confidenceThreshold: 0.95,
		detectComplexTypes:  true,
	}

	// Initialize patterns
	engine.initializePatterns()

	return engine
}

// InferSchema infers a complete schema from sample data
func (e *TypeInferenceEngine) InferSchema(name string, samples []map[string]interface{}) (*models.Schema, error) {
	if len(samples) == 0 {
		return nil, fmt.Errorf("no samples provided for inference")
	}

	// Collect all field names
	fieldMap := make(map[string][]interface{})
	for _, sample := range samples {
		for key, value := range sample {
			fieldMap[key] = append(fieldMap[key], value)
		}
	}

	// Infer type for each field
	fields := make([]models.Field, 0, len(fieldMap))
	for fieldName, values := range fieldMap {
		inferred := e.InferType(fieldName, values)

		field := models.Field{
			Name:        fieldName,
			Type:        e.mapInferredType(inferred),
			Required:    !inferred.Nullable,
			Description: e.generateFieldDescription(inferred),
		}

		fields = append(fields, field)
	}

	schema := &models.Schema{
		Name:    name,
		Fields:  fields,
		Version: "1.0",
	}

	return schema, nil
}

// InferType infers the type of a field from sample values
func (e *TypeInferenceEngine) InferType(fieldName string, values []interface{}) *InferredType {
	if len(values) == 0 {
		return &InferredType{
			Type:       "unknown",
			Confidence: 0,
			Nullable:   true,
		}
	}

	// Type counters
	typeCounts := make(map[string]int)
	nullCount := 0

	// Collect non-null values for analysis
	nonNullValues := make([]interface{}, 0, len(values))
	for _, v := range values {
		if v == nil {
			nullCount++
		} else {
			nonNullValues = append(nonNullValues, v)
		}
	}

	// Analyze each value
	for _, value := range nonNullValues {
		detectedType := e.detectValueType(value)
		typeCounts[detectedType]++
	}

	// Find dominant type
	var dominantType string
	maxCount := 0
	for typ, count := range typeCounts {
		if count > maxCount {
			maxCount = count
			dominantType = typ
		}
	}

	// Calculate confidence
	confidence := float64(maxCount) / float64(len(nonNullValues))
	if confidence < e.confidenceThreshold && len(typeCounts) > 1 {
		// Mixed types, default to string
		dominantType = "string"
	}

	// Create inferred type
	inferred := &InferredType{
		Type:        dominantType,
		Confidence:  confidence,
		Nullable:    nullCount > 0,
		Cardinality: len(getUniqueValues(nonNullValues)),
	}

	// Add detailed statistics based on type
	e.addTypeStatistics(inferred, nonNullValues)

	// Detect format for strings
	if dominantType == "string" {
		e.detectStringFormat(inferred, nonNullValues)
	}

	return inferred
}

// detectValueType detects the type of a single value
func (e *TypeInferenceEngine) detectValueType(value interface{}) string {
	switch v := value.(type) {
	case bool:
		return "boolean"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "integer"
	case float32, float64:
		return "float"
	case string:
		// Try to parse as other types
		if e.isBoolean(v) {
			return "boolean"
		}
		if e.isInteger(v) {
			return "integer"
		}
		if e.isFloat(v) {
			return "float"
		}
		if e.detectComplexTypes && e.isJSON(v) {
			return "json"
		}
		return "string"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	case time.Time:
		return "timestamp"
	default:
		// Check if it's a JSON-serializable complex type
		if _, err := jsonpool.Marshal(value); err == nil {
			return "object"
		}
		return "unknown"
	}
}

// detectStringFormat detects special string formats
func (e *TypeInferenceEngine) detectStringFormat(inferred *InferredType, values []interface{}) {
	formatCounts := make(map[string]int)

	for _, v := range values {
		str, ok := v.(string)
		if !ok {
			continue
		}

		if format := e.detectFormat(str); format != "" {
			formatCounts[format]++
		}
	}

	// Find dominant format
	var dominantFormat string
	maxCount := 0
	threshold := int(float64(len(values)) * 0.8) // 80% threshold

	for format, count := range formatCounts {
		if count > maxCount && count >= threshold {
			maxCount = count
			dominantFormat = format
		}
	}

	if dominantFormat != "" {
		inferred.Format = dominantFormat

		// Update type for temporal formats
		if dominantFormat == "date" || dominantFormat == "datetime" || dominantFormat == "timestamp" {
			inferred.Type = "timestamp"
		}
	}
}

// detectFormat detects the format of a string value
func (e *TypeInferenceEngine) detectFormat(value string) string {
	// Check temporal formats
	for _, pattern := range e.timestampPatterns {
		if pattern.MatchString(value) {
			return "timestamp"
		}
	}

	for _, pattern := range e.datePatterns {
		if pattern.MatchString(value) {
			return "date"
		}
	}

	// Check other formats
	if e.emailPattern.MatchString(value) {
		return "email"
	}

	if e.urlPattern.MatchString(value) {
		return "url"
	}

	if e.uuidPattern.MatchString(value) {
		return "uuid"
	}

	if e.jsonPattern.MatchString(value) {
		var js json.RawMessage
		if err := jsonpool.Unmarshal([]byte(value), &js); err == nil {
			return "json"
		}
	}

	return ""
}

// addTypeStatistics adds detailed statistics based on type
func (e *TypeInferenceEngine) addTypeStatistics(inferred *InferredType, values []interface{}) {
	switch inferred.Type {
	case "integer", "float":
		inferred.NumericStats = e.calculateNumericStats(values)
	case "string":
		inferred.StringStats = e.calculateStringStats(values)
	case "timestamp":
		inferred.TemporalStats = e.calculateTemporalStats(values)
	}

	// Add examples (up to 5)
	uniqueValues := getUniqueValues(values)
	if len(uniqueValues) <= 10 {
		inferred.Examples = uniqueValues
	} else {
		inferred.Examples = uniqueValues[:5]
	}
}

// Helper methods for type detection
func (e *TypeInferenceEngine) isBoolean(s string) bool {
	lower := strings.ToLower(s)
	return lower == "true" || lower == "false" || lower == "yes" || lower == "no" || lower == "1" || lower == "0"
}

func (e *TypeInferenceEngine) isInteger(s string) bool {
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

func (e *TypeInferenceEngine) isFloat(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func (e *TypeInferenceEngine) isJSON(s string) bool {
	var js json.RawMessage
	return jsonpool.Unmarshal([]byte(s), &js) == nil
}

// mapInferredType maps inferred type to schema type
func (e *TypeInferenceEngine) mapInferredType(inferred *InferredType) string {
	switch inferred.Type {
	case "timestamp":
		return "timestamp"
	case "json", "object":
		return "object"
	case "array":
		return "array"
	default:
		return inferred.Type
	}
}

// generateFieldDescription generates a description based on inference
func (e *TypeInferenceEngine) generateFieldDescription(inferred *InferredType) string {
	desc := fmt.Sprintf("Type: %s", inferred.Type)

	if inferred.Format != "" {
		desc += fmt.Sprintf(" (format: %s)", inferred.Format)
	}

	if inferred.Nullable {
		desc += ", nullable"
	}

	desc += fmt.Sprintf(", confidence: %.2f%%", inferred.Confidence*100)

	return desc
}

// initializePatterns initializes regex patterns for format detection
func (e *TypeInferenceEngine) initializePatterns() {
	// Date patterns
	e.datePatterns = []*regexp.Regexp{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`), // YYYY-MM-DD
		regexp.MustCompile(`^\d{2}/\d{2}/\d{4}$`), // MM/DD/YYYY
		regexp.MustCompile(`^\d{2}-\d{2}-\d{4}$`), // DD-MM-YYYY
		regexp.MustCompile(`^\d{4}/\d{2}/\d{2}$`), // YYYY/MM/DD
	}

	// Timestamp patterns
	e.timestampPatterns = []*regexp.Regexp{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`), // ISO 8601
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`), // SQL timestamp
		regexp.MustCompile(`^\d{10,13}$`),                          // Unix timestamp
	}

	// Other patterns
	e.emailPattern = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	e.urlPattern = regexp.MustCompile(`^https?://[^\s]+$`)
	e.uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	e.jsonPattern = regexp.MustCompile(`^[\{\[].*[\}\]]$`)
}

// calculateNumericStats calculates statistics for numeric values
func (e *TypeInferenceEngine) calculateNumericStats(values []interface{}) *NumericStats {
	numbers := make([]float64, 0, len(values))

	for _, v := range values {
		var num float64
		switch n := v.(type) {
		case int:
			num = float64(n)
		case int64:
			num = float64(n)
		case float64:
			num = n
		case string:
			if parsed, err := strconv.ParseFloat(n, 64); err == nil {
				num = parsed
			} else {
				continue
			}
		default:
			continue
		}
		numbers = append(numbers, num)
	}

	if len(numbers) == 0 {
		return nil
	}

	// Calculate basic stats
	stats := &NumericStats{}
	stats.Min = numbers[0]
	stats.Max = numbers[0]
	sum := 0.0

	for _, n := range numbers {
		if n < stats.Min {
			stats.Min = n
		}
		if n > stats.Max {
			stats.Max = n
		}
		sum += n
	}

	stats.Mean = sum / float64(len(numbers))

	// Calculate median (simplified)
	if len(numbers) > 0 {
		stats.Median = numbers[len(numbers)/2]
	}

	return stats
}

// calculateStringStats calculates statistics for string values
func (e *TypeInferenceEngine) calculateStringStats(values []interface{}) *StringStats {
	stats := &StringStats{
		MinLength: int(^uint(0) >> 1), // Max int
		MaxLength: 0,
	}

	totalLength := 0
	count := 0

	for _, v := range values {
		str, ok := v.(string)
		if !ok {
			continue
		}

		length := len(str)
		if length < stats.MinLength {
			stats.MinLength = length
		}
		if length > stats.MaxLength {
			stats.MaxLength = length
		}

		totalLength += length
		count++
	}

	if count > 0 {
		stats.AvgLength = float64(totalLength) / float64(count)
	}

	return stats
}

// calculateTemporalStats calculates statistics for temporal values
func (e *TypeInferenceEngine) calculateTemporalStats(values []interface{}) *TemporalStats {
	stats := &TemporalStats{}

	times := make([]time.Time, 0, len(values))
	for _, v := range values {
		switch t := v.(type) {
		case time.Time:
			times = append(times, t)
		case string:
			// Try to parse common formats
			formats := []string{
				time.RFC3339,
				"2006-01-02 15:04:05",
				"2006-01-02",
			}

			for _, format := range formats {
				if parsed, err := time.Parse(format, t); err == nil {
					times = append(times, parsed)
					stats.Format = format
					break
				}
			}
		}
	}

	if len(times) > 0 {
		stats.MinDate = times[0]
		stats.MaxDate = times[0]

		for _, t := range times {
			if t.Before(stats.MinDate) {
				stats.MinDate = t
			}
			if t.After(stats.MaxDate) {
				stats.MaxDate = t
			}
		}
	}

	return stats
}

// getUniqueValues returns unique values from a slice
func getUniqueValues(values []interface{}) []interface{} {
	seen := make(map[string]bool)
	unique := make([]interface{}, 0)

	for _, v := range values {
		key := stringpool.ValueToString(v)
		if !seen[key] {
			seen[key] = true
			unique = append(unique, v)
		}
	}

	return unique
}
