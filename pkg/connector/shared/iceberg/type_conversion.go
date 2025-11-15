package iceberg

import (
	"strings"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	iceberg "github.com/apache/iceberg-go"
)

// ConvertIcebergFieldToCore converts an Iceberg field to core field
func ConvertIcebergFieldToCore(field iceberg.NestedField) core.Field {
	var fieldType core.FieldType

	typeStr := field.Type.String()

	// Map Iceberg types to core types
	switch {
	case typeStr == "string":
		fieldType = core.FieldTypeString
	case typeStr == "int" || typeStr == "long":
		fieldType = core.FieldTypeInt
	case typeStr == "float" || typeStr == "double":
		fieldType = core.FieldTypeFloat
	case typeStr == "boolean":
		fieldType = core.FieldTypeBool
	case strings.HasPrefix(typeStr, "timestamp"):
		fieldType = core.FieldTypeTimestamp
	case typeStr == "date":
		fieldType = core.FieldTypeDate
	case typeStr == "time":
		fieldType = core.FieldTypeTime
	default:
		fieldType = core.FieldTypeString // fallback
	}

	return core.Field{
		Name:     field.Name,
		Type:     fieldType,
		Nullable: !field.Required,
	}
}

// SanitizeProperties removes sensitive information from properties for logging
func SanitizeProperties(props iceberg.Properties) map[string]string {
	sanitized := make(map[string]string)
	sensitiveKeys := map[string]bool{
		"s3.access-key-id":     true,
		"s3.secret-access-key": true,
		"access-key-id":        true,
		"secret-access-key":    true,
		"password":             true,
		"token":                true,
		"secret":               true,
	}

	for key, value := range props {
		lowerKey := strings.ToLower(key)
		isSensitive := false

		// Check if key is in sensitive list or contains sensitive keywords
		for sensitiveKey := range sensitiveKeys {
			if lowerKey == sensitiveKey || strings.Contains(lowerKey, sensitiveKey) {
				isSensitive = true
				break
			}
		}

		if isSensitive {
			sanitized[key] = "***REDACTED***"
		} else {
			sanitized[key] = value
		}
	}

	return sanitized
}
