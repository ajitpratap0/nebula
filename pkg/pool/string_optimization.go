package pool

import (
	"strconv"
	"sync"
)

// Pre-interned common field names to avoid runtime allocations
var (
	internedFieldNames = make(map[string]string)
	fieldNamesMutex    sync.RWMutex
)

func init() {
	// Pre-intern common column names (col_0 to col_99)
	for i := 0; i < 100; i++ {
		name := "col_" + strconv.Itoa(i)
		internedFieldNames[name] = name
	}

	// Pre-intern common field names (field_0 to field_99)
	for i := 0; i < 100; i++ {
		name := "field_" + strconv.Itoa(i)
		internedFieldNames[name] = name
	}

	// Pre-intern common record IDs (record_0 to record_999)
	for i := 0; i < 1000; i++ {
		name := "record_" + strconv.Itoa(i)
		internedFieldNames[name] = name
	}

	// Pre-intern other common field names
	commonNames := []string{
		"id", "name", "value", "timestamp", "type", "status",
		"created_at", "updated_at", "deleted_at", "version",
		"user_id", "account_id", "organization_id", "tenant_id",
		"metric_1", "metric_2", "metric_3", "dimension_1", "dimension_2",
	}
	for _, name := range commonNames {
		internedFieldNames[name] = name
	}
}

// GetFieldName returns an interned field name for common patterns
// This avoids allocations for frequently used field names
func GetFieldName(prefix string, index int) string {
	// Fast path for common patterns
	if index < 100 && (prefix == "col_" || prefix == "field_") {
		name := prefix + strconv.Itoa(index)
		return internedFieldNames[name]
	}

	if index < 1000 && prefix == "record_" {
		name := prefix + strconv.Itoa(index)
		return internedFieldNames[name]
	}

	// Slow path: intern on demand
	name := prefix + strconv.Itoa(index)
	return InternString(name)
}

// GetRecordID returns an interned record ID
// Optimized for sequential access patterns
func GetRecordID(index int) string {
	if index < 1000 {
		return internedFieldNames["record_"+strconv.Itoa(index)]
	}
	return InternString("record_" + strconv.Itoa(index))
}

// GetColumnName returns an interned column name
// Optimized for CSV and similar columnar data
func GetColumnName(index int) string {
	if index < 100 {
		return internedFieldNames["col_"+strconv.Itoa(index)]
	}
	return InternString("col_" + strconv.Itoa(index))
}

// PreInternFieldNames pre-interns a batch of field names
// Useful for known schemas or repeated patterns
func PreInternFieldNames(names []string) {
	fieldNamesMutex.Lock()
	defer fieldNamesMutex.Unlock()

	for _, name := range names {
		if _, exists := internedFieldNames[name]; !exists {
			internedFieldNames[name] = name
		}
	}
}
