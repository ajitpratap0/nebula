package pool

import (
	"sync"
	"sync/atomic"
)

// StringInternPool provides string interning to reduce memory allocations
// for frequently used strings (like field names, map keys, etc.)
type StringInternPool struct {
	mu      sync.RWMutex
	strings map[string]string
	maxSize int
	size    int64
	hits    int64
	misses  int64
}

// Global string intern pool with common field names pre-populated
var globalStringInternPool = &StringInternPool{
	strings: make(map[string]string, 1024),
	maxSize: 10000, // Limit to prevent unbounded growth
}

// Initialize the global pool with common strings
func init() {
	internCommonFields()
}

// internCommonFields pre-interns common field names
func internCommonFields() {
	commonFields := []string{
		// Common CSV fields
		"field_0", "field_1", "field_2", "field_3", "field_4",
		"field_5", "field_6", "field_7", "field_8", "field_9",

		// Common JSON fields
		"id", "name", "value", "type", "data", "timestamp",
		"created_at", "updated_at", "deleted_at", "status",

		// Common metric fields
		"metrics_impressions", "metrics_clicks", "metrics_cost",
		"metrics_conversions", "metrics_ctr", "metrics_cpc",

		// Common dimension fields
		"campaign_id", "campaign_name", "adgroup_id", "adgroup_name",
		"keyword_id", "keyword_text", "customer_id", "account_id",

		// Common metadata fields
		"source", "source_file", "row_number", "connector_version",
		"extracted_at", "partition", "offset", "operation",

		// Common CDC fields
		"before", "after", "op", "ts_ms", "transaction_id",
		"database", "schema", "table", "change_type",
	}

	for _, field := range commonFields {
		globalStringInternPool.Intern(field)
	}
}

// Intern returns an interned version of the string
func (p *StringInternPool) Intern(s string) string {
	// Fast path: check if already interned
	p.mu.RLock()
	if interned, ok := p.strings[s]; ok {
		p.mu.RUnlock()
		atomic.AddInt64(&p.hits, 1)
		return interned
	}
	p.mu.RUnlock()

	// Slow path: add to intern pool
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if interned, ok := p.strings[s]; ok {
		atomic.AddInt64(&p.hits, 1)
		return interned
	}

	// Check size limit
	currentSize := atomic.LoadInt64(&p.size)
	if currentSize >= int64(p.maxSize) {
		// Return original string if pool is full
		atomic.AddInt64(&p.misses, 1)
		return s
	}

	// Add to pool
	p.strings[s] = s
	atomic.AddInt64(&p.size, 1)
	atomic.AddInt64(&p.misses, 1)
	return s
}

// InternBytes interns a byte slice as a string
func (p *StringInternPool) InternBytes(b []byte) string {
	return p.Intern(string(b))
}

// Stats returns intern pool statistics
func (p *StringInternPool) Stats() (size, hits, misses int64) {
	return atomic.LoadInt64(&p.size),
		atomic.LoadInt64(&p.hits),
		atomic.LoadInt64(&p.misses)
}

// Clear clears the intern pool (useful for tests)
func (p *StringInternPool) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Create new map, old one will be GC'd
	p.strings = make(map[string]string, 1024)
	atomic.StoreInt64(&p.size, 0)
	atomic.StoreInt64(&p.hits, 0)
	atomic.StoreInt64(&p.misses, 0)

	// Re-intern common fields
	internCommonFields()
}

// Global functions for convenience

// InternString interns a string using the global pool
func InternString(s string) string {
	return globalStringInternPool.Intern(s)
}

// InternBytes interns a byte slice as a string using the global pool
func InternBytes(b []byte) string {
	return globalStringInternPool.InternBytes(b)
}

// GetInternStats returns global intern pool statistics
func GetInternStats() (size, hits, misses int64) {
	return globalStringInternPool.Stats()
}
