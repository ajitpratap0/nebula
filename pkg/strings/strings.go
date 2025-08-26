// Package strings provides high-performance, zero-copy string utilities with pooling for Nebula
package strings

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

// BytesToString converts byte slice to string without allocation
// WARNING: The returned string shares memory with the byte slice.
// Do not modify the byte slice after calling this function.
func BytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return *(*string)(unsafe.Pointer(&b))
}

// StringToBytes converts string to byte slice without allocation
// WARNING: The returned byte slice shares memory with the string.
// Do not modify the returned slice.
func StringToBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

// Builder provides efficient string building with zero-copy operations
type Builder struct {
	buf []byte
}

// NewBuilder creates a new string builder
func NewBuilder(capacity int) *Builder {
	return &Builder{
		buf: make([]byte, 0, capacity),
	}
}

// WriteString appends a string to the builder
func (b *Builder) WriteString(s string) {
	b.buf = append(b.buf, StringToBytes(s)...)
}

// WriteBytes appends bytes to the builder
func (b *Builder) WriteBytes(data []byte) {
	b.buf = append(b.buf, data...)
}

// WriteByte appends a single byte
func (b *Builder) WriteByte(c byte) {
	b.buf = append(b.buf, c)
}

// Write implements io.Writer interface
func (b *Builder) Write(p []byte) (n int, err error) {
	b.buf = append(b.buf, p...)
	return len(p), nil
}

// String returns the built string using zero-copy conversion
func (b *Builder) String() string {
	return BytesToString(b.buf)
}

// Bytes returns the underlying byte slice
func (b *Builder) Bytes() []byte {
	return b.buf
}

// Len returns the length of the built string
func (b *Builder) Len() int {
	return len(b.buf)
}

// Cap returns the capacity of the underlying buffer
func (b *Builder) Cap() int {
	return cap(b.buf)
}

// Reset resets the builder for reuse
func (b *Builder) Reset() {
	b.buf = b.buf[:0]
}

// Grow grows the buffer capacity
func (b *Builder) Grow(n int) {
	if cap(b.buf)-len(b.buf) < n {
		newSize := len(b.buf) + 2*cap(b.buf) + n
		newBuf := make([]byte, len(b.buf), newSize)
		copy(newBuf, b.buf)
		b.buf = newBuf
	}
}

// Pool manages a pool of string builders
type Pool struct {
	builders chan *Builder
	capacity int
}

// NewPool creates a new builder pool
func NewPool(poolSize, builderCapacity int) *Pool {
	p := &Pool{
		builders: make(chan *Builder, poolSize),
		capacity: builderCapacity,
	}

	// Pre-populate the pool
	for i := 0; i < poolSize; i++ {
		p.builders <- NewBuilder(builderCapacity)
	}

	return p
}

// Get retrieves a builder from the pool
func (p *Pool) Get() *Builder {
	select {
	case builder := <-p.builders:
		return builder
	default:
		// Pool is empty, create a new one
		return NewBuilder(p.capacity)
	}
}

// Put returns a builder to the pool
func (p *Pool) Put(builder *Builder) {
	builder.Reset()
	select {
	case p.builders <- builder:
		// Successfully returned to pool
	default:
		// Pool is full, let GC handle it
	}
}

// Clone creates a copy of a string (useful when you need to own the memory)
func Clone(s string) string {
	if len(s) == 0 {
		return ""
	}
	b := make([]byte, len(s))
	copy(b, StringToBytes(s))
	return BytesToString(b)
}

// Compare compares two strings without allocation
func Compare(a, b string) int {
	if a == b {
		return 0
	}
	if a < b {
		return -1
	}
	return 1
}

// Contains checks if string contains substring without allocation
func Contains(s, substr string) bool {
	return Index(s, substr) >= 0
}

// Index finds the index of substring in string without allocation
func Index(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(substr) > len(s) {
		return -1
	}

	// Simple implementation - could be optimized with better algorithms
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// HasPrefix checks if string has prefix without allocation
func HasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// HasSuffix checks if string has suffix without allocation
func HasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

// TrimSpace removes leading and trailing whitespace
func TrimSpace(s string) string {
	start := 0
	end := len(s)

	// Find first non-space character
	for start < end && isSpace(s[start]) {
		start++
	}

	// Find last non-space character
	for end > start && isSpace(s[end-1]) {
		end--
	}

	return s[start:end]
}

// isSpace checks if a byte is a whitespace character
func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

// Split splits string by delimiter without allocating intermediate strings
// Returns a slice of string views into the original string
func Split(s, delimiter string) []string {
	if len(delimiter) == 0 {
		return []string{s}
	}

	var result []string
	start := 0

	for {
		idx := Index(s[start:], delimiter)
		if idx == -1 {
			// No more delimiters found
			result = append(result, s[start:])
			break
		}

		// Found delimiter
		result = append(result, s[start:start+idx])
		start = start + idx + len(delimiter)
	}

	return result
}

// Join joins strings using a delimiter with minimal allocations
func Join(strings []string, delimiter string) string {
	if len(strings) == 0 {
		return ""
	}
	if len(strings) == 1 {
		return strings[0]
	}

	// Calculate total length
	totalLen := 0
	for _, s := range strings {
		totalLen += len(s)
	}
	totalLen += (len(strings) - 1) * len(delimiter)

	// Build result
	builder := NewBuilder(totalLen)
	builder.WriteString(strings[0])

	for i := 1; i < len(strings); i++ {
		builder.WriteString(delimiter)
		builder.WriteString(strings[i])
	}

	return builder.String()
}

// Intern provides string interning to reduce memory usage
type Intern struct {
	strings map[string]string
}

// NewIntern creates a new string interner
func NewIntern() *Intern {
	return &Intern{
		strings: make(map[string]string),
	}
}

// Get returns an interned version of the string
func (intern *Intern) Get(s string) string {
	if interned, exists := intern.strings[s]; exists {
		return interned
	}

	// Clone the string to ensure we own the memory
	cloned := Clone(s)
	intern.strings[cloned] = cloned
	return cloned
}

// Size returns the number of interned strings
func (intern *Intern) Size() int {
	return len(intern.strings)
}

// Clear removes all interned strings
func (intern *Intern) Clear() {
	intern.strings = make(map[string]string)
}

// ========== Enhanced Pooled String Building System ==========

// Global pools for different string building scenarios
var (
	// Small strings (< 1KB) - most common case
	smallBuilderPool = &sync.Pool{
		New: func() interface{} {
			return NewBuilder(1024) // 1KB
		},
	}

	// Medium strings (1KB - 16KB) - API responses, CSV rows
	mediumBuilderPool = &sync.Pool{
		New: func() interface{} {
			return NewBuilder(16 * 1024) // 16KB
		},
	}

	// Large strings (16KB+) - bulk operations, large CSV files
	largeBuilderPool = &sync.Pool{
		New: func() interface{} {
			return NewBuilder(64 * 1024) // 64KB
		},
	}
)

// BuilderSize represents different builder sizes
type BuilderSize int

const (
	Small  BuilderSize = iota // < 1KB
	Medium                    // 1KB - 16KB
	Large                     // 16KB+
)

// GetBuilder retrieves a pooled builder of the specified size
func GetBuilder(size BuilderSize) *Builder {
	var pool *sync.Pool
	switch size {
	case Small:
		pool = smallBuilderPool
	case Medium:
		pool = mediumBuilderPool
	case Large:
		pool = largeBuilderPool
	default:
		pool = smallBuilderPool
	}

	builder := pool.Get().(*Builder)
	builder.Reset()
	return builder
}

// PutBuilder returns a builder to the appropriate pool
func PutBuilder(builder *Builder, size BuilderSize) {
	if builder == nil {
		return
	}

	var pool *sync.Pool
	switch size {
	case Small:
		pool = smallBuilderPool
	case Medium:
		pool = mediumBuilderPool
	case Large:
		pool = largeBuilderPool
	default:
		pool = smallBuilderPool
	}

	builder.Reset()
	pool.Put(builder)
}

// Concat efficiently concatenates strings using pooled builder
func Concat(strings ...string) string {
	if len(strings) == 0 {
		return ""
	}
	if len(strings) == 1 {
		return strings[0]
	}

	// Calculate total length to choose appropriate pool
	totalLen := 0
	for _, s := range strings {
		totalLen += len(s)
	}

	size := Small
	if totalLen > 16*1024 {
		size = Large
	} else if totalLen > 1024 {
		size = Medium
	}

	builder := GetBuilder(size)
	defer PutBuilder(builder, size)

	for _, s := range strings {
		builder.WriteString(s)
	}

	return Clone(builder.String())
}

// Sprintf provides a pooled alternative to fmt.Sprintf
func Sprintf(format string, args ...interface{}) string {
	// For simple cases, use direct concatenation
	if len(args) == 0 {
		return format
	}

	// Estimate size based on format string and args
	estimatedSize := len(format) + len(args)*16 // rough estimate

	size := Small
	if estimatedSize > 16*1024 {
		size = Large
	} else if estimatedSize > 1024 {
		size = Medium
	}

	builder := GetBuilder(size)
	defer PutBuilder(builder, size)

	// Use fmt to write to our builder
	fmt.Fprintf(builder, format, args...)

	return Clone(builder.String())
}

// JoinPooled efficiently joins strings using pooled builder
func JoinPooled(strings []string, delimiter string) string {
	if len(strings) == 0 {
		return ""
	}
	if len(strings) == 1 {
		return strings[0]
	}

	// Calculate total length to choose appropriate pool
	totalLen := 0
	for _, s := range strings {
		totalLen += len(s)
	}
	totalLen += (len(strings) - 1) * len(delimiter)

	size := Small
	if totalLen > 16*1024 {
		size = Large
	} else if totalLen > 1024 {
		size = Medium
	}

	builder := GetBuilder(size)
	defer PutBuilder(builder, size)

	builder.WriteString(strings[0])
	for i := 1; i < len(strings); i++ {
		builder.WriteString(delimiter)
		builder.WriteString(strings[i])
	}

	return Clone(builder.String())
}

// ========== Specialized String Builders ==========

// CSVBuilder provides optimized CSV string building
type CSVBuilder struct {
	builder  *Builder
	size     BuilderSize
	rowCount int
}

// NewCSVBuilder creates a new CSV builder
func NewCSVBuilder(estimatedRows, estimatedCols int) *CSVBuilder {
	// Estimate size based on expected data
	estimatedSize := estimatedRows * estimatedCols * 20 // rough 20 chars per cell

	size := Small
	if estimatedSize > 16*1024 {
		size = Large
	} else if estimatedSize > 1024 {
		size = Medium
	}

	return &CSVBuilder{
		builder:  GetBuilder(size),
		size:     size,
		rowCount: 0,
	}
}

// WriteHeader writes CSV header
func (cb *CSVBuilder) WriteHeader(headers []string) {
	if len(headers) == 0 {
		return
	}

	cb.builder.WriteString(headers[0])
	for i := 1; i < len(headers); i++ {
		cb.builder.WriteByte(',')
		cb.writeCSVField(headers[i])
	}
	cb.builder.WriteByte('\n')
}

// WriteRow writes a CSV row
func (cb *CSVBuilder) WriteRow(fields []string) {
	if len(fields) == 0 {
		return
	}

	cb.writeCSVField(fields[0])
	for i := 1; i < len(fields); i++ {
		cb.builder.WriteByte(',')
		cb.writeCSVField(fields[i])
	}
	cb.builder.WriteByte('\n')
	cb.rowCount++
}

// writeCSVField writes a single CSV field with proper escaping
func (cb *CSVBuilder) writeCSVField(field string) {
	needsQuoting := Contains(field, ",") || Contains(field, "\"") || Contains(field, "\n")

	if needsQuoting {
		cb.builder.WriteByte('"')
		for i := 0; i < len(field); i++ {
			if field[i] == '"' {
				cb.builder.WriteString("\"\"") // Escape quotes
			} else {
				cb.builder.WriteByte(field[i])
			}
		}
		cb.builder.WriteByte('"')
	} else {
		cb.builder.WriteString(field)
	}
}

// String returns the built CSV string
func (cb *CSVBuilder) String() string {
	return Clone(cb.builder.String())
}

// Close releases the builder back to the pool
func (cb *CSVBuilder) Close() {
	if cb.builder != nil {
		PutBuilder(cb.builder, cb.size)
		cb.builder = nil
	}
}

// URLBuilder provides optimized URL building
type URLBuilder struct {
	builder   *Builder
	size      BuilderSize
	hasParams bool
}

// NewURLBuilder creates a new URL builder
func NewURLBuilder(baseURL string) *URLBuilder {
	size := Small // URLs are typically small
	if len(baseURL) > 1024 {
		size = Medium
	}

	builder := GetBuilder(size)
	builder.WriteString(baseURL)

	return &URLBuilder{
		builder:   builder,
		size:      size,
		hasParams: Contains(baseURL, "?"),
	}
}

// AddPath adds path segments to the URL
func (ub *URLBuilder) AddPath(segments ...string) *URLBuilder {
	for _, segment := range segments {
		if segment != "" {
			ub.builder.WriteByte('/')
			// Encode path segment
			ub.builder.WriteString(urlPathEscape(segment))
		}
	}
	return ub
}

// AddParam adds a URL parameter (with proper encoding)
func (ub *URLBuilder) AddParam(key, value string) *URLBuilder {
	if ub.hasParams {
		ub.builder.WriteByte('&')
	} else {
		ub.builder.WriteByte('?')
		ub.hasParams = true
	}

	ub.builder.WriteString(urlQueryEscape(key))
	ub.builder.WriteByte('=')
	ub.builder.WriteString(urlQueryEscape(value))

	return ub
}

// AddParamInt adds an integer parameter
func (ub *URLBuilder) AddParamInt(key string, value int) *URLBuilder {
	return ub.AddParam(key, strconv.Itoa(value))
}

// AddParamBool adds a boolean parameter
func (ub *URLBuilder) AddParamBool(key string, value bool) *URLBuilder {
	return ub.AddParam(key, strconv.FormatBool(value))
}

// AddParams adds multiple parameters
func (ub *URLBuilder) AddParams(params map[string]string) *URLBuilder {
	for k, v := range params {
		ub.AddParam(k, v)
	}
	return ub
}

// String returns the built URL
func (ub *URLBuilder) String() string {
	return Clone(ub.builder.String())
}

// Query returns just the query string (without the base URL)
// This is useful for form-encoded POST bodies
func (ub *URLBuilder) Query() string {
	if !ub.hasParams {
		return ""
	}

	// Find the start of query parameters
	s := ub.builder.String()
	idx := strings.IndexByte(s, '?')
	if idx >= 0 && idx+1 < len(s) {
		return Clone(s[idx+1:])
	}
	return ""
}

// Close releases the builder back to the pool
func (ub *URLBuilder) Close() {
	if ub.builder != nil {
		PutBuilder(ub.builder, ub.size)
		ub.builder = nil
	}
}

// NewFormBuilder creates a new form builder (for application/x-www-form-urlencoded)
// This is essentially a URLBuilder without a base URL
func NewFormBuilder() *URLBuilder {
	size := Small
	builder := GetBuilder(size)

	return &URLBuilder{
		builder:   builder,
		size:      size,
		hasParams: false,
	}
}

// urlQueryEscape escapes a string for use in URL query parameters
func urlQueryEscape(s string) string {
	// Fast path for common cases
	needEscape := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !isURLSafe(c) {
			needEscape = true
			break
		}
	}

	if !needEscape {
		return s
	}

	// Slow path with escaping
	builder := GetBuilder(Small)
	defer PutBuilder(builder, Small)

	for i := 0; i < len(s); i++ {
		c := s[i]
		if isURLSafe(c) {
			builder.WriteByte(c)
		} else if c == ' ' {
			builder.WriteByte('+')
		} else {
			builder.WriteByte('%')
			builder.WriteByte("0123456789ABCDEF"[c>>4])
			builder.WriteByte("0123456789ABCDEF"[c&15])
		}
	}

	return Clone(builder.String())
}

// urlPathEscape escapes a string for use in URL path segments
func urlPathEscape(s string) string {
	// Fast path for common cases
	needEscape := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !isURLPathSafe(c) {
			needEscape = true
			break
		}
	}

	if !needEscape {
		return s
	}

	// Slow path with escaping
	builder := GetBuilder(Small)
	defer PutBuilder(builder, Small)

	for i := 0; i < len(s); i++ {
		c := s[i]
		if isURLPathSafe(c) {
			builder.WriteByte(c)
		} else {
			builder.WriteByte('%')
			builder.WriteByte("0123456789ABCDEF"[c>>4])
			builder.WriteByte("0123456789ABCDEF"[c&15])
		}
	}

	return Clone(builder.String())
}

// isURLSafe returns true if the byte is safe in URL query strings
func isURLSafe(c byte) bool {
	return (c >= 'A' && c <= 'Z') ||
		(c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') ||
		c == '-' || c == '_' || c == '.' || c == '~'
}

// isURLPathSafe returns true if the byte is safe in URL paths
func isURLPathSafe(c byte) bool {
	return (c >= 'A' && c <= 'Z') ||
		(c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') ||
		c == '-' || c == '_' || c == '.' || c == '~' ||
		c == '/' || c == ':' || c == '@' || c == '!' ||
		c == '$' || c == '&' || c == '\'' || c == '(' ||
		c == ')' || c == '*' || c == '+' || c == ',' ||
		c == ';' || c == '='
}

// SQLBuilder provides optimized SQL query building
type SQLBuilder struct {
	builder *Builder
	size    BuilderSize
}

// NewSQLBuilder creates a new SQL builder
func NewSQLBuilder(estimatedLength int) *SQLBuilder {
	size := Small
	if estimatedLength > 16*1024 {
		size = Large
	} else if estimatedLength > 1024 {
		size = Medium
	}

	return &SQLBuilder{
		builder: GetBuilder(size),
		size:    size,
	}
}

// WriteQuery writes a SQL query part
func (sb *SQLBuilder) WriteQuery(query string) *SQLBuilder {
	sb.builder.WriteString(query)
	return sb
}

// WriteSpace adds a space
func (sb *SQLBuilder) WriteSpace() *SQLBuilder {
	sb.builder.WriteByte(' ')
	return sb
}

// WriteStringLiteral writes a quoted string literal
func (sb *SQLBuilder) WriteStringLiteral(value string) *SQLBuilder {
	sb.builder.WriteByte('\'')

	// Escape single quotes
	for i := 0; i < len(value); i++ {
		if value[i] == '\'' {
			sb.builder.WriteString("''")
		} else {
			sb.builder.WriteByte(value[i])
		}
	}

	sb.builder.WriteByte('\'')
	return sb
}

// WriteIdentifier writes a quoted identifier
func (sb *SQLBuilder) WriteIdentifier(name string) *SQLBuilder {
	sb.builder.WriteByte('"')
	sb.builder.WriteString(name)
	sb.builder.WriteByte('"')
	return sb
}

// WriteInt writes an integer value
func (sb *SQLBuilder) WriteInt(value int64) *SQLBuilder {
	sb.builder.WriteString(strconv.FormatInt(value, 10))
	return sb
}

// String returns the built SQL query
func (sb *SQLBuilder) String() string {
	return Clone(sb.builder.String())
}

// Close releases the builder back to the pool
func (sb *SQLBuilder) Close() {
	if sb.builder != nil {
		PutBuilder(sb.builder, sb.size)
		sb.builder = nil
	}
}

// ========== Convenience Functions ==========

// BuildWith provides a functional approach to string building
func BuildWith(size BuilderSize, fn func(*Builder)) string {
	builder := GetBuilder(size)
	defer PutBuilder(builder, size)

	fn(builder)
	return Clone(builder.String())
}

// BuildString provides a simple way to build strings with a function
func BuildString(fn func(*Builder)) string {
	return BuildWith(Small, fn)
}

// BuildMediumString provides a way to build medium-sized strings
func BuildMediumString(fn func(*Builder)) string {
	return BuildWith(Medium, fn)
}

// BuildLargeString provides a way to build large strings
func BuildLargeString(fn func(*Builder)) string {
	return BuildWith(Large, fn)
}

// ValueToString efficiently converts interface{} values to strings
// This replaces fmt.Sprintf("%v", value) in hot paths like CSV processing
func ValueToString(value interface{}) string {
	if value == nil {
		return ""
	}

	// Fast path for common types - avoid reflection and fmt overhead
	switch v := value.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case []byte:
		return BytesToString(v)
	default:
		// Fallback to pooled sprintf for complex types
		return Sprintf("%v", value)
	}
}
