// Package performance provides memory optimization utilities
package performance

import (
	"fmt"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ajitpratap0/nebula/pkg/models"
)

// MemoryOptimizer provides memory optimization capabilities
type MemoryOptimizer struct {
	config       *MemoryConfig
	objectPool   *ObjectPool
	stringIntern *StringIntern
	columnStore  *ColumnStore
	metrics      *MemoryMetrics
	mu           sync.RWMutex
}

// MemoryConfig configures memory optimization
type MemoryConfig struct {
	EnableObjectPooling   bool
	ObjectPoolSize        int
	EnableStringInterning bool
	StringInternSize      int
	EnableColumnStore     bool
	EnableCompaction      bool
	CompactionThreshold   float64
	MaxMemoryMB           int
}

// DefaultMemoryConfig returns default configuration
func DefaultMemoryConfig() *MemoryConfig {
	return &MemoryConfig{
		EnableObjectPooling:   true,
		ObjectPoolSize:        10000,
		EnableStringInterning: true,
		StringInternSize:      50000,
		EnableColumnStore:     true,
		EnableCompaction:      true,
		CompactionThreshold:   0.7,
		MaxMemoryMB:           1024,
	}
}

// MemoryMetrics tracks memory metrics
type MemoryMetrics struct {
	AllocatedMB     uint64
	UsedMB          uint64
	ObjectsPooled   int64
	StringsInterned int64
	CompactionCount int64
	BytesSaved      int64
}

// NewMemoryOptimizer creates a memory optimizer
func NewMemoryOptimizer(config *MemoryConfig) *MemoryOptimizer {
	if config == nil {
		config = DefaultMemoryConfig()
	}

	mo := &MemoryOptimizer{
		config:  config,
		metrics: &MemoryMetrics{},
	}

	if config.EnableObjectPooling {
		mo.objectPool = NewObjectPool(config.ObjectPoolSize)
	}

	if config.EnableStringInterning {
		mo.stringIntern = NewStringIntern(config.StringInternSize)
	}

	if config.EnableColumnStore {
		mo.columnStore = NewColumnStore()
	}

	return mo
}

// OptimizeRecords optimizes memory usage for records
func (mo *MemoryOptimizer) OptimizeRecords(records []*models.Record) []*models.Record {
	if len(records) == 0 {
		return records
	}

	// Use column store for better memory layout
	if mo.config.EnableColumnStore && len(records) > 100 {
		return mo.columnStore.OptimizeRecords(records)
	}

	// Optimize individual records
	optimized := make([]*models.Record, len(records))
	for i, record := range records {
		optimized[i] = mo.OptimizeRecord(record)
	}

	return optimized
}

// OptimizeRecord optimizes a single record
func (mo *MemoryOptimizer) OptimizeRecord(record *models.Record) *models.Record {
	if record == nil {
		return nil
	}

	// Get pooled record
	var optimized *models.Record
	if mo.objectPool != nil {
		optimized = mo.objectPool.GetRecord()
		atomic.AddInt64(&mo.metrics.ObjectsPooled, 1)
	} else {
		optimized = &models.Record{}
	}

	// Copy and optimize data
	optimized.Data = make(map[string]interface{}, len(record.Data))
	for k, v := range record.Data {
		// Intern strings
		if mo.stringIntern != nil {
			if str, ok := v.(string); ok {
				v = mo.stringIntern.Intern(str)
				atomic.AddInt64(&mo.metrics.StringsInterned, 1)
			}
			k = mo.stringIntern.Intern(k)
		}
		optimized.Data[k] = v
	}

	// Copy metadata
	optimized.Metadata = record.Metadata

	return optimized
}

// ReleaseRecord returns record to pool
func (mo *MemoryOptimizer) ReleaseRecord(record *models.Record) {
	if mo.objectPool != nil && record != nil {
		// Clear data
		for k := range record.Data {
			delete(record.Data, k)
		}
		// Reset metadata custom fields
		record.Metadata.Custom = nil
		mo.objectPool.PutRecord(record)
	}
}

// Compact performs memory compaction
func (mo *MemoryOptimizer) Compact() {
	mo.mu.Lock()
	defer mo.mu.Unlock()

	// Get current memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Check if compaction needed
	usageRatio := float64(memStats.HeapAlloc) / float64(memStats.HeapSys)
	if usageRatio < mo.config.CompactionThreshold {
		return
	}

	// Force GC
	runtime.GC()
	runtime.GC() // Run twice for better collection

	// Return memory to OS
	debug.FreeOSMemory()

	atomic.AddInt64(&mo.metrics.CompactionCount, 1)
}

// GetMetrics returns memory metrics
func (mo *MemoryOptimizer) GetMetrics() *MemoryMetrics {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	mo.metrics.AllocatedMB = memStats.Alloc / 1024 / 1024
	mo.metrics.UsedMB = memStats.HeapAlloc / 1024 / 1024

	return mo.metrics
}

// ObjectPool provides object pooling
type ObjectPool struct {
	recordPool sync.Pool
	mapPool    sync.Pool
	slicePool  sync.Pool
	capacity   int
	created    int64
	reused     int64
}

// NewObjectPool creates an object pool
func NewObjectPool(capacity int) *ObjectPool {
	op := &ObjectPool{
		capacity: capacity,
	}

	op.recordPool.New = func() interface{} {
		atomic.AddInt64(&op.created, 1)
		return &models.Record{
			Data: make(map[string]interface{}),
		}
	}

	op.mapPool.New = func() interface{} {
		return make(map[string]interface{})
	}

	op.slicePool.New = func() interface{} {
		return make([]interface{}, 0, 100)
	}

	// Pre-allocate objects
	records := make([]*models.Record, capacity/10)
	for i := range records {
		records[i] = &models.Record{
			Data: make(map[string]interface{}),
		}
	}
	for _, r := range records {
		op.recordPool.Put(r)
	}

	return op
}

// GetRecord gets a record from pool
func (op *ObjectPool) GetRecord() *models.Record {
	atomic.AddInt64(&op.reused, 1)
	return op.recordPool.Get().(*models.Record)
}

// PutRecord returns record to pool
func (op *ObjectPool) PutRecord(record *models.Record) {
	if record != nil {
		op.recordPool.Put(record)
	}
}

// GetMap gets a map from pool
func (op *ObjectPool) GetMap() map[string]interface{} {
	return op.mapPool.Get().(map[string]interface{})
}

// PutMap returns map to pool
func (op *ObjectPool) PutMap(m map[string]interface{}) {
	// Clear map
	for k := range m {
		delete(m, k)
	}
	op.mapPool.Put(m)
}

// StringIntern provides string interning
type StringIntern struct {
	table    map[string]string
	capacity int
	size     int64
	saved    int64
	mu       sync.RWMutex
}

// NewStringIntern creates a string intern table
func NewStringIntern(capacity int) *StringIntern {
	return &StringIntern{
		table:    make(map[string]string, capacity),
		capacity: capacity,
	}
}

// Intern interns a string
func (si *StringIntern) Intern(s string) string {
	if s == "" {
		return s
	}

	si.mu.RLock()
	if interned, ok := si.table[s]; ok {
		si.mu.RUnlock()
		atomic.AddInt64(&si.saved, int64(len(s)))
		return interned
	}
	si.mu.RUnlock()

	si.mu.Lock()
	defer si.mu.Unlock()

	// Double-check after acquiring write lock
	if interned, ok := si.table[s]; ok {
		atomic.AddInt64(&si.saved, int64(len(s)))
		return interned
	}

	// Add to table if under capacity
	if len(si.table) < si.capacity {
		si.table[s] = s
		atomic.AddInt64(&si.size, int64(len(s)))
		return s
	}

	// Table full, evict random entry
	for k := range si.table {
		delete(si.table, k)
		break
	}
	si.table[s] = s
	return s
}

// GetStats returns interning statistics
func (si *StringIntern) GetStats() (size, saved int64) {
	return atomic.LoadInt64(&si.size), atomic.LoadInt64(&si.saved)
}

// ColumnStore provides columnar storage optimization
type ColumnStore struct {
	columns map[string]*Column
	mu      sync.RWMutex
}

// Column represents a data column
type Column struct {
	name   string
	dtype  reflect.Type
	values []interface{}
	nulls  []bool
}

// NewColumnStore creates a column store
func NewColumnStore() *ColumnStore {
	return &ColumnStore{
		columns: make(map[string]*Column),
	}
}

// OptimizeRecords converts records to columnar format
func (cs *ColumnStore) OptimizeRecords(records []*models.Record) []*models.Record {
	if len(records) == 0 {
		return records
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Analyze schema from first record
	schema := make(map[string]reflect.Type)
	for k, v := range records[0].Data {
		if v != nil {
			schema[k] = reflect.TypeOf(v)
		}
	}

	// Create columns
	for name, dtype := range schema {
		cs.columns[name] = &Column{
			name:   name,
			dtype:  dtype,
			values: make([]interface{}, 0, len(records)),
			nulls:  make([]bool, 0, len(records)),
		}
	}

	// Populate columns
	for _, record := range records {
		for name, col := range cs.columns {
			if value, ok := record.Data[name]; ok && value != nil {
				col.values = append(col.values, value)
				col.nulls = append(col.nulls, false)
			} else {
				col.values = append(col.values, nil)
				col.nulls = append(col.nulls, true)
			}
		}
	}

	// Return optimized records (referencing columnar data)
	// In practice, this would return a different structure
	// that efficiently accesses columnar data
	return records
}

// MemoryProfile represents memory profile data
type MemoryProfile struct {
	Timestamp    int64
	AllocMB      uint64
	TotalAllocMB uint64
	SysMB        uint64
	NumGC        uint32
	PauseNs      uint64
	Objects      int64
}

// CaptureMemoryProfile captures current memory profile
func CaptureMemoryProfile() *MemoryProfile {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return &MemoryProfile{
		Timestamp:    time.Now().Unix(),
		AllocMB:      memStats.Alloc / 1024 / 1024,
		TotalAllocMB: memStats.TotalAlloc / 1024 / 1024,
		SysMB:        memStats.Sys / 1024 / 1024,
		NumGC:        memStats.NumGC,
		PauseNs:      memStats.PauseTotalNs,
		Objects:      int64(memStats.HeapObjects),
	}
}

// CompareMemoryProfiles compares two memory profiles
func CompareMemoryProfiles(before, after *MemoryProfile) string {
	allocDiff := int64(after.AllocMB) - int64(before.AllocMB)
	gcDiff := after.NumGC - before.NumGC
	objectDiff := after.Objects - before.Objects

	return fmt.Sprintf(
		"Memory Delta: Alloc=%+dMB, GCs=%d, Objects=%+d, GCPause=%dms",
		allocDiff, gcDiff, objectDiff,
		(after.PauseNs-before.PauseNs)/1000000,
	)
}

// UnsafeStringOptimizer provides unsafe string optimizations
type UnsafeStringOptimizer struct {
	enabled bool
}

// NewUnsafeStringOptimizer creates unsafe string optimizer
func NewUnsafeStringOptimizer(enabled bool) *UnsafeStringOptimizer {
	return &UnsafeStringOptimizer{enabled: enabled}
}

// BytesToString converts bytes to string without allocation
func (uso *UnsafeStringOptimizer) BytesToString(b []byte) string {
	if !uso.enabled {
		return string(b)
	}
	return *(*string)(unsafe.Pointer(&b))
}

// StringToBytes converts string to bytes without allocation
func (uso *UnsafeStringOptimizer) StringToBytes(s string) []byte {
	if !uso.enabled {
		return []byte(s)
	}
	return *(*[]byte)(unsafe.Pointer(&s))
}
