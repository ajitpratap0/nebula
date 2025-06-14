// Package pipeline provides data pipeline components
package pipeline

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/ajitpratap0/nebula/pkg/columnar"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// StorageMode defines the storage strategy
type StorageMode string

const (
	// StorageModeRow uses traditional row-based storage
	StorageModeRow StorageMode = "row"
	// StorageModeColumnar uses columnar storage for batch processing
	StorageModeColumnar StorageMode = "columnar"
	// StorageModeHybrid automatically selects based on workload
	StorageModeHybrid StorageMode = "hybrid"
)

// StorageAdapter provides unified interface for row/columnar storage
type StorageAdapter struct {
	mode          StorageMode
	config        *config.BaseConfig
	
	// Row-based storage
	rowBuffer     chan *pool.Record
	rowBatch      []*pool.Record
	
	// Columnar storage
	columnarStore *columnar.ColumnStore
	columnarBatch *columnar.DirectCSVToColumnar
	
	// Metrics
	recordCount   atomic.Int64
	bytesUsed     atomic.Int64
	
	// Synchronization
	mu            sync.RWMutex
	flushCh       chan struct{}
	done          chan struct{}
}

// NewStorageAdapter creates a new storage adapter
func NewStorageAdapter(mode StorageMode, cfg *config.BaseConfig) *StorageAdapter {
	adapter := &StorageAdapter{
		mode:     mode,
		config:   cfg,
		rowBuffer: make(chan *pool.Record, cfg.Performance.BatchSize),
		flushCh:  make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	
	// Initialize based on mode
	switch mode {
	case StorageModeColumnar:
		adapter.columnarStore = columnar.NewColumnStore()
		adapter.columnarBatch = columnar.NewDirectCSVToColumnar()
	case StorageModeRow:
		adapter.rowBatch = make([]*pool.Record, 0, cfg.Performance.BatchSize)
	case StorageModeHybrid:
		// Initialize both
		adapter.columnarStore = columnar.NewColumnStore()
		adapter.columnarBatch = columnar.NewDirectCSVToColumnar()
		adapter.rowBatch = make([]*pool.Record, 0, cfg.Performance.BatchSize)
	}
	
	// Start background flusher
	go adapter.backgroundFlusher()
	
	return adapter
}

// AddRecord adds a record to the adapter
func (a *StorageAdapter) AddRecord(record *pool.Record) error {
	a.recordCount.Add(1)
	
	switch a.mode {
	case StorageModeRow:
		return a.addRowRecord(record)
	case StorageModeColumnar:
		return a.addColumnarRecord(record)
	case StorageModeHybrid:
		return a.addHybridRecord(record)
	}
	
	return fmt.Errorf("unknown storage mode: %s", a.mode)
}

// addRowRecord adds to row-based storage
func (a *StorageAdapter) addRowRecord(record *pool.Record) error {
	// For streaming, send directly to channel
	if a.config.Performance.StreamingMode {
		select {
		case a.rowBuffer <- record:
			return nil
		default:
			// Buffer full, trigger flush
			a.triggerFlush()
			a.rowBuffer <- record
			return nil
		}
	}
	
	// For batch mode, accumulate
	a.mu.Lock()
	a.rowBatch = append(a.rowBatch, record)
	shouldFlush := len(a.rowBatch) >= a.config.Performance.BatchSize
	a.mu.Unlock()
	
	if shouldFlush {
		a.triggerFlush()
	}
	
	return nil
}

// addColumnarRecord adds to columnar storage
func (a *StorageAdapter) addColumnarRecord(record *pool.Record) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	
	// Convert record to map for columnar storage
	data := make(map[string]interface{})
	for k, v := range record.Data {
		data[k] = v
	}
	
	// Add metadata fields
	data["_id"] = record.ID
	data["_source"] = record.Metadata.Source
	data["_timestamp"] = record.Metadata.Timestamp.Unix()
	
	return a.columnarStore.AppendRow(data)
}

// addHybridRecord intelligently routes based on workload
func (a *StorageAdapter) addHybridRecord(record *pool.Record) error {
	// Simple heuristic: use columnar for large batches
	count := a.recordCount.Load()
	
	if count > 10000 {
		// Switch to columnar for large datasets
		return a.addColumnarRecord(record)
	}
	
	// Use row-based for small datasets or streaming
	return a.addRowRecord(record)
}

// GetRecords retrieves records based on storage mode
func (a *StorageAdapter) GetRecords(offset, limit int) ([]*pool.Record, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	
	switch a.mode {
	case StorageModeRow:
		return a.getRowRecords(offset, limit)
	case StorageModeColumnar:
		return a.getColumnarRecords(offset, limit)
	case StorageModeHybrid:
		// Check which storage has data
		if a.columnarStore != nil && a.columnarStore.RowCount() > 0 {
			return a.getColumnarRecords(offset, limit)
		}
		return a.getRowRecords(offset, limit)
	}
	
	return nil, fmt.Errorf("unknown storage mode: %s", a.mode)
}

// getRowRecords retrieves from row storage
func (a *StorageAdapter) getRowRecords(offset, limit int) ([]*pool.Record, error) {
	if offset >= len(a.rowBatch) {
		return nil, nil
	}
	
	end := offset + limit
	if end > len(a.rowBatch) {
		end = len(a.rowBatch)
	}
	
	result := make([]*pool.Record, end-offset)
	copy(result, a.rowBatch[offset:end])
	return result, nil
}

// getColumnarRecords retrieves from columnar storage
func (a *StorageAdapter) getColumnarRecords(offset, limit int) ([]*pool.Record, error) {
	records := make([]*pool.Record, 0, limit)
	
	for i := offset; i < offset+limit && i < a.columnarStore.RowCount(); i++ {
		row, err := a.columnarStore.GetRow(i)
		if err != nil {
			return nil, err
		}
		
		// Convert back to record
		record := pool.GetRecord()
		
		// Extract metadata
		if id, ok := row["_id"].(string); ok {
			record.ID = id
			delete(row, "_id")
		}
		if source, ok := row["_source"].(string); ok {
			record.Metadata.Source = source
			delete(row, "_source")
		}
		if ts, ok := row["_timestamp"].(int64); ok {
			record.Metadata.Timestamp = time.Unix(ts, 0)
			delete(row, "_timestamp")
		}
		
		// Copy data
		for k, v := range row {
			record.Data[k] = v
		}
		
		records = append(records, record)
	}
	
	return records, nil
}

// Flush forces a flush of buffered data
func (a *StorageAdapter) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	
	switch a.mode {
	case StorageModeRow:
		// Row mode doesn't need explicit flush
		return nil
	case StorageModeColumnar:
		// Optimize column types after batch
		if a.columnarBatch != nil {
			a.columnarBatch.OptimizeTypes()
		}
		return nil
	case StorageModeHybrid:
		// Flush both if needed
		if a.columnarBatch != nil {
			a.columnarBatch.OptimizeTypes()
		}
		return nil
	}
	
	return nil
}

// triggerFlush signals the background flusher
func (a *StorageAdapter) triggerFlush() {
	select {
	case a.flushCh <- struct{}{}:
	default:
		// Already pending
	}
}

// backgroundFlusher handles periodic flushing
func (a *StorageAdapter) backgroundFlusher() {
	ticker := time.NewTicker(time.Duration(a.config.Performance.FlushInterval) * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-a.done:
			return
		case <-a.flushCh:
			a.Flush()
		case <-ticker.C:
			a.Flush()
		}
	}
}

// GetMemoryUsage returns current memory usage
func (a *StorageAdapter) GetMemoryUsage() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	
	switch a.mode {
	case StorageModeRow:
		// Estimate: 225 bytes per record (current optimized)
		return int64(len(a.rowBatch)) * 225
	case StorageModeColumnar:
		if a.columnarStore != nil {
			return a.columnarStore.MemoryUsage()
		}
		return 0
	case StorageModeHybrid:
		total := int64(len(a.rowBatch)) * 225
		if a.columnarStore != nil {
			total += a.columnarStore.MemoryUsage()
		}
		return total
	}
	
	return 0
}

// GetMemoryPerRecord returns average memory per record
func (a *StorageAdapter) GetMemoryPerRecord() float64 {
	count := a.recordCount.Load()
	if count == 0 {
		return 0
	}
	
	usage := a.GetMemoryUsage()
	return float64(usage) / float64(count)
}

// Close shuts down the adapter
func (a *StorageAdapter) Close() error {
	close(a.done)
	close(a.rowBuffer)
	
	// Release row records
	a.mu.Lock()
	for _, rec := range a.rowBatch {
		rec.Release()
	}
	a.rowBatch = nil
	a.mu.Unlock()
	
	return nil
}

// GetStorageMode returns the current storage mode
func (a *StorageAdapter) GetStorageMode() StorageMode {
	return a.mode
}

// GetRecordCount returns the total number of records
func (a *StorageAdapter) GetRecordCount() int64 {
	return a.recordCount.Load()
}

// OptimizeStorage runs storage-specific optimizations
func (a *StorageAdapter) OptimizeStorage() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	
	switch a.mode {
	case StorageModeColumnar:
		// Run type optimization on columnar storage
		if a.columnarBatch != nil {
			return a.columnarBatch.OptimizeTypes()
		}
	}
	
	return nil
}