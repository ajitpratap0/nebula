package columnar

import (
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// RecordAdapter provides a pool.Record compatible interface over columnar storage
type RecordAdapter struct {
	store    *ColumnStore
	rowIndex int
	data     map[string]interface{} // Lazy-loaded cache
}

// NewRecordAdapter creates a new adapter for a specific row
func NewRecordAdapter(store *ColumnStore, rowIndex int) *RecordAdapter {
	return &RecordAdapter{
		store:    store,
		rowIndex: rowIndex,
	}
}

// loadData lazily loads row data into the cache
func (r *RecordAdapter) loadData() {
	if r.data == nil {
		r.data = make(map[string]interface{})
		r.store.mu.RLock()
		for name, col := range r.store.columns {
			if r.rowIndex < col.Len() {
				r.data[name] = col.Get(r.rowIndex)
			}
		}
		r.store.mu.RUnlock()
	}
}

// GetData retrieves a data field
func (r *RecordAdapter) GetData(key string) (interface{}, bool) {
	r.loadData()
	val, ok := r.data[key]
	return val, ok
}

// SetData sets a data field (note: this updates the cache, not the columnar store)
func (r *RecordAdapter) SetData(key string, value interface{}) {
	r.loadData()
	r.data[key] = value
}

// Data returns all data fields
func (r *RecordAdapter) Data() map[string]interface{} {
	r.loadData()
	return r.data
}

// Release returns the adapter to the pool (no-op for columnar)
func (r *RecordAdapter) Release() {
	// Clear cache
	r.data = nil
}

// BatchProcessor provides efficient batch processing over columnar data
type BatchProcessor struct {
	store     *ColumnStore
	batchSize int
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(store *ColumnStore, batchSize int) *BatchProcessor {
	return &BatchProcessor{
		store:     store,
		batchSize: batchSize,
	}
}

// Process applies a function to all records in batches
func (bp *BatchProcessor) Process(fn func(records []*pool.Record) error) error {
	totalRows := bp.store.RowCount()

	for start := 0; start < totalRows; start += bp.batchSize {
		end := start + bp.batchSize
		if end > totalRows {
			end = totalRows
		}

		// Create batch of record adapters
		batch := make([]*pool.Record, 0, end-start)

		for i := start; i < end; i++ {
			// Get record from pool
			rec := pool.GetRecord()

			// Fill with columnar data
			bp.store.mu.RLock()
			for name, col := range bp.store.columns {
				if i < col.Len() {
					rec.Data[name] = col.Get(i)
				}
			}
			bp.store.mu.RUnlock()

			batch = append(batch, rec)
		}

		// Process batch
		if err := fn(batch); err != nil {
			// Clean up
			for _, rec := range batch {
				rec.Release()
			}
			return err
		}

		// Release records back to pool
		for _, rec := range batch {
			rec.Release()
		}
	}

	return nil
}

// ColumnarPipeline provides a pipeline interface for columnar processing
type ColumnarPipeline struct {
	store *ColumnStore
}

// NewColumnarPipeline creates a new columnar pipeline
func NewColumnarPipeline() *ColumnarPipeline {
	return &ColumnarPipeline{
		store: NewColumnStore(),
	}
}

// AddRecord adds a record to the columnar store
func (p *ColumnarPipeline) AddRecord(record *pool.Record) error {
	return p.store.AppendRow(record.Data)
}

// AddBatch adds multiple records efficiently
func (p *ColumnarPipeline) AddBatch(records []*pool.Record) error {
	rows := make([]map[string]interface{}, len(records))
	for i, rec := range records {
		rows[i] = rec.Data
	}
	return p.store.AppendBatch(rows)
}

// GetStore returns the underlying columnar store
func (p *ColumnarPipeline) GetStore() *ColumnStore {
	return p.store
}

// Stats returns memory and performance statistics
func (p *ColumnarPipeline) Stats() map[string]interface{} {
	return map[string]interface{}{
		"row_count":        p.store.RowCount(),
		"column_count":     p.store.ColumnCount(),
		"total_memory":     p.store.MemoryUsage(),
		"bytes_per_record": p.store.MemoryPerRecord(),
		"columns":          p.store.ColumnNames(),
	}
}

// CSVToColumnar provides optimized CSV to columnar conversion
type CSVToColumnar struct {
	store         *ColumnStore
	headers       []string
	headerMap     map[string]int
	inferredTypes map[string]ColumnType
}

// NewCSVToColumnar creates a new CSV to columnar converter
func NewCSVToColumnar() *CSVToColumnar {
	return &CSVToColumnar{
		store:         NewColumnStore(),
		inferredTypes: make(map[string]ColumnType),
	}
}

// SetHeaders sets the CSV headers
func (c *CSVToColumnar) SetHeaders(headers []string) {
	c.headers = headers
	c.headerMap = make(map[string]int)

	// Create header map for fast lookup
	for i, h := range headers {
		c.headerMap[h] = i
		// Start with string type, will be refined later
		_ = c.store.AddColumn(h, ColumnTypeString) // Error ignored - column addition is expected to succeed
	}
}

// AddRow adds a CSV row to columnar storage
func (c *CSVToColumnar) AddRow(row []string) error {
	data := make(map[string]interface{})

	for i, value := range row {
		if i < len(c.headers) {
			data[c.headers[i]] = value
		}
	}

	return c.store.AppendRow(data)
}

// AddBatch adds multiple CSV rows efficiently
func (c *CSVToColumnar) AddBatch(rows [][]string) error {
	dataRows := make([]map[string]interface{}, len(rows))

	for i, row := range rows {
		data := make(map[string]interface{})
		for j, value := range row {
			if j < len(c.headers) {
				data[c.headers[j]] = value
			}
		}
		dataRows[i] = data
	}

	return c.store.AppendBatch(dataRows)
}

// GetStore returns the columnar store
func (c *CSVToColumnar) GetStore() *ColumnStore {
	return c.store
}

// OptimizeTypes analyzes data and converts columns to optimal types
func (c *CSVToColumnar) OptimizeTypes() error {
	// This would analyze the string data and convert to appropriate types
	// For now, keeping as strings for simplicity
	return nil
}

// StreamingColumnarWriter provides streaming write capabilities
type StreamingColumnarWriter struct {
	store      *ColumnStore
	bufferSize int
	buffer     []map[string]interface{}
}

// NewStreamingColumnarWriter creates a new streaming writer
func NewStreamingColumnarWriter(bufferSize int) *StreamingColumnarWriter {
	return &StreamingColumnarWriter{
		store:      NewColumnStore(),
		bufferSize: bufferSize,
		buffer:     make([]map[string]interface{}, 0, bufferSize),
	}
}

// Write adds a record to the buffer
func (w *StreamingColumnarWriter) Write(record *pool.Record) error {
	w.buffer = append(w.buffer, record.Data)

	if len(w.buffer) >= w.bufferSize {
		return w.Flush() // Ignore flush error
	}

	return nil
}

// Flush writes buffered records to columnar store
func (w *StreamingColumnarWriter) Flush() error {
	if len(w.buffer) == 0 {
		return nil
	}

	err := w.store.AppendBatch(w.buffer)
	w.buffer = w.buffer[:0]
	return err
}

// GetStore returns the columnar store
func (w *StreamingColumnarWriter) GetStore() *ColumnStore {
	return w.store
}

// Stats returns writer statistics
func (w *StreamingColumnarWriter) Stats() map[string]interface{} {
	return map[string]interface{}{
		"buffered_records": len(w.buffer),
		"store_rows":       w.store.RowCount(),
		"store_columns":    w.store.ColumnCount(),
		"memory_usage":     w.store.MemoryUsage(),
		"bytes_per_record": w.store.MemoryPerRecord(),
	}
}
