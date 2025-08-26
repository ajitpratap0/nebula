package pool

import (
	"sync"
)

// String batch pools for CSV and similar operations
var (
	// Pool for [][]string batches (CSV rows)
	StringBatchPool = &sync.Pool{
		New: func() interface{} {
			return make([][]string, 0, 5000)
		},
	}

	// Pool for []string slices (individual CSV rows)
	csvRowPool = &sync.Pool{
		New: func() interface{} {
			return make([]string, 0, 20) // Typical CSV column count
		},
	}
)

// GetStringBatch gets a [][]string from the pool
func GetStringBatch(capacity int) [][]string {
	obj := StringBatchPool.Get()
	if obj == nil {
		return make([][]string, 0, 5000)
	}
	batch := obj.([][]string)
	if cap(batch) < capacity {
		batch = make([][]string, 0, capacity)
	}
	return batch[:0]
}

// PutStringBatch returns a [][]string to the pool
func PutStringBatch(batch [][]string) {
	if batch == nil {
		return
	}
	// Clear references to allow GC
	for i := range batch {
		batch[i] = nil
	}
	batch = batch[:0]
	StringBatchPool.Put(&batch)
}

// GetCSVRow gets a []string from the pool for CSV row operations
func GetCSVRow(capacity int) []string {
	obj := csvRowPool.Get()
	if obj == nil {
		return make([]string, 0, 20)
	}
	slice := obj.([]string)
	if cap(slice) < capacity {
		return make([]string, 0, capacity)
	}
	return slice[:0]
}

// PutCSVRow returns a []string to the pool
func PutCSVRow(slice []string) {
	if slice == nil {
		return
	}
	// Clear references
	for i := range slice {
		slice[i] = ""
	}
	slice = slice[:0]
	csvRowPool.Put(&slice)
}
