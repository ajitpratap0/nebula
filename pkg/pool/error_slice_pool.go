package pool

import (
	"sync"
)

// ErrorSlicePool provides pooling for error slices
type ErrorSlicePool struct {
	small  sync.Pool // 0-10 errors
	medium sync.Pool // 10-100 errors
	large  sync.Pool // 100-1000 errors
}

// Global error slice pool
var globalErrorSlicePool = &ErrorSlicePool{
	small: sync.Pool{
		New: func() interface{} {
			return make([]error, 0, 10)
		},
	},
	medium: sync.Pool{
		New: func() interface{} {
			return make([]error, 0, 100)
		},
	},
	large: sync.Pool{
		New: func() interface{} {
			return make([]error, 0, 1000)
		},
	},
}

// GetErrorSlice gets an error slice from the pool based on expected size
func GetErrorSlice(expectedSize int) []error {
	if expectedSize <= 10 {
		if v, ok := globalErrorSlicePool.small.Get().([]error); ok {
			return v
		}
		return make([]error, 0, 10)
	} else if expectedSize <= 100 {
		if v, ok := globalErrorSlicePool.medium.Get().([]error); ok {
			return v
		}
		return make([]error, 0, 100)
	} else {
		if v, ok := globalErrorSlicePool.large.Get().([]error); ok {
			return v
		}
		return make([]error, 0, 1000)
	}
}

// PutErrorSlice returns an error slice to the appropriate pool
func PutErrorSlice(errs []error) {
	if errs == nil {
		return
	}

	// Clear the slice
	errs = errs[:0]

	// Return to appropriate pool based on capacity
	cap := cap(errs)
	if cap <= 10 {
		globalErrorSlicePool.small.Put(&errs)
	} else if cap <= 100 {
		globalErrorSlicePool.medium.Put(&errs)
	} else if cap <= 1000 {
		globalErrorSlicePool.large.Put(&errs)
	}
	// Don't pool very large slices to avoid memory bloat
}
