// Package performance provides optimization strategies
package performance

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/models"
)

// Optimizer provides performance optimization capabilities
type Optimizer struct {
	config       *OptimizerConfig
	bufferPool   *BufferPool
	workerPool   *WorkerPool
	batchManager *BatchManager
	metrics      *OptimizationMetrics
	mu           sync.RWMutex
}

// OptimizerConfig configures the optimizer
type OptimizerConfig struct {
	// Memory optimization
	EnableMemoryPool bool
	MemoryPoolSize   int
	BufferSize       int

	// CPU optimization
	WorkerCount    int
	MaxGoroutines  int
	EnableAffinity bool

	// Batch optimization
	OptimalBatchSize int
	AdaptiveBatching bool
	BatchTimeout     time.Duration

	// Pipeline optimization
	EnablePipelining bool
	PipelineDepth    int

	// Zero-copy optimization
	EnableZeroCopy bool

	// Cache optimization
	EnableCaching bool
	CacheSize     int
}

// DefaultOptimizerConfig returns optimized configuration
func DefaultOptimizerConfig() *OptimizerConfig {
	numCPU := runtime.NumCPU()
	return &OptimizerConfig{
		EnableMemoryPool: true,
		MemoryPoolSize:   100,
		BufferSize:       64 * 1024, // 64KB
		WorkerCount:      numCPU,
		MaxGoroutines:    numCPU * 100,
		EnableAffinity:   false,
		OptimalBatchSize: 10000,
		AdaptiveBatching: true,
		BatchTimeout:     100 * time.Millisecond,
		EnablePipelining: true,
		PipelineDepth:    4,
		EnableZeroCopy:   true,
		EnableCaching:    true,
		CacheSize:        1000,
	}
}

// OptimizationMetrics tracks optimization metrics
type OptimizationMetrics struct {
	MemoryPoolHits     int64
	MemoryPoolMisses   int64
	BatchesOptimized   int64
	PipelineStalls     int64
	ZeroCopyOperations int64
	CacheHits          int64
	CacheMisses        int64
	WorkerUtilization  float64
}

// NewOptimizer creates a new optimizer
func NewOptimizer(config *OptimizerConfig) *Optimizer {
	if config == nil {
		config = DefaultOptimizerConfig()
	}

	o := &Optimizer{
		config:  config,
		metrics: &OptimizationMetrics{},
	}

	if config.EnableMemoryPool {
		o.bufferPool = NewBufferPool(config.MemoryPoolSize, config.BufferSize)
	}

	o.workerPool = NewWorkerPool(config.WorkerCount, config.MaxGoroutines)
	o.batchManager = NewBatchManager(config.OptimalBatchSize, config.AdaptiveBatching)

	return o
}

// OptimizeMemory optimizes memory usage
func (o *Optimizer) OptimizeMemory() {
	// Force GC to free unused memory
	runtime.GC()

	// Limit memory allocation
	runtime.MemProfileRate = 512 * 1024

	// Return memory to OS
	debug.FreeOSMemory()
}

// OptimizeCPU optimizes CPU usage
func (o *Optimizer) OptimizeCPU() {
	// Set GOMAXPROCS based on config
	if o.config.WorkerCount > 0 {
		runtime.GOMAXPROCS(o.config.WorkerCount)
	}

	// Enable CPU affinity if supported
	if o.config.EnableAffinity {
		// Platform-specific CPU affinity setting
		// This would require platform-specific implementation
	}
}

// GetBuffer gets an optimized buffer
func (o *Optimizer) GetBuffer() []byte {
	if o.bufferPool != nil {
		atomic.AddInt64(&o.metrics.MemoryPoolHits, 1)
		return o.bufferPool.Get()
	}
	atomic.AddInt64(&o.metrics.MemoryPoolMisses, 1)
	return make([]byte, o.config.BufferSize)
}

// PutBuffer returns a buffer to the pool
func (o *Optimizer) PutBuffer(buf []byte) {
	if o.bufferPool != nil {
		o.bufferPool.Put(buf)
	}
}

// OptimizeBatch optimizes batch size based on performance
func (o *Optimizer) OptimizeBatch(currentSize int, throughput float64) int {
	if !o.config.AdaptiveBatching {
		return o.config.OptimalBatchSize
	}

	atomic.AddInt64(&o.metrics.BatchesOptimized, 1)
	return o.batchManager.OptimizeSize(currentSize, throughput)
}

// ExecuteParallel executes function in parallel with optimization
func (o *Optimizer) ExecuteParallel(ctx context.Context, items []interface{}, fn func(interface{}) error) error {
	return o.workerPool.Execute(ctx, items, fn)
}

// GetMetrics returns optimization metrics
func (o *Optimizer) GetMetrics() *OptimizationMetrics {
	o.mu.RLock()
	defer o.mu.RUnlock()

	metrics := *o.metrics
	if o.workerPool != nil {
		metrics.WorkerUtilization = o.workerPool.GetUtilization()
	}
	return &metrics
}

// BufferPool provides memory buffer pooling
type BufferPool struct {
	pool   sync.Pool
	size   int
	hits   int64
	misses int64
}

// NewBufferPool creates a buffer pool
func NewBufferPool(poolSize, bufferSize int) *BufferPool {
	bp := &BufferPool{
		size: bufferSize,
	}

	bp.pool.New = func() interface{} {
		return make([]byte, bufferSize)
	}

	// Pre-allocate buffers
	buffers := make([][]byte, poolSize)
	for i := 0; i < poolSize; i++ {
		buffers[i] = make([]byte, bufferSize)
	}
	for _, buf := range buffers {
		bp.pool.Put(buf)
	}

	return bp
}

// Get gets a buffer from pool
func (bp *BufferPool) Get() []byte {
	buf := bp.pool.Get().([]byte)
	atomic.AddInt64(&bp.hits, 1)
	return buf[:0] // Reset slice
}

// Put returns buffer to pool
func (bp *BufferPool) Put(buf []byte) {
	if cap(buf) >= bp.size {
		bp.pool.Put(buf)
	}
}

// WorkerPool manages worker goroutines
type WorkerPool struct {
	workers        int
	maxGoroutines  int
	semaphore      chan struct{}
	activeWorkers  int32
	totalTasks     int64
	completedTasks int64
}

// NewWorkerPool creates a worker pool
func NewWorkerPool(workers, maxGoroutines int) *WorkerPool {
	return &WorkerPool{
		workers:       workers,
		maxGoroutines: maxGoroutines,
		semaphore:     make(chan struct{}, maxGoroutines),
	}
}

// Execute executes tasks in parallel
func (wp *WorkerPool) Execute(ctx context.Context, items []interface{}, fn func(interface{}) error) error {
	if len(items) == 0 {
		return nil
	}

	// Determine optimal batch size per worker
	batchSize := len(items) / wp.workers
	if batchSize < 1 {
		batchSize = 1
	}

	var wg sync.WaitGroup
	errors := make(chan error, len(items))

	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}

		wg.Add(1)
		batch := items[i:end]

		go func(batch []interface{}) {
			defer wg.Done()

			// Acquire semaphore
			wp.semaphore <- struct{}{}
			defer func() { <-wp.semaphore }()

			atomic.AddInt32(&wp.activeWorkers, 1)
			defer atomic.AddInt32(&wp.activeWorkers, -1)

			for _, item := range batch {
				select {
				case <-ctx.Done():
					errors <- ctx.Err()
					return
				default:
					atomic.AddInt64(&wp.totalTasks, 1)
					if err := fn(item); err != nil {
						errors <- err
					} else {
						atomic.AddInt64(&wp.completedTasks, 1)
					}
				}
			}
		}(batch)
	}

	wg.Wait()
	close(errors)

	// Collect errors
	var errs []error
	for err := range errors {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("worker pool errors: %v", errs)
	}

	return nil
}

// GetUtilization returns worker utilization
func (wp *WorkerPool) GetUtilization() float64 {
	active := atomic.LoadInt32(&wp.activeWorkers)
	return float64(active) / float64(wp.workers)
}

// BatchManager manages dynamic batch sizing
type BatchManager struct {
	optimalSize       int
	adaptive          bool
	minSize           int
	maxSize           int
	currentSize       int
	throughputHistory []float64
	mu                sync.Mutex
}

// NewBatchManager creates a batch manager
func NewBatchManager(optimalSize int, adaptive bool) *BatchManager {
	return &BatchManager{
		optimalSize:       optimalSize,
		adaptive:          adaptive,
		minSize:           100,
		maxSize:           100000,
		currentSize:       optimalSize,
		throughputHistory: make([]float64, 0, 10),
	}
}

// OptimizeSize optimizes batch size based on throughput
func (bm *BatchManager) OptimizeSize(currentSize int, throughput float64) int {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if !bm.adaptive {
		return bm.optimalSize
	}

	// Track throughput history
	bm.throughputHistory = append(bm.throughputHistory, throughput)
	if len(bm.throughputHistory) > 10 {
		bm.throughputHistory = bm.throughputHistory[1:]
	}

	// Need at least 3 samples
	if len(bm.throughputHistory) < 3 {
		return bm.currentSize
	}

	// Calculate average throughput
	avgThroughput := 0.0
	for _, t := range bm.throughputHistory {
		avgThroughput += t
	}
	avgThroughput /= float64(len(bm.throughputHistory))

	// Adjust batch size based on throughput trend
	lastThroughput := bm.throughputHistory[len(bm.throughputHistory)-1]

	if lastThroughput > avgThroughput*1.1 {
		// Throughput improving, increase batch size
		bm.currentSize = int(float64(bm.currentSize) * 1.2)
	} else if lastThroughput < avgThroughput*0.9 {
		// Throughput degrading, decrease batch size
		bm.currentSize = int(float64(bm.currentSize) * 0.8)
	}

	// Apply bounds
	if bm.currentSize < bm.minSize {
		bm.currentSize = bm.minSize
	}
	if bm.currentSize > bm.maxSize {
		bm.currentSize = bm.maxSize
	}

	return bm.currentSize
}

// PipelineOptimizer optimizes data pipelines
type PipelineOptimizer struct {
	depth   int
	stages  []PipelineStage
	metrics *PipelineMetrics
	mu      sync.RWMutex
}

// PipelineStage represents a pipeline stage
type PipelineStage interface {
	Process(ctx context.Context, input interface{}) (interface{}, error)
	Name() string
}

// PipelineMetrics tracks pipeline metrics
type PipelineMetrics struct {
	StageLatencies map[string]time.Duration
	Throughput     float64
	Stalls         int64
}

// NewPipelineOptimizer creates a pipeline optimizer
func NewPipelineOptimizer(depth int) *PipelineOptimizer {
	return &PipelineOptimizer{
		depth: depth,
		metrics: &PipelineMetrics{
			StageLatencies: make(map[string]time.Duration),
		},
	}
}

// AddStage adds a pipeline stage
func (po *PipelineOptimizer) AddStage(stage PipelineStage) {
	po.mu.Lock()
	defer po.mu.Unlock()
	po.stages = append(po.stages, stage)
}

// Execute executes the pipeline
func (po *PipelineOptimizer) Execute(ctx context.Context, inputs []interface{}) ([]interface{}, error) {
	if len(po.stages) == 0 {
		return inputs, nil
	}

	// Create channels for each stage
	channels := make([]chan interface{}, len(po.stages)+1)
	for i := range channels {
		channels[i] = make(chan interface{}, po.depth)
	}

	// Error channel
	errChan := make(chan error, len(po.stages))

	// Start pipeline stages
	var wg sync.WaitGroup
	for i, stage := range po.stages {
		wg.Add(1)
		go func(stageIdx int, stage PipelineStage) {
			defer wg.Done()
			defer close(channels[stageIdx+1])

			for input := range channels[stageIdx] {
				start := time.Now()
				output, err := stage.Process(ctx, input)
				if err != nil {
					errChan <- err
					continue
				}

				po.mu.Lock()
				po.metrics.StageLatencies[stage.Name()] = time.Since(start)
				po.mu.Unlock()

				select {
				case channels[stageIdx+1] <- output:
				case <-ctx.Done():
					return
				}
			}
		}(i, stage)
	}

	// Feed inputs
	go func() {
		defer close(channels[0])
		for _, input := range inputs {
			select {
			case channels[0] <- input:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Collect outputs
	var outputs []interface{}
	outputDone := make(chan struct{})
	go func() {
		defer close(outputDone)
		for output := range channels[len(channels)-1] {
			outputs = append(outputs, output)
		}
	}()

	// Wait for completion
	wg.Wait()
	close(errChan)
	<-outputDone

	// Check for errors
	for err := range errChan {
		if err != nil {
			return outputs, err
		}
	}

	return outputs, nil
}

// ZeroCopyOptimizer provides zero-copy optimizations
type ZeroCopyOptimizer struct {
	enableUnsafe bool
	metrics      *ZeroCopyMetrics
}

// ZeroCopyMetrics tracks zero-copy metrics
type ZeroCopyMetrics struct {
	Operations int64
	BytesSaved int64
}

// NewZeroCopyOptimizer creates a zero-copy optimizer
func NewZeroCopyOptimizer(enableUnsafe bool) *ZeroCopyOptimizer {
	return &ZeroCopyOptimizer{
		enableUnsafe: enableUnsafe,
		metrics:      &ZeroCopyMetrics{},
	}
}

// OptimizeRecordTransfer optimizes record transfer
func (zco *ZeroCopyOptimizer) OptimizeRecordTransfer(records []*models.Record) []*models.Record {
	if !zco.enableUnsafe {
		return records
	}

	// Use pointer sharing instead of copying
	atomic.AddInt64(&zco.metrics.Operations, int64(len(records)))

	// In real implementation, this would use unsafe operations
	// to avoid copying data between records
	return records
}

// CacheOptimizer provides caching optimizations
type CacheOptimizer struct {
	cache    map[string]interface{}
	capacity int
	hits     int64
	misses   int64
	mu       sync.RWMutex
}

// NewCacheOptimizer creates a cache optimizer
func NewCacheOptimizer(capacity int) *CacheOptimizer {
	return &CacheOptimizer{
		cache:    make(map[string]interface{}),
		capacity: capacity,
	}
}

// Get gets value from cache
func (co *CacheOptimizer) Get(key string) (interface{}, bool) {
	co.mu.RLock()
	defer co.mu.RUnlock()

	value, found := co.cache[key]
	if found {
		atomic.AddInt64(&co.hits, 1)
	} else {
		atomic.AddInt64(&co.misses, 1)
	}
	return value, found
}

// Set sets value in cache
func (co *CacheOptimizer) Set(key string, value interface{}) {
	co.mu.Lock()
	defer co.mu.Unlock()

	// Simple LRU eviction
	if len(co.cache) >= co.capacity {
		// Remove first item (not true LRU, but simple)
		for k := range co.cache {
			delete(co.cache, k)
			break
		}
	}

	co.cache[key] = value
}

// GetHitRate returns cache hit rate
func (co *CacheOptimizer) GetHitRate() float64 {
	hits := atomic.LoadInt64(&co.hits)
	misses := atomic.LoadInt64(&co.misses)
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}
