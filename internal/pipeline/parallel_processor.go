package pipeline

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/lockfree"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

// ParallelProcessor implements high-performance parallel processing for CPU-bound operations
type ParallelProcessor struct {
	name          string
	logger        *zap.Logger
	numWorkers    int
	workerGroups  []*WorkerGroup
	inputQueue    *lockfree.Queue
	outputQueue   *lockfree.Queue
	transforms    []Transform
	
	// Performance metrics
	recordsProcessed int64
	processingTime   int64 // nanoseconds
	
	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// WorkerGroup represents a group of workers with CPU affinity
type WorkerGroup struct {
	groupID      int
	cpuID        int
	numWorkers   int
	localQueue   chan *models.Record
	outputQueue  chan *models.Record
	transforms   []Transform
	
	// Metrics
	processed    int64
	errors       int64
}

// ParallelConfig configures the parallel processor
type ParallelConfig struct {
	Name            string
	NumWorkers      int  // 0 = auto (NumCPU * 2)
	QueueSize       int  // Queue size per worker group
	EnableAffinity  bool // Enable CPU affinity
	BatchSize       int  // Process records in batches
	MaxBatchWait    time.Duration
}

// NewParallelProcessor creates a new parallel processor
func NewParallelProcessor(config ParallelConfig, transforms []Transform, logger *zap.Logger) *ParallelProcessor {
	if config.NumWorkers == 0 {
		config.NumWorkers = runtime.NumCPU() * 2
	}
	if config.QueueSize == 0 {
		config.QueueSize = 10000
	}
	if config.BatchSize == 0 {
		config.BatchSize = 100
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	p := &ParallelProcessor{
		name:       config.Name,
		logger:     logger,
		numWorkers: config.NumWorkers,
		transforms: transforms,
		ctx:        ctx,
		cancel:     cancel,
		inputQueue:  lockfree.NewQueue(config.QueueSize),
		outputQueue: lockfree.NewQueue(config.QueueSize),
	}
	
	// Create worker groups based on CPU topology
	p.createWorkerGroups(config)
	
	return p
}

// createWorkerGroups creates worker groups with CPU affinity
func (p *ParallelProcessor) createWorkerGroups(config ParallelConfig) {
	numCPU := runtime.NumCPU()
	groupsPerCPU := (p.numWorkers + numCPU - 1) / numCPU
	
	p.workerGroups = make([]*WorkerGroup, 0, numCPU)
	
	for i := 0; i < numCPU && i*groupsPerCPU < p.numWorkers; i++ {
		workersInGroup := groupsPerCPU
		if (i+1)*groupsPerCPU > p.numWorkers {
			workersInGroup = p.numWorkers - i*groupsPerCPU
		}
		
		group := &WorkerGroup{
			groupID:     i,
			cpuID:       i,
			numWorkers:  workersInGroup,
			localQueue:  make(chan *models.Record, config.QueueSize/numCPU),
			outputQueue: make(chan *models.Record, config.QueueSize/numCPU),
			transforms:  p.transforms,
		}
		
		p.workerGroups = append(p.workerGroups, group)
	}
	
	p.logger.Info("Created worker groups",
		zap.Int("num_groups", len(p.workerGroups)),
		zap.Int("total_workers", p.numWorkers),
		zap.Int("workers_per_group", groupsPerCPU))
}

// Start starts the parallel processor
func (p *ParallelProcessor) Start() {
	// Start worker groups
	for _, group := range p.workerGroups {
		p.wg.Add(1)
		go p.runWorkerGroup(group)
	}
	
	// Start input distributor
	p.wg.Add(1)
	go p.distributeInput()
	
	// Start output collector
	p.wg.Add(1)
	go p.collectOutput()
	
	p.logger.Info("Parallel processor started",
		zap.String("name", p.name),
		zap.Int("num_workers", p.numWorkers))
}

// Stop stops the parallel processor
func (p *ParallelProcessor) Stop() {
	p.cancel()
	p.wg.Wait()
	
	p.logger.Info("Parallel processor stopped",
		zap.String("name", p.name),
		zap.Int64("records_processed", atomic.LoadInt64(&p.recordsProcessed)),
		zap.Duration("avg_processing_time", p.getAvgProcessingTime()))
}

// Process processes a record through the parallel pipeline
func (p *ParallelProcessor) Process(record *models.Record) error {
	if !p.inputQueue.Enqueue(record) {
		return fmt.Errorf("input queue full")
	}
	return nil
}

// GetOutput returns the output channel for processed records
func (p *ParallelProcessor) GetOutput() <-chan *models.Record {
	output := make(chan *models.Record, 10000)
	
	go func() {
		defer close(output)
		for {
			item, ok := p.outputQueue.Dequeue()
			if !ok {
				return
			}
			if record, ok := item.(*models.Record); ok {
				select {
				case output <- record:
				case <-p.ctx.Done():
					return
				}
			}
		}
	}()
	
	return output
}

// distributeInput distributes input records to worker groups
func (p *ParallelProcessor) distributeInput() {
	defer p.wg.Done()
	
	// Round-robin distribution with work stealing
	groupIndex := 0
	
	for {
		select {
		case <-p.ctx.Done():
			// Close all input queues
			for _, group := range p.workerGroups {
				close(group.localQueue)
			}
			return
		default:
			item, ok := p.inputQueue.Dequeue()
			if !ok {
				time.Sleep(time.Microsecond)
				continue
			}
			
			if record, ok := item.(*models.Record); ok {
				// Try to send to the current group
				sent := false
				attempts := 0
				
				for !sent && attempts < len(p.workerGroups) {
					select {
					case p.workerGroups[groupIndex].localQueue <- record:
						sent = true
					default:
						// Queue full, try next group (work stealing)
						groupIndex = (groupIndex + 1) % len(p.workerGroups)
						attempts++
					}
				}
				
				if sent {
					groupIndex = (groupIndex + 1) % len(p.workerGroups)
				} else {
					// All queues full, block on current
					select {
					case p.workerGroups[groupIndex].localQueue <- record:
						groupIndex = (groupIndex + 1) % len(p.workerGroups)
					case <-p.ctx.Done():
						return
					}
				}
			}
		}
	}
}

// collectOutput collects output from all worker groups
func (p *ParallelProcessor) collectOutput() {
	defer p.wg.Done()
	
	cases := make([]<-chan *models.Record, len(p.workerGroups))
	for i, group := range p.workerGroups {
		cases[i] = group.outputQueue
	}
	
	for {
		for i, ch := range cases {
			select {
			case record, ok := <-ch:
				if !ok {
					// Channel closed, remove from cases
					cases[i] = nil
					
					// Check if all channels are closed
					allClosed := true
					for _, c := range cases {
						if c != nil {
							allClosed = false
							break
						}
					}
					if allClosed {
						return
					}
					continue
				}
				
				if !p.outputQueue.Enqueue(record) {
					p.logger.Error("Failed to enqueue output", zap.String("reason", "queue full"))
				}
				
			case <-p.ctx.Done():
				return
			default:
				// Non-blocking, continue to next channel
			}
		}
		
		// Small sleep to prevent busy waiting
		time.Sleep(time.Microsecond)
	}
}

// runWorkerGroup runs a group of workers
func (p *ParallelProcessor) runWorkerGroup(group *WorkerGroup) {
	defer p.wg.Done()
	
	// Set CPU affinity if supported (platform-specific)
	p.setCPUAffinity(group.cpuID)
	
	// Start workers in the group
	var wg sync.WaitGroup
	for i := 0; i < group.numWorkers; i++ {
		wg.Add(1)
		go p.runWorker(group, i, &wg)
	}
	
	wg.Wait()
	close(group.outputQueue)
}

// runWorker runs a single worker
func (p *ParallelProcessor) runWorker(group *WorkerGroup, workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	
	// Create batch for vectorized processing
	batch := pool.GetBatchSlice(100)

	defer pool.PutBatchSlice(batch)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case record, ok := <-group.localQueue:
			if !ok {
				// Process remaining batch
				if len(batch) > 0 {
					p.processBatch(group, batch)
				}
				return
			}
			
			batch = append(batch, record)
			if len(batch) >= 100 {
				p.processBatch(group, batch)
				batch = batch[:0]
			}
			
		case <-ticker.C:
			// Process partial batch on timeout
			if len(batch) > 0 {
				p.processBatch(group, batch)
				batch = batch[:0]
			}
			
		case <-p.ctx.Done():
			return
		}
	}
}

// processBatch processes a batch of records through transforms
func (p *ParallelProcessor) processBatch(group *WorkerGroup, batch []*models.Record) {
	start := time.Now()
	
	// Apply transforms to the batch
	for _, record := range batch {
		processedRecord := record
		var err error
		
		// Apply each transform
		for _, transform := range group.transforms {
			processedRecord, err = transform(p.ctx, processedRecord)
			if err != nil {
				atomic.AddInt64(&group.errors, 1)
				p.logger.Error("Transform error",
					zap.Error(err),
					zap.Int("group_id", group.groupID))
				break
			}
		}
		
		if err == nil && processedRecord != nil {
			select {
			case group.outputQueue <- processedRecord:
				atomic.AddInt64(&group.processed, 1)
				atomic.AddInt64(&p.recordsProcessed, 1)
			case <-p.ctx.Done():
				return
			}
		}
	}
	
	// Update metrics
	duration := time.Since(start).Nanoseconds()
	atomic.AddInt64(&p.processingTime, duration)
}

// getAvgProcessingTime returns the average processing time per record
func (p *ParallelProcessor) getAvgProcessingTime() time.Duration {
	records := atomic.LoadInt64(&p.recordsProcessed)
	if records == 0 {
		return 0
	}
	totalTime := atomic.LoadInt64(&p.processingTime)
	return time.Duration(totalTime / records)
}

// setCPUAffinity sets CPU affinity for the current goroutine (platform-specific)
func (p *ParallelProcessor) setCPUAffinity(cpuID int) {
	// This is platform-specific and would require build tags
	// For now, we rely on the Go scheduler
	// In production, use runtime.LockOSThread() and platform-specific syscalls
}

// VectorizedTransform applies transforms to multiple records at once
type VectorizedTransform interface {
	TransformBatch([]*models.Record) ([]*models.Record, error)
}

// ParallelFieldMapper implements parallel field mapping with vectorization
type ParallelFieldMapper struct {
	mappings map[string]string
	workers  int
}

// NewParallelFieldMapper creates a new parallel field mapper
func NewParallelFieldMapper(mappings map[string]string) *ParallelFieldMapper {
	return &ParallelFieldMapper{
		mappings: mappings,
		workers:  runtime.NumCPU(),
	}
}

// TransformBatch transforms a batch of records in parallel
func (m *ParallelFieldMapper) TransformBatch(batch []*models.Record) ([]*models.Record, error) {
	if len(batch) == 0 {
		return batch, nil
	}
	
	// For small batches, process sequentially
	if len(batch) < 100 {
		for i, record := range batch {
			newData := pool.GetMap()
			for k, v := range record.Data {
				if newKey, ok := m.mappings[k]; ok {
					newData[newKey] = v
				} else {
					newData[k] = v
				}
			}
			
			// Return old map to pool
			pool.PutMap(record.Data)
			record.Data = newData
			batch[i] = record
		}
		return batch, nil
	}
	
	// For large batches, process in parallel
	chunkSize := len(batch) / m.workers
	if chunkSize < 10 {
		chunkSize = 10
	}
	
	var wg sync.WaitGroup
	for i := 0; i < len(batch); i += chunkSize {
		end := i + chunkSize
		if end > len(batch) {
			end = len(batch)
		}
		
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for j := start; j < end; j++ {
				record := batch[j]
				newData := pool.GetMap()
				for k, v := range record.Data {
					if newKey, ok := m.mappings[k]; ok {
						newData[newKey] = v
					} else {
						newData[k] = v
					}
				}
				pool.PutMap(record.Data)
				record.Data = newData
			}
		}(i, end)
	}
	
	wg.Wait()
	return batch, nil
}