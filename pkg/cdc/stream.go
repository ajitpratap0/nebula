package cdc

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"
)

// DeadLetterQueue defines the interface for handling failed events
type DeadLetterQueue interface {
	Send(task ProcessingTask, err error) error
	Read(limit int) ([]ProcessingTask, error)
	Acknowledge(taskID string) error
	GetStats() DeadLetterStats
}

// DeadLetterStats contains statistics for the dead letter queue
type DeadLetterStats struct {
	TotalEvents     int64     `json:"total_events"`
	PendingEvents   int64     `json:"pending_events"`
	ProcessedEvents int64     `json:"processed_events"`
	OldestEvent     time.Time `json:"oldest_event"`
	LastAdded       time.Time `json:"last_added"`
}

// StreamProcessor handles real-time processing of CDC events
type StreamProcessor struct {
	logger *zap.Logger
	config StreamingConfig

	// Event handlers
	handlers      map[string][]EventHandler
	batchHandlers map[string][]BatchEventHandler
	handlersMutex sync.RWMutex

	// Processing workers
	workers     []*StreamWorker
	workerCount int
	workCh      chan ProcessingTask

	// Event filtering
	filters      []EventFilter
	filtersMutex sync.RWMutex

	// Checkpointing
	checkpointer *Checkpointer

	// Dead letter queue
	deadLetterQueue DeadLetterQueue

	// Metrics and monitoring
	metrics      StreamMetrics
	metricsMutex sync.RWMutex

	// State management
	running int32
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// StreamWorker processes events in parallel
type StreamWorker struct {
	id        int
	processor *StreamProcessor
	logger    *zap.Logger
	metrics   WorkerMetrics
	stopCh    chan struct{}
}

// ProcessingTask represents a task to process events
type ProcessingTask struct {
	Events     []ChangeEvent
	Handler    string
	Partition  int
	Timestamp  time.Time
	RetryCount int
	MaxRetries int
}

// StreamMetrics contains metrics for stream processing
type StreamMetrics struct {
	EventsReceived    int64         `json:"events_received"`
	EventsProcessed   int64         `json:"events_processed"`
	EventsFiltered    int64         `json:"events_filtered"`
	EventsErrored     int64         `json:"events_errored"`
	EventsRetried     int64         `json:"events_retried"`
	BatchesProcessed  int64         `json:"batches_processed"`
	ProcessingLatency time.Duration `json:"processing_latency"`
	ThroughputRPS     float64       `json:"throughput_rps"`
	BacklogSize       int64         `json:"backlog_size"`
	LastProcessedTime time.Time     `json:"last_processed_time"`

	// Per-handler metrics
	HandlerMetrics map[string]HandlerMetrics `json:"handler_metrics"`
}

// HandlerMetrics contains metrics for a specific handler
type HandlerMetrics struct {
	EventsProcessed int64         `json:"events_processed"`
	EventsErrored   int64         `json:"events_errored"`
	AverageLatency  time.Duration `json:"average_latency"`
	LastExecution   time.Time     `json:"last_execution"`
}

// WorkerMetrics contains metrics for a worker
type WorkerMetrics struct {
	TasksProcessed int64         `json:"tasks_processed"`
	TasksErrored   int64         `json:"tasks_errored"`
	AverageLatency time.Duration `json:"average_latency"`
	LastTaskTime   time.Time     `json:"last_task_time"`
}

// Checkpointer manages checkpoints for stream processing
type Checkpointer struct {
	storage        CheckpointStorage
	interval       time.Duration
	logger         *zap.Logger
	lastCheckpoint Checkpoint
	mutex          sync.RWMutex
}

// CheckpointStorage defines the interface for checkpoint storage
type CheckpointStorage interface {
	Save(checkpoint Checkpoint) error
	Load() (Checkpoint, error)
	Delete(checkpointID string) error
	List() ([]Checkpoint, error)
}

// MemoryCheckpointStorage provides in-memory checkpoint storage
type MemoryCheckpointStorage struct {
	checkpoints map[string]Checkpoint
	mutex       sync.RWMutex
}

// MemoryDeadLetterQueue provides in-memory dead letter queue implementation
type MemoryDeadLetterQueue struct {
	tasks        []ProcessingTask
	errors       map[string]error
	acknowledged map[string]bool
	stats        DeadLetterStats
	mutex        sync.RWMutex
	maxSize      int
}

// NewStreamProcessor creates a new stream processor
func NewStreamProcessor(config StreamingConfig, logger *zap.Logger) *StreamProcessor {
	if err := config.Validate(); err != nil {
		logger.Error("invalid streaming config", zap.Error(err))
		config = StreamingConfig{
			MaxBatchSize:    1000,
			BatchTimeout:    5 * time.Second,
			MaxRetries:      3,
			RetryBackoff:    1 * time.Second,
			ParallelWorkers: 4,
			ExactlyOnce:     false,
		}
	}

	sp := &StreamProcessor{
		logger:        logger.With(zap.String("component", "stream_processor")),
		config:        config,
		handlers:      make(map[string][]EventHandler),
		batchHandlers: make(map[string][]BatchEventHandler),
		workerCount:   config.ParallelWorkers,
		workCh:        make(chan ProcessingTask, config.ParallelWorkers*10),
		stopCh:        make(chan struct{}),
		metrics: StreamMetrics{
			HandlerMetrics: make(map[string]HandlerMetrics),
		},
	}

	// Initialize checkpointer
	checkpointStorage := NewMemoryCheckpointStorage()
	sp.checkpointer = NewCheckpointer(checkpointStorage, 30*time.Second, logger)

	// Initialize dead letter queue
	sp.deadLetterQueue = NewMemoryDeadLetterQueue(10000) // Max 10K failed events

	return sp
}

// Start starts the stream processor
func (sp *StreamProcessor) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&sp.running, 0, 1) {
		return fmt.Errorf("stream processor is already running")
	}

	sp.logger.Info("starting stream processor",
		zap.Int("workers", sp.workerCount),
		zap.Int("max_batch_size", sp.config.MaxBatchSize))

	// Start workers
	sp.workers = make([]*StreamWorker, sp.workerCount)
	for i := 0; i < sp.workerCount; i++ {
		worker := &StreamWorker{
			id:        i,
			processor: sp,
			logger:    sp.logger.With(zap.Int("worker_id", i)),
			stopCh:    make(chan struct{}),
		}
		sp.workers[i] = worker

		sp.wg.Add(1)
		go worker.run()
	}

	// Start checkpointing
	sp.wg.Add(1)
	go sp.runCheckpointing()

	// Start metrics collection
	sp.wg.Add(1)
	go sp.runMetricsCollection()

	sp.logger.Info("stream processor started successfully")

	return nil
}

// Stop stops the stream processor gracefully
func (sp *StreamProcessor) Stop() error {
	if !atomic.CompareAndSwapInt32(&sp.running, 1, 0) {
		return fmt.Errorf("stream processor is not running")
	}

	sp.logger.Info("stopping stream processor")

	// Signal stop to all components
	close(sp.stopCh)

	// Stop workers
	for _, worker := range sp.workers {
		close(worker.stopCh)
	}

	// Wait for all goroutines to complete
	sp.wg.Wait()

	// Final checkpoint
	if err := sp.checkpointer.Checkpoint(); err != nil {
		sp.logger.Error("failed to save final checkpoint", zap.Error(err))
	}

	sp.logger.Info("stream processor stopped")

	return nil
}

// RegisterHandler registers an event handler for a specific event type or table
func (sp *StreamProcessor) RegisterHandler(pattern string, handler EventHandler) {
	sp.handlersMutex.Lock()
	defer sp.handlersMutex.Unlock()

	if sp.handlers[pattern] == nil {
		sp.handlers[pattern] = make([]EventHandler, 0)
	}
	sp.handlers[pattern] = append(sp.handlers[pattern], handler)

	// Initialize metrics for this handler
	sp.metricsMutex.Lock()
	if sp.metrics.HandlerMetrics[pattern] == (HandlerMetrics{}) {
		sp.metrics.HandlerMetrics[pattern] = HandlerMetrics{}
	}
	sp.metricsMutex.Unlock()

	sp.logger.Info("registered event handler", zap.String("pattern", pattern))
}

// RegisterBatchHandler registers a batch event handler
func (sp *StreamProcessor) RegisterBatchHandler(pattern string, handler BatchEventHandler) {
	sp.handlersMutex.Lock()
	defer sp.handlersMutex.Unlock()

	if sp.batchHandlers[pattern] == nil {
		sp.batchHandlers[pattern] = make([]BatchEventHandler, 0)
	}
	sp.batchHandlers[pattern] = append(sp.batchHandlers[pattern], handler)

	sp.logger.Info("registered batch event handler", zap.String("pattern", pattern))
}

// AddFilter adds an event filter
func (sp *StreamProcessor) AddFilter(filter EventFilter) {
	sp.filtersMutex.Lock()
	defer sp.filtersMutex.Unlock()

	sp.filters = append(sp.filters, filter)

	sp.logger.Info("added event filter")
}

// ProcessEvent processes a single event
func (sp *StreamProcessor) ProcessEvent(ctx context.Context, event ChangeEvent) error {
	return sp.ProcessEvents(ctx, []ChangeEvent{event})
}

// ProcessEvents processes multiple events
func (sp *StreamProcessor) ProcessEvents(ctx context.Context, events []ChangeEvent) error {
	if atomic.LoadInt32(&sp.running) == 0 {
		return fmt.Errorf("stream processor is not running")
	}

	start := time.Now()
	defer func() {
		sp.updateMetrics(func(m *StreamMetrics) {
			m.EventsReceived += int64(len(events))
			m.ProcessingLatency = time.Since(start)
			m.LastProcessedTime = time.Now()
		})
	}()

	// Filter events
	filteredEvents := sp.filterEvents(events)
	if len(filteredEvents) == 0 {
		sp.updateMetrics(func(m *StreamMetrics) {
			m.EventsFiltered += int64(len(events))
		})
		return nil
	}

	// Group events by handler pattern and partition
	taskGroups := sp.groupEventsByHandler(filteredEvents)

	// Submit tasks to workers
	for _, task := range taskGroups {
		select {
		case sp.workCh <- task:
			// Task submitted successfully
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Channel is full, update backlog metric
			sp.updateMetrics(func(m *StreamMetrics) {
				m.BacklogSize = int64(len(sp.workCh))
			})

			// Try again with timeout
			select {
			case sp.workCh <- task:
				// Task submitted successfully
			case <-time.After(100 * time.Millisecond):
				return fmt.Errorf("work channel is full, cannot submit task")
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}

// filterEvents applies all registered filters to events
func (sp *StreamProcessor) filterEvents(events []ChangeEvent) []ChangeEvent {
	sp.filtersMutex.RLock()
	defer sp.filtersMutex.RUnlock()

	if len(sp.filters) == 0 {
		return events
	}

	filtered := make([]ChangeEvent, 0, len(events))

	for _, event := range events {
		include := true
		for _, filter := range sp.filters {
			if !filter.ShouldInclude(event) {
				include = false
				break
			}
		}

		if include {
			filtered = append(filtered, event)
		}
	}

	return filtered
}

// groupEventsByHandler groups events by handler pattern and partition
func (sp *StreamProcessor) groupEventsByHandler(events []ChangeEvent) []ProcessingTask {
	sp.handlersMutex.RLock()
	defer sp.handlersMutex.RUnlock()

	// Create a map to group events
	taskMap := make(map[string]map[int][]ChangeEvent)

	for _, event := range events {
		// Find matching handlers
		for pattern := range sp.handlers {
			if sp.matchesPattern(event, pattern) {
				if taskMap[pattern] == nil {
					taskMap[pattern] = make(map[int][]ChangeEvent)
				}

				// Calculate partition for ordering
				partition := sp.calculatePartition(event)
				if taskMap[pattern][partition] == nil {
					taskMap[pattern][partition] = make([]ChangeEvent, 0)
				}

				taskMap[pattern][partition] = append(taskMap[pattern][partition], event)
			}
		}
	}

	// Convert to tasks
	tasks := make([]ProcessingTask, 0)
	for pattern, partitions := range taskMap {
		for partition, eventList := range partitions {
			// Split large batches
			for i := 0; i < len(eventList); i += sp.config.MaxBatchSize {
				end := i + sp.config.MaxBatchSize
				if end > len(eventList) {
					end = len(eventList)
				}

				task := ProcessingTask{
					Events:     eventList[i:end],
					Handler:    pattern,
					Partition:  partition,
					Timestamp:  time.Now(),
					MaxRetries: sp.config.MaxRetries,
				}

				tasks = append(tasks, task)
			}
		}
	}

	return tasks
}

// matchesPattern checks if an event matches a handler pattern
func (sp *StreamProcessor) matchesPattern(event ChangeEvent, pattern string) bool {
	// Simple pattern matching - could be enhanced with regex or glob patterns
	switch pattern {
	case "*":
		return true
	case event.Table:
		return true
	case stringpool.Sprintf("%s.%s", event.Database, event.Table):
		return true
	case string(event.Operation):
		return true
	case stringpool.Sprintf("%s:%s", event.Table, event.Operation):
		return true
	default:
		// Check for exact match
		return pattern == stringpool.Sprintf("%s.%s", event.Database, event.Table)
	}
}

// calculatePartition calculates a partition for an event to ensure ordering
func (sp *StreamProcessor) calculatePartition(event ChangeEvent) int {
	if sp.config.OrderingKey == "" {
		// Use table name as default ordering key
		return sp.hashString(event.Table) % sp.workerCount
	}

	// Try to extract ordering key from event data
	var keyValue string
	if event.After != nil {
		if val, exists := event.After[sp.config.OrderingKey]; exists {
			keyValue = stringpool.Sprintf("%v", val)
		}
	}
	if keyValue == "" && event.Before != nil {
		if val, exists := event.Before[sp.config.OrderingKey]; exists {
			keyValue = stringpool.Sprintf("%v", val)
		}
	}

	if keyValue == "" {
		keyValue = event.Table
	}

	return sp.hashString(keyValue) % sp.workerCount
}

// hashString calculates a hash for a string
func (sp *StreamProcessor) hashString(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

// GetMetrics returns current stream processing metrics
func (sp *StreamProcessor) GetMetrics() StreamMetrics {
	sp.metricsMutex.RLock()
	defer sp.metricsMutex.RUnlock()

	// Calculate throughput
	metrics := sp.metrics
	if !metrics.LastProcessedTime.IsZero() {
		elapsed := time.Since(metrics.LastProcessedTime)
		if elapsed > 0 {
			metrics.ThroughputRPS = float64(metrics.EventsProcessed) / elapsed.Seconds()
		}
	}

	return metrics
}

// GetDeadLetterQueueStats returns dead letter queue statistics
func (sp *StreamProcessor) GetDeadLetterQueueStats() DeadLetterStats {
	if sp.deadLetterQueue != nil {
		return sp.deadLetterQueue.GetStats()
	}
	return DeadLetterStats{}
}

// updateMetrics updates the metrics with a function
func (sp *StreamProcessor) updateMetrics(updateFn func(*StreamMetrics)) {
	sp.metricsMutex.Lock()
	defer sp.metricsMutex.Unlock()

	updateFn(&sp.metrics)
}

// updateHandlerMetrics updates metrics for a specific handler
func (sp *StreamProcessor) updateHandlerMetrics(pattern string, updateFn func(*HandlerMetrics)) {
	sp.metricsMutex.Lock()
	defer sp.metricsMutex.Unlock()

	if sp.metrics.HandlerMetrics[pattern] == (HandlerMetrics{}) {
		sp.metrics.HandlerMetrics[pattern] = HandlerMetrics{}
	}

	metrics := sp.metrics.HandlerMetrics[pattern]
	updateFn(&metrics)
	sp.metrics.HandlerMetrics[pattern] = metrics
}

// runCheckpointing runs the checkpointing process
func (sp *StreamProcessor) runCheckpointing() {
	defer sp.wg.Done()

	ticker := time.NewTicker(sp.checkpointer.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := sp.checkpointer.Checkpoint(); err != nil {
				sp.logger.Error("failed to save checkpoint", zap.Error(err))
			}

		case <-sp.stopCh:
			return
		}
	}
}

// runMetricsCollection runs the metrics collection process
func (sp *StreamProcessor) runMetricsCollection() {
	defer sp.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			metrics := sp.GetMetrics()
			sp.logger.Debug("stream processing metrics",
				zap.Int64("events_received", metrics.EventsReceived),
				zap.Int64("events_processed", metrics.EventsProcessed),
				zap.Int64("events_errored", metrics.EventsErrored),
				zap.Float64("throughput_rps", metrics.ThroughputRPS),
				zap.Int64("backlog_size", metrics.BacklogSize))

		case <-sp.stopCh:
			return
		}
	}
}

// StreamWorker implementation

// run runs the worker's main processing loop
func (w *StreamWorker) run() {
	defer w.processor.wg.Done()

	w.logger.Info("stream worker started")

	for {
		select {
		case task := <-w.processor.workCh:
			w.processTask(task)

		case <-w.stopCh:
			w.logger.Info("stream worker stopped")
			return
		}
	}
}

// processTask processes a single task
func (w *StreamWorker) processTask(task ProcessingTask) {
	start := time.Now()

	defer func() {
		w.metrics.TasksProcessed++
		w.metrics.AverageLatency = time.Since(start)
		w.metrics.LastTaskTime = time.Now()
	}()

	ctx := context.Background()

	// Get handlers for the pattern
	w.processor.handlersMutex.RLock()
	handlers := w.processor.handlers[task.Handler]
	batchHandlers := w.processor.batchHandlers[task.Handler]
	w.processor.handlersMutex.RUnlock()

	// Process with individual handlers
	for _, handler := range handlers {
		for _, event := range task.Events {
			if err := w.processEventWithHandler(ctx, event, handler, task.Handler); err != nil {
				w.handleError(task, err)
				return
			}
		}
	}

	// Process with batch handlers
	for _, handler := range batchHandlers {
		if err := w.processBatchWithHandler(ctx, task.Events, handler, task.Handler); err != nil {
			w.handleError(task, err)
			return
		}
	}

	// Update metrics
	w.processor.updateMetrics(func(m *StreamMetrics) {
		m.EventsProcessed += int64(len(task.Events))
		m.BatchesProcessed++
	})
}

// processEventWithHandler processes an event with a specific handler
func (w *StreamWorker) processEventWithHandler(ctx context.Context, event ChangeEvent, handler EventHandler, pattern string) error {
	start := time.Now()

	defer func() {
		w.processor.updateHandlerMetrics(pattern, func(m *HandlerMetrics) {
			m.EventsProcessed++
			m.AverageLatency = time.Since(start)
			m.LastExecution = time.Now()
		})
	}()

	if err := handler(ctx, event); err != nil {
		w.processor.updateHandlerMetrics(pattern, func(m *HandlerMetrics) {
			m.EventsErrored++
		})
		return fmt.Errorf("handler failed for pattern %s: %w", pattern, err)
	}

	return nil
}

// processBatchWithHandler processes events with a batch handler
func (w *StreamWorker) processBatchWithHandler(ctx context.Context, events []ChangeEvent, handler BatchEventHandler, pattern string) error {
	start := time.Now()

	defer func() {
		w.processor.updateHandlerMetrics(pattern, func(m *HandlerMetrics) {
			m.EventsProcessed += int64(len(events))
			m.AverageLatency = time.Since(start)
			m.LastExecution = time.Now()
		})
	}()

	if err := handler(ctx, events); err != nil {
		w.processor.updateHandlerMetrics(pattern, func(m *HandlerMetrics) {
			m.EventsErrored += int64(len(events))
		})
		return fmt.Errorf("batch handler failed for pattern %s: %w", pattern, err)
	}

	return nil
}

// handleError handles errors during task processing
func (w *StreamWorker) handleError(task ProcessingTask, err error) {
	w.metrics.TasksErrored++

	w.logger.Error("task processing failed",
		zap.String("handler", task.Handler),
		zap.Int("partition", task.Partition),
		zap.Int("event_count", len(task.Events)),
		zap.Int("retry_count", task.RetryCount),
		zap.Error(err))

	// Retry logic
	if task.RetryCount < task.MaxRetries {
		task.RetryCount++

		// Exponential backoff
		backoff := w.processor.config.RetryBackoff * time.Duration(1<<task.RetryCount)

		w.logger.Info("retrying task",
			zap.String("handler", task.Handler),
			zap.Int("retry_count", task.RetryCount),
			zap.Duration("backoff", backoff))

		// Schedule retry
		go func() {
			time.Sleep(backoff)
			select {
			case w.processor.workCh <- task:
				w.processor.updateMetrics(func(m *StreamMetrics) {
					m.EventsRetried++
				})
			case <-w.processor.stopCh:
				// Processor is stopping, discard retry
			}
		}()
	} else {
		// Max retries exceeded, send to dead letter queue if configured
		if w.processor.config.DeadLetterQueue != "" {
			w.sendToDeadLetterQueue(task, err)
		}

		w.processor.updateMetrics(func(m *StreamMetrics) {
			m.EventsErrored += int64(len(task.Events))
		})
	}
}

// sendToDeadLetterQueue sends failed events to the dead letter queue
func (w *StreamWorker) sendToDeadLetterQueue(task ProcessingTask, err error) {
	w.logger.Error("sending events to dead letter queue",
		zap.String("handler", task.Handler),
		zap.Int("event_count", len(task.Events)),
		zap.String("error", err.Error()))

	// Send to dead letter queue
	if w.processor.deadLetterQueue != nil {
		if dlqErr := w.processor.deadLetterQueue.Send(task, err); dlqErr != nil {
			w.logger.Error("failed to send to dead letter queue",
				zap.Error(dlqErr),
				zap.String("original_error", err.Error()))
		}
	}
}

// Checkpointer implementation

// NewCheckpointer creates a new checkpointer
func NewCheckpointer(storage CheckpointStorage, interval time.Duration, logger *zap.Logger) *Checkpointer {
	return &Checkpointer{
		storage:  storage,
		interval: interval,
		logger:   logger.With(zap.String("component", "checkpointer")),
	}
}

// Checkpoint saves the current processing state
func (c *Checkpointer) Checkpoint() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	checkpoint := Checkpoint{
		ID:          stringpool.Sprintf("checkpoint_%d", time.Now().UnixNano()),
		Timestamp:   time.Now(),
		ProcessedAt: time.Now(),
		Metadata:    make(map[string]interface{}),
	}

	if err := c.storage.Save(checkpoint); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	c.lastCheckpoint = checkpoint
	c.logger.Debug("saved checkpoint", zap.String("checkpoint_id", checkpoint.ID))

	return nil
}

// GetLastCheckpoint returns the last saved checkpoint
func (c *Checkpointer) GetLastCheckpoint() Checkpoint {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.lastCheckpoint
}

// MemoryCheckpointStorage implementation

// NewMemoryCheckpointStorage creates a new in-memory checkpoint storage
func NewMemoryCheckpointStorage() *MemoryCheckpointStorage {
	return &MemoryCheckpointStorage{
		checkpoints: make(map[string]Checkpoint),
	}
}

// Save saves a checkpoint
func (m *MemoryCheckpointStorage) Save(checkpoint Checkpoint) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.checkpoints[checkpoint.ID] = checkpoint
	return nil
}

// Load loads the latest checkpoint
func (m *MemoryCheckpointStorage) Load() (Checkpoint, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var latest Checkpoint
	for _, checkpoint := range m.checkpoints {
		if checkpoint.Timestamp.After(latest.Timestamp) {
			latest = checkpoint
		}
	}

	if latest.ID == "" {
		return Checkpoint{}, fmt.Errorf("no checkpoints found")
	}

	return latest, nil
}

// Delete deletes a checkpoint
func (m *MemoryCheckpointStorage) Delete(checkpointID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.checkpoints, checkpointID)
	return nil
}

// List lists all checkpoints
func (m *MemoryCheckpointStorage) List() ([]Checkpoint, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	checkpoints := make([]Checkpoint, 0, len(m.checkpoints))
	for _, checkpoint := range m.checkpoints {
		checkpoints = append(checkpoints, checkpoint)
	}

	return checkpoints, nil
}

// Dead Letter Queue implementation

// NewMemoryDeadLetterQueue creates a new in-memory dead letter queue
func NewMemoryDeadLetterQueue(maxSize int) *MemoryDeadLetterQueue {
	return &MemoryDeadLetterQueue{
		tasks:        make([]ProcessingTask, 0),
		errors:       make(map[string]error),
		acknowledged: make(map[string]bool),
		maxSize:      maxSize,
		stats: DeadLetterStats{
			TotalEvents:     0,
			PendingEvents:   0,
			ProcessedEvents: 0,
		},
	}
}

// Send adds a failed task to the dead letter queue
func (dlq *MemoryDeadLetterQueue) Send(task ProcessingTask, err error) error {
	dlq.mutex.Lock()
	defer dlq.mutex.Unlock()

	// Check max size
	if len(dlq.tasks) >= dlq.maxSize {
		return fmt.Errorf("dead letter queue is full (max: %d)", dlq.maxSize)
	}

	// Generate unique ID for the task
	taskID := stringpool.Sprintf("dlq_%d_%s", time.Now().UnixNano(), task.Handler)

	// Store task with ID in metadata
	if task.Timestamp.IsZero() {
		task.Timestamp = time.Now()
	}

	dlq.tasks = append(dlq.tasks, task)
	dlq.errors[taskID] = err

	// Update stats
	dlq.stats.TotalEvents += int64(len(task.Events))
	dlq.stats.PendingEvents += int64(len(task.Events))
	dlq.stats.LastAdded = time.Now()
	if dlq.stats.OldestEvent.IsZero() {
		dlq.stats.OldestEvent = task.Timestamp
	}

	return nil
}

// Read retrieves pending tasks from the dead letter queue
func (dlq *MemoryDeadLetterQueue) Read(limit int) ([]ProcessingTask, error) {
	dlq.mutex.RLock()
	defer dlq.mutex.RUnlock()

	result := make([]ProcessingTask, 0, limit)
	count := 0

	for _, task := range dlq.tasks {
		taskID := stringpool.Sprintf("dlq_%d_%s", task.Timestamp.UnixNano(), task.Handler)

		// Skip acknowledged tasks
		if dlq.acknowledged[taskID] {
			continue
		}

		result = append(result, task)
		count++

		if count >= limit {
			break
		}
	}

	return result, nil
}

// Acknowledge marks a task as processed
func (dlq *MemoryDeadLetterQueue) Acknowledge(taskID string) error {
	dlq.mutex.Lock()
	defer dlq.mutex.Unlock()

	dlq.acknowledged[taskID] = true

	// Update stats
	for _, task := range dlq.tasks {
		id := stringpool.Sprintf("dlq_%d_%s", task.Timestamp.UnixNano(), task.Handler)
		if id == taskID {
			dlq.stats.PendingEvents -= int64(len(task.Events))
			dlq.stats.ProcessedEvents += int64(len(task.Events))
			break
		}
	}

	return nil
}

// GetStats returns dead letter queue statistics
func (dlq *MemoryDeadLetterQueue) GetStats() DeadLetterStats {
	dlq.mutex.RLock()
	defer dlq.mutex.RUnlock()

	return dlq.stats
}
