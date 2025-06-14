package cdc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Manager coordinates multiple CDC connectors and event processing
type Manager struct {
	logger *zap.Logger
	config ManagerConfig

	// CDC connectors
	connectors     map[string]CDCConnector
	connectorMutex sync.RWMutex

	// Event processing
	streamProcessor *StreamProcessor
	kafkaProducer   *KafkaProducer

	// Health monitoring
	healthMonitor *HealthMonitor

	// State management
	running      bool
	runningMutex sync.RWMutex
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// ManagerConfig contains configuration for the CDC manager
type ManagerConfig struct {
	// Connector configurations
	Connectors map[string]CDCConfig `json:"connectors"`

	// Stream processing configuration
	Streaming StreamingConfig `json:"streaming"`

	// Kafka configuration
	Kafka       KafkaConfig `json:"kafka"`
	EnableKafka bool        `json:"enable_kafka"`

	// Health monitoring
	HealthCheck HealthCheckConfig `json:"health_check"`

	// Global settings
	GlobalTimeout time.Duration `json:"global_timeout"`
	RetryPolicy   RetryPolicy   `json:"retry_policy"`

	// Monitoring and metrics
	MetricsEnabled bool `json:"metrics_enabled"`
	MetricsPort    int  `json:"metrics_port"`
}

// HealthCheckConfig contains health monitoring configuration
type HealthCheckConfig struct {
	Enabled          bool          `json:"enabled"`
	Interval         time.Duration `json:"interval"`
	Timeout          time.Duration `json:"timeout"`
	FailureThreshold int           `json:"failure_threshold"`
	SuccessThreshold int           `json:"success_threshold"`
}

// RetryPolicy defines retry behavior
type RetryPolicy struct {
	MaxRetries     int           `json:"max_retries"`
	InitialBackoff time.Duration `json:"initial_backoff"`
	MaxBackoff     time.Duration `json:"max_backoff"`
	Multiplier     float64       `json:"multiplier"`
}

// HealthMonitor monitors the health of CDC components
type HealthMonitor struct {
	logger  *zap.Logger
	config  HealthCheckConfig
	manager *Manager

	// Health state
	componentHealth map[string]*ComponentHealth
	healthMutex     sync.RWMutex

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// ComponentHealth tracks the health of a single component
type ComponentHealth struct {
	Name         string        `json:"name"`
	Status       string        `json:"status"`
	LastCheck    time.Time     `json:"last_check"`
	FailureCount int           `json:"failure_count"`
	SuccessCount int           `json:"success_count"`
	LastError    string        `json:"last_error,omitempty"`
	ResponseTime time.Duration `json:"response_time"`
}

// ManagerStatus represents the overall status of the CDC manager
type ManagerStatus struct {
	Running         bool                        `json:"running"`
	StartTime       time.Time                   `json:"start_time"`
	Uptime          time.Duration               `json:"uptime"`
	Connectors      map[string]HealthStatus     `json:"connectors"`
	StreamProcessor *StreamMetrics              `json:"stream_processor,omitempty"`
	KafkaProducer   *KafkaMetrics               `json:"kafka_producer,omitempty"`
	ComponentHealth map[string]*ComponentHealth `json:"component_health"`
	OverallHealth   string                      `json:"overall_health"`
}

// NewManager creates a new CDC manager
func NewManager(config ManagerConfig, logger *zap.Logger) *Manager {
	manager := &Manager{
		logger:     logger.With(zap.String("component", "cdc_manager")),
		config:     config,
		connectors: make(map[string]CDCConnector),
		stopCh:     make(chan struct{}),
	}

	// Initialize stream processor
	if config.Streaming.MaxBatchSize > 0 {
		manager.streamProcessor = NewStreamProcessor(config.Streaming, logger)
	}

	// Initialize Kafka producer if enabled
	if config.EnableKafka {
		manager.kafkaProducer = NewKafkaProducer(config.Kafka, logger)
	}

	// Initialize health monitor
	if config.HealthCheck.Enabled {
		manager.healthMonitor = NewHealthMonitor(config.HealthCheck, manager, logger)
	}

	return manager
}

// Start starts the CDC manager and all its components
func (m *Manager) Start(ctx context.Context) error {
	m.runningMutex.Lock()
	defer m.runningMutex.Unlock()

	if m.running {
		return fmt.Errorf("CDC manager is already running")
	}

	m.logger.Info("starting CDC manager")

	// Start stream processor
	if m.streamProcessor != nil {
		if err := m.streamProcessor.Start(ctx); err != nil {
			return fmt.Errorf("failed to start stream processor: %w", err)
		}

		// Register Kafka handler if Kafka is enabled
		if m.kafkaProducer != nil {
			m.streamProcessor.RegisterBatchHandler("*", m.createKafkaHandler())
		}
	}

	// Start Kafka producer
	if m.kafkaProducer != nil {
		if err := m.kafkaProducer.Connect(); err != nil {
			return fmt.Errorf("failed to start Kafka producer: %w", err)
		}
	}

	// Initialize and start connectors
	for name, config := range m.config.Connectors {
		if err := m.startConnector(name, config); err != nil {
			m.logger.Error("failed to start connector",
				zap.String("connector", name), zap.Error(err))
			continue
		}
	}

	// Start health monitor
	if m.healthMonitor != nil {
		m.healthMonitor.Start()
	}

	// Start background tasks
	m.wg.Add(1)
	go m.runEventCollection()

	m.running = true

	m.logger.Info("CDC manager started successfully",
		zap.Int("connectors", len(m.connectors)),
		zap.Bool("stream_processor", m.streamProcessor != nil),
		zap.Bool("kafka_enabled", m.kafkaProducer != nil))

	return nil
}

// Stop stops the CDC manager and all its components
func (m *Manager) Stop() error {
	m.runningMutex.Lock()
	defer m.runningMutex.Unlock()

	if !m.running {
		return fmt.Errorf("CDC manager is not running")
	}

	m.logger.Info("stopping CDC manager")

	// Signal stop
	close(m.stopCh)

	// Stop connectors
	m.connectorMutex.Lock()
	for name, connector := range m.connectors {
		if err := connector.Stop(); err != nil {
			m.logger.Error("failed to stop connector",
				zap.String("connector", name), zap.Error(err))
		}
	}
	m.connectorMutex.Unlock()

	// Stop stream processor
	if m.streamProcessor != nil {
		if err := m.streamProcessor.Stop(); err != nil {
			m.logger.Error("failed to stop stream processor", zap.Error(err))
		}
	}

	// Stop Kafka producer
	if m.kafkaProducer != nil {
		if err := m.kafkaProducer.Close(); err != nil {
			m.logger.Error("failed to stop Kafka producer", zap.Error(err))
		}
	}

	// Stop health monitor
	if m.healthMonitor != nil {
		m.healthMonitor.Stop()
	}

	// Wait for background tasks
	m.wg.Wait()

	m.running = false

	m.logger.Info("CDC manager stopped")

	return nil
}

// AddConnector adds a new CDC connector
func (m *Manager) AddConnector(name string, config CDCConfig) error {
	m.connectorMutex.Lock()
	defer m.connectorMutex.Unlock()

	if _, exists := m.connectors[name]; exists {
		return fmt.Errorf("connector %s already exists", name)
	}

	// If manager is running, start the connector immediately
	if m.running {
		return m.startConnector(name, config)
	} else {
		// Otherwise, just add to config for later startup
		m.config.Connectors[name] = config
	}

	return nil
}

// RemoveConnector removes a CDC connector
func (m *Manager) RemoveConnector(name string) error {
	m.connectorMutex.Lock()
	defer m.connectorMutex.Unlock()

	connector, exists := m.connectors[name]
	if !exists {
		return fmt.Errorf("connector %s not found", name)
	}

	// Stop the connector
	if err := connector.Stop(); err != nil {
		m.logger.Error("failed to stop connector during removal",
			zap.String("connector", name), zap.Error(err))
	}

	// Remove from maps
	delete(m.connectors, name)
	delete(m.config.Connectors, name)

	m.logger.Info("removed connector", zap.String("connector", name))

	return nil
}

// startConnector starts a single connector
func (m *Manager) startConnector(name string, config CDCConfig) error {
	// Create connector based on type
	var connector CDCConnector
	var err error

	switch config.Type {
	case ConnectorPostgreSQL:
		connector = NewPostgreSQLConnector(m.logger)
	case ConnectorMySQL:
		connector = NewMySQLConnector(m.logger)
	case ConnectorMongoDB:
		connector = NewMongoDBConnector(m.logger)
	default:
		return fmt.Errorf("unsupported connector type: %s", config.Type)
	}

	// Connect to the data source
	if err = connector.Connect(config); err != nil {
		return fmt.Errorf("failed to connect connector %s: %w", name, err)
	}

	// Subscribe to tables
	if err = connector.Subscribe(config.Tables); err != nil {
		return fmt.Errorf("failed to subscribe connector %s: %w", name, err)
	}

	// Store connector
	m.connectors[name] = connector

	m.logger.Info("started connector",
		zap.String("connector", name),
		zap.String("type", string(config.Type)),
		zap.Strings("tables", config.Tables))

	return nil
}

// runEventCollection collects events from all connectors
func (m *Manager) runEventCollection() {
	defer m.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.collectAndProcessEvents()

		case <-m.stopCh:
			return
		}
	}
}

// collectAndProcessEvents collects events from all connectors and processes them
func (m *Manager) collectAndProcessEvents() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m.connectorMutex.RLock()
	connectors := make(map[string]CDCConnector)
	for name, connector := range m.connectors {
		connectors[name] = connector
	}
	m.connectorMutex.RUnlock()

	// Collect events from all connectors
	var allEvents []ChangeEvent

	for name, connector := range connectors {
		eventCh, err := connector.ReadChanges(ctx)
		if err != nil {
			m.logger.Error("failed to read changes from connector",
				zap.String("connector", name), zap.Error(err))
			continue
		}

		// Read available events (non-blocking)
		events := m.readAvailableEvents(eventCh, 100) // Max 100 events per connector per batch
		if len(events) > 0 {
			allEvents = append(allEvents, events...)

			m.logger.Debug("collected events from connector",
				zap.String("connector", name),
				zap.Int("event_count", len(events)))
		}
	}

	// Process events if any were collected
	if len(allEvents) > 0 && m.streamProcessor != nil {
		if err := m.streamProcessor.ProcessEvents(ctx, allEvents); err != nil {
			m.logger.Error("failed to process events",
				zap.Int("event_count", len(allEvents)), zap.Error(err))
		}
	}
}

// readAvailableEvents reads available events from a channel without blocking
func (m *Manager) readAvailableEvents(eventCh <-chan ChangeEvent, maxEvents int) []ChangeEvent {
	events := make([]ChangeEvent, 0, maxEvents)

	for i := 0; i < maxEvents; i++ {
		select {
		case event := <-eventCh:
			events = append(events, event)
		default:
			// No more events available
			return events
		}
	}

	return events
}

// createKafkaHandler creates a batch handler for Kafka
func (m *Manager) createKafkaHandler() BatchEventHandler {
	return func(ctx context.Context, events []ChangeEvent) error {
		if m.kafkaProducer == nil {
			return fmt.Errorf("Kafka producer not available")
		}

		return m.kafkaProducer.ProduceEvents(ctx, events)
	}
}

// GetStatus returns the current status of the CDC manager
func (m *Manager) GetStatus() ManagerStatus {
	m.runningMutex.RLock()
	running := m.running
	m.runningMutex.RUnlock()

	status := ManagerStatus{
		Running:    running,
		Connectors: make(map[string]HealthStatus),
	}

	if !running {
		status.OverallHealth = "stopped"
		return status
	}

	// Get connector statuses
	m.connectorMutex.RLock()
	for name, connector := range m.connectors {
		status.Connectors[name] = connector.Health()
	}
	m.connectorMutex.RUnlock()

	// Get stream processor metrics
	if m.streamProcessor != nil {
		metrics := m.streamProcessor.GetMetrics()
		status.StreamProcessor = &metrics
	}

	// Get Kafka producer metrics
	if m.kafkaProducer != nil {
		metrics := m.kafkaProducer.GetMetrics()
		status.KafkaProducer = &metrics
	}

	// Get component health
	if m.healthMonitor != nil {
		status.ComponentHealth = m.healthMonitor.GetComponentHealth()
	}

	// Determine overall health
	status.OverallHealth = m.calculateOverallHealth(status)

	return status
}

// calculateOverallHealth calculates the overall health based on component health
func (m *Manager) calculateOverallHealth(status ManagerStatus) string {
	if !status.Running {
		return "stopped"
	}

	healthyComponents := 0
	totalComponents := 0

	// Check connector health
	for _, health := range status.Connectors {
		totalComponents++
		if health.IsHealthy() {
			healthyComponents++
		}
	}

	// Check component health
	for _, health := range status.ComponentHealth {
		totalComponents++
		if health.Status == "healthy" {
			healthyComponents++
		}
	}

	if totalComponents == 0 {
		return "unknown"
	}

	healthPercentage := float64(healthyComponents) / float64(totalComponents)

	switch {
	case healthPercentage >= 0.9:
		return "healthy"
	case healthPercentage >= 0.7:
		return "degraded"
	default:
		return "unhealthy"
	}
}

// HealthMonitor implementation

// NewHealthMonitor creates a new health monitor
func NewHealthMonitor(config HealthCheckConfig, manager *Manager, logger *zap.Logger) *HealthMonitor {
	return &HealthMonitor{
		logger:          logger.With(zap.String("component", "health_monitor")),
		config:          config,
		manager:         manager,
		componentHealth: make(map[string]*ComponentHealth),
		stopCh:          make(chan struct{}),
	}
}

// Start starts the health monitor
func (hm *HealthMonitor) Start() {
	hm.logger.Info("starting health monitor",
		zap.Duration("interval", hm.config.Interval))

	hm.wg.Add(1)
	go hm.runHealthChecks()
}

// Stop stops the health monitor
func (hm *HealthMonitor) Stop() {
	close(hm.stopCh)
	hm.wg.Wait()

	hm.logger.Info("health monitor stopped")
}

// runHealthChecks runs periodic health checks
func (hm *HealthMonitor) runHealthChecks() {
	defer hm.wg.Done()

	ticker := time.NewTicker(hm.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.performHealthChecks()

		case <-hm.stopCh:
			return
		}
	}
}

// performHealthChecks performs health checks on all components
func (hm *HealthMonitor) performHealthChecks() {
	ctx, cancel := context.WithTimeout(context.Background(), hm.config.Timeout)
	defer cancel()

	// Check connectors
	hm.manager.connectorMutex.RLock()
	for name, connector := range hm.manager.connectors {
		hm.checkConnectorHealth(ctx, name, connector)
	}
	hm.manager.connectorMutex.RUnlock()

	// Check stream processor
	if hm.manager.streamProcessor != nil {
		hm.checkStreamProcessorHealth(ctx)
	}

	// Check Kafka producer
	if hm.manager.kafkaProducer != nil {
		hm.checkKafkaProducerHealth(ctx)
	}
}

// checkConnectorHealth checks the health of a connector
func (hm *HealthMonitor) checkConnectorHealth(ctx context.Context, name string, connector CDCConnector) {
	start := time.Now()

	health := &ComponentHealth{
		Name:         name,
		LastCheck:    start,
		ResponseTime: 0,
	}

	// Get connector health
	connectorHealth := connector.Health()

	health.ResponseTime = time.Since(start)

	if connectorHealth.IsHealthy() {
		health.Status = "healthy"
		health.SuccessCount++
		health.FailureCount = 0
	} else {
		health.Status = "unhealthy"
		health.FailureCount++
		health.LastError = connectorHealth.Message
	}

	hm.updateComponentHealth(name, health)
}

// checkStreamProcessorHealth checks the health of the stream processor
func (hm *HealthMonitor) checkStreamProcessorHealth(ctx context.Context) {
	start := time.Now()

	health := &ComponentHealth{
		Name:         "stream_processor",
		LastCheck:    start,
		ResponseTime: time.Since(start),
	}

	// Check if stream processor is responsive
	metrics := hm.manager.streamProcessor.GetMetrics()

	// Consider healthy if processing events recently
	if time.Since(metrics.LastProcessedTime) < 5*time.Minute || metrics.EventsReceived == 0 {
		health.Status = "healthy"
		health.SuccessCount++
		health.FailureCount = 0
	} else {
		health.Status = "unhealthy"
		health.FailureCount++
		health.LastError = "no recent event processing activity"
	}

	hm.updateComponentHealth("stream_processor", health)
}

// checkKafkaProducerHealth checks the health of the Kafka producer
func (hm *HealthMonitor) checkKafkaProducerHealth(ctx context.Context) {
	start := time.Now()

	health := &ComponentHealth{
		Name:         "kafka_producer",
		LastCheck:    start,
		ResponseTime: time.Since(start),
	}

	// Check Kafka producer metrics
	metrics := hm.manager.kafkaProducer.GetMetrics()

	// Consider healthy if no recent failures
	if metrics.MessagesFailed == 0 || time.Since(metrics.LastProducedTime) < time.Minute {
		health.Status = "healthy"
		health.SuccessCount++
		health.FailureCount = 0
	} else {
		health.Status = "unhealthy"
		health.FailureCount++
		health.LastError = "Kafka producer failures detected"
	}

	hm.updateComponentHealth("kafka_producer", health)
}

// updateComponentHealth updates the health of a component
func (hm *HealthMonitor) updateComponentHealth(name string, health *ComponentHealth) {
	hm.healthMutex.Lock()
	defer hm.healthMutex.Unlock()

	if existing, exists := hm.componentHealth[name]; exists {
		// Preserve counters
		if health.Status == "healthy" {
			health.SuccessCount = existing.SuccessCount + 1
			health.FailureCount = 0
		} else {
			health.FailureCount = existing.FailureCount + 1
			health.SuccessCount = 0
		}
	}

	hm.componentHealth[name] = health

	hm.logger.Debug("updated component health",
		zap.String("component", name),
		zap.String("status", health.Status),
		zap.Duration("response_time", health.ResponseTime))
}

// GetComponentHealth returns the health of all components
func (hm *HealthMonitor) GetComponentHealth() map[string]*ComponentHealth {
	hm.healthMutex.RLock()
	defer hm.healthMutex.RUnlock()

	result := make(map[string]*ComponentHealth)
	for name, health := range hm.componentHealth {
		// Create a copy to avoid data races
		healthCopy := *health
		result[name] = &healthCopy
	}

	return result
}
