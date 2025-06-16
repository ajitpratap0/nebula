package registry

import (
	"fmt"
	"sync"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/errors"
	"github.com/ajitpratap0/nebula/pkg/logger"
	"go.uber.org/zap"
)

// Registry manages connector registration and instantiation
type Registry struct {
	sources      map[string]SourceFactory
	destinations map[string]DestinationFactory
	mu           sync.RWMutex
	logger       *zap.Logger
}

// SourceFactory is a function that creates source connector instances.
// It takes a BaseConfig and returns a configured Source connector or an error.
type SourceFactory func(config *config.BaseConfig) (core.Source, error)

// DestinationFactory is a function that creates destination connector instances.
// It takes a BaseConfig and returns a configured Destination connector or an error.
type DestinationFactory func(config *config.BaseConfig) (core.Destination, error)

// Global registry instance
var globalRegistry = NewRegistry()

// NewRegistry creates a new connector registry
func NewRegistry() *Registry {
	return &Registry{
		sources:      make(map[string]SourceFactory),
		destinations: make(map[string]DestinationFactory),
		logger:       logger.Get().With(zap.String("component", "connector_registry")),
	}
}

// RegisterSource registers a source connector factory
func (r *Registry) RegisterSource(name string, factory SourceFactory) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.sources[name]; exists {
		return errors.New(errors.ErrorTypeConfig, fmt.Sprintf("source connector %s already registered", name))
	}

	r.sources[name] = factory
	r.logger.Info("source connector registered", zap.String("name", name))
	return nil
}

// RegisterDestination registers a destination connector factory
func (r *Registry) RegisterDestination(name string, factory DestinationFactory) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.destinations[name]; exists {
		return errors.New(errors.ErrorTypeConfig, fmt.Sprintf("destination connector %s already registered", name))
	}

	r.destinations[name] = factory
	r.logger.Info("destination connector registered", zap.String("name", name))
	return nil
}

// CreateSource creates a source connector instance
func (r *Registry) CreateSource(name string, config *config.BaseConfig) (core.Source, error) {
	r.mu.RLock()
	factory, exists := r.sources[name]
	r.mu.RUnlock()

	if !exists {
		return nil, errors.New(errors.ErrorTypeConfig, fmt.Sprintf("source connector %s not found", name))
	}

	source, err := factory(config)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConfig, fmt.Sprintf("failed to create source connector %s", name))
	}

	return source, nil
}

// CreateDestination creates a destination connector instance
func (r *Registry) CreateDestination(name string, config *config.BaseConfig) (core.Destination, error) {
	r.mu.RLock()
	factory, exists := r.destinations[name]
	r.mu.RUnlock()

	if !exists {
		return nil, errors.New(errors.ErrorTypeConfig, fmt.Sprintf("destination connector %s not found", name))
	}

	destination, err := factory(config)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConfig, fmt.Sprintf("failed to create destination connector %s", name))
	}

	return destination, nil
}

// ListSources returns a list of registered source connectors
func (r *Registry) ListSources() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	sources := make([]string, 0, len(r.sources))
	for name := range r.sources {
		sources = append(sources, name)
	}
	return sources
}

// ListDestinations returns a list of registered destination connectors
func (r *Registry) ListDestinations() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	destinations := make([]string, 0, len(r.destinations))
	for name := range r.destinations {
		destinations = append(destinations, name)
	}
	return destinations
}

// HasSource checks if a source connector is registered
func (r *Registry) HasSource(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.sources[name]
	return exists
}

// HasDestination checks if a destination connector is registered
func (r *Registry) HasDestination(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.destinations[name]
	return exists
}

// Clear removes all registered connectors (mainly for testing)
func (r *Registry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sources = make(map[string]SourceFactory)
	r.destinations = make(map[string]DestinationFactory)
}

// Global registry functions

// RegisterSource registers a source connector in the global registry
func RegisterSource(name string, factory SourceFactory) error {
	return globalRegistry.RegisterSource(name, factory)
}

// RegisterDestination registers a destination connector in the global registry
func RegisterDestination(name string, factory DestinationFactory) error {
	return globalRegistry.RegisterDestination(name, factory)
}

// CreateSource creates a source connector from the global registry
func CreateSource(name string, config *config.BaseConfig) (core.Source, error) {
	return globalRegistry.CreateSource(name, config)
}

// CreateDestination creates a destination connector from the global registry
func CreateDestination(name string, config *config.BaseConfig) (core.Destination, error) {
	return globalRegistry.CreateDestination(name, config)
}

// ListSources returns registered sources from the global registry
func ListSources() []string {
	return globalRegistry.ListSources()
}

// ListDestinations returns registered destinations from the global registry
func ListDestinations() []string {
	return globalRegistry.ListDestinations()
}

// HasSource checks if a source is registered in the global registry
func HasSource(name string) bool {
	return globalRegistry.HasSource(name)
}

// HasDestination checks if a destination is registered in the global registry
func HasDestination(name string) bool {
	return globalRegistry.HasDestination(name)
}

// GetRegistry returns the global registry instance.
// This is the primary way to access the connector registry.
func GetRegistry() *Registry {
	return globalRegistry
}

// Factory is a generic connector factory interface that can create both
// source and destination connectors. It provides a unified interface for
// connector instantiation.
type Factory interface {
	CreateSource(connectorType string, config *config.BaseConfig) (core.Source, error)
	CreateDestination(connectorType string, config *config.BaseConfig) (core.Destination, error)
	ListSources() []string
	ListDestinations() []string
}

// DefaultFactory implements the Factory interface using the global registry.
// It provides a simple way to create connectors without directly accessing
// the registry.
type DefaultFactory struct {
	registry *Registry
}

// NewDefaultFactory creates a new default factory
func NewDefaultFactory() *DefaultFactory {
	return &DefaultFactory{
		registry: globalRegistry,
	}
}

// CreateSource creates a source connector
func (f *DefaultFactory) CreateSource(connectorType string, config *config.BaseConfig) (core.Source, error) {
	return f.registry.CreateSource(connectorType, config)
}

// CreateDestination creates a destination connector
func (f *DefaultFactory) CreateDestination(connectorType string, config *config.BaseConfig) (core.Destination, error) {
	return f.registry.CreateDestination(connectorType, config)
}

// ListSources lists available source connectors
func (f *DefaultFactory) ListSources() []string {
	return f.registry.ListSources()
}

// ListDestinations lists available destination connectors
func (f *DefaultFactory) ListDestinations() []string {
	return f.registry.ListDestinations()
}

// ConnectorInfo provides information about a connector
type ConnectorInfo struct {
	Name         string                 `json:"name"`
	Type         string                 `json:"type"`
	Description  string                 `json:"description"`
	Version      string                 `json:"version"`
	Author       string                 `json:"author"`
	Capabilities []string               `json:"capabilities"`
	ConfigSchema map[string]interface{} `json:"config_schema"`
}

// ConnectorCatalog manages connector metadata
type ConnectorCatalog struct {
	connectors map[string]*ConnectorInfo
	mu         sync.RWMutex
}

// NewConnectorCatalog creates a new connector catalog
func NewConnectorCatalog() *ConnectorCatalog {
	return &ConnectorCatalog{
		connectors: make(map[string]*ConnectorInfo),
	}
}

// Register adds a connector to the catalog
func (c *ConnectorCatalog) Register(info *ConnectorInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.connectors[info.Name]; exists {
		return errors.New(errors.ErrorTypeConfig, fmt.Sprintf("connector %s already in catalog", info.Name))
	}

	c.connectors[info.Name] = info
	return nil
}

// Get retrieves connector information
func (c *ConnectorCatalog) Get(name string) (*ConnectorInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info, exists := c.connectors[name]
	if !exists {
		return nil, errors.New(errors.ErrorTypeConfig, fmt.Sprintf("connector %s not found in catalog", name))
	}

	return info, nil
}

// List returns all connectors in the catalog
func (c *ConnectorCatalog) List() []*ConnectorInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	infos := make([]*ConnectorInfo, 0, len(c.connectors))
	for _, info := range c.connectors {
		infos = append(infos, info)
	}
	return infos
}

// Global catalog instance
var globalCatalog = NewConnectorCatalog()

// RegisterConnectorInfo registers connector information in the global catalog
func RegisterConnectorInfo(info *ConnectorInfo) error {
	return globalCatalog.Register(info)
}

// GetConnectorInfo retrieves connector information from the global catalog
func GetConnectorInfo(name string) (*ConnectorInfo, error) {
	return globalCatalog.Get(name)
}

// ListConnectorInfo lists all connectors in the global catalog
func ListConnectorInfo() []*ConnectorInfo {
	return globalCatalog.List()
}
