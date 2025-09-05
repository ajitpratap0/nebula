// Package schema provides advanced schema evolution and type inference capabilities
package schema

import (
	"context"
	"fmt"
	"sync"
	"time"

	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"
)

// SchemaVersion represents a version of a schema
type SchemaVersion struct {
	Version       int                    `json:"version"`
	Schema        *models.Schema         `json:"schema"`
	CreatedAt     time.Time              `json:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at"`
	Fingerprint   string                 `json:"fingerprint"`
	Compatibility CompatibilityMode      `json:"compatibility"`
	Metadata      map[string]interface{} `json:"metadata"`
}

// CompatibilityMode defines how schema changes are validated
type CompatibilityMode string

const (
	// CompatibilityNone allows any schema change
	CompatibilityNone CompatibilityMode = "NONE"
	// CompatibilityBackward ensures new schema can read old data
	CompatibilityBackward CompatibilityMode = "BACKWARD"
	// CompatibilityForward ensures old schema can read new data
	CompatibilityForward CompatibilityMode = "FORWARD"
	// CompatibilityFull ensures bidirectional compatibility
	CompatibilityFull CompatibilityMode = "FULL"
	// CompatibilityBackwardTransitive ensures compatibility with all previous versions
	CompatibilityBackwardTransitive CompatibilityMode = "BACKWARD_TRANSITIVE"
)

// Registry manages schema versions and evolution
type Registry struct {
	schemas       map[string][]*SchemaVersion // subject -> versions
	latest        map[string]int              // subject -> latest version
	compatibility map[string]CompatibilityMode
	mu            sync.RWMutex
	logger        *zap.Logger

	// Hooks for schema changes
	onSchemaChange []func(subject string, old, new *SchemaVersion)

	// Type inference engine
	typeInference *TypeInferenceEngine

	// Schema evolution manager
	evolution *EvolutionManager
}

// NewRegistry creates a new schema registry
func NewRegistry(logger *zap.Logger) *Registry {
	r := &Registry{
		schemas:       make(map[string][]*SchemaVersion),
		latest:        make(map[string]int),
		compatibility: make(map[string]CompatibilityMode),
		logger:        logger,
		typeInference: NewTypeInferenceEngine(logger),
	}

	r.evolution = NewEvolutionManager(r, logger)
	return r
}

// RegisterSchema registers a new schema version
func (r *Registry) RegisterSchema(ctx context.Context, subject string, schema *models.Schema) (*SchemaVersion, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Get compatibility mode for subject
	compatMode := r.getCompatibilityMode(subject)

	// Check if schema already exists
	if existing := r.findExistingSchema(subject, schema); existing != nil {
		r.logger.Info("schema already registered",
			zap.String("subject", subject),
			zap.Int("version", existing.Version))
		return existing, nil
	}

	// Validate compatibility with previous versions
	if len(r.schemas[subject]) > 0 {
		latestVersion := r.schemas[subject][len(r.schemas[subject])-1]
		if err := r.evolution.CheckCompatibility(latestVersion.Schema, schema, compatMode); err != nil {
			return nil, fmt.Errorf("schema incompatible with mode %s: %w", compatMode, err)
		}
	}

	// Create new version
	version := &SchemaVersion{
		Version:       len(r.schemas[subject]) + 1,
		Schema:        schema,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
		Fingerprint:   r.calculateFingerprint(schema),
		Compatibility: compatMode,
		Metadata:      make(map[string]interface{}),
	}

	// Store schema
	r.schemas[subject] = append(r.schemas[subject], version)
	r.latest[subject] = version.Version

	// Notify listeners
	if len(r.schemas[subject]) > 1 {
		oldVersion := r.schemas[subject][len(r.schemas[subject])-2]
		for _, hook := range r.onSchemaChange {
			go hook(subject, oldVersion, version)
		}
	}

	r.logger.Info("schema registered",
		zap.String("subject", subject),
		zap.Int("version", version.Version),
		zap.String("fingerprint", version.Fingerprint))

	return version, nil
}

// GetSchema retrieves a specific schema version
func (r *Registry) GetSchema(subject string, version int) (*SchemaVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, exists := r.schemas[subject]
	if !exists || len(versions) == 0 {
		return nil, fmt.Errorf("subject %s not found", subject)
	}

	if version <= 0 || version > len(versions) {
		return nil, fmt.Errorf("version %d not found for subject %s", version, subject)
	}

	return versions[version-1], nil
}

// GetLatestSchema retrieves the latest schema version
func (r *Registry) GetLatestSchema(subject string) (*SchemaVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	latest, exists := r.latest[subject]
	if !exists {
		return nil, fmt.Errorf("subject %s not found", subject)
	}

	return r.schemas[subject][latest-1], nil
}

// GetSchemaHistory returns all versions of a schema
func (r *Registry) GetSchemaHistory(subject string) ([]*SchemaVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, exists := r.schemas[subject]
	if !exists {
		return nil, fmt.Errorf("subject %s not found", subject)
	}

	// Return a copy to prevent external modifications
	history := make([]*SchemaVersion, len(versions))
	copy(history, versions)
	return history, nil
}

// SetCompatibilityMode sets the compatibility mode for a subject
func (r *Registry) SetCompatibilityMode(subject string, mode CompatibilityMode) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.compatibility[subject] = mode
	r.logger.Info("compatibility mode set",
		zap.String("subject", subject),
		zap.String("mode", string(mode)))
}

// InferSchema uses advanced type inference to detect schema from data
func (r *Registry) InferSchema(ctx context.Context, subject string, samples []map[string]interface{}) (*models.Schema, error) {
	return r.typeInference.InferSchema(subject, samples)
}

// EvolveSchema attempts to evolve a schema based on new data
func (r *Registry) EvolveSchema(ctx context.Context, subject string, newData []map[string]interface{}) (*SchemaVersion, error) {
	// Get current schema
	current, err := r.GetLatestSchema(subject)
	if err != nil {
		// No existing schema, infer from data
		schema, err := r.InferSchema(ctx, subject, newData)
		if err != nil {
			return nil, fmt.Errorf("failed to infer schema: %w", err)
		}
		return r.RegisterSchema(ctx, subject, schema)
	}

	// Detect schema changes
	evolved, err := r.evolution.EvolveSchema(current.Schema, newData)
	if err != nil {
		return nil, fmt.Errorf("failed to evolve schema: %w", err)
	}

	// Register evolved schema if changed
	if !r.schemasEqual(current.Schema, evolved) {
		return r.RegisterSchema(ctx, subject, evolved)
	}

	return current, nil
}

// OnSchemaChange registers a callback for schema changes
func (r *Registry) OnSchemaChange(callback func(subject string, old, new *SchemaVersion)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onSchemaChange = append(r.onSchemaChange, callback)
}

// Export exports the registry state
func (r *Registry) Export() ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	state := struct {
		Schemas       map[string][]*SchemaVersion  `json:"schemas"`
		Latest        map[string]int               `json:"latest"`
		Compatibility map[string]CompatibilityMode `json:"compatibility"`
	}{
		Schemas:       r.schemas,
		Latest:        r.latest,
		Compatibility: r.compatibility,
	}

	return jsonpool.MarshalIndent(state, "", "  ")
}

// Import imports registry state
func (r *Registry) Import(data []byte) error {
	var state struct {
		Schemas       map[string][]*SchemaVersion  `json:"schemas"`
		Latest        map[string]int               `json:"latest"`
		Compatibility map[string]CompatibilityMode `json:"compatibility"`
	}

	if err := jsonpool.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("failed to unmarshal registry state: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.schemas = state.Schemas
	r.latest = state.Latest
	r.compatibility = state.Compatibility

	return nil
}

// getCompatibilityMode returns the compatibility mode for a subject
func (r *Registry) getCompatibilityMode(subject string) CompatibilityMode {
	if mode, exists := r.compatibility[subject]; exists {
		return mode
	}
	return CompatibilityBackward // Default
}

// findExistingSchema checks if schema already exists
func (r *Registry) findExistingSchema(subject string, schema *models.Schema) *SchemaVersion {
	fingerprint := r.calculateFingerprint(schema)

	for _, version := range r.schemas[subject] {
		if version.Fingerprint == fingerprint {
			return version
		}
	}

	return nil
}

// calculateFingerprint generates a unique fingerprint for a schema
func (r *Registry) calculateFingerprint(schema *models.Schema) string {
	// Simple fingerprint based on field names and types
	// In production, use a proper hash function
	return stringpool.BuildString(func(builder *stringpool.Builder) {
		for _, field := range schema.Fields {
			builder.WriteString(field.Name)
			_ = builder.WriteByte(':')
			builder.WriteString(field.Type)
			_ = builder.WriteByte(';')
		}
	})
}

// schemasEqual checks if two schemas are equal
func (r *Registry) schemasEqual(s1, s2 *models.Schema) bool {
	return r.calculateFingerprint(s1) == r.calculateFingerprint(s2)
}

// GetEvolutionManager returns the evolution manager
func (r *Registry) GetEvolutionManager() *EvolutionManager {
	return r.evolution
}
