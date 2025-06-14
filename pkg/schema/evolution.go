package schema

import (
	"fmt"
	"sort"

	"github.com/ajitpratap0/nebula/pkg/models"
	"go.uber.org/zap"
)

// EvolutionManager handles schema evolution and compatibility
type EvolutionManager struct {
	registry *Registry
	logger   *zap.Logger

	// Evolution strategies
	strategies map[string]EvolutionStrategy
}

// EvolutionStrategy defines how schema changes are handled
type EvolutionStrategy interface {
	CanEvolve(old, new *models.Schema) bool
	Evolve(old, new *models.Schema) (*models.Schema, error)
	GetMigrationPlan(old, new *models.Schema) *MigrationPlan
}

// MigrationPlan describes how to migrate data between schema versions
type MigrationPlan struct {
	FromVersion int                    `json:"from_version"`
	ToVersion   int                    `json:"to_version"`
	Changes     []SchemaChange         `json:"changes"`
	Strategy    string                 `json:"strategy"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// SchemaChange represents a single change in a schema
type SchemaChange struct {
	Type      ChangeType    `json:"type"`
	Field     string        `json:"field"`
	OldField  *models.Field `json:"old_field,omitempty"`
	NewField  *models.Field `json:"new_field,omitempty"`
	Migration MigrationRule `json:"migration,omitempty"`
}

// ChangeType represents the type of schema change
type ChangeType string

const (
	ChangeTypeAddField       ChangeType = "ADD_FIELD"
	ChangeTypeRemoveField    ChangeType = "REMOVE_FIELD"
	ChangeTypeRenameField    ChangeType = "RENAME_FIELD"
	ChangeTypeModifyType     ChangeType = "MODIFY_TYPE"
	ChangeTypeModifyRequired ChangeType = "MODIFY_REQUIRED"
)

// MigrationRule defines how to migrate data for a field
type MigrationRule struct {
	Type           string                        `json:"type"`
	DefaultValue   interface{}                   `json:"default_value,omitempty"`
	Transformation string                        `json:"transformation,omitempty"`
	CustomLogic    func(interface{}) interface{} `json:"-"`
}

// NewEvolutionManager creates a new evolution manager
func NewEvolutionManager(registry *Registry, logger *zap.Logger) *EvolutionManager {
	em := &EvolutionManager{
		registry:   registry,
		logger:     logger,
		strategies: make(map[string]EvolutionStrategy),
	}

	// Register default strategies
	em.RegisterStrategy("default", &DefaultEvolutionStrategy{logger: logger})
	em.RegisterStrategy("strict", &StrictEvolutionStrategy{logger: logger})
	em.RegisterStrategy("flexible", &FlexibleEvolutionStrategy{logger: logger})

	return em
}

// RegisterStrategy registers a custom evolution strategy
func (em *EvolutionManager) RegisterStrategy(name string, strategy EvolutionStrategy) {
	em.strategies[name] = strategy
}

// CheckCompatibility checks if two schemas are compatible
func (em *EvolutionManager) CheckCompatibility(old, new *models.Schema, mode CompatibilityMode) error {
	switch mode {
	case CompatibilityNone:
		return nil // Any change is allowed

	case CompatibilityBackward:
		return em.checkBackwardCompatibility(old, new)

	case CompatibilityForward:
		return em.checkForwardCompatibility(old, new)

	case CompatibilityFull:
		if err := em.checkBackwardCompatibility(old, new); err != nil {
			return err
		}
		return em.checkForwardCompatibility(old, new)

	case CompatibilityBackwardTransitive:
		// Check compatibility with all previous versions
		// This requires access to the full schema history
		return em.checkBackwardTransitiveCompatibility(old, new)

	default:
		return fmt.Errorf("unknown compatibility mode: %s", mode)
	}
}

// EvolveSchema evolves a schema based on new data
func (em *EvolutionManager) EvolveSchema(current *models.Schema, newData []map[string]interface{}) (*models.Schema, error) {
	// Infer schema from new data
	inferredSchema, err := em.registry.typeInference.InferSchema(current.Name, newData)
	if err != nil {
		return nil, fmt.Errorf("failed to infer schema from data: %w", err)
	}

	// Detect changes
	changes := em.detectChanges(current, inferredSchema)
	if len(changes) == 0 {
		return current, nil // No changes needed
	}

	// Apply evolution strategy
	strategy := em.strategies["default"]
	if !strategy.CanEvolve(current, inferredSchema) {
		return nil, fmt.Errorf("schema evolution not allowed by strategy")
	}

	evolved, err := strategy.Evolve(current, inferredSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to evolve schema: %w", err)
	}

	em.logger.Info("schema evolved",
		zap.String("schema", current.Name),
		zap.Int("changes", len(changes)))

	return evolved, nil
}

// GetMigrationPlan creates a migration plan between schema versions
func (em *EvolutionManager) GetMigrationPlan(fromVersion, toVersion *SchemaVersion) (*MigrationPlan, error) {
	if fromVersion.Version >= toVersion.Version {
		return nil, fmt.Errorf("cannot migrate backwards from version %d to %d",
			fromVersion.Version, toVersion.Version)
	}

	changes := em.detectChanges(fromVersion.Schema, toVersion.Schema)

	plan := &MigrationPlan{
		FromVersion: fromVersion.Version,
		ToVersion:   toVersion.Version,
		Changes:     changes,
		Strategy:    "default",
		Metadata:    make(map[string]interface{}),
	}

	// Add migration rules for each change
	for i := range plan.Changes {
		plan.Changes[i].Migration = em.createMigrationRule(&plan.Changes[i])
	}

	return plan, nil
}

// detectChanges detects changes between two schemas
func (em *EvolutionManager) detectChanges(old, new *models.Schema) []SchemaChange {
	changes := []SchemaChange{}

	// Create field maps for easy lookup
	oldFields := make(map[string]*models.Field)
	newFields := make(map[string]*models.Field)

	for i := range old.Fields {
		oldFields[old.Fields[i].Name] = &old.Fields[i]
	}

	for i := range new.Fields {
		newFields[new.Fields[i].Name] = &new.Fields[i]
	}

	// Check for removed fields
	for name, oldField := range oldFields {
		if _, exists := newFields[name]; !exists {
			changes = append(changes, SchemaChange{
				Type:     ChangeTypeRemoveField,
				Field:    name,
				OldField: oldField,
			})
		}
	}

	// Check for added or modified fields
	for name, newField := range newFields {
		oldField, exists := oldFields[name]
		if !exists {
			// Field added
			changes = append(changes, SchemaChange{
				Type:     ChangeTypeAddField,
				Field:    name,
				NewField: newField,
			})
		} else {
			// Check for modifications
			if oldField.Type != newField.Type {
				changes = append(changes, SchemaChange{
					Type:     ChangeTypeModifyType,
					Field:    name,
					OldField: oldField,
					NewField: newField,
				})
			}

			if oldField.Required != newField.Required {
				changes = append(changes, SchemaChange{
					Type:     ChangeTypeModifyRequired,
					Field:    name,
					OldField: oldField,
					NewField: newField,
				})
			}
		}
	}

	// Sort changes for consistent ordering
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].Type != changes[j].Type {
			return changes[i].Type < changes[j].Type
		}
		return changes[i].Field < changes[j].Field
	})

	return changes
}

// checkBackwardCompatibility checks if new schema can read old data
func (em *EvolutionManager) checkBackwardCompatibility(old, new *models.Schema) error {
	// New schema must be able to read data written with old schema
	// This means:
	// 1. Cannot remove required fields
	// 2. Cannot change field types (unless compatible)
	// 3. Can add optional fields

	oldFields := make(map[string]*models.Field)
	for i := range old.Fields {
		oldFields[old.Fields[i].Name] = &old.Fields[i]
	}

	newFields := make(map[string]*models.Field)
	for i := range new.Fields {
		newFields[new.Fields[i].Name] = &new.Fields[i]
	}

	// Check removed required fields
	for name, oldField := range oldFields {
		if oldField.Required {
			if _, exists := newFields[name]; !exists {
				return fmt.Errorf("cannot remove required field '%s'", name)
			}
		}
	}

	// Check type changes
	for name, newField := range newFields {
		if oldField, exists := oldFields[name]; exists {
			if !em.areTypesCompatible(oldField.Type, newField.Type) {
				return fmt.Errorf("incompatible type change for field '%s': %s -> %s",
					name, oldField.Type, newField.Type)
			}
		}
	}

	// Check new required fields (not allowed in backward compatibility)
	for name, newField := range newFields {
		if newField.Required {
			if _, exists := oldFields[name]; !exists {
				return fmt.Errorf("cannot add required field '%s'", name)
			}
		}
	}

	return nil
}

// checkForwardCompatibility checks if old schema can read new data
func (em *EvolutionManager) checkForwardCompatibility(old, new *models.Schema) error {
	// Old schema must be able to read data written with new schema
	// This means:
	// 1. Cannot add required fields
	// 2. Cannot change field types (unless compatible)
	// 3. Can remove optional fields

	oldFields := make(map[string]*models.Field)
	for i := range old.Fields {
		oldFields[old.Fields[i].Name] = &old.Fields[i]
	}

	newFields := make(map[string]*models.Field)
	for i := range new.Fields {
		newFields[new.Fields[i].Name] = &new.Fields[i]
	}

	// Check added required fields
	for name, newField := range newFields {
		if newField.Required {
			if _, exists := oldFields[name]; !exists {
				return fmt.Errorf("cannot add required field '%s' in forward compatibility", name)
			}
		}
	}

	return nil
}

// checkBackwardTransitiveCompatibility checks compatibility with all previous versions
func (em *EvolutionManager) checkBackwardTransitiveCompatibility(old, new *models.Schema) error {
	// This would require access to full schema history
	// For now, just check immediate backward compatibility
	return em.checkBackwardCompatibility(old, new)
}

// areTypesCompatible checks if two types are compatible
func (em *EvolutionManager) areTypesCompatible(oldType, newType string) bool {
	// Same type is always compatible
	if oldType == newType {
		return true
	}

	// Define compatibility rules
	compatibilityRules := map[string][]string{
		"integer": {"float", "string"},
		"float":   {"string"},
		"boolean": {"string"},
		// Add more rules as needed
	}

	if compatibleTypes, exists := compatibilityRules[oldType]; exists {
		for _, compatible := range compatibleTypes {
			if compatible == newType {
				return true
			}
		}
	}

	return false
}

// createMigrationRule creates a migration rule for a schema change
func (em *EvolutionManager) createMigrationRule(change *SchemaChange) MigrationRule {
	switch change.Type {
	case ChangeTypeAddField:
		// For added fields, provide a default value
		return MigrationRule{
			Type:         "default",
			DefaultValue: em.getDefaultValue(change.NewField.Type),
		}

	case ChangeTypeModifyType:
		// For type changes, define transformation
		return MigrationRule{
			Type:           "transform",
			Transformation: fmt.Sprintf("%s_to_%s", change.OldField.Type, change.NewField.Type),
		}

	default:
		return MigrationRule{Type: "none"}
	}
}

// getDefaultValue returns a default value for a type
func (em *EvolutionManager) getDefaultValue(fieldType string) interface{} {
	switch fieldType {
	case "string":
		return ""
	case "integer":
		return 0
	case "float":
		return 0.0
	case "boolean":
		return false
	case "array":
		return []interface{}{}
	case "object":
		return map[string]interface{}{}
	default:
		return nil
	}
}

// DefaultEvolutionStrategy allows most schema changes
type DefaultEvolutionStrategy struct {
	logger *zap.Logger
}

func (s *DefaultEvolutionStrategy) CanEvolve(old, new *models.Schema) bool {
	// Allow most changes by default
	return true
}

func (s *DefaultEvolutionStrategy) Evolve(old, new *models.Schema) (*models.Schema, error) {
	// Merge schemas, preferring new field definitions
	evolved := &models.Schema{
		Name:    new.Name,
		Version: new.Version,
		Fields:  make([]models.Field, 0),
	}

	// Create field maps
	oldFields := make(map[string]*models.Field)
	for i := range old.Fields {
		oldFields[old.Fields[i].Name] = &old.Fields[i]
	}

	// Add all fields from new schema
	evolved.Fields = append(evolved.Fields, new.Fields...)

	// Add fields that exist in old but not in new (for backward compatibility)
	for _, oldField := range old.Fields {
		found := false
		for _, newField := range new.Fields {
			if oldField.Name == newField.Name {
				found = true
				break
			}
		}

		if !found {
			// Mark removed fields as optional
			optionalField := models.Field{
				Name:        oldField.Name,
				Type:        oldField.Type,
				Description: oldField.Description,
				Required:    false,
			}
			evolved.Fields = append(evolved.Fields, optionalField)
		}
	}

	return evolved, nil
}

func (s *DefaultEvolutionStrategy) GetMigrationPlan(old, new *models.Schema) *MigrationPlan {
	// Default migration plan
	return &MigrationPlan{
		Strategy: "default",
		Metadata: map[string]interface{}{
			"auto_generated": true,
		},
	}
}

// StrictEvolutionStrategy allows only safe schema changes
type StrictEvolutionStrategy struct {
	logger *zap.Logger
}

func (s *StrictEvolutionStrategy) CanEvolve(old, new *models.Schema) bool {
	// Only allow adding optional fields
	oldFields := make(map[string]bool)
	for _, field := range old.Fields {
		oldFields[field.Name] = true
	}

	for _, field := range new.Fields {
		if !oldFields[field.Name] && field.Required {
			return false // Cannot add required fields
		}
	}

	return true
}

func (s *StrictEvolutionStrategy) Evolve(old, new *models.Schema) (*models.Schema, error) {
	if !s.CanEvolve(old, new) {
		return nil, fmt.Errorf("evolution not allowed by strict strategy")
	}
	return new, nil
}

func (s *StrictEvolutionStrategy) GetMigrationPlan(old, new *models.Schema) *MigrationPlan {
	return &MigrationPlan{
		Strategy: "strict",
		Metadata: map[string]interface{}{
			"validation": "strict",
		},
	}
}

// FlexibleEvolutionStrategy allows all changes with migration support
type FlexibleEvolutionStrategy struct {
	logger *zap.Logger
}

func (s *FlexibleEvolutionStrategy) CanEvolve(old, new *models.Schema) bool {
	// Allow all changes
	return true
}

func (s *FlexibleEvolutionStrategy) Evolve(old, new *models.Schema) (*models.Schema, error) {
	// Accept new schema as-is
	return new, nil
}

func (s *FlexibleEvolutionStrategy) GetMigrationPlan(old, new *models.Schema) *MigrationPlan {
	return &MigrationPlan{
		Strategy: "flexible",
		Metadata: map[string]interface{}{
			"allow_data_loss": true,
		},
	}
}
