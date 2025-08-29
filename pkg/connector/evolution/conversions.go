package evolution

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
)

// convertCoreToModelsSchema converts core.Schema to models.Schema
func convertCoreToModelsSchema(coreSchema *core.Schema) *models.Schema {
	if coreSchema == nil {
		return nil
	}

	// Convert fields
	fields := make([]models.Field, len(coreSchema.Fields))
	for i, coreField := range coreSchema.Fields {
		fields[i] = models.Field{
			Name:        coreField.Name,
			Type:        string(coreField.Type),
			Description: coreField.Description,
			Required:    !coreField.Nullable,
		}
	}

	return &models.Schema{
		Name:    coreSchema.Name,
		Fields:  fields,
		Version: strconv.Itoa(coreSchema.Version),
	}
}

// convertModelsToCoreSchema converts models.Schema to core.Schema
func convertModelsToCoreSchema(modelsSchema *models.Schema) *core.Schema {
	if modelsSchema == nil {
		return nil
	}

	// Convert fields
	fields := make([]core.Field, len(modelsSchema.Fields))
	for i, modelField := range modelsSchema.Fields {
		fields[i] = core.Field{
			Name:        modelField.Name,
			Type:        core.FieldType(modelField.Type),
			Description: modelField.Description,
			Nullable:    !modelField.Required,
		}
	}

	// Convert version
	version := 1
	if v, err := strconv.Atoi(modelsSchema.Version); err == nil {
		version = v
	}

	return &core.Schema{
		Name:        modelsSchema.Name,
		Description: fmt.Sprintf("Version %s", modelsSchema.Version),
		Fields:      fields,
		Version:     version,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
}
