package evolution

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/destinations/bigquery"
	"github.com/ajitpratap0/nebula/pkg/connector/destinations/snowflake"
	"github.com/ajitpratap0/nebula/pkg/errors"
	"github.com/ajitpratap0/nebula/pkg/schema"
)

// WrapDestinationWithEvolution wraps any destination with schema evolution capabilities
func WrapDestinationWithEvolution(dest core.Destination, config *EvolutionConfig) (core.Destination, error) {
	if dest == nil {
		return nil, errors.New(errors.ErrorTypeConfig, "destination cannot be nil")
	}

	return NewSchemaEvolutionDestination(dest, config)
}

// CreateSnowflakeWithEvolution creates a Snowflake destination with schema evolution
func CreateSnowflakeWithEvolution(name string, config *config.BaseConfig, evolutionConfig *EvolutionConfig) (core.Destination, error) {
	// Create base Snowflake destination
	snowflakeDest, err := snowflake.NewSnowflakeOptimizedDestination(name, config)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConfig, "failed to create Snowflake destination")
	}

	// Wrap with evolution
	return WrapDestinationWithEvolution(snowflakeDest, evolutionConfig)
}

// CreateBigQueryWithEvolution creates a BigQuery destination with schema evolution
func CreateBigQueryWithEvolution(name string, config *config.BaseConfig, evolutionConfig *EvolutionConfig) (core.Destination, error) {
	// Create base BigQuery destination
	bigqueryDest, err := bigquery.NewBigQueryDestination(name, config)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConfig, "failed to create BigQuery destination")
	}

	// Wrap with evolution
	return WrapDestinationWithEvolution(bigqueryDest, evolutionConfig)
}

// CreateEvolutionConfig creates evolution configuration from properties
func CreateEvolutionConfig(properties map[string]interface{}) *EvolutionConfig {
	config := DefaultEvolutionConfig()

	if strategy, ok := properties["evolution_strategy"].(string); ok {
		config.Strategy = strategy
	}

	if mode, ok := properties["compatibility_mode"].(string); ok {
		switch mode {
		case "backward":
			config.CompatibilityMode = schema.CompatibilityBackward
		case "forward":
			config.CompatibilityMode = schema.CompatibilityForward
		case "full":
			config.CompatibilityMode = schema.CompatibilityFull
		case "none":
			config.CompatibilityMode = schema.CompatibilityNone
		}
	}

	if enable, ok := properties["enable_auto_evolution"].(bool); ok {
		config.EnableAutoEvolution = enable
	}

	if batchSize, ok := properties["schema_inference_batch_size"].(int); ok {
		config.BatchSizeForInference = batchSize
	}

	if fail, ok := properties["fail_on_incompatible"].(bool); ok {
		config.FailOnIncompatible = fail
	}

	if preserve, ok := properties["preserve_old_fields"].(bool); ok {
		config.PreserveOldFields = preserve
	}

	return config
}