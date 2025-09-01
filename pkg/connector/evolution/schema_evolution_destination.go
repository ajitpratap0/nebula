package evolution

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/logger"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/ajitpratap0/nebula/pkg/schema"
	"go.uber.org/zap"
)

// SchemaEvolutionDestination wraps a destination with automatic schema evolution capabilities
type SchemaEvolutionDestination struct {
	// Wrapped destination
	destination core.Destination

	// Schema evolution components
	evolutionManager *schema.EvolutionManager
	schemaRegistry   *schema.Registry
	currentSchema    *models.Schema
	schemaMutex      sync.RWMutex

	// Configuration
	config           *EvolutionConfig
	logger           *zap.Logger
	lastSchemaUpdate time.Time

	// Metrics
	schemaChanges     int64
	evolutionFailures int64
	recordsProcessed  int64
}

// EvolutionConfig configures schema evolution behavior
type EvolutionConfig struct {
	// Evolution strategy: "default", "strict", "flexible"
	Strategy string `json:"strategy"`

	// Compatibility mode
	CompatibilityMode schema.CompatibilityMode `json:"compatibility_mode"`

	// Auto-evolution settings
	EnableAutoEvolution   bool          `json:"enable_auto_evolution"`
	SchemaCheckInterval   time.Duration `json:"schema_check_interval"`
	BatchSizeForInference int           `json:"batch_size_for_inference"`

	// Schema handling
	FailOnIncompatible bool `json:"fail_on_incompatible"`
	PreserveOldFields  bool `json:"preserve_old_fields"`

	// Performance
	CacheSchemaLocally  bool `json:"cache_schema_locally"`
	MaxSchemaAgeMinutes int  `json:"max_schema_age_minutes"`
}

// DefaultEvolutionConfig returns default configuration
func DefaultEvolutionConfig() *EvolutionConfig {
	return &EvolutionConfig{
		Strategy:              "default",
		CompatibilityMode:     schema.CompatibilityBackward,
		EnableAutoEvolution:   true,
		SchemaCheckInterval:   5 * time.Minute,
		BatchSizeForInference: 100,
		FailOnIncompatible:    false,
		PreserveOldFields:     true,
		CacheSchemaLocally:    true,
		MaxSchemaAgeMinutes:   60,
	}
}

// NewSchemaEvolutionDestination creates a new destination with schema evolution
func NewSchemaEvolutionDestination(dest core.Destination, config *EvolutionConfig) (*SchemaEvolutionDestination, error) {
	if dest == nil {
		return nil, nebulaerrors.New(nebulaerrors.ErrorTypeConfig, "destination cannot be nil")
	}

	if config == nil {
		config = DefaultEvolutionConfig()
	}

	log := logger.Get()

	// Create schema registry
	registry := schema.NewRegistry(log)

	// Get evolution manager from registry
	evolutionManager := registry.GetEvolutionManager()

	return &SchemaEvolutionDestination{
		destination:      dest,
		evolutionManager: evolutionManager,
		schemaRegistry:   registry,
		config:           config,
		logger:           log.With(zap.String("component", "schema_evolution")),
	}, nil
}

// Initialize initializes both the destination and schema evolution
func (sed *SchemaEvolutionDestination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	// Initialize the wrapped destination
	if err := sed.destination.Initialize(ctx, config); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig, "failed to initialize destination")
	}

	// Check if schema evolution is enabled in Advanced config
	if !config.Advanced.EnableSchemaEvolution {
		sed.logger.Warn("schema evolution is disabled in configuration")
	}

	sed.logger.Info("schema evolution destination initialized",
		zap.String("strategy", sed.config.Strategy),
		zap.String("compatibility", string(sed.config.CompatibilityMode)),
		zap.Bool("auto_evolution", sed.config.EnableAutoEvolution))

	return nil
}

// CreateSchema creates the initial schema with evolution tracking
func (sed *SchemaEvolutionDestination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	sed.schemaMutex.Lock()
	defer sed.schemaMutex.Unlock()

	// Convert to models.Schema for registry
	modelsSchema := convertCoreToModelsSchema(schema)

	// Register schema in registry
	version, err := sed.schemaRegistry.RegisterSchema(ctx, modelsSchema.Name, modelsSchema)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to register schema")
	}

	// Create schema in destination
	if err := sed.destination.CreateSchema(ctx, schema); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to create schema in destination")
	}

	sed.currentSchema = modelsSchema
	sed.lastSchemaUpdate = time.Now()

	sed.logger.Info("schema created with evolution tracking",
		zap.String("schema", schema.Name),
		zap.Int("version", version.Version),
		zap.Int("fields", len(schema.Fields)))

	return nil
}

// AlterSchema handles schema alterations with evolution
func (sed *SchemaEvolutionDestination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	sed.schemaMutex.Lock()
	defer sed.schemaMutex.Unlock()

	// Convert to models.Schema for evolution checks
	oldModelsSchema := convertCoreToModelsSchema(oldSchema)
	newModelsSchema := convertCoreToModelsSchema(newSchema)

	// Check compatibility
	if err := sed.evolutionManager.CheckCompatibility(oldModelsSchema, newModelsSchema, sed.config.CompatibilityMode); err != nil {
		if sed.config.FailOnIncompatible {
			return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeValidation, "schema incompatible")
		}
		sed.logger.Warn("schema compatibility check failed, proceeding anyway",
			zap.Error(err))
	}

	// Get migration plan
	oldVersion, _ := sed.schemaRegistry.GetLatestSchema(oldModelsSchema.Name)
	newVersion, err := sed.schemaRegistry.RegisterSchema(ctx, newModelsSchema.Name, newModelsSchema)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to register new schema version")
	}

	migrationPlan, err := sed.evolutionManager.GetMigrationPlan(oldVersion, newVersion)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to create migration plan")
	}

	// Log migration plan
	sed.logger.Info("schema migration plan created",
		zap.String("schema", newSchema.Name),
		zap.Int("from_version", migrationPlan.FromVersion),
		zap.Int("to_version", migrationPlan.ToVersion),
		zap.Int("changes", len(migrationPlan.Changes)))

	// Apply schema changes to destination
	if err := sed.destination.AlterSchema(ctx, oldSchema, newSchema); err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConnection, "failed to alter schema in destination")
	}

	sed.currentSchema = newModelsSchema
	sed.lastSchemaUpdate = time.Now()
	sed.schemaChanges++

	return nil
}

// Write writes records with automatic schema evolution
func (sed *SchemaEvolutionDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	if !sed.config.EnableAutoEvolution {
		// Pass through to destination without evolution
		return sed.destination.Write(ctx, stream)
	}

	// Create channels for evolved stream
	recordsChan := make(chan *models.Record, 1000)
	errorsChan := make(chan error, 1)

	// Create evolved stream
	evolvedStream := &core.RecordStream{
		Records: recordsChan,
		Errors:  errorsChan,
	}

	// Start evolution worker
	go func() {
		defer close(recordsChan)

		batch := make([]map[string]interface{}, 0, sed.config.BatchSizeForInference)
		batchRecords := pool.GetBatchSlice(sed.config.BatchSizeForInference)

		defer pool.PutBatchSlice(batchRecords)

		for {
			select {
			case record, ok := <-stream.Records:
				if !ok {
					// Process remaining batch
					if len(batch) > 0 {
						if err := sed.processBatchWithEvolution(ctx, batch, batchRecords, recordsChan); err != nil {
							select {
							case errorsChan <- err:
							default:
							}
						}
					}
					return
				}

				// Add to batch
				batch = append(batch, record.Data)
				batchRecords = append(batchRecords, record)
				sed.recordsProcessed++

				// Process batch if full
				if len(batch) >= sed.config.BatchSizeForInference {
					if err := sed.processBatchWithEvolution(ctx, batch, batchRecords, recordsChan); err != nil {
						select {
						case errorsChan <- err:
						default:
						}
						return
					}
					batch = batch[:0]
					batchRecords = batchRecords[:0]
				}

			case err := <-stream.Errors:
				select {
				case errorsChan <- err:
				default:
				}

			case <-ctx.Done():
				select {
				case errorsChan <- ctx.Err():
				default:
				}
				return
			}
		}
	}()

	// Write evolved stream to destination
	return sed.destination.Write(ctx, evolvedStream)
}

// WriteBatch writes batches with automatic schema evolution
func (sed *SchemaEvolutionDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	if !sed.config.EnableAutoEvolution {
		// Pass through to destination without evolution
		return sed.destination.WriteBatch(ctx, stream)
	}

	// Create channels for evolved stream
	batchesChan := make(chan []*models.Record, 10)
	errorsChan := make(chan error, 1)

	// Create evolved stream
	evolvedStream := &core.BatchStream{
		Batches: batchesChan,
		Errors:  errorsChan,
	}

	// Start evolution worker
	go func() {
		defer close(batchesChan)

		for {
			select {
			case batch, ok := <-stream.Batches:
				if !ok {
					return
				}

				// Check if schema evolution is needed
				needsEvolution, err := sed.checkSchemaEvolution(ctx, batch)
				if err != nil {
					select {
					case errorsChan <- err:
					default:
					}
					return
				}

				if needsEvolution {
					// Evolve schema
					if err := sed.evolveSchemaFromBatch(ctx, batch); err != nil {
						sed.evolutionFailures++
						if sed.config.FailOnIncompatible {
							select {
							case errorsChan <- err:
							default:
							}
							return
						}
						sed.logger.Warn("schema evolution failed, using existing schema",
							zap.Error(err))
					}
				}

				// Send batch to destination
				select {
				case batchesChan <- batch:
					sed.recordsProcessed += int64(len(batch))
				case <-ctx.Done():
					return
				}

			case err := <-stream.Errors:
				select {
				case errorsChan <- err:
				default:
				}

			case <-ctx.Done():
				select {
				case errorsChan <- ctx.Err():
				default:
				}
				return
			}
		}
	}()

	// Write evolved stream to destination
	return sed.destination.WriteBatch(ctx, evolvedStream)
}

// processBatchWithEvolution processes a batch and evolves schema if needed
func (sed *SchemaEvolutionDestination) processBatchWithEvolution(
	ctx context.Context,
	data []map[string]interface{},
	records []*models.Record,
	output chan<- *models.Record,
) error {
	// Check if we need to update schema
	if sed.shouldCheckSchema() {
		sed.schemaMutex.RLock()
		currentSchema := sed.currentSchema
		sed.schemaMutex.RUnlock()

		if currentSchema != nil {
			// Try to evolve schema based on new data
			evolved, err := sed.evolutionManager.EvolveSchema(currentSchema, data)
			if err != nil {
				sed.logger.Warn("failed to evolve schema from data",
					zap.Error(err))
			} else if evolved.Version > currentSchema.Version {
				// Schema evolved, update destination
				oldCoreSchema := convertModelsToCoreSchema(currentSchema)
				newCoreSchema := convertModelsToCoreSchema(evolved)
				if err := sed.AlterSchema(ctx, oldCoreSchema, newCoreSchema); err != nil {
					sed.logger.Error("failed to alter schema in destination",
						zap.Error(err))
					sed.evolutionFailures++
				}
			}
		}
	}

	// Send records to output
	for _, record := range records {
		select {
		case output <- record:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// checkSchemaEvolution checks if schema evolution is needed for a batch
func (sed *SchemaEvolutionDestination) checkSchemaEvolution(ctx context.Context, batch []*models.Record) (bool, error) {
	if !sed.shouldCheckSchema() {
		return false, nil
	}

	sed.schemaMutex.RLock()
	currentSchema := sed.currentSchema
	sed.schemaMutex.RUnlock()

	if currentSchema == nil {
		return false, nil
	}

	// Convert batch to data for evolution check
	data := make([]map[string]interface{}, len(batch))
	for i, record := range batch {
		data[i] = record.Data
	}

	// Check if evolution is needed by trying to evolve
	evolved, err := sed.evolutionManager.EvolveSchema(currentSchema, data)
	if err != nil {
		return false, nil
	}

	// If version increased, schema has changed
	return evolved.Version > currentSchema.Version, nil
}

// evolveSchemaFromBatch evolves schema based on batch data
func (sed *SchemaEvolutionDestination) evolveSchemaFromBatch(ctx context.Context, batch []*models.Record) error {
	sed.schemaMutex.RLock()
	currentSchema := sed.currentSchema
	sed.schemaMutex.RUnlock()

	if currentSchema == nil {
		// No current schema, cannot evolve
		return nil
	}

	// Convert batch to data for evolution
	data := make([]map[string]interface{}, len(batch))
	for i, record := range batch {
		data[i] = record.Data
	}

	// Evolve schema
	evolved, err := sed.evolutionManager.EvolveSchema(currentSchema, data)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData, "failed to evolve schema")
	}

	// If schema changed, update destination
	if evolved.Version > currentSchema.Version {
		oldCoreSchema := convertModelsToCoreSchema(currentSchema)
		newCoreSchema := convertModelsToCoreSchema(evolved)
		return sed.AlterSchema(ctx, oldCoreSchema, newCoreSchema)
	}

	return nil
}

// shouldCheckSchema determines if it's time to check for schema changes
func (sed *SchemaEvolutionDestination) shouldCheckSchema() bool {
	sed.schemaMutex.RLock()
	lastUpdate := sed.lastSchemaUpdate
	sed.schemaMutex.RUnlock()

	return time.Since(lastUpdate) >= sed.config.SchemaCheckInterval
}

// Close closes the destination
func (sed *SchemaEvolutionDestination) Close(ctx context.Context) error {
	sed.logger.Info("closing schema evolution destination",
		zap.Int64("schema_changes", sed.schemaChanges),
		zap.Int64("evolution_failures", sed.evolutionFailures),
		zap.Int64("records_processed", sed.recordsProcessed))

	return sed.destination.Close(ctx)
}

// Delegate other methods to wrapped destination

func (sed *SchemaEvolutionDestination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	return sed.destination.BulkLoad(ctx, reader, format)
}

func (sed *SchemaEvolutionDestination) SupportsBulkLoad() bool {
	return sed.destination.SupportsBulkLoad()
}

func (sed *SchemaEvolutionDestination) SupportsTransactions() bool {
	return sed.destination.SupportsTransactions()
}

func (sed *SchemaEvolutionDestination) SupportsUpsert() bool {
	return sed.destination.SupportsUpsert()
}

func (sed *SchemaEvolutionDestination) SupportsBatch() bool {
	return sed.destination.SupportsBatch()
}

func (sed *SchemaEvolutionDestination) SupportsStreaming() bool {
	return sed.destination.SupportsStreaming()
}

func (sed *SchemaEvolutionDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return sed.destination.BeginTransaction(ctx)
}

func (sed *SchemaEvolutionDestination) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	return sed.destination.Upsert(ctx, records, keys)
}

func (sed *SchemaEvolutionDestination) DropSchema(ctx context.Context, schema *core.Schema) error {
	return sed.destination.DropSchema(ctx, schema)
}

func (sed *SchemaEvolutionDestination) Health(ctx context.Context) error {
	return sed.destination.Health(ctx)
}

func (sed *SchemaEvolutionDestination) Metrics() map[string]interface{} {
	metrics := sed.destination.Metrics()

	// Add evolution metrics
	metrics["schema_changes"] = sed.schemaChanges
	metrics["evolution_failures"] = sed.evolutionFailures
	metrics["records_processed"] = sed.recordsProcessed
	metrics["current_schema_version"] = 0

	if sed.currentSchema != nil {
		version := 1
		if v, err := strconv.Atoi(sed.currentSchema.Version); err == nil {
			version = v
		}
		metrics["current_schema_version"] = version
		metrics["current_schema_fields"] = len(sed.currentSchema.Fields)
	}

	return metrics
}
