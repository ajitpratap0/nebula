package sdk

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
)

// ExampleRESTAPISource demonstrates how to build a REST API source connector using the SDK
func ExampleRESTAPISource() core.Source {
	// Create a source builder
	builder := NewSourceBuilder().
		WithName("rest-api-source").
		WithVersion("1.0.0").
		WithDescription("REST API source connector built with SDK").
		WithAuthor("SDK Example").
		WithIncremental(true).
		WithBatch(true).
		WithConfigProperty("base_url", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"description": "Base URL of the REST API",
		}).
		WithConfigProperty("api_key", map[string]interface{}{
			"type":        "string",
			"required":    false,
			"description": "API key for authentication",
		}).
		WithConfigProperty("endpoint", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"description": "API endpoint to fetch data from",
		}).
		WithDefaults(
			1000,           // batch size
			5000,           // buffer size
			5,              // max concurrency
			30*time.Second, // request timeout
			3*time.Second,  // retry delay
			3,              // retry attempts
			100,            // rate limit (100 req/sec)
		).
		WithConfigValidator(validateRESTAPIConfig).
		WithInitHook(initRESTAPISource).
		WithDiscoverHook(discoverRESTAPISchema).
		WithReadHook(readRESTAPIRecords).
		WithReadBatchHook(readRESTAPIBatches).
		WithStateManagement(
			getRESTAPIPosition,
			setRESTAPIPosition,
			getRESTAPIState,
			setRESTAPIState,
		).
		WithHealthHook(checkRESTAPIHealth).
		WithCloseHook(closeRESTAPISource)

	// Create the connector
	connector, err := NewSDKSourceConnector(builder)
	if err != nil {
		panic(fmt.Sprintf("Failed to create REST API source: %v", err))
	}

	return connector
}

// ExampleDatabaseDestination demonstrates how to build a database destination connector using the SDK
func ExampleDatabaseDestination() core.Destination {
	// Create a destination builder
	builder := NewDestinationBuilder().
		WithName("database-destination").
		WithVersion("1.0.0").
		WithDescription("Database destination connector built with SDK").
		WithAuthor("SDK Example").
		WithBatch(true).
		WithStreaming(true).
		WithTransactions(true).
		WithUpsert(true).
		WithBulkLoad(true).
		WithConfigProperty("connection_string", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"description": "Database connection string",
		}).
		WithConfigProperty("table", map[string]interface{}{
			"type":        "string",
			"required":    true,
			"description": "Target table name",
		}).
		WithConfigProperty("batch_size", map[string]interface{}{
			"type":        "integer",
			"required":    false,
			"default":     1000,
			"description": "Batch size for bulk operations",
		}).
		WithDefaults(
			1000,           // batch size
			10000,          // buffer size
			10,             // max concurrency
			60*time.Second, // request timeout
			5*time.Second,  // retry delay
			3,              // retry attempts
			0,              // no rate limit
		).
		WithConfigValidator(validateDatabaseConfig).
		WithInitHook(initDatabaseDestination).
		WithCreateSchemaHook(createDatabaseSchema).
		WithWriteHook(writeDatabaseRecords).
		WithWriteBatchHook(writeDatabaseBatches).
		WithBulkLoadHook(bulkLoadDatabase).
		WithTransactionHook(beginDatabaseTransaction).
		WithUpsertHook(upsertDatabaseRecords).
		WithSchemaManagement(alterDatabaseSchema, dropDatabaseSchema).
		WithHealthHook(checkDatabaseHealth).
		WithCloseHook(closeDatabaseDestination)

	// Create the connector
	connector, err := NewSDKDestinationConnector(builder)
	if err != nil {
		panic(fmt.Sprintf("Failed to create database destination: %v", err))
	}

	return connector
}

// REST API Source Implementation Functions

var restAPIState = struct {
	baseURL    string
	apiKey     string
	endpoint   string
	lastCursor string
}{}

func validateRESTAPIConfig(config *config.BaseConfig) error {
	validator := NewConfigValidator()

	// Validate required properties
	if err := validator.ValidateRequired(config, "base_url", "endpoint"); err != nil {
		return err
	}

	// Validate base URL format
	if err := validator.ValidateString(config, "base_url", 1, 255, `^https?://`); err != nil {
		return err
	}

	// Validate endpoint
	if err := validator.ValidateString(config, "endpoint", 1, 100, ""); err != nil {
		return err
	}

	return nil
}

func initRESTAPISource(ctx context.Context, config *config.BaseConfig) error {
	restAPIState.baseURL = config.Security.Credentials["base_url"]
	restAPIState.endpoint = config.Security.Credentials["endpoint"]

	if apiKey, ok := config.Security.Credentials["api_key"]; ok && apiKey != "" {
		restAPIState.apiKey = apiKey
	}

	// Initialize HTTP client, etc.
	// ...

	return nil
}

func discoverRESTAPISchema(ctx context.Context) (*core.Schema, error) {
	// Build schema based on API response structure
	schema := NewSchemaBuilder("rest_api_data").
		WithDescription("Schema discovered from REST API").
		AddFieldWithOptions("id", core.FieldTypeString, false, true, true, nil, "Record ID").
		AddFieldWithOptions("name", core.FieldTypeString, true, false, false, nil, "Record name").
		AddFieldWithOptions("created_at", core.FieldTypeTimestamp, true, false, false, nil, "Creation timestamp").
		AddFieldWithOptions("data", core.FieldTypeJSON, true, false, false, nil, "Additional data").
		Build()

	return schema, nil
}

func readRESTAPIRecords(ctx context.Context) (*core.RecordStream, error) {
	recordChan := make(chan *models.Record, 100)
	errorChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errorChan)

		// Simulate API calls and record creation
		for i := 0; i < 100; i++ {
			record := NewRecordBuilder("rest-api").
				WithID(fmt.Sprintf("api_record_%d", i)).
				AddField("id", fmt.Sprintf("api_record_%d", i)).
				AddField("name", fmt.Sprintf("Record %d", i)).
				AddField("created_at", time.Now()).
				AddField("data", map[string]interface{}{
					"index": i,
					"type":  "api_record",
				}).
				AddMetadata("api_endpoint", restAPIState.endpoint).
				Build()

			select {
			case recordChan <- record:
			case <-ctx.Done():
				return
			}
		}
	}()

	return &core.RecordStream{
		Records: recordChan,
		Errors:  errorChan,
	}, nil
}

func readRESTAPIBatches(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	batchChan := make(chan []*models.Record, 10)
	errorChan := make(chan error, 1)

	go func() {
		defer close(batchChan)
		defer close(errorChan)

		// Simulate batch API calls
		for batchNum := 0; batchNum < 5; batchNum++ {
			batch := make([]*models.Record, batchSize)
			for i := 0; i < batchSize; i++ {
				recordIndex := batchNum*batchSize + i
				batch[i] = NewRecordBuilder("rest-api").
					WithID(fmt.Sprintf("batch_record_%d", recordIndex)).
					AddField("id", fmt.Sprintf("batch_record_%d", recordIndex)).
					AddField("name", fmt.Sprintf("Batch Record %d", recordIndex)).
					AddField("batch_number", batchNum).
					Build()
			}

			select {
			case batchChan <- batch:
			case <-ctx.Done():
				return
			}
		}
	}()

	return &core.BatchStream{
		Batches: batchChan,
		Errors:  errorChan,
	}, nil
}

func getRESTAPIPosition() core.Position {
	// Return current cursor position
	return &simplePosition{cursor: restAPIState.lastCursor}
}

func setRESTAPIPosition(position core.Position) error {
	if pos, ok := position.(*simplePosition); ok {
		restAPIState.lastCursor = pos.cursor
	}
	return nil
}

func getRESTAPIState() core.State {
	return core.State{
		"last_cursor": restAPIState.lastCursor,
		"base_url":    restAPIState.baseURL,
		"endpoint":    restAPIState.endpoint,
	}
}

func setRESTAPIState(state core.State) error {
	if cursor, ok := state["last_cursor"].(string); ok {
		restAPIState.lastCursor = cursor
	}
	return nil
}

func checkRESTAPIHealth(ctx context.Context) error {
	// Perform health check against API
	// Make a simple GET request to health endpoint
	return nil
}

func closeRESTAPISource(ctx context.Context) error {
	// Clean up HTTP clients, connections, etc.
	return nil
}

// Database Destination Implementation Functions

var databaseState = struct {
	connectionString string
	tableName        string
	batchSize        int
}{}

func validateDatabaseConfig(config *config.BaseConfig) error {
	validator := NewConfigValidator()

	// Validate required properties
	if err := validator.ValidateRequired(config, "connection_string", "table"); err != nil {
		return err
	}

	// Validate connection string
	if err := validator.ValidateString(config, "connection_string", 1, 500, ""); err != nil {
		return err
	}

	// Validate table name
	if err := validator.ValidateString(config, "table", 1, 100, `^[a-zA-Z][a-zA-Z0-9_]*$`); err != nil {
		return err
	}

	// Validate batch size
	if err := validator.ValidateInt(config, "batch_size", 1, 10000); err != nil {
		return err
	}

	return nil
}

func initDatabaseDestination(ctx context.Context, config *config.BaseConfig) error {
	databaseState.connectionString = config.Security.Credentials["connection_string"]
	databaseState.tableName = config.Security.Credentials["table"]

	if batchSizeStr, ok := config.Security.Credentials["batch_size"]; ok && batchSizeStr != "" {
		if batchSize, err := strconv.Atoi(batchSizeStr); err == nil {
			databaseState.batchSize = batchSize
		} else {
			databaseState.batchSize = 1000
		}
	} else {
		databaseState.batchSize = 1000
	}

	// Initialize database connection pool, etc.
	// ...

	return nil
}

func createDatabaseSchema(ctx context.Context, schema *core.Schema) error {
	// Create table based on schema
	// Generate CREATE TABLE statement
	// Execute against database
	return nil
}

func writeDatabaseRecords(ctx context.Context, stream *core.RecordStream) error {
	recordCount := 0

	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				// Stream closed
				return nil
			}

			// Insert single record
			// Prepare INSERT statement
			// Execute against database
			_ = record // Use the record
			recordCount++

		case err := <-stream.Errors:
			if err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func writeDatabaseBatches(ctx context.Context, stream *core.BatchStream) error {
	batchCount := 0

	for {
		select {
		case batch, ok := <-stream.Batches:
			if !ok {
				// Stream closed
				return nil
			}

			// Insert batch of records
			// Prepare batch INSERT statement
			// Execute against database
			_ = batch // Use the batch
			batchCount++

		case err := <-stream.Errors:
			if err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func bulkLoadDatabase(ctx context.Context, reader interface{}, format string) error {
	// Perform bulk load operation
	// Use database-specific bulk load utilities
	return nil
}

func beginDatabaseTransaction(ctx context.Context) (core.Transaction, error) {
	// Begin database transaction
	// Return transaction wrapper
	return &simpleTransaction{}, nil
}

func upsertDatabaseRecords(ctx context.Context, records []*models.Record, keys []string) error {
	// Perform upsert operation
	// Generate UPSERT/MERGE statement
	// Execute against database
	return nil
}

func alterDatabaseSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	// Generate ALTER TABLE statements
	// Compare schemas and determine changes
	// Execute against database
	return nil
}

func dropDatabaseSchema(ctx context.Context, schema *core.Schema) error {
	// Drop table
	// Execute DROP TABLE statement
	return nil
}

func checkDatabaseHealth(ctx context.Context) error {
	// Check database connectivity
	// Execute simple SELECT 1 query
	return nil
}

func closeDatabaseDestination(ctx context.Context) error {
	// Close database connections
	// Clean up resources
	return nil
}

// Helper types for examples

type simplePosition struct {
	cursor string
}

func (sp *simplePosition) String() string {
	return sp.cursor
}

func (sp *simplePosition) Compare(other core.Position) int {
	if otherPos, ok := other.(*simplePosition); ok {
		switch {
		case sp.cursor == otherPos.cursor:
			return 0
		case sp.cursor < otherPos.cursor:
			return -1
		default:
			return 1
		}
	}
	return 0
}

type simpleTransaction struct{}

func (st *simpleTransaction) Commit(ctx context.Context) error {
	// Commit transaction
	return nil
}

func (st *simpleTransaction) Rollback(ctx context.Context) error {
	// Rollback transaction
	return nil
}

// ExampleCustomConnectorRegistration demonstrates how to register custom connectors
func ExampleCustomConnectorRegistration() {
	registry := NewConnectorRegistry()

	// Register the REST API source
	restAPIBuilder := NewSourceBuilder().
		WithName("rest-api").
		WithVersion("1.0.0").
		WithDescription("REST API source connector").
		WithIncremental(true).
		WithBatch(true)

	err := registry.RegisterSourceBuilder("rest-api", restAPIBuilder)
	if err != nil {
		panic(err)
	}

	// Register the database destination
	databaseBuilder := NewDestinationBuilder().
		WithName("database").
		WithVersion("1.0.0").
		WithDescription("Database destination connector").
		WithBatch(true).
		WithTransactions(true).
		WithUpsert(true)

	err = registry.RegisterDestinationBuilder("database", databaseBuilder)
	if err != nil {
		panic(err)
	}
}

// ExampleSchemaInference demonstrates how to infer schemas from Go structs
func ExampleSchemaInference() {
	// Define a sample struct
	type User struct {
		ID        int                    `json:"id" description:"User ID"`
		Name      string                 `json:"name" description:"User name"`
		Email     *string                `json:"email" description:"User email (optional)"`
		CreatedAt time.Time              `json:"created_at" description:"Creation timestamp"`
		Metadata  map[string]interface{} `json:"metadata" description:"Additional metadata"`
	}

	// Infer schema from struct
	helper := NewReflectionHelper()
	schema := helper.InferSchemaFromStruct(User{}, "users")

	fmt.Printf("Inferred schema: %+v\n", schema)
}

// ExampleStreamBuilding demonstrates how to build test streams
func ExampleStreamBuilding() {
	// Build test records
	record1 := NewRecordBuilder("test").
		WithID("test_1").
		AddField("name", "Test Record 1").
		AddField("value", 42).
		Build()

	record2 := NewRecordBuilder("test").
		WithID("test_2").
		AddField("name", "Test Record 2").
		AddField("value", 84).
		Build()

	// Build record stream
	streamBuilder := NewStreamBuilder().
		WithBufferSize(10).
		AddRecord(record1).
		AddRecord(record2)

	recordStream := streamBuilder.BuildRecordStream()
	batchStream := streamBuilder.BuildBatchStream(1)

	fmt.Printf("Built streams: %+v, %+v\n", recordStream, batchStream)
}
