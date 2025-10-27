package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/destinations/bigquery"
	"github.com/ajitpratap0/nebula/pkg/connector/evolution"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/ajitpratap0/nebula/pkg/schema"
)

func main() {
	ctx := context.Background()

	// Example 1: Basic schema evolution with Snowflake
	basicSnowflakeExample(ctx)

	// Example 2: Advanced schema evolution with BigQuery
	advancedBigQueryExample(ctx)

	// Example 3: Schema evolution with custom strategy
	customStrategyExample(ctx)
}

// basicSnowflakeExample demonstrates basic schema evolution with Snowflake
func basicSnowflakeExample(ctx context.Context) {
	fmt.Println("\n=== Basic Schema Evolution with Snowflake ===")

	// Configure Snowflake destination
	snowflakeConfig := config.NewBaseConfig("snowflake-evolution", "destination")
	// Configure Snowflake-specific settings
	snowflakeConfig.Security.Credentials["account"] = "myaccount"
	snowflakeConfig.Security.Credentials["user"] = "myuser"
	snowflakeConfig.Security.Credentials["password"] = "mypassword"
	snowflakeConfig.Security.Credentials["database"] = "MYDB"
	snowflakeConfig.Security.Credentials["schema"] = "PUBLIC"
	snowflakeConfig.Security.Credentials["warehouse"] = "MYWH"
	snowflakeConfig.Security.Credentials["table"] = "EVOLVED_TABLE"

	// Create evolution configuration
	evolutionConfig := &evolution.EvolutionConfig{
		Strategy:              "default",
		CompatibilityMode:     schema.CompatibilityBackward,
		EnableAutoEvolution:   true,
		SchemaCheckInterval:   0, // Check on every batch
		BatchSizeForInference: 10,
		FailOnIncompatible:    false,
		PreserveOldFields:     true,
	}

	// Create Snowflake destination with evolution
	dest, err := evolution.CreateSnowflakeWithEvolution("snowflake-evolved", snowflakeConfig, evolutionConfig)
	if err != nil {
		log.Fatal("Failed to create Snowflake destination:", err)
	}

	// Initialize destination
	if err := dest.Initialize(ctx, snowflakeConfig); err != nil {
		log.Fatal("Failed to initialize destination:", err)
	}
	defer func() {
		if err := dest.Close(ctx); err != nil {
			log.Printf("Failed to close destination: %v", err)
		}
	}()

	// Create initial schema
	initialSchema := &core.Schema{
		Name:    "user_events",
		Version: 1,
		Fields: []core.Field{
			{Name: "user_id", Type: core.FieldTypeString, Nullable: false},
			{Name: "event_type", Type: core.FieldTypeString, Nullable: false},
			{Name: "timestamp", Type: core.FieldTypeTimestamp, Nullable: false},
		},
	}

	// Create schema in destination
	if err := dest.CreateSchema(ctx, initialSchema); err != nil {
		log.Printf("Failed to create schema: %v", err)
		return
	}

	fmt.Println("Initial schema created with 3 fields")

	// Simulate data with evolved schema (new fields)
	evolvedData := []*pool.Record{
		pool.NewRecord("schema-evolution", map[string]interface{}{
			"user_id":    "user123",
			"event_type": "login",
			"timestamp":  "2024-01-10T10:00:00Z",
			"ip_address": "192.168.1.1",    // New field
			"user_agent": "Mozilla/5.0...", // New field
		}),
		pool.NewRecord("schema-evolution", map[string]interface{}{
			"user_id":    "user456",
			"event_type": "purchase",
			"timestamp":  "2024-01-10T10:05:00Z",
			"ip_address": "192.168.1.2",
			"user_agent": "Chrome/120.0...",
			"amount":     99.99, // Another new field
		}),
	}

	// Create channels for stream
	recordsChan := make(chan *pool.Record, len(evolvedData))
	errorsChan := make(chan error, 1)

	// Write evolved data - schema will automatically evolve
	stream := &core.RecordStream{
		Records: recordsChan,
		Errors:  errorsChan,
	}

	// Send records
	go func() {
		for _, record := range evolvedData {
			recordsChan <- record
		}
		close(recordsChan)
	}()

	// Write with automatic schema evolution
	if err := dest.Write(ctx, stream); err != nil {
		log.Printf("Failed to write data: %v", err)
		return
	}

	fmt.Println("Data written successfully - schema evolved automatically!")
	fmt.Println("New fields added: ip_address, user_agent, amount")
}

// advancedBigQueryExample demonstrates advanced schema evolution with BigQuery
func advancedBigQueryExample(ctx context.Context) {
	fmt.Println("\n=== Advanced Schema Evolution with BigQuery ===")

	// Configure BigQuery destination
	bigqueryConfig := config.NewBaseConfig("bigquery-evolution", "destination")
	// Configure BigQuery-specific settings
	bigqueryConfig.Security.Credentials["project_id"] = "my-project"
	bigqueryConfig.Security.Credentials["dataset_id"] = "my_dataset"
	bigqueryConfig.Security.Credentials["table_id"] = "evolved_table"
	bigqueryConfig.Security.Credentials["location"] = "US"

	// Create advanced evolution configuration
	evolutionConfig := &evolution.EvolutionConfig{
		Strategy:              "flexible", // Allow all changes
		CompatibilityMode:     schema.CompatibilityFull,
		EnableAutoEvolution:   true,
		SchemaCheckInterval:   0,
		BatchSizeForInference: 100,
		FailOnIncompatible:    false,
		PreserveOldFields:     true,
		CacheSchemaLocally:    true,
		MaxSchemaAgeMinutes:   30,
	}

	// Create BigQuery destination with evolution
	dest, err := evolution.CreateBigQueryWithEvolution("bigquery-evolved", bigqueryConfig, evolutionConfig)
	if err != nil {
		log.Fatal("Failed to create BigQuery destination:", err)
	}

	// Initialize destination
	if err := dest.Initialize(ctx, bigqueryConfig); err != nil {
		log.Fatal("Failed to initialize destination:", err)
	}
	defer func() {
		if err := dest.Close(ctx); err != nil {
			log.Printf("Failed to close destination: %v", err)
		}
	}()

	// Example of schema evolution with type changes
	fmt.Println("Demonstrating schema evolution with type changes...")

	// Initial schema with simple types
	v1Schema := &core.Schema{
		Name:    "analytics_events",
		Version: 1,
		Fields: []core.Field{
			{Name: "event_id", Type: core.FieldTypeString, Nullable: false},
			{Name: "user_id", Type: core.FieldTypeInt, Nullable: false}, // Will change to string
			{Name: "value", Type: core.FieldTypeInt, Nullable: true},    // Will change to float
		},
	}

	if err := dest.CreateSchema(ctx, v1Schema); err != nil {
		log.Printf("Failed to create v1 schema: %v", err)
		return
	}

	// Evolved data with type changes
	evolvedBatch := []*pool.Record{
		pool.NewRecord("analytics-evolution", map[string]interface{}{
			"event_id": "evt123",
			"user_id":  "user_abc", // Changed from int to string
			"value":    123.45,     // Changed from int to float
			"metadata": map[string]interface{}{ // New nested field
				"source":  "mobile_app",
				"version": "2.0",
			},
		}),
	}

	// Create channels for batch stream
	batchesChan := make(chan []*pool.Record, 1)
	errorsChan2 := make(chan error, 1)

	// Use batch write for bulk operations
	batchStream := &core.BatchStream{
		Batches: batchesChan,
		Errors:  errorsChan2,
	}

	go func() {
		batchesChan <- evolvedBatch
		close(batchesChan)
	}()

	if err := dest.WriteBatch(ctx, batchStream); err != nil {
		log.Printf("Failed to write batch: %v", err)
		return
	}

	fmt.Println("Schema evolved with type changes and nested fields!")
}

// customStrategyExample demonstrates custom evolution strategy
func customStrategyExample(ctx context.Context) {
	fmt.Println("\n=== Custom Schema Evolution Strategy ===")

	// Configure destination (using BigQuery as example)
	baseConfig := config.NewBaseConfig("custom-evolution", "destination")
	// Configure BigQuery-specific settings
	baseConfig.Security.Credentials["project_id"] = "my-project"
	baseConfig.Security.Credentials["dataset_id"] = "my_dataset"
	baseConfig.Security.Credentials["table_id"] = "custom_evolution_table"

	// Create destination
	baseDest, err := bigquery.NewBigQueryDestination("bigquery-base", baseConfig)
	if err != nil {
		log.Fatal("Failed to create base destination:", err)
	}

	// Create custom evolution config
	customEvolutionConfig := &evolution.EvolutionConfig{
		Strategy:              "strict", // Only allow safe changes
		CompatibilityMode:     schema.CompatibilityBackwardTransitive,
		EnableAutoEvolution:   true,
		SchemaCheckInterval:   0,
		BatchSizeForInference: 50,
		FailOnIncompatible:    true, // Fail if incompatible
		PreserveOldFields:     true,
	}

	// Wrap with evolution
	evolvedDest, err := evolution.WrapDestinationWithEvolution(baseDest, customEvolutionConfig)
	if err != nil {
		log.Fatal("Failed to wrap destination:", err)
	}

	if err := evolvedDest.Initialize(ctx, baseConfig); err != nil {
		log.Fatal("Failed to initialize:", err)
	}
	defer func() {
		if err := evolvedDest.Close(ctx); err != nil {
			log.Printf("Failed to close evolved destination: %v", err)
		}
	}()

	// Demonstrate strict evolution (only allows adding optional fields)
	baseSchema := &core.Schema{
		Name:    "strict_table",
		Version: 1,
		Fields: []core.Field{
			{Name: "id", Type: core.FieldTypeString, Nullable: false},
			{Name: "name", Type: core.FieldTypeString, Nullable: false},
		},
	}

	if err := evolvedDest.CreateSchema(ctx, baseSchema); err != nil {
		log.Printf("Failed to create base schema: %v", err)
		return
	}

	// Try to add optional field (should succeed)
	goodEvolution := &core.Schema{
		Name:    "strict_table",
		Version: 2,
		Fields: []core.Field{
			{Name: "id", Type: core.FieldTypeString, Nullable: false},
			{Name: "name", Type: core.FieldTypeString, Nullable: false},
			{Name: "email", Type: core.FieldTypeString, Nullable: true}, // Optional field - OK
		},
	}

	if err := evolvedDest.AlterSchema(ctx, baseSchema, goodEvolution); err != nil {
		log.Printf("Failed to evolve schema (expected with strict mode): %v", err)
	} else {
		fmt.Println("Successfully added optional field with strict evolution!")
	}

	// Try to add required field (should fail with strict mode)
	badEvolution := &core.Schema{
		Name:    "strict_table",
		Version: 3,
		Fields: []core.Field{
			{Name: "id", Type: core.FieldTypeString, Nullable: false},
			{Name: "name", Type: core.FieldTypeString, Nullable: false},
			{Name: "email", Type: core.FieldTypeString, Nullable: true},
			{Name: "phone", Type: core.FieldTypeString, Nullable: false}, // Required field - NOT OK
		},
	}

	if err := evolvedDest.AlterSchema(ctx, goodEvolution, badEvolution); err != nil {
		fmt.Printf("Expected failure: Cannot add required field in strict mode: %v\n", err)
	}

	// Get metrics
	metrics := evolvedDest.Metrics()
	fmt.Printf("\nEvolution Metrics:\n")
	fmt.Printf("- Schema Changes: %v\n", metrics["schema_changes"])
	fmt.Printf("- Evolution Failures: %v\n", metrics["evolution_failures"])
	fmt.Printf("- Records Processed: %v\n", metrics["records_processed"])
}
