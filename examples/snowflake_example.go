//go:build ignore
// +build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
	"github.com/ajitpratap0/nebula/pkg/pool"
)

func RunSnowflakeExample() {
	ctx := context.Background()

	// Create Snowflake configuration using BaseConfig
	baseConfig := config.NewBaseConfig("snowflake-example", "destination")
	// Configure performance settings
	baseConfig.Performance.BatchSize = 10000
	baseConfig.Performance.Workers = 4
	baseConfig.Performance.MaxConcurrency = 8
	baseConfig.Performance.AsyncOperations = true
	baseConfig.Performance.FlushInterval = 5 * time.Second
	// Configure timeouts
	baseConfig.Timeouts.Connection = 30 * time.Second
	baseConfig.Timeouts.Request = 60 * time.Second

	// Configure Snowflake-specific settings via properties
	// In a real implementation, we'd add these to security.credentials
	// For now, using properties approach for the connector factory

	// Create Snowflake destination using the registry
	dest, err := registry.CreateDestination("snowflake", baseConfig)
	if err != nil {
		log.Fatalf("Failed to create Snowflake destination: %v", err)
	}

	// Initialize the destination
	if err := dest.Initialize(ctx, baseConfig); err != nil {
		log.Fatalf("Failed to initialize Snowflake destination: %v", err)
	}
	defer dest.Close(ctx)

	// Create schema
	schema := &core.Schema{
		Name: "test_table",
		Fields: []core.Field{
			{Name: "id", Type: core.FieldTypeInt, Primary: true},
			{Name: "name", Type: core.FieldTypeString},
			{Name: "email", Type: core.FieldTypeString},
			{Name: "created_at", Type: core.FieldTypeTimestamp},
		},
	}

	if err := dest.CreateSchema(ctx, schema); err != nil {
		log.Fatalf("Failed to create schema: %v", err)
	}

	// Create a record stream
	recordChan := make(chan *pool.Record, 100)
	errorChan := make(chan error, 1)
	stream := &core.RecordStream{
		Records: recordChan,
		Errors:  errorChan,
	}

	// Start writing records in a goroutine
	go func() {
		defer close(recordChan)

		// Send some test records using the unified record system
		for i := 1; i <= 100; i++ {
			// Create record using pool constructor
			record := pool.NewRecord("test", map[string]interface{}{
				"id":         i,
				"name":       fmt.Sprintf("User %d", i),
				"email":      fmt.Sprintf("user%d@example.com", i),
				"created_at": time.Now().Format(time.RFC3339),
			})
			// Set the record ID
			record.ID = fmt.Sprintf("record-%d", i)
			// Set timestamp in metadata
			record.SetTimestamp(time.Now())

			select {
			case recordChan <- record:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Write the stream to Snowflake
	if err := dest.Write(ctx, stream); err != nil {
		log.Fatalf("Failed to write to Snowflake: %v", err)
	}

	fmt.Println("Successfully wrote 100 records to Snowflake!")
}

// For running as standalone
func main() {
	RunSnowflakeExample()
}
