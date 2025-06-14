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

func RunBigQueryExample() {
	ctx := context.Background()

	// Create BigQuery configuration using BaseConfig
	baseConfig := config.NewBaseConfig("bigquery-example", "destination")
	// Configure performance settings
	baseConfig.Performance.BatchSize = 10000
	baseConfig.Performance.Workers = 4
	baseConfig.Performance.MaxConcurrency = 10
	baseConfig.Performance.EnableStreaming = true
	baseConfig.Performance.AsyncOperations = true
	baseConfig.Performance.FlushInterval = 5 * time.Second
	// Configure timeouts
	baseConfig.Timeouts.Connection = 30 * time.Second
	baseConfig.Timeouts.Request = 60 * time.Second

	// Configure BigQuery-specific settings via credentials
	baseConfig.Security.Credentials["project_id"] = "your-project-id"
	baseConfig.Security.Credentials["dataset_id"] = "your_dataset"
	baseConfig.Security.Credentials["table_id"] = "test_table"
	baseConfig.Security.Credentials["location"] = "US"

	// Create BigQuery destination using the registry
	dest, err := registry.CreateDestination("bigquery", baseConfig)
	if err != nil {
		log.Fatalf("Failed to create BigQuery destination: %v", err)
	}

	// Initialize the destination
	if err := dest.Initialize(ctx, baseConfig); err != nil {
		log.Fatalf("Failed to initialize BigQuery destination: %v", err)
	}
	defer dest.Close(ctx)

	// Create schema
	schema := &core.Schema{
		Name: "test_table",
		Fields: []core.Field{
			{Name: "user_id", Type: core.FieldTypeString, Primary: true},
			{Name: "event_type", Type: core.FieldTypeString},
			{Name: "value", Type: core.FieldTypeFloat},
			{Name: "created_at", Type: core.FieldTypeTimestamp},
			{Name: "metadata", Type: core.FieldTypeJSON, Nullable: true},
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
				"user_id":     fmt.Sprintf("user-%d", i%10),
				"event_type":  []string{"click", "view", "purchase"}[i%3],
				"value":       float64(i) * 10.5,
				"created_at":  time.Now().Format(time.RFC3339),
				"metadata": map[string]interface{}{
					"source": "test",
					"batch":  i / 10,
				},
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

	// Write the stream to BigQuery
	if err := dest.Write(ctx, stream); err != nil {
		log.Fatalf("Failed to write to BigQuery: %v", err)
	}

	fmt.Println("Successfully wrote 100 records to BigQuery!")
	
	// Example of writing a batch
	batch := make([]*pool.Record, 50)
	for i := 0; i < 50; i++ {
		// Create record using pool constructor
		batch[i] = pool.NewRecord("batch-test", map[string]interface{}{
			"user_id":     fmt.Sprintf("batch-user-%d", i%5),
			"event_type":  "batch_event",
			"value":       float64(i) * 5.0,
			"created_at":  time.Now().Format(time.RFC3339),
		})
		// Set the record ID
		batch[i].ID = fmt.Sprintf("batch-record-%d", i)
		// Set timestamp in metadata
		batch[i].SetTimestamp(time.Now())
	}

	// Create batch stream
	batchChan := make(chan []*pool.Record, 1)
	batchStream := &core.BatchStream{
		Batches: batchChan,
		Errors:  errorChan,
	}
	
	// Send batch
	batchChan <- batch
	close(batchChan)
	
	// Write batch
	if err := dest.WriteBatch(ctx, batchStream); err != nil {
		log.Fatalf("Failed to write batch to BigQuery: %v", err)
	}
	
	fmt.Println("Successfully wrote batch of 50 records to BigQuery!")
}

// For running as standalone
func main() {
	RunBigQueryExample()
}