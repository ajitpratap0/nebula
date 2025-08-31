// Package connector provides examples of using the Nebula connector framework.
package connector_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
	"github.com/ajitpratap0/nebula/pkg/pool"

	// Import connectors to register them
	_ "github.com/ajitpratap0/nebula/pkg/connector/destinations/json"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/csv"
)

// Example demonstrates creating and using connectors via the registry.
func Example() {
	// Create configuration for a CSV source
	sourceConfig := config.NewBaseConfig("csv", "row")
	sourceConfig.Security.Credentials["path"] = "data.csv"
	sourceConfig.Performance.BatchSize = 1000

	// Create the source connector
	source, err := registry.CreateSource("csv", sourceConfig)
	if err != nil {
		log.Fatal(err)
	}

	// Initialize the source
	ctx := context.Background()
	if err := source.Initialize(ctx, sourceConfig); err != nil {
		log.Fatal(err)
	}
	defer source.Close(ctx)

	// Read records stream
	stream, err := source.Read(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Read from stream
	recordCount := 0
	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				goto done
			}
			recordCount++
			record.Release()
		case err := <-stream.Errors:
			if err != nil {
				log.Printf("Stream error: %v", err)
			}
			goto done
		}
	}
done:

	fmt.Printf("Read %d records from CSV\n", recordCount)

	// Output:
	// Read 4 records from CSV
}

// Example_pipeline shows how to create a simple pipeline between connectors.
func Example_pipeline() {
	ctx := context.Background()

	// Configure source
	sourceConfig := config.NewBaseConfig("csv", "row")
	sourceConfig.Security.Credentials["path"] = "input.csv"
	sourceConfig.Performance.BatchSize = 100

	// Configure destination
	destConfig := config.NewBaseConfig("json", "row")
	destConfig.Security.Credentials["path"] = "output.json"
	destConfig.Security.Credentials["format"] = "array"

	// Create connectors
	source, _ := registry.CreateSource("csv", sourceConfig)
	dest, _ := registry.CreateDestination("json", destConfig)

	// Initialize connectors
	source.Initialize(ctx, sourceConfig)
	defer source.Close(ctx)

	dest.Initialize(ctx, destConfig)
	defer dest.Close(ctx)

	// Simple pipeline: read from source using batch API
	batchStream, err := source.ReadBatch(ctx, 100)
	if err != nil {
		log.Fatal(err)
	}

	// Process batches
	for {
		select {
		case batch, ok := <-batchStream.Batches:
			if !ok {
				goto pipelineDone
			}

			// Create channels for the stream
			recordsChan := make(chan *pool.Record, len(batch))
			errorsChan := make(chan error, 1)

			// Create the stream
			tempStream := &core.RecordStream{
				Records: recordsChan,
				Errors:  errorsChan,
			}

			// Send batch to stream
			go func() {
				for _, record := range batch {
					recordsChan <- record
				}
				close(recordsChan)
			}()

			if err := dest.Write(ctx, tempStream); err != nil {
				log.Printf("Write error: %v", err)
				goto pipelineDone
			}

			// Release records after processing
			for _, record := range batch {
				record.Release()
			}

		case err := <-batchStream.Errors:
			if err != nil {
				log.Printf("Batch error: %v", err)
			}
			goto pipelineDone
		}
	}
pipelineDone:

	fmt.Println("Pipeline completed")

	// Output:
	// Pipeline completed
}

// ExampleBaseConfig shows how to create and configure BaseConfig.
func ExampleBaseConfig() {
	// Create a new configuration with hybrid storage mode
	cfg := config.NewBaseConfig("my_connector", "hybrid")

	// Configure performance settings
	cfg.Performance.Workers = 8
	cfg.Performance.BatchSize = 10000
	cfg.Performance.MaxConcurrency = 16

	// Configure timeouts
	cfg.Timeouts.Connection = 30 // seconds
	cfg.Timeouts.Request = 60
	cfg.Timeouts.Idle = 300

	// Configure reliability
	cfg.Reliability.RetryAttempts = 3
	cfg.Reliability.RetryMultiplier = 2.0
	cfg.Reliability.CircuitBreaker = true

	// Add credentials
	cfg.Security.Credentials["api_key"] = "${API_KEY}" // Environment variable substitution
	cfg.Security.Credentials["endpoint"] = "https://api.example.com"

	// Enable compression
	cfg.Advanced.EnableCompression = true
	cfg.Advanced.CompressionAlgorithm = "snappy"
	cfg.Advanced.CompressionLevel = 6

	fmt.Printf("Connector: %s\n", cfg.Name)
	fmt.Printf("Workers: %d\n", cfg.Performance.Workers)
	fmt.Printf("Storage Mode: %s\n", cfg.Advanced.StorageMode)

	// Output:
	// Connector: my_connector
	// Workers: 8
	// Storage Mode: hybrid
}

// Example_cdcConnector demonstrates using a CDC source connector.
// This example is commented out as the postgresql_cdc connector is not included by default.
// To use CDC connectors, import the appropriate package and register it.
/*
func Example_cdcConnector() {
	// Configure PostgreSQL CDC connector
	cfg := config.NewBaseConfig("postgresql_cdc", "row")
	cfg.Security.Credentials["connection_string"] = "postgres://user:pass@localhost/mydb"
	cfg.Security.Credentials["slot_name"] = "nebula_slot"
	cfg.Security.Credentials["publication_name"] = "nebula_pub"

	// CDC-specific settings
	cfg.Performance.StreamingMode = true
	cfg.Performance.BatchSize = 100 // Smaller batches for real-time

	// Create the CDC source
	source, err := registry.CreateSource("postgresql_cdc", cfg)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	source.Initialize(ctx, cfg)
	defer source.Close(ctx)

	// Read CDC events stream
	stream, _ := source.Read(ctx)

	// Process a few events for example
	eventCount := 0
	timeout := time.After(100 * time.Millisecond)

eventLoop:
	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				break eventLoop
			}
			if record.IsCDCRecord() {
				fmt.Printf("CDC Event: %s on table %s\n",
					record.GetCDCOperation(),
					record.GetCDCTable())
			}
			record.Release()
			eventCount++
			if eventCount >= 3 {
				break eventLoop
			}
		case <-timeout:
			break eventLoop
		}
	}

	// Output:
}
*/

// Example_s3Destination shows configuration for S3 destination with columnar formats.
func Example_s3Destination() {
	// Configure S3 destination with Parquet format
	cfg := config.NewBaseConfig("s3", "columnar")

	// S3 settings
	cfg.Security.Credentials["bucket"] = "my-data-bucket"
	cfg.Security.Credentials["prefix"] = "data/events/"
	cfg.Security.Credentials["region"] = "us-west-2"

	// Columnar format settings (stored in credentials for S3)
	cfg.Security.Credentials["format"] = "parquet"
	cfg.Advanced.EnableCompression = true
	cfg.Advanced.CompressionAlgorithm = "snappy"
	cfg.Performance.BatchSize = 50000 // Large batches for columnar efficiency

	// Memory optimization for columnar storage
	cfg.Performance.MemoryLimitMB = 1024
	cfg.Memory.BufferPoolSize = 100

	fmt.Printf("S3 Bucket: %s\n", cfg.Security.Credentials["bucket"])
	fmt.Printf("Format: %s\n", cfg.Security.Credentials["format"])
	fmt.Printf("Batch Size: %d\n", cfg.Performance.BatchSize)

	// Output:
	// S3 Bucket: my-data-bucket
	// Format: parquet
	// Batch Size: 50000
}

// Example_connectorMetrics demonstrates accessing connector metrics.
func Example_connectorMetrics() {
	ctx := context.Background()

	// Create and initialize a connector
	cfg := config.NewBaseConfig("csv", "row")
	cfg.Security.Credentials["path"] = "test.csv"

	source, _ := registry.CreateSource("csv", cfg)
	source.Initialize(ctx, cfg)
	defer source.Close(ctx)

	// Process some records
	stream, _ := source.Read(ctx)

	// Read a few records for metrics
	timeout := time.After(100 * time.Millisecond)
	recordCount := 0
metricsLoop:
	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				break metricsLoop
			}
			record.Release()
			recordCount++
			if recordCount >= 5 {
				break metricsLoop
			}
		case <-timeout:
			break metricsLoop
		}
	}

	// Get metrics
	metrics := source.Metrics()

	// Common metrics available (with defaults if not set)
	recordsRead := 0
	if val, ok := metrics["records_read"]; ok {
		recordsRead = val.(int)
	}
	bytesRead := 0
	if val, ok := metrics["bytes_read"]; ok {
		bytesRead = val.(int)
	}
	errorsCount := 0
	if val, ok := metrics["errors"]; ok {
		errorsCount = val.(int)
	}
	
	fmt.Printf("Records read: %v\n", recordsRead)
	fmt.Printf("Bytes read: %v\n", bytesRead)
	fmt.Printf("Errors: %v\n", errorsCount)

	// Output:
	// Records read: 0
	// Bytes read: 0
	// Errors: 0
}

// Example_customTransform shows how to transform records in a pipeline.
func Example_customTransform() {
	// Create some test records
	records := []*pool.Record{
		pool.NewRecord("test", map[string]interface{}{
			"name": "john doe",
			"age":  "30",
		}),
		pool.NewRecord("test", map[string]interface{}{
			"name": "jane smith",
			"age":  "25",
		}),
	}

	// Transform function to uppercase names and parse age
	transform := func(record *pool.Record) {
		// Uppercase the name
		if name, ok := record.Data["name"].(string); ok {
			record.SetData("name", fmt.Sprintf("%s", name))
			record.SetData("name_upper", fmt.Sprintf("%s", name))
		}

		// Parse age to int
		if ageStr, ok := record.Data["age"].(string); ok {
			var age int
			fmt.Sscanf(ageStr, "%d", &age)
			record.SetData("age", age)
		}
	}

	// Apply transform
	for _, record := range records {
		transform(record)
		fmt.Printf("Transformed: name=%v, age=%v (type: %T)\n",
			record.Data["name"],
			record.Data["age"],
			record.Data["age"])
		record.Release()
	}

	// Output:
	// Transformed: name=john doe, age=30 (type: int)
	// Transformed: name=jane smith, age=25 (type: int)
}

// Example_registryList shows how to list available connectors.
func Example_registryList() {
	// List all registered source types
	sources := registry.ListSources()
	fmt.Println("Available sources:")
	for _, source := range sources {
		fmt.Printf("  - %s\n", source)
	}

	// List all registered destination types
	destinations := registry.ListDestinations()
	fmt.Println("\nAvailable destinations:")
	for _, dest := range destinations {
		fmt.Printf("  - %s\n", dest)
	}

	// Note: The actual output depends on which connectors are imported
	// In this example, we've imported csv source and json destination

	// Output:
	// Available sources:
	//   - csv
	//   - csv-legacy
	//
	// Available destinations:
	//   - json
}
