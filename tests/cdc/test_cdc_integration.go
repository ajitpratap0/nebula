package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/mongodb_cdc"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/mysql_cdc"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/postgresql_cdc"
)

func main() {
	ctx := context.Background()

	// Test PostgreSQL CDC integration
	fmt.Println("Testing PostgreSQL CDC Source...")
	testPostgreSQLCDC(ctx)

	// Test MySQL CDC integration
	fmt.Println("\nTesting MySQL CDC Source...")
	testMySQLCDC(ctx)

	// Test MongoDB CDC integration
	fmt.Println("\nTesting MongoDB CDC Source...")
	testMongoDBCDC(ctx)

	fmt.Println("\n✨ CDC integration test completed!")
}

func testPostgreSQLCDC(ctx context.Context) {
	cfg := config.NewBaseConfig("postgresql-cdc-test", "source")
	cfg.Version = "1.0.0"

	// Configure performance settings
	cfg.Performance.BatchSize = 1000
	cfg.Performance.BufferSize = 10000

	// Configure database connection in credentials
	cfg.Security.Credentials = map[string]string{
		// NOTE: These are example values - replace with your actual database
		"connection_string": "postgres://user:password@localhost:5432/testdb?sslmode=disable",
		"database":          "testdb",
		"tables":            "users,orders", // Comma-separated string
		"slot_name":         "nebula_slot",
		"publication":       "nebula_pub",
	}

	// Create PostgreSQL CDC source
	source, err := registry.GetRegistry().CreateSource("postgresql-cdc", cfg)
	if err != nil {
		log.Printf("Note: PostgreSQL CDC requires a running PostgreSQL instance with logical replication enabled")
		log.Printf("Error creating PostgreSQL CDC source: %v", err)
		return
	}

	// Initialize the source
	if err := source.Initialize(ctx, cfg); err != nil {
		log.Printf("Error initializing PostgreSQL CDC source: %v", err)
		log.Printf("Make sure PostgreSQL is configured with:")
		log.Printf("  - wal_level = logical")
		log.Printf("  - max_replication_slots >= 1")
		log.Printf("  - max_wal_senders >= 1")
		return
	}

	// Discover schema
	schema, err := source.Discover(ctx)
	if err != nil {
		log.Printf("Error discovering schema: %v", err)
		return
	}

	fmt.Printf("✅ PostgreSQL CDC source initialized successfully!\n")
	fmt.Printf("   Schema: %s\n", schema.Name)
	fmt.Printf("   Fields: %d\n", len(schema.Fields))
	fmt.Printf("   Supports Incremental: %v\n", source.SupportsIncremental())
	fmt.Printf("   Supports Realtime: %v\n", source.SupportsRealtime())

	// Clean up
	_ = source.Close(ctx)
}

func testMySQLCDC(ctx context.Context) {
	cfg := config.NewBaseConfig("mysql-cdc-test", "source")
	cfg.Version = "1.0.0"

	// Configure performance settings
	cfg.Performance.BatchSize = 1000
	cfg.Performance.BufferSize = 10000

	// Configure database connection in credentials
	cfg.Security.Credentials = map[string]string{
		// NOTE: These are example values - replace with your actual database
		"connection_string": "user:password@tcp(localhost:3306)/testdb",
		"database":          "testdb",
		"tables":            "users,orders", // Comma-separated string
		"server_id":         "999",          // Unique server ID for replication
	}

	// Create MySQL CDC source
	source, err := registry.GetRegistry().CreateSource("mysql-cdc", cfg)
	if err != nil {
		log.Printf("Note: MySQL CDC requires a running MySQL instance with binary logging enabled")
		log.Printf("Error creating MySQL CDC source: %v", err)
		return
	}

	// Initialize the source
	if err := source.Initialize(ctx, cfg); err != nil {
		log.Printf("Error initializing MySQL CDC source: %v", err)
		log.Printf("Make sure MySQL is configured with:")
		log.Printf("  - log_bin = ON")
		log.Printf("  - binlog_format = ROW")
		log.Printf("  - binlog_row_image = FULL")
		return
	}

	// Discover schema
	schema, err := source.Discover(ctx)
	if err != nil {
		log.Printf("Error discovering schema: %v", err)
		return
	}

	fmt.Printf("✅ MySQL CDC source initialized successfully!\n")
	fmt.Printf("   Schema: %s\n", schema.Name)
	fmt.Printf("   Fields: %d\n", len(schema.Fields))
	fmt.Printf("   Supports Incremental: %v\n", source.SupportsIncremental())
	fmt.Printf("   Supports Realtime: %v\n", source.SupportsRealtime())

	// Clean up
	_ = source.Close(ctx)
}

func testMongoDBCDC(ctx context.Context) {
	cfg := config.NewBaseConfig("mongodb-cdc-test", "source")
	cfg.Version = "1.0.0"

	// Configure performance settings
	cfg.Performance.BatchSize = 1000
	cfg.Performance.BufferSize = 10000

	// Configure database connection in credentials
	cfg.Security.Credentials = map[string]string{
		// NOTE: These are example values - replace with your actual database
		"connection_string":           "mongodb://user:password@localhost:27017",
		"database":                    "testdb",
		"collections":                 "users,orders", // Comma-separated string
		"full_document":               "updateLookup",
		"full_document_before_change": "whenAvailable",
	}

	// Create MongoDB CDC source
	source, err := registry.GetRegistry().CreateSource("mongodb-cdc", cfg)
	if err != nil {
		log.Printf("Note: MongoDB CDC requires a running MongoDB replica set or sharded cluster")
		log.Printf("Error creating MongoDB CDC source: %v", err)
		return
	}

	// Initialize the source
	if err := source.Initialize(ctx, cfg); err != nil {
		log.Printf("Error initializing MongoDB CDC source: %v", err)
		log.Printf("Make sure MongoDB is configured as:")
		log.Printf("  - Replica set or sharded cluster")
		log.Printf("  - Change streams are supported (MongoDB 3.6+)")
		return
	}

	// Discover schema
	schema, err := source.Discover(ctx)
	if err != nil {
		log.Printf("Error discovering schema: %v", err)
		return
	}

	fmt.Printf("✅ MongoDB CDC source initialized successfully!\n")
	fmt.Printf("   Schema: %s\n", schema.Name)
	fmt.Printf("   Fields: %d\n", len(schema.Fields))
	fmt.Printf("   Supports Incremental: %v\n", source.SupportsIncremental())
	fmt.Printf("   Supports Realtime: %v\n", source.SupportsRealtime())

	// Clean up
	_ = source.Close(ctx)
}

// Example: Reading CDC events
func readCDCEvents(ctx context.Context, source core.Source) { //nolint:unused // Example function for demonstration purposes
	// Start reading changes
	stream, err := source.Read(ctx)
	if err != nil {
		log.Printf("Error starting CDC stream: %v", err)
		return
	}

	// Read events for 10 seconds
	timeout := time.After(10 * time.Second)
	eventCount := 0

	fmt.Println("Reading CDC events for 10 seconds...")

	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				fmt.Println("Stream closed")
				return
			}

			eventCount++
			fmt.Printf("Event %d: %s (table: %s)\n",
				eventCount,
				record.Metadata.Operation,
				record.Metadata.Table)

		case err := <-stream.Errors:
			log.Printf("Error in stream: %v", err)

		case <-timeout:
			fmt.Printf("Timeout reached. Received %d events\n", eventCount)
			return

		case <-ctx.Done():
			return
		}
	}
}
