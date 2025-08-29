// Package pool provides example usage of the unified memory pool system.
package pool_test

import (
	"fmt"
	"sync"

	"github.com/ajitpratap0/nebula/pkg/pool"
)

// Example demonstrates basic usage of the unified record pool system.
// This shows how to get records from the pool and return them after use.
func Example() {
	// Get a record from the pool
	record := pool.GetRecord()
	defer record.Release() // Always release records when done

	// Set data on the record
	record.SetData("name", "John Doe")
	record.SetData("age", 30)
	record.SetData("email", "john@example.com")

	// Access data
	if name, ok := record.Data["name"]; ok {
		fmt.Printf("Name: %v\n", name)
	}

	// Set metadata
	record.Metadata.Source = "user_api"
	record.SetMetadata("request_id", "123456")

	// Output:
	// Name: John Doe
}

// ExampleGetRecord shows how to safely use records from the pool.
func ExampleGetRecord() {
	// Get a record from the pool
	record := pool.GetRecord()

	// Always use defer to ensure the record is released
	defer record.Release()

	// Use the record
	record.SetData("product", "Nebula")
	record.SetData("version", "v0.3.0")

	fmt.Printf("Product: %v\n", record.Data["product"])

	// Output:
	// Product: Nebula
}

// ExampleNewRecord demonstrates creating records with initial data.
func ExampleNewRecord() {
	// Create initial data
	data := map[string]interface{}{
		"id":     12345,
		"status": "active",
		"score":  98.5,
	}

	// Create a new record with source and data
	record := pool.NewRecord("analytics", data)
	defer record.Release()

	// The record is pre-populated
	fmt.Printf("Source: %s\n", record.Metadata.Source)
	fmt.Printf("Status: %v\n", record.Data["status"])

	// Output:
	// Source: analytics
	// Status: active
}

// ExampleNewCDCRecord shows how to create Change Data Capture records.
func ExampleNewCDCRecord() {
	// Data before the change
	before := map[string]interface{}{
		"id":    1,
		"name":  "Old Name",
		"email": "old@example.com",
	}

	// Data after the change
	after := map[string]interface{}{
		"id":    1,
		"name":  "New Name",
		"email": "new@example.com",
	}

	// Create a CDC record for an UPDATE operation
	record := pool.NewCDCRecord("postgres", "users", "UPDATE", before, after)
	defer record.Release()

	// Access CDC-specific fields
	fmt.Printf("Operation: %s\n", record.GetCDCOperation())
	fmt.Printf("Table: %s\n", record.GetCDCTable())
	fmt.Printf("Changed fields: name, email\n")

	// Output:
	// Operation: UPDATE
	// Table: users
	// Changed fields: name, email
}

// ExampleNew demonstrates creating and using a generic pool.
func ExampleNew() {
	// Define a simple struct to pool
	type Buffer struct {
		data []byte
	}

	// Create a pool for Buffer objects
	bufferPool := pool.New(
		func() *Buffer {
			return &Buffer{
				data: make([]byte, 0, 1024), // Pre-allocate 1KB
			}
		},
		func(b *Buffer) {
			b.data = b.data[:0] // Reset the buffer
		},
	)

	// Get a buffer from the pool
	buf := bufferPool.Get()
	defer bufferPool.Put(buf)

	// Use the buffer
	buf.data = append(buf.data, []byte("Hello, Nebula!")...)
	fmt.Printf("Buffer contains: %s\n", string(buf.data))

	// Output:
	// Buffer contains: Hello, Nebula!
}

// Example_concurrentUsage demonstrates thread-safe pool usage.
func Example_concurrentUsage() {
	var wg sync.WaitGroup
	recordCount := 0
	var mu sync.Mutex

	// Process records concurrently
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Get record from pool
			record := pool.GetRecord()
			defer record.Release()

			// Simulate processing
			record.SetData("worker_id", id)
			record.SetData("processed", true)

			// Count processed records (thread-safe)
			mu.Lock()
			recordCount++
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	fmt.Printf("Processed %d records concurrently\n", recordCount)

	// Output:
	// Processed 3 records concurrently
}

// Example_batchProcessing shows efficient batch processing with pools.
func Example_batchProcessing() {
	// Process a batch of records
	batchSize := 5
	records := make([]*pool.Record, 0, batchSize)

	// Get records for the batch
	for i := 0; i < batchSize; i++ {
		record := pool.GetRecord()
		record.SetData("index", i)
		record.SetData("batch_id", "batch_001")
		records = append(records, record)
	}

	// Process the batch
	fmt.Printf("Processing batch of %d records\n", len(records))

	// Release all records after processing
	for _, record := range records {
		record.Release()
	}

	fmt.Println("Batch processing complete")

	// Output:
	// Processing batch of 5 records
	// Batch processing complete
}

// ExampleGetMap demonstrates using the global map pool.
func ExampleGetMap() {
	// Get a map from the pool
	m := pool.GetMap()
	defer pool.PutMap(m)

	// Use the map
	m["key1"] = "value1"
	m["key2"] = "value2"

	fmt.Printf("Map size: %d\n", len(m))

	// Output:
	// Map size: 2
}

// ExampleGetStringSlice shows string slice pool usage.
func ExampleGetStringSlice() {
	// Get a string slice from the pool
	slice := pool.GetStringSlice()
	defer pool.PutStringSlice(slice)

	// Append strings
	slice = append(slice, "apple", "banana", "cherry")

	fmt.Printf("Fruits: %v\n", slice)

	// Output:
	// Fruits: [apple banana cherry]
}

// ExampleGetByteSlice demonstrates byte slice pool usage for I/O operations.
func ExampleGetByteSlice() {
	// Get a byte slice from the pool (default 1KB)
	buffer := pool.GetByteSlice()
	defer pool.PutByteSlice(buffer)

	// Use the buffer for data
	data := []byte("High-performance data processing with Nebula")
	buffer = append(buffer, data...)

	fmt.Printf("Buffer content: %s\n", string(buffer))

	// Output:
	// Buffer content: High-performance data processing with Nebula
}
