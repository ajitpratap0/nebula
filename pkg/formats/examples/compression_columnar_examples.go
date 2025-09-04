// Package examples demonstrates compression and columnar format usage
package examples

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ajitpratap0/nebula/pkg/compression"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/formats/columnar"
	"github.com/ajitpratap0/nebula/pkg/models"
)

// Example 1: Basic compression usage
func BasicCompressionExample() {
	fmt.Println("=== Basic Compression Example ===")

	// Sample data
	data := []byte("This is some sample data that we want to compress. " +
		"Repetitive data compresses better. Repetitive data compresses better.")

	// Try different algorithms
	algorithms := []compression.Algorithm{
		compression.Gzip,
		compression.Snappy,
		compression.LZ4,
		compression.Zstd,
	}

	for _, algo := range algorithms {
		config := &compression.Config{
			Algorithm: algo,
			Level:     compression.Default,
		}

		compressor, err := compression.NewCompressor(config)
		if err != nil {
			log.Printf("Failed to create %s compressor: %v", algo, err)
			continue
		}

		// Compress
		compressed, err := compressor.Compress(data)
		if err != nil {
			log.Printf("Failed to compress with %s: %v", algo, err)
			continue
		}

		// Decompress
		decompressed, err := compressor.Decompress(compressed)
		if err != nil {
			log.Printf("Failed to decompress with %s: %v", algo, err)
			continue
		}

		ratio := float64(len(data)) / float64(len(compressed))
		fmt.Printf("%s: %d -> %d bytes (%.2fx compression)\n",
			algo, len(data), len(compressed), ratio)

		// Verify
		if !bytes.Equal(data, decompressed) {
			log.Printf("Data mismatch for %s!", algo)
		}
	}
}

// Example 2: Streaming compression
func StreamingCompressionExample() {
	fmt.Println("\n=== Streaming Compression Example ===")

	// Create a large source
	source := bytes.NewReader(bytes.Repeat([]byte("Hello World! "), 10000))

	// Compress stream
	compressed := &bytes.Buffer{}
	config := &compression.Config{
		Algorithm: compression.Zstd,
		Level:     compression.Better,
	}

	compressor, err := compression.NewCompressor(config)
	if err != nil {
		log.Fatal(err)
	}

	err = compressor.CompressStream(compressed, source)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Compressed stream size: %d bytes\n", compressed.Len())

	// Decompress stream
	decompressed := &bytes.Buffer{}
	err = compressor.DecompressStream(decompressed, compressed)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Decompressed stream size: %d bytes\n", decompressed.Len())
}

// Example 3: Compression with pooling
func CompressionPoolExample() {
	fmt.Println("\n=== Compression Pool Example ===")

	// Create pool for better performance
	pool := compression.NewCompressorPool(&compression.Config{
		Algorithm: compression.Snappy,
		Level:     compression.Default,
	})

	// Simulate concurrent compression
	data := []byte("Data to compress")

	// Using pool
	compressed, err := pool.Compress(data)
	if err != nil {
		log.Fatal(err)
	}

	decompressed, err := pool.Decompress(compressed)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Pool compression successful: %d -> %d bytes\n",
		len(data), len(compressed))
	_ = decompressed
}

// Example 4: Basic columnar format usage
func BasicColumnarExample() {
	fmt.Println("\n=== Basic Columnar Format Example ===")

	// Define schema
	schema := &core.Schema{
		Name: "sales_data",
		Fields: []core.Field{
			{Name: "id", Type: core.FieldTypeInt, Nullable: false},
			{Name: "product", Type: core.FieldTypeString, Nullable: false},
			{Name: "quantity", Type: core.FieldTypeInt, Nullable: false},
			{Name: "price", Type: core.FieldTypeFloat, Nullable: false},
			{Name: "timestamp", Type: core.FieldTypeTimestamp, Nullable: false},
		},
	}

	// Create sample records
	records := []*models.Record{
		{
			Data: map[string]interface{}{
				"id":        1,
				"product":   "Widget A",
				"quantity":  10,
				"price":     99.99,
				"timestamp": time.Now(),
			},
		},
		{
			Data: map[string]interface{}{
				"id":        2,
				"product":   "Widget B",
				"quantity":  5,
				"price":     149.99,
				"timestamp": time.Now(),
			},
		},
	}

	// Write to Parquet
	buf := &bytes.Buffer{}
	writerConfig := &columnar.WriterConfig{
		Format:      columnar.Parquet,
		Schema:      schema,
		Compression: "snappy",
		BatchSize:   1000,
		EnableStats: true,
	}

	writer, err := columnar.NewWriter(buf, writerConfig)
	if err != nil {
		log.Fatal(err)
	}

	err = writer.WriteRecords(records) // Ignore write records error
	if err != nil {
		log.Fatal(err)
	}

	err = writer.Close() // Ignore close error
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Written %d records to Parquet (%d bytes)\n",
		writer.RecordsWritten(), buf.Len())

	// Read back
	readerConfig := &columnar.ReaderConfig{
		Format: columnar.Parquet,
	}

	reader, err := columnar.NewReader(bytes.NewReader(buf.Bytes()), readerConfig)
	if err != nil {
		log.Fatal(err)
	}

	readRecords, err := reader.ReadRecords()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Read %d records back from Parquet\n", len(readRecords))
	_ = reader.Close() // Error ignored for cleanup
}

// Example 5: Streaming columnar writes
func StreamingColumnarExample() {
	fmt.Println("\n=== Streaming Columnar Example ===")

	// Define schema
	schema := &core.Schema{
		Name: "events",
		Fields: []core.Field{
			{Name: "event_id", Type: core.FieldTypeString, Nullable: false},
			{Name: "user_id", Type: core.FieldTypeInt, Nullable: false},
			{Name: "event_type", Type: core.FieldTypeString, Nullable: false},
			{Name: "timestamp", Type: core.FieldTypeTimestamp, Nullable: false},
		},
	}

	// Create Arrow writer
	buf := &bytes.Buffer{}
	config := &columnar.WriterConfig{
		Format:      columnar.Arrow,
		Schema:      schema,
		Compression: "lz4",
		BatchSize:   100, // Small batches for streaming
	}

	writer, err := columnar.NewWriter(buf, config)
	if err != nil {
		log.Fatal(err)
	}

	// Stream records
	for i := 0; i < 1000; i++ {
		record := &models.Record{
			Data: map[string]interface{}{
				"event_id":   fmt.Sprintf("evt_%d", i),
				"user_id":    i % 100,
				"event_type": []string{"click", "view", "purchase"}[i%3],
				"timestamp":  time.Now().Add(-time.Duration(i) * time.Minute),
			},
		}

		err = writer.WriteRecord(record)
		if err != nil {
			log.Fatal(err)
		}

		// Simulate streaming - flush periodically
		if i%100 == 0 {
			_ = writer.Flush() // Error ignored for cleanup
		}
	}

	_ = writer.Close() // Error ignored for cleanup
	fmt.Printf("Streamed 1000 events to Arrow format (%d bytes)\n", buf.Len())
}

// Example 6: Columnar format with compression comparison
func ColumnarCompressionComparisonExample() {
	fmt.Println("\n=== Columnar Compression Comparison ===")

	// Create schema and data
	schema := &core.Schema{
		Name: "metrics",
		Fields: []core.Field{
			{Name: "metric_name", Type: core.FieldTypeString, Nullable: false},
			{Name: "value", Type: core.FieldTypeFloat, Nullable: false},
			{Name: "tags", Type: core.FieldTypeJSON, Nullable: true},
		},
	}

	// Generate many records with repetitive data
	records := make([]*models.Record, 10000)
	for i := 0; i < len(records); i++ {
		records[i] = &models.Record{
			Data: map[string]interface{}{
				"metric_name": fmt.Sprintf("cpu.usage.%d", i%10),
				"value":       float64(i%100) + 0.5,
				"tags":        `{"host": "server1", "region": "us-east"}`,
			},
		}
	}

	// Try different compressions
	compressions := []string{"none", "snappy", "gzip", "zstd"}

	for _, compression := range compressions {
		buf := &bytes.Buffer{}
		config := &columnar.WriterConfig{
			Format:      columnar.Parquet,
			Schema:      schema,
			Compression: compression,
			BatchSize:   1000,
		}

		writer, err := columnar.NewWriter(buf, config)
		if err != nil {
			log.Printf("Failed with %s: %v", compression, err)
			continue
		}

		start := time.Now()
		err = writer.WriteRecords(records) // Ignore write records error
		if err != nil {
			log.Fatal(err)
		}
		_ = writer.Close() // Error ignored for cleanup
		elapsed := time.Since(start)

		fmt.Printf("Parquet/%s: %d bytes, %.2fms\n",
			compression, buf.Len(), elapsed.Seconds()*1000)
	}
}

// Example 7: Compressed columnar data pipeline
func CompressedColumnarPipelineExample() {
	fmt.Println("\n=== Compressed Columnar Pipeline ===")

	// Step 1: Generate source data
	schema := &core.Schema{
		Name: "transactions",
		Fields: []core.Field{
			{Name: "tx_id", Type: core.FieldTypeString, Nullable: false},
			{Name: "amount", Type: core.FieldTypeFloat, Nullable: false},
			{Name: "currency", Type: core.FieldTypeString, Nullable: false},
			{Name: "timestamp", Type: core.FieldTypeTimestamp, Nullable: false},
		},
	}

	records := make([]*models.Record, 1000)
	for i := 0; i < len(records); i++ {
		records[i] = &models.Record{
			Data: map[string]interface{}{
				"tx_id":     fmt.Sprintf("TX%06d", i),
				"amount":    float64(i*100) + 0.99,
				"currency":  []string{"USD", "EUR", "GBP"}[i%3],
				"timestamp": time.Now().Add(-time.Duration(i) * time.Second),
			},
		}
	}

	// Step 2: Write to Parquet with compression
	parquetBuf := &bytes.Buffer{}
	parquetConfig := &columnar.WriterConfig{
		Format:            columnar.Parquet,
		Schema:            schema,
		Compression:       "zstd",
		BatchSize:         100,
		EnableStats:       true,
		EnableBloomFilter: true,
	}

	writer, err := columnar.NewWriter(parquetBuf, parquetConfig)
	if err != nil {
		log.Fatal(err)
	}

	err = writer.WriteRecords(records) // Ignore write records error
	if err != nil {
		log.Fatal(err)
	}
	_ = writer.Close() // Error ignored for cleanup

	// Step 3: Further compress the Parquet file
	compressor, err := compression.NewCompressor(&compression.Config{
		Algorithm: compression.Zstd,
		Level:     compression.Best,
	})
	if err != nil {
		log.Fatal(err)
	}

	compressed, err := compressor.Compress(parquetBuf.Bytes())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Pipeline results:\n")
	fmt.Printf("  Original records: %d\n", len(records))
	fmt.Printf("  Parquet size: %d bytes\n", parquetBuf.Len())
	fmt.Printf("  Double compressed: %d bytes\n", len(compressed))
	fmt.Printf("  Total compression: %.2fx\n",
		float64(len(records)*200)/float64(len(compressed)))

	// Step 4: Read back through pipeline
	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		log.Fatal(err)
	}

	reader, err := columnar.NewReader(bytes.NewReader(decompressed),
		&columnar.ReaderConfig{Format: columnar.Parquet})
	if err != nil {
		log.Fatal(err)
	}

	readBack, err := reader.ReadRecords()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("  Records read back: %d\n", len(readBack))
}

// Example 8: File-based columnar operations
func FileBasedColumnarExample() {
	fmt.Println("\n=== File-Based Columnar Example ===")

	// Write Parquet file
	filename := "/tmp/data.parquet"

	schema := &core.Schema{
		Name: "users",
		Fields: []core.Field{
			{Name: "user_id", Type: core.FieldTypeInt, Nullable: false},
			{Name: "username", Type: core.FieldTypeString, Nullable: false},
			{Name: "email", Type: core.FieldTypeString, Nullable: false},
			{Name: "created", Type: core.FieldTypeTimestamp, Nullable: false},
		},
	}

	// Create file
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}

	// Write data
	writer, err := columnar.NewWriter(file, &columnar.WriterConfig{
		Format:      columnar.Parquet,
		Schema:      schema,
		Compression: "snappy",
		BatchSize:   1000,
	})
	if err != nil {
		_ = file.Close() // Error ignored for cleanup
		log.Fatal(err)
	}

	// Write some records
	for i := 0; i < 100; i++ {
		record := &models.Record{
			Data: map[string]interface{}{
				"user_id":  i,
				"username": fmt.Sprintf("user%d", i),
				"email":    fmt.Sprintf("user%d@example.com", i),
				"created":  time.Now(),
			},
		}
		_ = writer.WriteRecord(record) // Error ignored for example
	}

	_ = writer.Close() // Error ignored for cleanup
	_ = file.Close()   // Error ignored for cleanup

	// Read file info
	info, err := os.Stat(filename)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Created Parquet file: %s (%d bytes)\n", filename, info.Size())

	// Read back with projection
	readFile, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = readFile.Close() }() // Ignore close error

	reader, err := columnar.NewReader(readFile, &columnar.ReaderConfig{
		Format:     columnar.Parquet,
		Projection: []string{"user_id", "username"}, // Only read some columns
	})
	if err != nil {
		log.Fatal(err)
	}

	count := 0
	for reader.HasNext() {
		record, err := reader.Next()
		if err != nil {
			break
		}
		if record != nil {
			count++
		}
	}

	fmt.Printf("Read %d records with projection\n", count)
	_ = reader.Close() // Error ignored for cleanup

	// Cleanup
	_ = os.Remove(filename) // Best effort cleanup
}

// Main function to run all examples
func RunCompressionColumnarExamples() {
	BasicCompressionExample()
	StreamingCompressionExample()
	CompressionPoolExample()
	BasicColumnarExample()
	StreamingColumnarExample()
	ColumnarCompressionComparisonExample()
	CompressedColumnarPipelineExample()
	FileBasedColumnarExample()
}
