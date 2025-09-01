// Package columnar provides columnar format benchmarks
package columnar

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// Generate test schema
func generateTestSchema() *core.Schema {
	return &core.Schema{
		Name: "benchmark_schema",
		Fields: []core.Field{
			{Name: "id", Type: core.FieldTypeInt},
			{Name: "name", Type: core.FieldTypeString},
			{Name: "email", Type: core.FieldTypeString},
			{Name: "age", Type: core.FieldTypeInt},
			{Name: "score", Type: core.FieldTypeFloat},
			{Name: "active", Type: core.FieldTypeBool},
			{Name: "created_at", Type: core.FieldTypeTimestamp},
			{Name: "metadata", Type: core.FieldTypeJSON},
		},
	}
}

// Generate test records
func generateTestRecords(count int) []*pool.Record {
	records := make([]*pool.Record, count)
	for i := 0; i < count; i++ {
		record := pool.GetRecord()
		record.SetData("id", i)
		record.SetData("name", fmt.Sprintf("User %d", i))
		record.SetData("email", fmt.Sprintf("user%d@example.com", i))
		record.SetData("age", rand.Intn(80)+20)
		record.SetData("score", rand.Float64()*100)
		record.SetData("active", rand.Intn(2) == 1)
		record.SetData("created_at", time.Now().Add(-time.Duration(rand.Intn(365*24))*time.Hour))
		record.SetData("metadata", `{"key1": "value1", "key2": "value2"}`)
		records[i] = record
	}
	return records
}

// Benchmark write performance
func BenchmarkColumnarWrite(b *testing.B) {
	formats := []Format{
		Parquet,
		// ORC,   // Stub implementation
		Arrow,
		// Avro,  // Requires goavro dependency
	}

	recordCounts := []int{
		1000,
		10000,
		100000,
	}

	compressions := []string{
		"none",
		"snappy",
		"gzip",
		"zstd",
	}

	schema := generateTestSchema()

	for _, format := range formats {
		for _, count := range recordCounts {
			for _, compression := range compressions {
				// Skip unsupported combinations
				if format == Arrow && compression == "zstd" {
					continue
				}

				records := generateTestRecords(count)

				b.Run(fmt.Sprintf("%s/%d/%s", format, count, compression), func(b *testing.B) {
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						var buf bytes.Buffer
						config := &WriterConfig{
							Format:      format,
							Schema:      schema,
							Compression: compression,
							BatchSize:   1000,
							PageSize:    8192,
							EnableStats: true,
						}

						writer, err := NewWriter(&buf, config)
						if err != nil {
							b.Fatal(err)
						}

						err = writer.WriteRecords(records) // Ignore write records error
						if err != nil {
							b.Fatal(err)
						}

						err = writer.Close() // Ignore close error
						if err != nil {
							b.Fatal(err)
						}

						b.SetBytes(int64(buf.Len()))
					}
				})
			}
		}
	}
}

// Benchmark read performance
func BenchmarkColumnarRead(b *testing.B) {
	formats := []Format{
		Parquet,
		Arrow,
	}

	recordCount := 10000
	compression := "snappy"
	schema := generateTestSchema()
	records := generateTestRecords(recordCount)

	// Pre-generate files for each format
	files := make(map[Format][]byte)
	for _, format := range formats {
		var buf bytes.Buffer
		config := &WriterConfig{
			Format:      format,
			Schema:      schema,
			Compression: compression,
			BatchSize:   1000,
			PageSize:    8192,
			EnableStats: true,
		}

		writer, err := NewWriter(&buf, config)
		if err != nil {
			b.Fatal(err)
		}

		err = writer.WriteRecords(records) // Ignore write records error
		if err != nil {
			b.Fatal(err)
		}

		err = writer.Close() // Ignore close error
		if err != nil {
			b.Fatal(err)
		}

		files[format] = buf.Bytes()
	}

	for _, format := range formats {
		data := files[format]

		b.Run(string(format), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				config := &ReaderConfig{
					Format:    format,
					BatchSize: 1000,
				}

				reader, err := NewReader(bytes.NewReader(data), config)
				if err != nil {
					b.Fatal(err)
				}

				readRecords, err := reader.ReadRecords()
				if err != nil {
					b.Fatal(err)
				}

				if len(readRecords) != recordCount {
					b.Fatalf("expected %d records, got %d", recordCount, len(readRecords))
				}

				err = reader.Close() // Ignore close error
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark streaming read
func BenchmarkColumnarStreamingRead(b *testing.B) {
	formats := []Format{
		Parquet,
		Arrow,
	}

	recordCount := 100000
	compression := "snappy"
	schema := generateTestSchema()
	records := generateTestRecords(recordCount)

	// Pre-generate files
	files := make(map[Format][]byte)
	for _, format := range formats {
		var buf bytes.Buffer
		config := &WriterConfig{
			Format:      format,
			Schema:      schema,
			Compression: compression,
			BatchSize:   1000,
			PageSize:    8192,
			EnableStats: true,
		}

		writer, err := NewWriter(&buf, config)
		if err != nil {
			b.Fatal(err)
		}

		err = writer.WriteRecords(records) // Ignore write records error
		if err != nil {
			b.Fatal(err)
		}

		err = writer.Close() // Ignore close error
		if err != nil {
			b.Fatal(err)
		}

		files[format] = buf.Bytes()
	}

	for _, format := range formats {
		data := files[format]

		b.Run(string(format), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				config := &ReaderConfig{
					Format:    format,
					BatchSize: 1000,
				}

				reader, err := NewReader(bytes.NewReader(data), config)
				if err != nil {
					b.Fatal(err)
				}

				count := 0
				for reader.HasNext() {
					record, err := reader.Next()
					if err != nil {
						b.Fatal(err)
					}
					if record == nil {
						break
					}
					count++
				}

				if count != recordCount {
					b.Fatalf("expected %d records, got %d", recordCount, count)
				}

				err = reader.Close() // Ignore close error
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark file sizes and compression ratios
func BenchmarkColumnarFileSizes(b *testing.B) {
	formats := []Format{
		Parquet,
		Arrow,
	}

	compressions := []string{
		"none",
		"snappy",
		"gzip",
		"zstd",
	}

	recordCount := 10000
	schema := generateTestSchema()
	records := generateTestRecords(recordCount)

	// Calculate raw size
	rawSize := 0
	for _, record := range records {
		for _, v := range record.Data {
			switch val := v.(type) {
			case string:
				rawSize += len(val)
			case int, int64:
				rawSize += 8
			case float64:
				rawSize += 8
			case bool:
				rawSize += 1
			case time.Time:
				rawSize += 8
			}
		}
	}

	b.Logf("\nFile sizes for %d records (raw size: %s):", recordCount, formatBytes(rawSize))
	b.Logf("%-10s %-10s %-15s %-10s", "Format", "Compression", "Size", "Ratio")
	b.Logf("%s", strings.Repeat("-", 50))

	for _, format := range formats {
		for _, compression := range compressions {
			// Skip unsupported combinations
			if format == Arrow && compression == "zstd" {
				continue
			}

			var buf bytes.Buffer
			config := &WriterConfig{
				Format:      format,
				Schema:      schema,
				Compression: compression,
				BatchSize:   1000,
				PageSize:    8192,
				EnableStats: true,
			}

			writer, err := NewWriter(&buf, config)
			if err != nil {
				b.Logf("%-10s %-10s Error: %v", format, compression, err)
				continue
			}

			err = writer.WriteRecords(records) // Ignore write records error
			if err != nil {
				b.Logf("%-10s %-10s Error: %v", format, compression, err)
				continue
			}

			err = writer.Close() // Ignore close error
			if err != nil {
				b.Logf("%-10s %-10s Error: %v", format, compression, err)
				continue
			}

			size := buf.Len()
			ratio := float64(rawSize) / float64(size)
			b.Logf("%-10s %-10s %-15s %.2fx", format, compression, formatBytes(size), ratio)
		}
	}
}

// Helper function to format bytes
func formatBytes(bytes int) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
