// Package compression provides compression benchmarks
package compression

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// Test data generators
func generateJSONData(size int) []byte {
	records := make([]map[string]interface{}, size/100)
	for i := range records {
		records[i] = map[string]interface{}{
			"id":        i,
			"name":      fmt.Sprintf("User %d", i),
			"email":     fmt.Sprintf("user%d@example.com", i),
			"age":       rand.Intn(80) + 20,
			"score":     rand.Float64() * 100,
			"active":    rand.Intn(2) == 1,
			"tags":      []string{"tag1", "tag2", "tag3"},
			"metadata":  map[string]string{"key1": "value1", "key2": "value2"},
			"timestamp": "2024-01-15T10:30:00Z",
		}
	}
	data, _ := jsonpool.Marshal(records)
	return data
}

func generateCSVData(size int) []byte {
	buf := pool.GlobalBufferPool.Get(size * 2) // Estimate buffer size
	defer pool.GlobalBufferPool.Put(buf)

	// Use bytes.Buffer for writer operations
	var writer bytes.Buffer
	writer.WriteString("id,name,email,age,score,active\n")
	for i := 0; i < size/50; i++ {
		fmt.Fprintf(&writer, "%d,User %d,user%d@example.com,%d,%.2f,%t\n",
			i, i, i, rand.Intn(80)+20, rand.Float64()*100, rand.Intn(2) == 1)
	}
	return writer.Bytes()
}

func generateTextData(size int) []byte {
	words := []string{"the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog"}

	// Use bytes.Buffer for writer operations
	var writer bytes.Buffer
	for writer.Len() < size {
		writer.WriteString(words[rand.Intn(len(words))])
		writer.WriteString(" ")
	}
	result := writer.Bytes()
	if len(result) > size {
		return result[:size]
	}
	return result
}

func generateBinaryData(size int) []byte {
	data := pool.GetByteSlice()

	if cap(data) < size {

		data = make([]byte, size)

	}

	defer pool.PutByteSlice(data)
	rand.Read(data)
	return data
}

// Benchmark compression algorithms
func BenchmarkCompression(b *testing.B) {
	algorithms := []Algorithm{
		Gzip,
		Snappy,
		LZ4,
		Zstd,
		S2,
		Deflate,
	}

	dataSizes := []int{
		1024,     // 1KB
		10240,    // 10KB
		102400,   // 100KB
		1048576,  // 1MB
		10485760, // 10MB
	}

	dataTypes := map[string]func(int) []byte{
		"JSON":   generateJSONData,
		"CSV":    generateCSVData,
		"Text":   generateTextData,
		"Binary": generateBinaryData,
	}

	for _, algo := range algorithms {
		for _, size := range dataSizes {
			for dataType, generator := range dataTypes {
				testData := generator(size)

				b.Run(fmt.Sprintf("%s/%s/%s", algo, dataType, formatBytes(size)), func(b *testing.B) {
					config := &Config{
						Algorithm:  algo,
						Level:      Default,
						BufferSize: 64 * 1024,
					}

					compressor, err := NewCompressor(config)
					if err != nil {
						b.Fatal(err)
					}

					b.ResetTimer()
					b.SetBytes(int64(len(testData)))

					for i := 0; i < b.N; i++ {
						compressed, err := compressor.Compress(testData)
						if err != nil {
							b.Fatal(err)
						}
						_ = compressed
					}
				})
			}
		}
	}
}

// Benchmark decompression
func BenchmarkDecompression(b *testing.B) {
	algorithms := []Algorithm{
		Gzip,
		Snappy,
		LZ4,
		Zstd,
		S2,
		Deflate,
	}

	size := 1048576 // 1MB
	testData := generateJSONData(size)

	for _, algo := range algorithms {
		config := &Config{
			Algorithm:  algo,
			Level:      Default,
			BufferSize: 64 * 1024,
		}

		compressor, err := NewCompressor(config)
		if err != nil {
			b.Fatal(err)
		}

		compressed, err := compressor.Compress(testData)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(string(algo), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(len(compressed)))

			for i := 0; i < b.N; i++ {
				decompressed, err := compressor.Decompress(compressed)
				if err != nil {
					b.Fatal(err)
				}
				_ = decompressed
			}
		})
	}
}

// Benchmark compression ratios
func BenchmarkCompressionRatio(b *testing.B) {
	algorithms := []Algorithm{
		Gzip,
		Snappy,
		LZ4,
		Zstd,
		S2,
		Deflate,
	}

	levels := []Level{
		Fastest,
		Default,
		Better,
		Best,
	}

	size := 1048576 // 1MB
	dataTypes := map[string]func(int) []byte{
		"JSON":   generateJSONData,
		"CSV":    generateCSVData,
		"Text":   generateTextData,
		"Binary": generateBinaryData,
	}

	for dataType, generator := range dataTypes {
		testData := generator(size)
		b.Logf("\n%s Data (%s):", dataType, formatBytes(len(testData)))
		b.Logf("%-10s %-10s %-15s %-10s", "Algorithm", "Level", "Compressed", "Ratio")
		b.Logf("%s", strings.Repeat("-", 50))

		for _, algo := range algorithms {
			for _, level := range levels {
				// Skip unsupported combinations
				if (algo == Snappy || algo == S2) && level != Default {
					continue
				}

				config := &Config{
					Algorithm:  algo,
					Level:      level,
					BufferSize: 64 * 1024,
				}

				compressor, err := NewCompressor(config)
				if err != nil {
					continue
				}

				compressed, err := compressor.Compress(testData)
				if err != nil {
					b.Logf("%-10s %-10s Error: %v", algo, level, err)
					continue
				}

				ratio := float64(len(testData)) / float64(len(compressed))
				b.Logf("%-10s %-10s %-15s %.2fx", algo, levelString(level),
					formatBytes(len(compressed)), ratio)
			}
		}
		b.Logf("")
	}
}

// Benchmark streaming compression
func BenchmarkStreamingCompression(b *testing.B) {
	algorithms := []Algorithm{
		Gzip,
		Snappy,
		LZ4,
		Zstd,
		S2,
		Deflate,
	}

	size := 10485760 // 10MB
	testData := generateJSONData(size)

	for _, algo := range algorithms {
		b.Run(string(algo), func(b *testing.B) {
			config := &Config{
				Algorithm:  algo,
				Level:      Default,
				BufferSize: 64 * 1024,
			}

			compressor, err := NewCompressor(config)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.SetBytes(int64(len(testData)))

			for i := 0; i < b.N; i++ {
				buf := pool.GlobalBufferPool.Get(len(testData) * 2) // Estimate compressed size
				defer pool.GlobalBufferPool.Put(buf)

				// Use bytes.Buffer for writer operations
				var writer bytes.Buffer
				err := compressor.CompressStream(&writer, bytes.NewReader(testData))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark with compressor pool
func BenchmarkCompressorPool(b *testing.B) {
	config := &Config{
		Algorithm:  Snappy,
		Level:      Default,
		BufferSize: 64 * 1024,
	}

	pool := NewCompressorPool(config)
	size := 102400 // 100KB
	testData := generateJSONData(size)

	b.Run("WithPool", func(b *testing.B) {
		b.SetBytes(int64(len(testData)))
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				compressed, err := pool.Compress(testData)
				if err != nil {
					b.Fatal(err)
				}
				_ = compressed
			}
		})
	})

	b.Run("WithoutPool", func(b *testing.B) {
		b.SetBytes(int64(len(testData)))
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				compressor, err := NewCompressor(config)
				if err != nil {
					b.Fatal(err)
				}
				compressed, err := compressor.Compress(testData)
				if err != nil {
					b.Fatal(err)
				}
				_ = compressed
			}
		})
	})
}

// Helper functions
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

func levelString(level Level) string {
	switch level {
	case Fastest:
		return "Fastest"
	case Default:
		return "Default"
	case Better:
		return "Better"
	case Best:
		return "Best"
	default:
		return "Unknown"
	}
}
