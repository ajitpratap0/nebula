package json

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/ajitpratap0/nebula/pkg/pool"
	gojson "github.com/goccy/go-json"
)

// Test data structures
type testRecord struct {
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
	Value     float64                `json:"value"`
	Tags      []string               `json:"tags"`
	Metadata  map[string]interface{} `json:"metadata"`
	Timestamp int64                  `json:"timestamp"`
}

func generateTestRecords(n int) []*testRecord {
	records := make([]*testRecord, n)
	for i := 0; i < n; i++ {
		records[i] = &testRecord{
			ID:    pool.GenerateID("test"),
			Name:  "Test Record",
			Value: float64(i) * 1.5,
			Tags:  []string{"tag1", "tag2", "tag3"},
			Metadata: map[string]interface{}{
				"source":   "benchmark",
				"version":  "1.0",
				"index":    i,
				"category": "test",
			},
			Timestamp: 1234567890,
		}
	}
	return records
}

func generatePoolRecords(n int) []*pool.Record {
	records := make([]*pool.Record, n)
	for i := 0; i < n; i++ {
		rec := pool.GetRecord()
		rec.ID = pool.GenerateID("test")
		rec.Data["name"] = "Test Record"
		rec.Data["value"] = float64(i) * 1.5
		rec.Data["tags"] = []string{"tag1", "tag2", "tag3"}
		rec.Data["timestamp"] = 1234567890
		rec.SetMetadata("source", "benchmark")
		rec.SetMetadata("version", "1.0")
		rec.SetMetadata("index", i)
		records[i] = rec
	}
	return records
}

// Benchmark standard library json.Marshal
func BenchmarkStdMarshal(b *testing.B) {
	records := generateTestRecords(100)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, record := range records {
			_, err := json.Marshal(record)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportMetric(float64(len(records)*b.N), "records/op")
}

// Benchmark goccy/go-json Marshal
func BenchmarkGoccyMarshal(b *testing.B) {
	records := generateTestRecords(100)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, record := range records {
			_, err := gojson.Marshal(record)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportMetric(float64(len(records)*b.N), "records/op")
}

// Benchmark optimized Marshal
func BenchmarkOptimizedMarshal(b *testing.B) {
	records := generateTestRecords(100)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, record := range records {
			_, err := Marshal(record)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportMetric(float64(len(records)*b.N), "records/op")
}

// Benchmark standard library encoder
func BenchmarkStdEncoder(b *testing.B) {
	records := generateTestRecords(100)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)

		for _, record := range records {
			if err := enc.Encode(record); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportMetric(float64(len(records)*b.N), "records/op")
}

// Benchmark pooled encoder
func BenchmarkPooledEncoder(b *testing.B) {
	records := generateTestRecords(100)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := GetBuffer()
		enc := GetEncoder(buf)

		for _, record := range records {
			if err := enc.Encode(record); err != nil {
				b.Fatal(err)
			}
		}

		PutEncoder(enc)
		PutBuffer(buf)
	}

	b.ReportMetric(float64(len(records)*b.N), "records/op")
}

// Benchmark streaming encoder
func BenchmarkStreamingEncoder(b *testing.B) {
	records := generateTestRecords(100)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		enc := NewStreamingEncoder(&buf, false)

		for _, record := range records {
			if err := enc.Encode(record); err != nil {
				b.Fatal(err)
			}
		}

		_ = enc.Close() // Ignore close error
	}

	b.ReportMetric(float64(len(records)*b.N), "records/op")
}

// Benchmark MarshalRecordsArray
func BenchmarkMarshalRecordsArray(b *testing.B) {
	records := generatePoolRecords(100)
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := MarshalRecordsArray(records)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(len(records)*b.N), "records/op")
}

// Benchmark MarshalRecordsLines
func BenchmarkMarshalRecordsLines(b *testing.B) {
	records := generatePoolRecords(100)
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := MarshalRecordsLines(records)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(len(records)*b.N), "records/op")
}

// Benchmark with different record counts
func BenchmarkMarshalScaling(b *testing.B) {
	recordCounts := []int{10, 100, 1000, 10000}

	for _, count := range recordCounts {
		b.Run(b.Name()+"/StdLib", func(b *testing.B) {
			records := generateTestRecords(count)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				var data []map[string]interface{}
				for _, r := range records {
					m := map[string]interface{}{
						"id":        r.ID,
						"name":      r.Name,
						"value":     r.Value,
						"tags":      r.Tags,
						"metadata":  r.Metadata,
						"timestamp": r.Timestamp,
					}
					data = append(data, m)
				}

				_, err := json.Marshal(data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(b.Name()+"/Optimized", func(b *testing.B) {
			records := generatePoolRecords(count)
			defer func() {
				for _, r := range records {
					r.Release()
				}
			}()

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := MarshalRecordsArray(records)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Test correctness
func TestMarshalCorrectness(t *testing.T) {
	// Create test data
	record := &testRecord{
		ID:    "test-123",
		Name:  "Test Record",
		Value: 42.5,
		Tags:  []string{"tag1", "tag2"},
		Metadata: map[string]interface{}{
			"key": "value",
		},
		Timestamp: 1234567890,
	}

	// Compare standard and optimized output
	stdData, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}

	optData, err := Marshal(record)
	if err != nil {
		t.Fatal(err)
	}

	// The output should be functionally equivalent
	var stdResult, optResult map[string]interface{}
	if err := json.Unmarshal(stdData, &stdResult); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(optData, &optResult); err != nil {
		t.Fatal(err)
	}

	// Compare the parsed results
	if stdResult["id"] != optResult["id"] {
		t.Errorf("ID mismatch: %v != %v", stdResult["id"], optResult["id"])
	}
	if stdResult["name"] != optResult["name"] {
		t.Errorf("Name mismatch: %v != %v", stdResult["name"], optResult["name"])
	}
}
