package benchmarks

import (
	"fmt"
	"runtime"
	"strconv"
	"testing"

	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
)

// BenchmarkStringOptimization compares string allocation methods
func BenchmarkStringOptimization(b *testing.B) {
	testCases := []struct {
		name string
		fn   func(int) string
	}{
		{
			name: "fmt.Sprintf",
			fn: func(i int) string {
				return fmt.Sprintf("col_%d", i)
			},
		},
		{
			name: "strconv+concat",
			fn: func(i int) string {
				return "col_" + strconv.Itoa(i)
			},
		},
		{
			name: "pool.GetColumnName",
			fn: func(i int) string {
				return pool.GetColumnName(i)
			},
		},
		{
			name: "stringpool.Sprintf",
			fn: func(i int) string {
				return stringpool.Sprintf("col_%d", i)
			},
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// Simulate accessing 20 columns
				for j := 0; j < 20; j++ {
					_ = tc.fn(j)
				}
			}
		})
	}
}

// BenchmarkRecordCreationWithStrings measures memory usage for record creation
func BenchmarkRecordCreationWithStrings(b *testing.B) {
	testCases := []struct {
		name string
		fn   func() *pool.Record
	}{
		{
			name: "WithFmtSprintf",
			fn: func() *pool.Record {
				record := pool.GetRecord()
				for i := 0; i < 20; i++ {
					record.SetData(fmt.Sprintf("col_%d", i), fmt.Sprintf("value_%d", i))
				}
				return record
			},
		},
		{
			name: "WithStringOptimization",
			fn: func() *pool.Record {
				record := pool.GetRecord()
				for i := 0; i < 20; i++ {
					record.SetData(pool.GetColumnName(i), "value_"+strconv.Itoa(i))
				}
				return record
			},
		},
		{
			name: "WithFullOptimization",
			fn: func() *pool.Record {
				record := pool.GetRecord()
				for i := 0; i < 20; i++ {
					// Pre-interned column names and interned values
					record.SetData(pool.GetColumnName(i), pool.InternString("value_"+strconv.Itoa(i)))
				}
				return record
			},
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()

			// Measure memory before
			var m1, m2 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m1)

			records := make([]*pool.Record, 0, 1000)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				record := tc.fn()
				records = append(records, record)

				// Release records periodically to simulate real usage
				if len(records) >= 1000 {
					for _, r := range records {
						r.Release()
					}
					records = records[:0]
				}
			}

			// Clean up remaining records
			for _, r := range records {
				r.Release()
			}

			b.StopTimer()
			runtime.GC()
			runtime.ReadMemStats(&m2)

			bytesPerOp := float64(m2.TotalAlloc-m1.TotalAlloc) / float64(b.N)
			b.ReportMetric(bytesPerOp, "bytes/record")
		})
	}
}

// BenchmarkStringInternImpact measures the impact of string interning
func BenchmarkStringInternImpact(b *testing.B) {
	// Pre-populate some common strings
	commonStrings := []string{
		"status", "active", "inactive", "pending", "completed",
		"type", "id", "name", "value", "timestamp",
	}

	for _, s := range commonStrings {
		pool.InternString(s)
	}

	b.Run("WithoutInterning", func(b *testing.B) {
		b.ReportAllocs()
		m := make(map[string]interface{})

		for i := 0; i < b.N; i++ {
			// Simulate field access without interning
			m["status"] = "active"
			m["type"] = "user"
			m["name"] = "John Doe"
			_ = m["status"]
			_ = m["type"]
			_ = m["name"]

			// Clear map
			for k := range m {
				delete(m, k)
			}
		}
	})

	b.Run("WithInterning", func(b *testing.B) {
		b.ReportAllocs()
		m := make(map[string]interface{})

		for i := 0; i < b.N; i++ {
			// Simulate field access with interning
			m[pool.InternString("status")] = pool.InternString("active")
			m[pool.InternString("type")] = pool.InternString("user")
			m[pool.InternString("name")] = "John Doe" // Names vary, don't intern
			_ = m[pool.InternString("status")]
			_ = m[pool.InternString("type")]
			_ = m[pool.InternString("name")]

			// Clear map
			for k := range m {
				delete(m, k)
			}
		}
	})
}
