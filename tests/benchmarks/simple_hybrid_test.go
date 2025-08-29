package benchmarks

import (
	"runtime"
	"testing"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/pipeline"
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// BenchmarkSimpleHybridComparison tests the storage adapter directly
func BenchmarkSimpleHybridComparison(b *testing.B) {
	modes := []struct {
		name string
		mode pipeline.StorageMode
	}{
		{"Row", pipeline.StorageModeRow},
		{"Columnar", pipeline.StorageModeColumnar},
		{"Hybrid", pipeline.StorageModeHybrid},
	}

	for _, mode := range modes {
		b.Run(string(mode.mode), func(b *testing.B) {
			cfg := config.NewBaseConfig("test", "benchmark")
			cfg.Performance.BatchSize = 10000

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)

				adapter := pipeline.NewStorageAdapter(mode.mode, cfg)

				// Add 50K records
				for j := 0; j < 50000; j++ {
					record := pool.GetRecord()
					record.SetData("id", j)
					record.SetData("name", "user_"+string(rune(j%1000)))
					record.SetData("category", "cat_"+string(rune(j%10)))
					record.SetData("amount", float64(j)*1.5)
					record.SetData("active", j%2 == 0)

					adapter.AddRecord(record)
					record.Release()
				}

				adapter.Flush()

				runtime.GC()
				runtime.ReadMemStats(&m2)

				memUsed := m2.TotalAlloc - m1.TotalAlloc
				bytesPerRecord := float64(memUsed) / 50000.0

				b.ReportMetric(bytesPerRecord, "bytes/record")
				b.ReportMetric(float64(adapter.GetRecordCount()), "total_records")

				adapter.Close()
			}
		})
	}
}

// BenchmarkColumnarOptimization specifically tests columnar optimization
func BenchmarkColumnarOptimization(b *testing.B) {
	sizes := []int{10000, 50000, 100000}

	for _, size := range sizes {
		b.Run("Records_"+string(rune(size/1000))+"K", func(b *testing.B) {
			cfg := config.NewBaseConfig("test", "benchmark")
			cfg.Performance.BatchSize = size / 10

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)

				adapter := pipeline.NewStorageAdapter(pipeline.StorageModeColumnar, cfg)

				// Add records with repeated patterns (good for columnar)
				statuses := []string{"active", "inactive", "pending", "expired"}
				categories := []string{"A", "B", "C", "D", "E"}

				for j := 0; j < size; j++ {
					record := pool.GetRecord()
					record.SetData("id", j)
					record.SetData("status", statuses[j%len(statuses)])
					record.SetData("category", categories[j%len(categories)])
					record.SetData("value", j/100) // Integer that repeats
					record.SetData("active", j%2 == 0)

					adapter.AddRecord(record)
					record.Release()
				}

				adapter.OptimizeStorage()

				runtime.GC()
				runtime.ReadMemStats(&m2)

				memUsed := m2.TotalAlloc - m1.TotalAlloc
				bytesPerRecord := float64(memUsed) / float64(size)

				b.ReportMetric(bytesPerRecord, "bytes/record")
				b.ReportMetric(float64(size), "total_records")

				adapter.Close()
			}
		})
	}
}

// BenchmarkHybridDecisionMaking tests the hybrid mode intelligence
func BenchmarkHybridDecisionMaking(b *testing.B) {
	scenarios := []struct {
		name        string
		recordCount int
	}{
		{"Small_1K", 1000},
		{"Medium_10K", 10000},
		{"Large_100K", 100000},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			cfg := config.NewBaseConfig("test", "benchmark")
			cfg.Advanced.StorageMode = "hybrid"

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)

				adapter := pipeline.NewStorageAdapter(pipeline.StorageModeHybrid, cfg)

				for j := 0; j < scenario.recordCount; j++ {
					record := pool.GetRecord()
					record.SetData("id", j)
					record.SetData("data", "record_"+string(rune(j)))

					adapter.AddRecord(record)
					record.Release()
				}

				runtime.GC()
				runtime.ReadMemStats(&m2)

				memUsed := m2.TotalAlloc - m1.TotalAlloc
				bytesPerRecord := float64(memUsed) / float64(scenario.recordCount)
				actualMode := adapter.GetStorageMode()

				b.ReportMetric(bytesPerRecord, "bytes/record")
				b.ReportMetric(float64(len(string(actualMode))), "mode_choice")

				adapter.Close()
			}
		})
	}
}
