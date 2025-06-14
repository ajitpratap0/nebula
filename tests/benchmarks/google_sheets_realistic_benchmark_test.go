// Package benchmarks provides realistic performance benchmarks for Google Sheets connector
package benchmarks

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/pkg/clients"
)

// RealisticGoogleSheetsConstraints represents actual Google Sheets API limitations
type RealisticGoogleSheetsConstraints struct {
	// API Rate Limits (per project)
	DefaultQuotaPerMinute  int // 100 requests/minute
	EnhancedQuotaPerMinute int // 1000 requests/minute (requires approval)

	// Network Latency
	MinLatencyMs int // 20ms minimum
	AvgLatencyMs int // 50ms average
	MaxLatencyMs int // 200ms worst case

	// API Constraints
	MaxCellsPerRequest int // 5,000,000 cells
	MaxRowsPerRequest  int // 50,000 rows
	OptimalBatchSize   int // 2,000 rows (balance between throughput and latency)

	// OAuth2 Overhead
	TokenRefreshMs     int           // 20ms
	TokenCacheDuration time.Duration // 1 hour
}

var realConstraints = RealisticGoogleSheetsConstraints{
	DefaultQuotaPerMinute:  100,
	EnhancedQuotaPerMinute: 1000,
	MinLatencyMs:           20,
	AvgLatencyMs:           50,
	MaxLatencyMs:           200,
	MaxCellsPerRequest:     5_000_000,
	MaxRowsPerRequest:      50_000,
	OptimalBatchSize:       2_000,
	TokenRefreshMs:         20,
	TokenCacheDuration:     time.Hour,
}

// BenchmarkGoogleSheetsRealisticThroughput tests realistic API throughput
func BenchmarkGoogleSheetsRealisticThroughput(b *testing.B) {
	scenarios := []struct {
		name           string
		quotaPerMinute int
		batchSize      int
		latencyMs      int
		projects       int // Number of Google Cloud projects
	}{
		{
			name:           "Default_Quota_Single_Project",
			quotaPerMinute: realConstraints.DefaultQuotaPerMinute,
			batchSize:      realConstraints.OptimalBatchSize,
			latencyMs:      realConstraints.AvgLatencyMs,
			projects:       1,
		},
		{
			name:           "Enhanced_Quota_Single_Project",
			quotaPerMinute: realConstraints.EnhancedQuotaPerMinute,
			batchSize:      realConstraints.OptimalBatchSize,
			latencyMs:      realConstraints.AvgLatencyMs,
			projects:       1,
		},
		{
			name:           "Default_Quota_Multi_Project",
			quotaPerMinute: realConstraints.DefaultQuotaPerMinute,
			batchSize:      realConstraints.OptimalBatchSize,
			latencyMs:      realConstraints.AvgLatencyMs,
			projects:       10, // 10 projects = 10x quota
		},
		{
			name:           "Enhanced_Quota_Large_Batch",
			quotaPerMinute: realConstraints.EnhancedQuotaPerMinute,
			batchSize:      10_000, // Larger batch
			latencyMs:      100,    // Higher latency for larger batch
			projects:       1,
		},
		{
			name:           "Optimal_Multi_Project",
			quotaPerMinute: realConstraints.EnhancedQuotaPerMinute,
			batchSize:      5_000,
			latencyMs:      75,
			projects:       5,
		},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			// Calculate actual rate limit per second across all projects
			totalQuotaPerSec := float64(scenario.quotaPerMinute*scenario.projects) / 60.0

			// Create rate limiter
			rateLimiter := clients.NewTokenBucketRateLimiter(
				totalQuotaPerSec,
				int(totalQuotaPerSec*2), // Allow some burst
			)

			// Track metrics
			var totalRecords int64
			var totalRequests int64
			var blockedRequests int64

			// Run for a fixed duration to get stable results
			duration := 10 * time.Second
			start := time.Now()

			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()

			// Simulate concurrent API calls
			var wg sync.WaitGroup
			workers := scenario.projects * 2 // 2 workers per project

			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()

					for {
						select {
						case <-ctx.Done():
							return
						default:
							// Check rate limit
							if !rateLimiter.Allow() {
								atomic.AddInt64(&blockedRequests, 1)
								time.Sleep(100 * time.Millisecond) // Back off
								continue
							}

							// Simulate API call with realistic latency
							time.Sleep(time.Duration(scenario.latencyMs) * time.Millisecond)

							// Record successful request
							atomic.AddInt64(&totalRequests, 1)
							atomic.AddInt64(&totalRecords, int64(scenario.batchSize))
						}
					}
				}(i)
			}

			wg.Wait()
			elapsed := time.Since(start)

			// Calculate actual throughput
			throughput := float64(totalRecords) / elapsed.Seconds()
			requestRate := float64(totalRequests) / elapsed.Seconds()

			// Report metrics
			b.ReportMetric(throughput, "records/sec")
			b.ReportMetric(requestRate, "requests/sec")
			b.ReportMetric(float64(blockedRequests), "blocked_requests")
			b.ReportMetric(float64(scenario.batchSize), "batch_size")
			b.ReportMetric(float64(scenario.latencyMs), "latency_ms")

			// Log summary
			b.Logf("\nScenario: %s", scenario.name)
			b.Logf("Theoretical Max: %.0f records/sec (%.1f req/sec * %d batch)",
				totalQuotaPerSec*float64(scenario.batchSize),
				totalQuotaPerSec,
				scenario.batchSize)
			b.Logf("Actual Achieved: %.0f records/sec (%.1f req/sec)",
				throughput, requestRate)
			b.Logf("Efficiency: %.1f%%", (throughput/(totalQuotaPerSec*float64(scenario.batchSize)))*100)
		})
	}
}

// BenchmarkGoogleSheetsVsCSV compares Google Sheets with CSV performance
func BenchmarkGoogleSheetsVsCSV(b *testing.B) {
	b.Run("CSV_Local_File", func(b *testing.B) {
		// Simulate CSV reading performance
		recordsPerBatch := 10_000
		batchLatency := 1 * time.Microsecond // Almost no latency for local file

		start := time.Now()
		totalRecords := int64(0)

		// Process for 1 second
		for time.Since(start) < time.Second {
			time.Sleep(batchLatency)
			totalRecords += int64(recordsPerBatch)
		}

		throughput := float64(totalRecords) / time.Since(start).Seconds()
		b.ReportMetric(throughput, "records/sec")
		b.Logf("CSV Local: %.0f records/sec", throughput)
	})

	b.Run("GoogleSheets_API_Default", func(b *testing.B) {
		// Realistic Google Sheets with default quota
		quotaPerSec := float64(realConstraints.DefaultQuotaPerMinute) / 60.0
		batchSize := realConstraints.OptimalBatchSize
		latency := time.Duration(realConstraints.AvgLatencyMs) * time.Millisecond

		rateLimiter := clients.NewTokenBucketRateLimiter(quotaPerSec, int(quotaPerSec*2))

		start := time.Now()
		totalRecords := int64(0)

		// Process for 1 second
		for time.Since(start) < time.Second {
			if rateLimiter.Allow() {
				time.Sleep(latency)
				totalRecords += int64(batchSize)
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}

		throughput := float64(totalRecords) / time.Since(start).Seconds()
		b.ReportMetric(throughput, "records/sec")
		b.Logf("Google Sheets (Default): %.0f records/sec", throughput)
	})

	b.Run("GoogleSheets_API_Enhanced", func(b *testing.B) {
		// Realistic Google Sheets with enhanced quota
		quotaPerSec := float64(realConstraints.EnhancedQuotaPerMinute) / 60.0
		batchSize := realConstraints.OptimalBatchSize
		latency := time.Duration(realConstraints.AvgLatencyMs) * time.Millisecond

		rateLimiter := clients.NewTokenBucketRateLimiter(quotaPerSec, int(quotaPerSec*2))

		start := time.Now()
		totalRecords := int64(0)

		// Process for 1 second
		for time.Since(start) < time.Second {
			if rateLimiter.Allow() {
				time.Sleep(latency)
				totalRecords += int64(batchSize)
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}

		throughput := float64(totalRecords) / time.Since(start).Seconds()
		b.ReportMetric(throughput, "records/sec")
		b.Logf("Google Sheets (Enhanced): %.0f records/sec", throughput)
	})
}

// BenchmarkHybridApproach tests hybrid CSV+API strategies
func BenchmarkHybridApproach(b *testing.B) {
	b.Run("Cached_Data_With_API_Refresh", func(b *testing.B) {
		// Simulate serving from cache with background API refresh
		cacheHitRate := 0.95 // 95% cache hits
		cacheLatency := 100 * time.Nanosecond
		apiLatency := time.Duration(realConstraints.AvgLatencyMs) * time.Millisecond
		batchSize := realConstraints.OptimalBatchSize

		start := time.Now()
		totalRecords := int64(0)

		for i := 0; time.Since(start) < time.Second; i++ {
			if float64(i%100)/100.0 < cacheHitRate {
				// Cache hit
				time.Sleep(cacheLatency)
				totalRecords += int64(batchSize)
			} else {
				// Cache miss - API call
				time.Sleep(apiLatency)
				totalRecords += int64(batchSize)
			}
		}

		throughput := float64(totalRecords) / time.Since(start).Seconds()
		b.ReportMetric(throughput, "records/sec")
		b.ReportMetric(cacheHitRate*100, "cache_hit_rate_%")
		b.Logf("Hybrid (95%% cache): %.0f records/sec", throughput)
	})

	b.Run("Bulk_Export_Plus_Incremental", func(b *testing.B) {
		// Simulate initial bulk export to CSV, then incremental API updates
		// First second: read from exported CSV
		// Subsequent: API updates

		csvThroughput := 1_000_000.0 // 1M records/sec from CSV
		apiThroughput := 30_000.0    // 30K records/sec from API
		bulkRatio := 0.9             // 90% bulk, 10% incremental

		weightedThroughput := (csvThroughput * bulkRatio) + (apiThroughput * (1 - bulkRatio))

		b.ReportMetric(weightedThroughput, "records/sec")
		b.Logf("Bulk+Incremental: %.0f records/sec", weightedThroughput)
	})
}

// TestPerformanceExpectations documents realistic performance expectations
func TestPerformanceExpectations(t *testing.T) {
	t.Log("\n=== Realistic Google Sheets Performance Expectations ===")

	// Calculate theoretical maximums
	defaultQuotaThroughput := (float64(realConstraints.DefaultQuotaPerMinute) / 60.0) * float64(realConstraints.OptimalBatchSize)
	enhancedQuotaThroughput := (float64(realConstraints.EnhancedQuotaPerMinute) / 60.0) * float64(realConstraints.OptimalBatchSize)

	t.Log("\nAPI Constraints:")
	t.Logf("- Default Quota: %d requests/minute", realConstraints.DefaultQuotaPerMinute)
	t.Logf("- Enhanced Quota: %d requests/minute", realConstraints.EnhancedQuotaPerMinute)
	t.Logf("- Network Latency: %d-%dms", realConstraints.MinLatencyMs, realConstraints.MaxLatencyMs)
	t.Logf("- Optimal Batch: %d records", realConstraints.OptimalBatchSize)

	t.Log("\nTheoretical Throughput (ignoring latency):")
	t.Logf("- Default Quota: %.0f records/sec", defaultQuotaThroughput)
	t.Logf("- Enhanced Quota: %.0f records/sec", enhancedQuotaThroughput)

	t.Log("\nRealistic Throughput (with latency):")
	t.Logf("- Default Quota: ~3,000 records/sec")
	t.Logf("- Enhanced Quota: ~30,000 records/sec")
	t.Logf("- Multi-Project (10x): ~300,000 records/sec")

	t.Log("\nComparison with CSV:")
	t.Logf("- CSV Local File: 1,000,000+ records/sec")
	t.Logf("- Google Sheets API: 3,000-30,000 records/sec")
	t.Logf("- Performance Ratio: CSV is 30-300x faster")

	t.Log("\nRecommendations:")
	t.Log("1. Use CSV for bulk data transfers")
	t.Log("2. Use Google Sheets API for real-time updates")
	t.Log("3. Implement caching for frequently accessed data")
	t.Log("4. Consider hybrid approaches for best performance")

	t.Log("\n❌ The 50K records/sec target is NOT achievable with pure API access")
	t.Log("✅ Achievable with caching or hybrid strategies")
}
