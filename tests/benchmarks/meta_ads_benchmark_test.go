// Package benchmarks provides performance benchmarks for Meta Ads connector
package benchmarks

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/pkg/clients"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/sources"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
)

// MockMetaAdsAPI provides a mock Meta Ads API server
type MockMetaAdsAPI struct {
	server        *httptest.Server
	requestCount  int64
	rateLimitTier string
	asyncJobs     map[string]*AsyncJobMock
	insightsData  []map[string]interface{}
	mu            sync.RWMutex
}

// AsyncJobMock represents a mock async job
type AsyncJobMock struct {
	ID              string
	Status          string
	PercentComplete int
	CreatedAt       time.Time
	CompletedAt     time.Time
	RecordCount     int
}

// NewMockMetaAdsAPI creates a new mock API server
func NewMockMetaAdsAPI(tier string) *MockMetaAdsAPI {
	mock := &MockMetaAdsAPI{
		rateLimitTier: tier,
		asyncJobs:     make(map[string]*AsyncJobMock),
		insightsData:  generateMockInsightsData(10000), // 10K records
	}

	mock.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&mock.requestCount, 1)

		// Route requests
		switch {
		case r.URL.Path == "/v18.0/me":
			mock.handleValidation(w, r)
		case r.URL.Path == "/v18.0/act_123456789/insights" && r.Method == "POST":
			mock.handleCreateAsyncJob(w, r)
		case r.URL.Path == "/v18.0/act_123456789/insights" && r.Method == "GET":
			mock.handleSyncInsights(w, r)
		case r.URL.Path == "/v18.0/act_123456789/campaigns":
			mock.handleCampaigns(w, r)
		case r.URL.Path == "/v18.0/act_123456789/adsets":
			mock.handleAdsets(w, r)
		case r.URL.Path == "/v18.0/act_123456789/ads":
			mock.handleAds(w, r)
		default:
			// Check if it's a job status check
			jobID := r.URL.Path[len("/v18.0/"):]
			if job, exists := mock.asyncJobs[jobID]; exists {
				mock.handleJobStatus(w, r, job)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		}
	}))

	return mock
}

func (m *MockMetaAdsAPI) handleValidation(w http.ResponseWriter, r *http.Request) {
	data, _ := jsonpool.Marshal(map[string]interface{}{
		"id":   "123456789",
		"name": "Test User",
	})
	w.Write(data)
}

func (m *MockMetaAdsAPI) handleCreateAsyncJob(w http.ResponseWriter, r *http.Request) {
	jobID := fmt.Sprintf("job_%d", time.Now().UnixNano())

	m.mu.Lock()
	m.asyncJobs[jobID] = &AsyncJobMock{
		ID:              jobID,
		Status:          "Job Running",
		PercentComplete: 0,
		CreatedAt:       time.Now(),
		RecordCount:     len(m.insightsData),
	}
	m.mu.Unlock()

	// Simulate async processing
	go m.processAsyncJob(jobID)

	data, _ := jsonpool.Marshal(map[string]interface{}{
		"report_run_id": jobID,
	})
	w.Write(data)
}

func (m *MockMetaAdsAPI) processAsyncJob(jobID string) {
	// Simulate processing time based on rate limit tier
	processingTime := map[string]time.Duration{
		"development": 10 * time.Second,
		"standard":    5 * time.Second,
		"premium":     2 * time.Second,
	}

	duration := processingTime[m.rateLimitTier]
	steps := 10
	stepDuration := duration / time.Duration(steps)

	for i := 1; i <= steps; i++ {
		time.Sleep(stepDuration)

		m.mu.Lock()
		if job, exists := m.asyncJobs[jobID]; exists {
			job.PercentComplete = i * 10
			if i == steps {
				job.Status = "Job Completed"
				job.CompletedAt = time.Now()
			}
		}
		m.mu.Unlock()
	}
}

func (m *MockMetaAdsAPI) handleJobStatus(w http.ResponseWriter, r *http.Request, job *AsyncJobMock) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	response := map[string]interface{}{
		"id":                       job.ID,
		"async_status":             job.Status,
		"async_percent_completion": job.PercentComplete,
	}

	if job.Status == "Job Completed" {
		response["result_url"] = fmt.Sprintf("%s/v18.0/%s/insights", m.server.URL, job.ID)
	}

	data, _ := jsonpool.Marshal(response)
	w.Write(data)
}

func (m *MockMetaAdsAPI) handleSyncInsights(w http.ResponseWriter, r *http.Request) {
	// Simulate rate limiting based on tier
	latency := map[string]time.Duration{
		"development": 100 * time.Millisecond,
		"standard":    50 * time.Millisecond,
		"premium":     20 * time.Millisecond,
	}

	time.Sleep(latency[m.rateLimitTier])

	// Return paginated results
	limit := 200
	page := 0

	start := page * limit
	end := start + limit
	if end > len(m.insightsData) {
		end = len(m.insightsData)
	}

	response := map[string]interface{}{
		"data": m.insightsData[start:end],
	}

	if end < len(m.insightsData) {
		response["paging"] = map[string]interface{}{
			"next": fmt.Sprintf("%s/v18.0/act_123456789/insights?page=%d", m.server.URL, page+1),
		}
	}

	data, _ := jsonpool.Marshal(response)
	w.Write(data)
}

func (m *MockMetaAdsAPI) handleCampaigns(w http.ResponseWriter, r *http.Request) {
	campaigns := generateMockCampaigns(100)
	data, _ := jsonpool.Marshal(map[string]interface{}{
		"data": campaigns,
	})
	w.Write(data)
}

func (m *MockMetaAdsAPI) handleAdsets(w http.ResponseWriter, r *http.Request) {
	adsets := generateMockAdsets(500)
	data, _ := jsonpool.Marshal(map[string]interface{}{
		"data": adsets,
	})
	w.Write(data)
}

func (m *MockMetaAdsAPI) handleAds(w http.ResponseWriter, r *http.Request) {
	ads := generateMockAds(2000)
	data, _ := jsonpool.Marshal(map[string]interface{}{
		"data": ads,
	})
	w.Write(data)
}

func (m *MockMetaAdsAPI) Close() {
	m.server.Close()
}

func (m *MockMetaAdsAPI) URL() string {
	return m.server.URL
}

// Helper functions to generate mock data

func generateMockInsightsData(count int) []map[string]interface{} {
	data := make([]map[string]interface{}, count)

	for i := 0; i < count; i++ {
		data[i] = map[string]interface{}{
			"account_id":  "act_123456789",
			"campaign_id": fmt.Sprintf("campaign_%d", i%100),
			"adset_id":    fmt.Sprintf("adset_%d", i%500),
			"ad_id":       fmt.Sprintf("ad_%d", i),
			"date_start":  time.Now().AddDate(0, 0, -30).Format("2006-01-02"),
			"date_stop":   time.Now().AddDate(0, 0, -29).Format("2006-01-02"),
			"impressions": 1000 + i*10,
			"clicks":      10 + i%50,
			"spend":       float64(50 + i%100),
			"reach":       800 + i*5,
			"frequency":   1.2 + float64(i%10)/10,
			"cpm":         5.5 + float64(i%20)/10,
			"cpc":         0.5 + float64(i%30)/10,
			"ctr":         float64(1+i%5) / 100,
		}
	}

	return data
}

func generateMockCampaigns(count int) []map[string]interface{} {
	campaigns := make([]map[string]interface{}, count)

	for i := 0; i < count; i++ {
		campaigns[i] = map[string]interface{}{
			"id":           fmt.Sprintf("campaign_%d", i),
			"name":         fmt.Sprintf("Campaign %d", i),
			"status":       "ACTIVE",
			"objective":    "CONVERSIONS",
			"created_time": time.Now().AddDate(0, -1, 0).Format(time.RFC3339),
			"daily_budget": 1000 + i*10,
		}
	}

	return campaigns
}

func generateMockAdsets(count int) []map[string]interface{} {
	adsets := make([]map[string]interface{}, count)

	for i := 0; i < count; i++ {
		adsets[i] = map[string]interface{}{
			"id":           fmt.Sprintf("adset_%d", i),
			"name":         fmt.Sprintf("Ad Set %d", i),
			"campaign_id":  fmt.Sprintf("campaign_%d", i%100),
			"status":       "ACTIVE",
			"created_time": time.Now().AddDate(0, -1, 0).Format(time.RFC3339),
			"daily_budget": 100 + i*5,
		}
	}

	return adsets
}

func generateMockAds(count int) []map[string]interface{} {
	ads := make([]map[string]interface{}, count)

	for i := 0; i < count; i++ {
		ads[i] = map[string]interface{}{
			"id":           fmt.Sprintf("ad_%d", i),
			"name":         fmt.Sprintf("Ad %d", i),
			"adset_id":     fmt.Sprintf("adset_%d", i%500),
			"campaign_id":  fmt.Sprintf("campaign_%d", i%100),
			"status":       "ACTIVE",
			"created_time": time.Now().AddDate(0, -1, 0).Format(time.RFC3339),
		}
	}

	return ads
}

// Benchmarks

// BenchmarkMetaAdsAsyncInsights benchmarks async insights performance
func BenchmarkMetaAdsAsyncInsights(b *testing.B) {
	tiers := []string{"development", "standard", "premium"}

	for _, tier := range tiers {
		b.Run(fmt.Sprintf("Tier_%s", tier), func(b *testing.B) {
			mockAPI := NewMockMetaAdsAPI(tier)
			defer mockAPI.Close()

			ctx := context.Background()

			config := &core.Config{
				Properties: map[string]interface{}{
					"access_token":    "test_token",
					"account_id":      "act_123456789",
					"object_type":     "insights",
					"async_reports":   true,
					"rate_limit_tier": tier,
					"api_base_url":    mockAPI.URL(),
					"fields": []string{
						"impressions", "clicks", "spend", "reach",
						"frequency", "cpm", "cpc", "ctr",
					},
				},
			}

			b.ResetTimer()

			totalRecords := int64(0)
			totalTime := int64(0)

			for i := 0; i < b.N; i++ {
				start := time.Now()

				source, err := sources.NewMetaAdsSource("test", config)
				if err != nil {
					b.Fatal(err)
				}

				if err := source.Initialize(ctx, config); err != nil {
					b.Fatal(err)
				}

				stream, err := source.Read(ctx)
				if err != nil {
					b.Fatal(err)
				}

				recordCount := int64(0)

				// Consume stream
				done := make(chan bool)
				go func() {
					for records := range stream.Records {
						atomic.AddInt64(&recordCount, int64(len(records)))
					}
					done <- true
				}()

				go func() {
					for range stream.Errors {
						// Discard errors
					}
				}()

				<-done

				elapsed := time.Since(start)
				atomic.AddInt64(&totalRecords, recordCount)
				atomic.AddInt64(&totalTime, elapsed.Nanoseconds())

				source.Close()
			}

			// Calculate metrics
			avgTimeNs := totalTime / int64(b.N)
			avgRecords := totalRecords / int64(b.N)
			throughput := float64(avgRecords) / (float64(avgTimeNs) / 1e9)

			b.ReportMetric(throughput, "records/sec")
			b.ReportMetric(float64(avgRecords), "records/op")
			b.ReportMetric(float64(mockAPI.requestCount)/float64(b.N), "api_calls/op")

			// Rate limit constraints
			limits := sources.MetaAdsRateLimits[tier]
			maxThroughput := float64(limits.CallsPerHour) / 3600.0 * 200 // 200 records per call

			b.Logf("Tier: %s", tier)
			b.Logf("Throughput: %.0f records/sec", throughput)
			b.Logf("Max theoretical: %.0f records/sec", maxThroughput)
			b.Logf("Efficiency: %.1f%%", (throughput/maxThroughput)*100)
		})
	}
}

// BenchmarkMetaAdsSyncInsights benchmarks synchronous insights performance
func BenchmarkMetaAdsSyncInsights(b *testing.B) {
	mockAPI := NewMockMetaAdsAPI("standard")
	defer mockAPI.Close()

	ctx := context.Background()

	config := &core.Config{
		Properties: map[string]interface{}{
			"access_token":    "test_token",
			"account_id":      "act_123456789",
			"object_type":     "insights",
			"async_reports":   false,
			"rate_limit_tier": "standard",
			"api_base_url":    mockAPI.URL(),
			"batch_size":      200,
			"fields": []string{
				"impressions", "clicks", "spend", "reach",
			},
		},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		source, _ := sources.NewMetaAdsSource("test", config)
		source.Initialize(ctx, config)

		stream, _ := source.Read(ctx)

		recordCount := 0
		done := make(chan bool)

		go func() {
			for records := range stream.Records {
				recordCount += len(records)
			}
			done <- true
		}()

		go func() {
			for range stream.Errors {
			}
		}()

		<-done
		source.Close()

		b.ReportMetric(float64(recordCount), "records")
	}
}

// BenchmarkMetaAdsObjectTypes benchmarks different object types
func BenchmarkMetaAdsObjectTypes(b *testing.B) {
	objectTypes := []struct {
		name          string
		objectType    string
		expectedCount int
	}{
		{"Campaigns", "campaigns", 100},
		{"AdSets", "adsets", 500},
		{"Ads", "ads", 2000},
	}

	for _, obj := range objectTypes {
		b.Run(obj.name, func(b *testing.B) {
			mockAPI := NewMockMetaAdsAPI("standard")
			defer mockAPI.Close()

			ctx := context.Background()

			config := &core.Config{
				Properties: map[string]interface{}{
					"access_token": "test_token",
					"account_id":   "act_123456789",
					"object_type":  obj.objectType,
					"api_base_url": mockAPI.URL(),
					"fields": []string{
						"id", "name", "status", "created_time",
					},
				},
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				source, _ := sources.NewMetaAdsSource("test", config)
				source.Initialize(ctx, config)

				stream, _ := source.Read(ctx)

				recordCount := 0
				done := make(chan bool)

				go func() {
					for records := range stream.Records {
						recordCount += len(records)
					}
					done <- true
				}()

				go func() {
					for range stream.Errors {
					}
				}()

				<-done
				source.Close()

				if recordCount != obj.expectedCount {
					b.Errorf("Expected %d records, got %d", obj.expectedCount, recordCount)
				}
			}
		})
	}
}

// BenchmarkMetaAdsRateLimiting benchmarks rate limiting performance
func BenchmarkMetaAdsRateLimiting(b *testing.B) {
	tiers := []struct {
		name         string
		tier         string
		callsPerHour int
	}{
		{"Development", "development", 200},
		{"Standard", "standard", 600},
		{"Premium", "premium", 3600},
	}

	for _, tier := range tiers {
		b.Run(tier.name, func(b *testing.B) {
			limiter := clients.NewTokenBucketRateLimiter(
				float64(tier.callsPerHour)/3600.0,
				sources.MetaAdsRateLimits[tier.tier].BurstSize,
			)

			allowed := int64(0)
			blocked := int64(0)

			b.ResetTimer()

			start := time.Now()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if limiter.Allow() {
						atomic.AddInt64(&allowed, 1)
					} else {
						atomic.AddInt64(&blocked, 1)
					}
				}
			})

			elapsed := time.Since(start)
			actualRate := float64(allowed) / elapsed.Seconds()
			expectedRate := float64(tier.callsPerHour) / 3600.0

			b.ReportMetric(actualRate, "actual_rate/sec")
			b.ReportMetric(expectedRate, "expected_rate/sec")
			b.ReportMetric(float64(allowed), "allowed")
			b.ReportMetric(float64(blocked), "blocked")
			b.ReportMetric(actualRate/expectedRate*100, "% of expected")
		})
	}
}

// TestMetaAdsPerformanceSummary summarizes Meta Ads connector performance
func TestMetaAdsPerformanceSummary(t *testing.T) {
	t.Log("\n=== Meta Ads Connector Performance Summary ===")

	t.Log("\nRate Limit Tiers:")
	for tier, limits := range sources.MetaAdsRateLimits {
		maxThroughput := float64(limits.CallsPerHour) / 3600.0 * 200
		t.Logf("- %s: %d calls/hour = %.0f records/sec max",
			tier, limits.CallsPerHour, maxThroughput)
	}

	t.Log("\nAsync vs Sync Performance:")
	t.Log("- Async Reports: Better for large datasets (10K+ records)")
	t.Log("- Sync Requests: Better for real-time, smaller datasets")

	t.Log("\nKey Findings:")
	t.Log("- Async jobs take 2-10 seconds to complete")
	t.Log("- Rate limiting is the primary bottleneck")
	t.Log("- Premium tier offers 18x more throughput than development")

	t.Log("\nOptimizations:")
	t.Log("- Use async reports for insights")
	t.Log("- Batch multiple metrics in single request")
	t.Log("- Cache frequently accessed data")
	t.Log("- Use appropriate rate limit tier")

	t.Log("\nRealistic Throughput:")
	t.Log("- Development: ~11 records/sec")
	t.Log("- Standard: ~33 records/sec")
	t.Log("- Premium: ~200 records/sec")
}
