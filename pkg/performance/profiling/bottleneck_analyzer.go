package profiling

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ajitpratap0/nebula/pkg/errors"
	"go.uber.org/zap"
)

// BottleneckType represents the type of bottleneck detected
type BottleneckType string

const (
	CPUBottleneck         BottleneckType = "cpu"
	MemoryBottleneck      BottleneckType = "memory"
	IOBottleneck          BottleneckType = "io"
	ConcurrencyBottleneck BottleneckType = "concurrency"
	GCBottleneck          BottleneckType = "gc"
)

// Bottleneck represents a detected performance bottleneck
type Bottleneck struct {
	Type        BottleneckType
	Severity    string // "critical", "high", "medium", "low"
	Component   string
	Description string
	Impact      float64 // Percentage impact on performance
	Suggestions []string
	Details     map[string]interface{}
}

// BottleneckAnalyzer analyzes profiles to identify bottlenecks
type BottleneckAnalyzer struct {
	logger      *zap.Logger
	profileDir  string
	bottlenecks []Bottleneck
}

// AnalysisResult contains the complete analysis results
type AnalysisResult struct {
	Timestamp       time.Time
	Bottlenecks     []Bottleneck
	Summary         string
	Metrics         *RuntimeMetrics
	Recommendations []string
}

// NewBottleneckAnalyzer creates a new bottleneck analyzer
func NewBottleneckAnalyzer(profileDir string, logger *zap.Logger) *BottleneckAnalyzer {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &BottleneckAnalyzer{
		logger:      logger,
		profileDir:  profileDir,
		bottlenecks: make([]Bottleneck, 0),
	}
}

// Analyze performs comprehensive bottleneck analysis
func (b *BottleneckAnalyzer) Analyze(metrics *RuntimeMetrics) (*AnalysisResult, error) {
	b.bottlenecks = make([]Bottleneck, 0)

	// Analyze CPU usage
	if err := b.analyzeCPU(); err != nil {
		b.logger.Error("failed to analyze CPU profile", zap.Error(err))
	}

	// Analyze memory usage
	b.analyzeMemory(metrics)

	// Analyze GC pressure
	b.analyzeGC(metrics)

	// Analyze concurrency
	b.analyzeConcurrency(metrics)

	// Sort bottlenecks by severity
	b.sortBottlenecks()

	// Generate recommendations
	recommendations := b.generateRecommendations()

	result := &AnalysisResult{
		Timestamp:       time.Now(),
		Bottlenecks:     b.bottlenecks,
		Summary:         b.generateSummary(),
		Metrics:         metrics,
		Recommendations: recommendations,
	}

	// Save analysis report
	if err := b.saveAnalysisReport(result); err != nil {
		b.logger.Error("failed to save analysis report", zap.Error(err))
	}

	return result, nil
}

// analyzeCPU analyzes CPU profile for hotspots
func (b *BottleneckAnalyzer) analyzeCPU() error {
	// Find the most recent CPU profile
	cpuProfile := b.findLatestProfile("cpu")
	if cpuProfile == "" {
		return nil // No CPU profile available
	}

	file, err := os.Open(cpuProfile)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeInternal, "failed to open CPU profile")
	}
	defer file.Close() // Ignore close error

	// Parse is from the profile package, not pprof
	// For now, we'll skip detailed CPU analysis
	// In a real implementation, you'd parse the profile file properly

	// For now, return early as we need proper profile parsing
	// This is a simplified implementation
	// In production, you would parse the profile file properly
	return nil
}

// analyzeMemory analyzes memory usage patterns
func (b *BottleneckAnalyzer) analyzeMemory(metrics *RuntimeMetrics) {
	if metrics == nil || len(metrics.Samples) < 2 {
		return
	}

	// Calculate memory growth rate
	firstSample := metrics.Samples[0]
	lastSample := metrics.Samples[len(metrics.Samples)-1]
	duration := lastSample.Timestamp.Sub(firstSample.Timestamp)

	if duration > 0 {
		memGrowthMB := float64(lastSample.AllocBytes-firstSample.AllocBytes) / (1024 * 1024)
		memGrowthRate := memGrowthMB / duration.Seconds() // MB/s

		// Check for memory leak
		if memGrowthRate > 1.0 { // Growing more than 1MB/s
			severity := "medium"
			if memGrowthRate > 10.0 {
				severity = "critical"
			} else if memGrowthRate > 5.0 {
				severity = "high"
			}

			bottleneck := Bottleneck{
				Type:        MemoryBottleneck,
				Severity:    severity,
				Component:   "memory_allocator",
				Description: fmt.Sprintf("Memory growing at %.2f MB/s - potential memory leak", memGrowthRate),
				Impact:      memGrowthRate * 10, // Arbitrary impact score
				Suggestions: []string{
					"Profile heap allocations to identify leak source",
					"Check for unbounded data structures",
					"Ensure proper cleanup of resources",
					"Use object pools for frequently allocated objects",
				},
				Details: map[string]interface{}{
					"growth_rate_mb_per_s": memGrowthRate,
					"total_growth_mb":      memGrowthMB,
					"duration":             duration.String(),
				},
			}

			b.bottlenecks = append(b.bottlenecks, bottleneck)
		}

		// Check for high memory usage
		currentMemMB := float64(metrics.AllocBytes) / (1024 * 1024)
		systemMemMB := float64(metrics.SysBytes) / (1024 * 1024)

		if currentMemMB > 1000 { // Using more than 1GB
			severity := "medium"
			if currentMemMB > 5000 {
				severity = "critical"
			} else if currentMemMB > 2000 {
				severity = "high"
			}

			bottleneck := Bottleneck{
				Type:        MemoryBottleneck,
				Severity:    severity,
				Component:   "heap",
				Description: fmt.Sprintf("High memory usage: %.2f MB allocated, %.2f MB system", currentMemMB, systemMemMB),
				Impact:      currentMemMB / 100, // Normalize impact
				Suggestions: []string{
					"Optimize data structures",
					"Use streaming instead of loading all data in memory",
					"Implement pagination for large datasets",
					"Consider using memory-mapped files",
				},
				Details: map[string]interface{}{
					"alloc_mb":       currentMemMB,
					"sys_mb":         systemMemMB,
					"total_alloc_mb": float64(metrics.TotalAllocBytes) / (1024 * 1024),
				},
			}

			b.bottlenecks = append(b.bottlenecks, bottleneck)
		}
	}
}

// analyzeGC analyzes garbage collection pressure
func (b *BottleneckAnalyzer) analyzeGC(metrics *RuntimeMetrics) {
	if metrics == nil {
		return
	}

	// Calculate GC overhead
	if metrics.NumGC > 0 && len(metrics.Samples) > 0 {
		totalTime := metrics.Samples[len(metrics.Samples)-1].Timestamp.Sub(metrics.Samples[0].Timestamp)
		if totalTime > 0 {
			gcOverhead := float64(metrics.GCPauseTotal) / float64(totalTime) * 100

			if gcOverhead > 5.0 { // GC taking more than 5% of time
				severity := "medium"
				if gcOverhead > 20.0 {
					severity = "critical"
				} else if gcOverhead > 10.0 {
					severity = "high"
				}

				bottleneck := Bottleneck{
					Type:        GCBottleneck,
					Severity:    severity,
					Component:   "garbage_collector",
					Description: fmt.Sprintf("GC overhead %.1f%% - excessive garbage collection", gcOverhead),
					Impact:      gcOverhead,
					Suggestions: []string{
						"Reduce allocation rate",
						"Use object pools",
						"Pre-allocate slices with appropriate capacity",
						"Consider using value types instead of pointers where appropriate",
						"Profile allocations to identify high-frequency allocators",
					},
					Details: map[string]interface{}{
						"gc_overhead_percent": gcOverhead,
						"num_gc":              metrics.NumGC,
						"gc_pause_total_ms":   metrics.GCPauseTotal.Milliseconds(),
						"gc_pause_last_ms":    metrics.GCPauseLast.Milliseconds(),
					},
				}

				b.bottlenecks = append(b.bottlenecks, bottleneck)
			}
		}
	}
}

// analyzeConcurrency analyzes concurrency issues
func (b *BottleneckAnalyzer) analyzeConcurrency(metrics *RuntimeMetrics) {
	if metrics == nil {
		return
	}

	// Check for goroutine leaks
	if len(metrics.Samples) > 10 {
		// Calculate goroutine growth rate
		firstSample := metrics.Samples[0]
		lastSample := metrics.Samples[len(metrics.Samples)-1]
		goroutineGrowth := lastSample.NumGoroutines - firstSample.NumGoroutines

		if goroutineGrowth > 100 {
			severity := "medium"
			if goroutineGrowth > 1000 {
				severity = "critical"
			} else if goroutineGrowth > 500 {
				severity = "high"
			}

			bottleneck := Bottleneck{
				Type:        ConcurrencyBottleneck,
				Severity:    severity,
				Component:   "goroutine_management",
				Description: fmt.Sprintf("Goroutine leak detected: %d new goroutines", goroutineGrowth),
				Impact:      float64(goroutineGrowth) / 10,
				Suggestions: []string{
					"Ensure all goroutines have proper exit conditions",
					"Use context for goroutine lifecycle management",
					"Implement worker pools instead of spawning unlimited goroutines",
					"Add timeouts to blocking operations",
				},
				Details: map[string]interface{}{
					"goroutine_growth":   goroutineGrowth,
					"current_goroutines": lastSample.NumGoroutines,
				},
			}

			b.bottlenecks = append(b.bottlenecks, bottleneck)
		}
	}

	// Check for CPU underutilization
	if metrics.NumCPU > 1 && metrics.NumGoroutines < metrics.NumCPU {
		bottleneck := Bottleneck{
			Type:        ConcurrencyBottleneck,
			Severity:    "low",
			Component:   "parallelism",
			Description: fmt.Sprintf("CPU underutilization: %d goroutines on %d CPUs", metrics.NumGoroutines, metrics.NumCPU),
			Impact:      float64(metrics.NumCPU-metrics.NumGoroutines) / float64(metrics.NumCPU) * 100,
			Suggestions: []string{
				"Increase parallelism for CPU-bound operations",
				"Use worker pools sized to CPU count",
				"Consider data partitioning for parallel processing",
			},
			Details: map[string]interface{}{
				"num_cpu":        metrics.NumCPU,
				"num_goroutines": metrics.NumGoroutines,
				"gomaxprocs":     metrics.GOMAXPROCS,
			},
		}

		b.bottlenecks = append(b.bottlenecks, bottleneck)
	}
}

// getCPUSuggestions returns suggestions for CPU bottlenecks
func (b *BottleneckAnalyzer) getCPUSuggestions(function string, cpuPercent float64) []string {
	suggestions := []string{
		"Profile the specific function to identify optimization opportunities",
		"Consider algorithmic improvements",
		"Check for unnecessary computations in loops",
	}

	// Function-specific suggestions
	if strings.Contains(function, "json") || strings.Contains(function, "Marshal") {
		suggestions = append(suggestions,
			"Consider using a faster JSON library (sonic, jsoniter)",
			"Use streaming JSON processing for large data")
	}

	if strings.Contains(function, "sort") {
		suggestions = append(suggestions,
			"Check if sorting is necessary for all operations",
			"Consider using a more efficient sorting algorithm",
			"Pre-sort data where possible")
	}

	if strings.Contains(function, "regex") || strings.Contains(function, "Regexp") {
		suggestions = append(suggestions,
			"Cache compiled regular expressions",
			"Consider simpler string operations if regex is overkill")
	}

	if cpuPercent > 30.0 {
		suggestions = append(suggestions,
			"Consider parallelizing this operation",
			"Implement caching for expensive computations")
	}

	return suggestions
}

// sortBottlenecks sorts bottlenecks by severity and impact
func (b *BottleneckAnalyzer) sortBottlenecks() {
	severityOrder := map[string]int{
		"critical": 0,
		"high":     1,
		"medium":   2,
		"low":      3,
	}

	sort.Slice(b.bottlenecks, func(i, j int) bool {
		if severityOrder[b.bottlenecks[i].Severity] != severityOrder[b.bottlenecks[j].Severity] {
			return severityOrder[b.bottlenecks[i].Severity] < severityOrder[b.bottlenecks[j].Severity]
		}
		return b.bottlenecks[i].Impact > b.bottlenecks[j].Impact
	})
}

// generateSummary creates a summary of the analysis
func (b *BottleneckAnalyzer) generateSummary() string {
	if len(b.bottlenecks) == 0 {
		return "No significant bottlenecks detected. System is performing well."
	}

	criticalCount := 0
	highCount := 0
	for _, bottleneck := range b.bottlenecks {
		switch bottleneck.Severity {
		case "critical":
			criticalCount++
		case "high":
			highCount++
		}
	}

	summary := fmt.Sprintf("Found %d bottlenecks: ", len(b.bottlenecks))
	if criticalCount > 0 {
		summary += fmt.Sprintf("%d critical, ", criticalCount)
	}
	if highCount > 0 {
		summary += fmt.Sprintf("%d high severity, ", highCount)
	}

	// Add primary bottleneck
	if len(b.bottlenecks) > 0 {
		primary := b.bottlenecks[0]
		summary += fmt.Sprintf("\nPrimary bottleneck: %s (%s)", primary.Description, primary.Type)
	}

	return summary
}

// generateRecommendations creates prioritized recommendations
func (b *BottleneckAnalyzer) generateRecommendations() []string {
	recommendations := make([]string, 0)
	seen := make(map[string]bool)

	// Add unique recommendations from critical and high severity bottlenecks
	for _, bottleneck := range b.bottlenecks {
		if bottleneck.Severity == "critical" || bottleneck.Severity == "high" {
			for _, suggestion := range bottleneck.Suggestions {
				if !seen[suggestion] {
					recommendations = append(recommendations, suggestion)
					seen[suggestion] = true
				}
			}
		}
	}

	// Add general recommendations if needed
	if len(recommendations) == 0 {
		recommendations = append(recommendations,
			"Continue monitoring performance metrics",
			"Establish performance baselines",
			"Set up continuous profiling",
		)
	}

	return recommendations
}

// findLatestProfile finds the most recent profile of a given type
func (b *BottleneckAnalyzer) findLatestProfile(profileType string) string {
	// This is a simplified implementation
	// In production, you'd scan the directory for matching files
	matches, _ := os.ReadDir(b.profileDir)

	var latestFile string
	var latestTime time.Time

	for _, entry := range matches {
		if strings.HasPrefix(entry.Name(), profileType+"_") && strings.HasSuffix(entry.Name(), ".prof") {
			info, err := entry.Info()
			if err == nil && info.ModTime().After(latestTime) {
				latestTime = info.ModTime()
				latestFile = fmt.Sprintf("%s/%s", b.profileDir, entry.Name())
			}
		}
	}

	return latestFile
}

// saveAnalysisReport saves the analysis results to a file
func (b *BottleneckAnalyzer) saveAnalysisReport(result *AnalysisResult) error {
	filename := fmt.Sprintf("%s/bottleneck_analysis_%s.txt", b.profileDir, time.Now().Format("20060102_150405"))
	file, err := os.Create(filename)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeInternal, "failed to create analysis report")
	}
	defer file.Close() // Ignore close error

	fmt.Fprintf(file, "Nebula Performance Bottleneck Analysis\n")
	fmt.Fprintf(file, "=====================================\n\n")
	fmt.Fprintf(file, "Timestamp: %s\n", result.Timestamp.Format(time.RFC3339))
	fmt.Fprintf(file, "Summary: %s\n\n", result.Summary)

	if len(result.Bottlenecks) > 0 {
		fmt.Fprintf(file, "Bottlenecks:\n")
		fmt.Fprintf(file, "-----------\n\n")

		for i, bottleneck := range result.Bottlenecks {
			fmt.Fprintf(file, "%d. [%s] %s\n", i+1, strings.ToUpper(bottleneck.Severity), bottleneck.Description)
			fmt.Fprintf(file, "   Type: %s\n", bottleneck.Type)
			fmt.Fprintf(file, "   Component: %s\n", bottleneck.Component)
			fmt.Fprintf(file, "   Impact: %.1f%%\n", bottleneck.Impact)

			if len(bottleneck.Suggestions) > 0 {
				fmt.Fprintf(file, "   Suggestions:\n")
				for _, suggestion := range bottleneck.Suggestions {
					fmt.Fprintf(file, "   - %s\n", suggestion)
				}
			}

			if len(bottleneck.Details) > 0 {
				fmt.Fprintf(file, "   Details:\n")
				for key, value := range bottleneck.Details {
					fmt.Fprintf(file, "   - %s: %v\n", key, value)
				}
			}
			fmt.Fprintf(file, "\n")
		}
	}

	if len(result.Recommendations) > 0 {
		fmt.Fprintf(file, "Prioritized Recommendations:\n")
		fmt.Fprintf(file, "---------------------------\n")
		for i, rec := range result.Recommendations {
			fmt.Fprintf(file, "%d. %s\n", i+1, rec)
		}
	}

	b.logger.Info("bottleneck analysis report saved", zap.String("file", filename))
	return nil
}
