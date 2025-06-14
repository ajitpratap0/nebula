package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// IntegrationTestSuite provides base functionality for integration tests
type IntegrationTestSuite struct {
	suite.Suite
	ctx       context.Context
	cancel    context.CancelFunc
	tempDir   string
	startTime time.Time
}

// SetupSuite runs before all tests in the suite
func (s *IntegrationTestSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
	s.startTime = time.Now()

	// Create temp directory for test files
	tempDir, err := os.MkdirTemp("", "nebula-test-*")
	require.NoError(s.T(), err)
	s.tempDir = tempDir

	s.T().Logf("Integration test suite started in %s", s.tempDir)
}

// TearDownSuite runs after all tests in the suite
func (s *IntegrationTestSuite) TearDownSuite() {
	s.cancel()

	// Clean up temp directory
	if s.tempDir != "" {
		os.RemoveAll(s.tempDir)
	}

	duration := time.Since(s.startTime)
	s.T().Logf("Integration test suite completed in %v", duration)
}

// Context returns the test context
func (s *IntegrationTestSuite) Context() context.Context {
	return s.ctx
}

// TempDir returns the temporary directory path
func (s *IntegrationTestSuite) TempDir() string {
	return s.tempDir
}

// CreateTempFile creates a temporary file with content
func (s *IntegrationTestSuite) CreateTempFile(name string, content []byte) string {
	path := filepath.Join(s.tempDir, name)
	err := os.WriteFile(path, content, 0644)
	require.NoError(s.T(), err)
	return path
}

// IntegrationTest marks a test as an integration test
func IntegrationTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
}

// TestEnvironment represents a test environment
type TestEnvironment struct {
	t       *testing.T
	ctx     context.Context
	cancel  context.CancelFunc
	tempDir string
	cleanup []func()
}

// NewTestEnvironment creates a new test environment
func NewTestEnvironment(t *testing.T) *TestEnvironment {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	tempDir, err := os.MkdirTemp("", "nebula-test-*")
	require.NoError(t, err)

	env := &TestEnvironment{
		t:       t,
		ctx:     ctx,
		cancel:  cancel,
		tempDir: tempDir,
		cleanup: []func(){},
	}

	// Add cleanup for temp directory
	env.AddCleanup(func() {
		os.RemoveAll(tempDir)
	})

	return env
}

// Context returns the test context
func (e *TestEnvironment) Context() context.Context {
	return e.ctx
}

// TempDir returns the temporary directory
func (e *TestEnvironment) TempDir() string {
	return e.tempDir
}

// AddCleanup adds a cleanup function to be called during teardown
func (e *TestEnvironment) AddCleanup(fn func()) {
	e.cleanup = append(e.cleanup, fn)
}

// Cleanup runs all cleanup functions
func (e *TestEnvironment) Cleanup() {
	e.cancel()

	// Run cleanup in reverse order
	for i := len(e.cleanup) - 1; i >= 0; i-- {
		e.cleanup[i]()
	}
}

// CreateTestData creates test data files
func CreateTestData(t *testing.T, dir string, numFiles int, recordsPerFile int) []string {
	t.Helper()

	var files []string

	for i := 0; i < numFiles; i++ {
		filename := filepath.Join(dir, fmt.Sprintf("test_data_%d.csv", i))
		file, err := os.Create(filename)
		require.NoError(t, err)

		// Write header
		_, err = file.WriteString("id,name,value,timestamp\n")
		require.NoError(t, err)

		// Write records
		for j := 0; j < recordsPerFile; j++ {
			record := fmt.Sprintf("%d,Record_%d_%d,%.2f,%s\n",
				i*recordsPerFile+j,
				i, j,
				float64(j)*1.23,
				time.Now().Format(time.RFC3339),
			)
			_, err = file.WriteString(record)
			require.NoError(t, err)
		}

		file.Close()
		files = append(files, filename)
	}

	return files
}

// PerformanceTest provides utilities for performance testing
type PerformanceTest struct {
	t         *testing.T
	name      string
	threshold struct {
		minThroughput float64 // records/sec
		maxLatency    time.Duration
		maxMemory     int64 // bytes
	}
}

// NewPerformanceTest creates a new performance test
func NewPerformanceTest(t *testing.T, name string) *PerformanceTest {
	return &PerformanceTest{
		t:    t,
		name: name,
	}
}

// WithThroughputTarget sets minimum throughput requirement
func (p *PerformanceTest) WithThroughputTarget(recordsPerSec float64) *PerformanceTest {
	p.threshold.minThroughput = recordsPerSec
	return p
}

// WithLatencyTarget sets maximum latency requirement
func (p *PerformanceTest) WithLatencyTarget(maxLatency time.Duration) *PerformanceTest {
	p.threshold.maxLatency = maxLatency
	return p
}

// WithMemoryTarget sets maximum memory usage
func (p *PerformanceTest) WithMemoryTarget(maxBytes int64) *PerformanceTest {
	p.threshold.maxMemory = maxBytes
	return p
}

// Run executes the performance test
func (p *PerformanceTest) Run(fn func() (recordsProcessed int64, duration time.Duration)) {
	p.t.Helper()

	// Capture initial memory
	initialMem := CaptureMemoryProfile()

	// Run the test
	records, duration := fn()

	// Calculate metrics
	throughput := float64(records) / duration.Seconds()
	avgLatency := duration / time.Duration(records)

	// Capture final memory
	finalMem := CaptureMemoryProfile()
	memoryUsed := int64(finalMem.AllocBytes - initialMem.AllocBytes)

	// Log results
	p.t.Logf("Performance Test: %s", p.name)
	p.t.Logf("  Records: %d", records)
	p.t.Logf("  Duration: %v", duration)
	p.t.Logf("  Throughput: %.0f records/sec", throughput)
	p.t.Logf("  Avg Latency: %v", avgLatency)
	p.t.Logf("  Memory Used: %s", formatBytes(memoryUsed))

	// Check thresholds
	if p.threshold.minThroughput > 0 && throughput < p.threshold.minThroughput {
		p.t.Errorf("Throughput %.0f records/sec below target %.0f records/sec",
			throughput, p.threshold.minThroughput)
	}

	if p.threshold.maxLatency > 0 && avgLatency > p.threshold.maxLatency {
		p.t.Errorf("Latency %v exceeds target %v", avgLatency, p.threshold.maxLatency)
	}

	if p.threshold.maxMemory > 0 && memoryUsed > p.threshold.maxMemory {
		p.t.Errorf("Memory usage %s exceeds target %s",
			formatBytes(memoryUsed), formatBytes(p.threshold.maxMemory))
	}
}

// MemoryProfile captures memory statistics
type MemoryProfile struct {
	AllocBytes uint64
	TotalAlloc uint64
	Sys        uint64
	Mallocs    uint64
	Frees      uint64
	HeapAlloc  uint64
	HeapSys    uint64
	HeapInuse  uint64
	StackInuse uint64
}

// CaptureMemoryProfile captures current memory profile
func CaptureMemoryProfile() *MemoryProfile {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return &MemoryProfile{
		AllocBytes: m.Alloc,
		TotalAlloc: m.TotalAlloc,
		Sys:        m.Sys,
		Mallocs:    m.Mallocs,
		Frees:      m.Frees,
		HeapAlloc:  m.HeapAlloc,
		HeapSys:    m.HeapSys,
		HeapInuse:  m.HeapInuse,
		StackInuse: m.StackInuse,
	}
}

// formatBytes formats bytes into human-readable string
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
