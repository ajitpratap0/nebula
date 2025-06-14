#!/bin/bash
# Establish baseline performance metrics

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Establishing Nebula Performance Baseline${NC}"
echo "========================================"
echo ""

# Create directories
mkdir -p benchmark-results
mkdir -p docs/performance

# System information
echo -e "${YELLOW}System Information:${NC}"
echo "OS: $(uname -s)"
echo "Arch: $(uname -m)"
echo "CPUs: $(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 'unknown')"
echo "Go Version: $(go version)"
echo ""

# Run comprehensive benchmarks
echo -e "${YELLOW}Running baseline benchmarks...${NC}"
echo "This may take a few minutes..."
echo ""

# Run benchmarks with multiple settings
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Standard benchmarks
echo "1/4 Running standard benchmarks..."
go test -bench=. -benchmem -benchtime=10s -count=5 ./tests/benchmarks/... > benchmark-results/baseline_standard_${TIMESTAMP}.txt 2>&1

# CPU Profile
echo "2/4 Generating CPU profile..."
go test -bench=BenchmarkPipelineStages -benchtime=10s -cpuprofile=benchmark-results/baseline_cpu_${TIMESTAMP}.prof ./tests/benchmarks/... > /dev/null 2>&1

# Memory Profile
echo "3/4 Generating memory profile..."
go test -bench=BenchmarkPipelineStages -benchtime=10s -memprofile=benchmark-results/baseline_mem_${TIMESTAMP}.prof ./tests/benchmarks/... > /dev/null 2>&1

# Trace
echo "4/4 Generating execution trace..."
go test -bench=BenchmarkPipelineStages -benchtime=1s -trace=benchmark-results/baseline_trace_${TIMESTAMP}.out ./tests/benchmarks/... > /dev/null 2>&1

# Copy as current baseline
cp benchmark-results/baseline_standard_${TIMESTAMP}.txt benchmark-results/baseline.txt

# Generate summary report
echo -e "\n${YELLOW}Generating baseline report...${NC}"

cat > benchmark-results/baseline_report_${TIMESTAMP}.md << EOF
# Nebula Performance Baseline Report

Generated: $(date)

## System Information
- OS: $(uname -s) $(uname -r)
- Architecture: $(uname -m)
- CPUs: $(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 'unknown')
- Go Version: $(go version)
- Commit: $(git rev-parse HEAD 2>/dev/null || echo "unknown")

## Baseline Metrics Summary

### Memory Allocation Benchmarks
\`\`\`
$(grep "BenchmarkMemoryAllocation" benchmark-results/baseline_standard_${TIMESTAMP}.txt | head -10)
\`\`\`

### Channel Throughput Benchmarks
\`\`\`
$(grep "BenchmarkChannelThroughput" benchmark-results/baseline_standard_${TIMESTAMP}.txt | head -10)
\`\`\`

### Pipeline Performance
\`\`\`
$(grep "BenchmarkPipelineStages" benchmark-results/baseline_standard_${TIMESTAMP}.txt | head -5)
\`\`\`

### JSON Parsing Performance
\`\`\`
$(grep "BenchmarkJSONParsing" benchmark-results/baseline_standard_${TIMESTAMP}.txt | head -5)
\`\`\`

## Performance Analysis

### Top Allocators
\`\`\`
$(go tool pprof -top -alloc_space benchmark-results/baseline_mem_${TIMESTAMP}.prof 2>/dev/null | head -15 || echo "Memory profile analysis not available")
\`\`\`

### CPU Hotspots
\`\`\`
$(go tool pprof -top benchmark-results/baseline_cpu_${TIMESTAMP}.prof 2>/dev/null | head -15 || echo "CPU profile analysis not available")
\`\`\`

## Files Generated
- Standard benchmarks: baseline_standard_${TIMESTAMP}.txt
- CPU profile: baseline_cpu_${TIMESTAMP}.prof
- Memory profile: baseline_mem_${TIMESTAMP}.prof
- Execution trace: baseline_trace_${TIMESTAMP}.out

## Next Steps
1. Review the baseline metrics
2. Set performance budgets based on these values
3. Configure CI to track regressions
4. Begin optimization work

---
EOF

# Generate dashboard
echo -e "${YELLOW}Generating performance dashboard...${NC}"
./scripts/generate-performance-dashboard.sh

# Summary
echo ""
echo -e "${GREEN}Baseline establishment complete!${NC}"
echo ""
echo "Results saved to:"
echo "  - benchmark-results/baseline.txt (latest baseline)"
echo "  - benchmark-results/baseline_report_${TIMESTAMP}.md (detailed report)"
echo "  - performance-dashboard.html (visual dashboard)"
echo ""
echo "To view profiles:"
echo "  go tool pprof benchmark-results/baseline_cpu_${TIMESTAMP}.prof"
echo "  go tool pprof -alloc_space benchmark-results/baseline_mem_${TIMESTAMP}.prof"
echo "  go tool trace benchmark-results/baseline_trace_${TIMESTAMP}.out"
echo ""
echo -e "${YELLOW}Recommended next steps:${NC}"
echo "1. Review baseline metrics in the report"
echo "2. Adjust performance budgets in config/performance-budgets.yaml"
echo "3. Commit baseline.txt to track performance over time"
echo "4. Set up CI to run benchmarks on every PR"