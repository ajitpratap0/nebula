#!/bin/bash
# Benchmark script for Nebula

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configuration
BENCH_DIR="tests/benchmarks"
RESULTS_DIR="benchmark-results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULTS_FILE="${RESULTS_DIR}/benchmark-${TIMESTAMP}.txt"
BASELINE_FILE="${RESULTS_DIR}/baseline.txt"

# Parse command line arguments
CONNECTOR=""
TARGET_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --connector)
            CONNECTOR="$2"
            shift 2
            ;;
        --target)
            TARGET_ONLY=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# Create results directory
mkdir -p ${RESULTS_DIR}

echo -e "${GREEN}Nebula Performance Benchmark${NC}"
echo "================================"
echo "Timestamp: ${TIMESTAMP}"
echo ""

# Run benchmarks
if [ -n "$CONNECTOR" ]; then
    # Run specific connector benchmark
    echo -e "${YELLOW}Running ${CONNECTOR} benchmarks...${NC}"
    
    if [ "$CONNECTOR" = "google_sheets" ]; then
        if [ "$TARGET_ONLY" = true ]; then
            go test -bench=BenchmarkGoogleSheets50KTarget -benchmem -benchtime=10s -count=3 ./${BENCH_DIR}/... | tee ${RESULTS_FILE}
        else
            go test -bench=BenchmarkGoogleSheets -benchmem -benchtime=10s -count=3 ./${BENCH_DIR}/... | tee ${RESULTS_FILE}
        fi
    fi
else
    # Run all benchmarks
    echo -e "${YELLOW}Running all benchmarks...${NC}"
    go test -bench=. -benchmem -benchtime=10s -count=3 ./${BENCH_DIR}/... | tee ${RESULTS_FILE}
fi

# Create baseline if it doesn't exist
if [ ! -f ${BASELINE_FILE} ]; then
    echo -e "${YELLOW}Creating baseline...${NC}"
    cp ${RESULTS_FILE} ${BASELINE_FILE}
    echo -e "${GREEN}Baseline created at ${BASELINE_FILE}${NC}"
else
    # Compare with baseline
    echo -e "${YELLOW}Comparing with baseline...${NC}"
    
    # Install benchcmp if not available
    if ! command -v benchcmp &> /dev/null; then
        echo "Installing benchcmp..."
        go install golang.org/x/tools/cmd/benchcmp@latest
    fi
    
    # Run comparison
    benchcmp ${BASELINE_FILE} ${RESULTS_FILE} || true
fi

# Generate CPU and memory profiles
echo -e "${YELLOW}Generating profiles...${NC}"
go test -bench=BenchmarkPipelineStages -cpuprofile=${RESULTS_DIR}/cpu-${TIMESTAMP}.prof ./${BENCH_DIR}/...
go test -bench=BenchmarkPipelineStages -memprofile=${RESULTS_DIR}/mem-${TIMESTAMP}.prof ./${BENCH_DIR}/...

echo ""
echo -e "${GREEN}Benchmark complete!${NC}"
echo "Results saved to: ${RESULTS_FILE}"
echo ""
echo "To view CPU profile:"
echo "  go tool pprof ${RESULTS_DIR}/cpu-${TIMESTAMP}.prof"
echo ""
echo "To view memory profile:"
echo "  go tool pprof ${RESULTS_DIR}/mem-${TIMESTAMP}.prof"
echo ""
echo "To update baseline:"
echo "  cp ${RESULTS_FILE} ${BASELINE_FILE}"