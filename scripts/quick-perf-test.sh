#!/bin/bash

# Quick Performance Test Script for Nebula
# Tests basic pipeline performance with different data sizes

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_header() {
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}========================================${NC}"
}

# Generate test data
generate_test_data() {
    local size=$1
    local filename=$2
    
    log_info "Generating test data: $size records -> $filename"
    
    echo "id,name,email,age,department,salary,city,country" > "$filename"
    
    for ((i=1; i<=size; i++)); do
        echo "$i,User$i,user$i@example.com,$((20 + RANDOM % 40)),Dept$((RANDOM % 5)),\$$(( (RANDOM % 50000) + 30000)),City$((RANDOM % 100)),Country$((RANDOM % 10))" >> "$filename"
    done
    
    log_success "Generated $size records in $filename"
}

# Run performance test
run_perf_test() {
    local test_name=$1
    local input_file=$2
    local output_file=$3
    local format=${4:-"array"}
    
    log_info "Running test: $test_name"
    
    # Build if needed
    if [ ! -f "bin/nebula" ]; then
        log_info "Building Nebula binary..."
        go build -o bin/nebula ./cmd/nebula
    fi
    
    # Run the test and capture timing
    start_time=$(date +%s.%N)
    
    ./bin/nebula pipeline csv json \
        --source-path "$input_file" \
        --dest-path "$output_file" \
        --format "$format" > /dev/null 2>&1
    
    end_time=$(date +%s.%N)
    
    # Calculate performance metrics
    duration=$(echo "$end_time - $start_time" | bc)
    record_count=$(wc -l < "$input_file")
    record_count=$((record_count - 1)) # Subtract header
    
    if (( $(echo "$duration > 0" | bc -l) )); then
        throughput=$(echo "scale=2; $record_count / $duration" | bc)
    else
        throughput="N/A"
    fi
    
    # Get file sizes
    input_size=$(du -h "$input_file" | cut -f1)
    output_size=$(du -h "$output_file" | cut -f1)
    
    # Display results
    printf "%-20s | %8d | %10s | %12s | %8s | %8s\n" \
        "$test_name" "$record_count" "${duration}s" "${throughput} rec/s" "$input_size" "$output_size"
}

# Run memory benchmark
run_memory_benchmark() {
    local input_file=$1
    
    log_info "Running memory benchmark with $input_file"
    
    # Build with memory profiling
    go build -o bin/nebula ./cmd/nebula
    
    # Run with memory profiling
    ./bin/nebula pipeline csv json \
        --source-path "$input_file" \
        --dest-path "memory_test_output.json" \
        --format "array" &
    
    # Get the PID
    nebula_pid=$!
    
    # Monitor memory usage
    peak_memory=0
    while kill -0 $nebula_pid 2>/dev/null; do
        if [ "$(uname)" = "Darwin" ]; then
            # macOS
            memory=$(ps -o rss= -p $nebula_pid 2>/dev/null || echo "0")
        else
            # Linux
            memory=$(ps -o rss= -p $nebula_pid 2>/dev/null || echo "0")
        fi
        
        if [ "$memory" -gt "$peak_memory" ]; then
            peak_memory=$memory
        fi
        
        sleep 0.1
    done
    
    # Wait for process to complete
    wait $nebula_pid
    
    # Convert to MB
    peak_memory_mb=$(echo "scale=2; $peak_memory / 1024" | bc)
    
    echo "Peak memory usage: ${peak_memory_mb} MB"
    
    # Clean up
    rm -f memory_test_output.json
}

# Clean up test files
cleanup_test_files() {
    log_info "Cleaning up test files..."
    rm -f test_*.csv test_*.json memory_test_output.json
    log_success "Cleanup completed"
}

# Main performance test suite
run_test_suite() {
    log_header "NEBULA QUICK PERFORMANCE TEST SUITE"
    
    echo "Test Environment:"
    echo "  • Go Version: $(go version)"
    echo "  • OS: $(uname -s) $(uname -m)"
    echo "  • Timestamp: $(date)"
    echo ""
    
    # Test different data sizes
    sizes=(100 1000 10000 50000)
    
    echo "Generating test data..."
    for size in "${sizes[@]}"; do
        generate_test_data $size "test_${size}.csv"
    done
    
    echo ""
    log_header "PERFORMANCE RESULTS"
    
    printf "%-20s | %8s | %10s | %12s | %8s | %8s\n" \
        "Test Name" "Records" "Duration" "Throughput" "Input" "Output"
    echo "--------------------------------------------------------------------------------"
    
    # Run tests
    for size in "${sizes[@]}"; do
        run_perf_test "CSV->JSON ($size)" "test_${size}.csv" "test_${size}.json" "array"
    done
    
    echo ""
    
    # Memory benchmark with largest dataset
    if [ -f "test_50000.csv" ]; then
        log_header "MEMORY BENCHMARK"
        run_memory_benchmark "test_50000.csv"
    fi
    
    echo ""
    log_header "HYBRID STORAGE BENCHMARK"
    
    # Test with different formats
    if [ -f "test_10000.csv" ]; then
        run_perf_test "Array Format" "test_10000.csv" "test_array.json" "array"
        run_perf_test "Lines Format" "test_10000.csv" "test_lines.json" "lines"
    fi
    
    echo ""
    log_success "Performance test suite completed!"
    
    # Show summary
    echo ""
    echo "Summary:"
    echo "  • All tests completed successfully"
    echo "  • Output files generated for inspection"
    echo "  • Performance results shown above"
    echo ""
    echo "Next steps:"
    echo "  • Run 'go test -bench=. ./tests/benchmarks/...' for detailed benchmarks"
    echo "  • Use './scripts/dev-monitor.sh metrics' to see real-time metrics"
    echo "  • Check memory usage with 'go test -bench=. -memprofile=mem.prof'"
}

# Continuous performance monitoring
continuous_perf_monitor() {
    log_info "Starting continuous performance monitoring (Press Ctrl+C to stop)"
    
    # Generate a medium-sized test file
    generate_test_data 5000 "continuous_test.csv"
    
    while true; do
        clear
        echo -e "${GREEN}📊 Continuous Performance Monitor${NC}"
        echo -e "${YELLOW}$(date)${NC}"
        echo ""
        
        echo "Running pipeline test..."
        start_time=$(date +%s.%N)
        
        ./bin/nebula pipeline csv json \
            --source-path "continuous_test.csv" \
            --dest-path "continuous_test.json" \
            --format "array" > /dev/null 2>&1
        
        end_time=$(date +%s.%N)
        duration=$(echo "$end_time - $start_time" | bc)
        throughput=$(echo "scale=2; 5000 / $duration" | bc)
        
        echo -e "${CYAN}Latest Performance:${NC}"
        echo "  • Records: 5,000"
        echo "  • Duration: ${duration}s"
        echo "  • Throughput: ${throughput} records/sec"
        echo ""
        
        # Show memory usage if available
        if command -v ps &> /dev/null; then
            echo -e "${CYAN}System Resources:${NC}"
            echo "  • Memory Available: $(free -h 2>/dev/null | awk '/^Mem:/ {print $7}' || echo 'N/A')"
            echo "  • Load Average: $(uptime | awk -F'load average:' '{print $2}' || echo 'N/A')"
        fi
        
        echo ""
        echo -e "\n${BLUE}Next test in 5 seconds...${NC}"
        sleep 5
    done
}

# Main execution
case "${1:-suite}" in
    "suite"|"")
        run_test_suite
        ;;
    "continuous"|"watch")
        continuous_perf_monitor
        ;;
    "memory")
        if [ -z "$2" ]; then
            generate_test_data 10000 "memory_test.csv"
            run_memory_benchmark "memory_test.csv"
            rm -f memory_test.csv
        else
            run_memory_benchmark "$2"
        fi
        ;;
    "cleanup")
        cleanup_test_files
        ;;
    "quick")
        log_info "Running quick test with 1000 records..."
        generate_test_data 1000 "quick_test.csv"
        run_perf_test "Quick Test" "quick_test.csv" "quick_test.json" "array"
        rm -f quick_test.csv quick_test.json
        ;;
    "help")
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  suite       - Run full performance test suite (default)"
        echo "  continuous  - Continuous performance monitoring"
        echo "  memory      - Memory usage benchmark"
        echo "  quick       - Quick single test"
        echo "  cleanup     - Clean up test files"
        echo "  help        - Show this help"
        ;;
    *)
        log_error "Unknown command: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac