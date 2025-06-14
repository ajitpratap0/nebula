#!/bin/bash

# Memory Optimization Benchmark Script
# Measures the memory impact of pooling optimizations

echo "=== Nebula Memory Optimization Benchmark ==="
echo "Date: $(date)"
echo ""

# Create results directory
RESULTS_DIR="benchmark_results/memory_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

# Function to run benchmark and extract memory metrics
run_memory_benchmark() {
    local name=$1
    local bench_pattern=$2
    local output_file="$RESULTS_DIR/${name}.txt"
    
    echo "Running $name benchmark..."
    
    # Run benchmark with memory profiling
    go test -bench="$bench_pattern" -benchmem -benchtime=10s -memprofile="$RESULTS_DIR/${name}_mem.prof" \
        ./tests/benchmarks/... > "$output_file" 2>&1
    
    # Extract key metrics
    echo "Results for $name:"
    grep -E "allocs/op|B/op|ns/op" "$output_file" | tail -5
    echo ""
}

# Run benchmarks
echo "1. Testing Direct Pipeline Performance (Memory Focus)..."
run_memory_benchmark "direct_pipeline" "BenchmarkDirectPipelinePerformance"

echo "2. Testing CSV Processing (Memory Focus)..."
run_memory_benchmark "csv_processing" "BenchmarkCSV"

echo "3. Testing Batch Processing (Memory Focus)..."
run_memory_benchmark "batch_processing" "BenchmarkBatch"

# Generate memory profile analysis
echo "Analyzing memory profiles..."
for prof in "$RESULTS_DIR"/*_mem.prof; do
    if [ -f "$prof" ]; then
        base=$(basename "$prof" _mem.prof)
        echo "Memory profile for $base:"
        go tool pprof -top -nodefraction=0.01 "$prof" | head -20 > "$RESULTS_DIR/${base}_analysis.txt"
        echo "Top allocations saved to ${base}_analysis.txt"
    fi
done

# Calculate memory per record
echo ""
echo "=== Memory Per Record Analysis ==="
echo ""

# Extract records/sec and allocations from direct pipeline benchmark
if [ -f "$RESULTS_DIR/direct_pipeline.txt" ]; then
    records_per_sec=$(grep -o '[0-9.]*M records/sec' "$RESULTS_DIR/direct_pipeline.txt" | grep -o '[0-9.]*' | head -1)
    bytes_per_op=$(grep -o '[0-9]* B/op' "$RESULTS_DIR/direct_pipeline.txt" | grep -o '[0-9]*' | head -1)
    allocs_per_op=$(grep -o '[0-9]* allocs/op' "$RESULTS_DIR/direct_pipeline.txt" | grep -o '[0-9]*' | head -1)
    
    if [ -n "$records_per_sec" ] && [ -n "$bytes_per_op" ]; then
        # Rough calculation of bytes per record
        # Assuming benchmark processes 100K records
        records_in_bench=100000
        bytes_per_record=$(echo "scale=2; $bytes_per_op / $records_in_bench" | bc)
        
        echo "Performance Metrics:"
        echo "- Throughput: ${records_per_sec}M records/sec"
        echo "- Memory allocated: $bytes_per_op bytes/operation"
        echo "- Allocations: $allocs_per_op allocs/operation"
        echo "- Estimated memory per record: $bytes_per_record bytes"
        echo ""
        
        # Check against target
        target_bytes=50
        if [ $(echo "$bytes_per_record < $target_bytes" | bc) -eq 1 ]; then
            echo "✅ MEETING TARGET: $bytes_per_record bytes/record < $target_bytes bytes target"
        else
            reduction_needed=$(echo "scale=1; ($bytes_per_record - $target_bytes) / $bytes_per_record * 100" | bc)
            echo "❌ ABOVE TARGET: Need ${reduction_needed}% reduction to reach $target_bytes bytes/record"
        fi
    fi
fi

# Summary report
echo ""
echo "=== Summary Report ==="
echo "Results saved to: $RESULTS_DIR"
echo ""
echo "Key files generated:"
echo "- direct_pipeline.txt: Direct pipeline benchmark results"
echo "- csv_processing.txt: CSV processing benchmark results"  
echo "- batch_processing.txt: Batch processing benchmark results"
echo "- *_mem.prof: Memory profiles for detailed analysis"
echo "- *_analysis.txt: Top memory allocators"
echo ""
echo "To view detailed memory profile:"
echo "go tool pprof -http=:8080 $RESULTS_DIR/direct_pipeline_mem.prof"