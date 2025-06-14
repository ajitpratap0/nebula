#!/bin/bash
# Generate performance dashboard from benchmark results

set -e

# Configuration
RESULTS_DIR="benchmark-results"
OUTPUT_FILE="performance-dashboard.html"
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

# Create results directory if it doesn't exist
mkdir -p ${RESULTS_DIR}

# Run benchmarks and capture output
echo "Running benchmarks..."
go test -bench=. -benchmem -count=3 ./tests/benchmarks/... > ${RESULTS_DIR}/latest.txt 2>&1

# Parse benchmark results
echo "Generating dashboard..."

cat > ${OUTPUT_FILE} << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Nebula Performance Dashboard</title>
    <meta charset="utf-8">
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }
        h1, h2 {
            color: #2c3e50;
        }
        .header {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        .metric-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .metric-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .metric-value {
            font-size: 2em;
            font-weight: bold;
            color: #3498db;
        }
        .metric-label {
            color: #7f8c8d;
            font-size: 0.9em;
            text-transform: uppercase;
        }
        .benchmark-table {
            width: 100%;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th {
            background: #34495e;
            color: white;
            padding: 12px;
            text-align: left;
        }
        td {
            padding: 12px;
            border-bottom: 1px solid #ecf0f1;
        }
        tr:hover {
            background: #f8f9fa;
        }
        .good { color: #27ae60; }
        .warn { color: #f39c12; }
        .bad { color: #e74c3c; }
        .timestamp {
            color: #7f8c8d;
            font-size: 0.9em;
        }
        pre {
            background: #2c3e50;
            color: #ecf0f1;
            padding: 15px;
            border-radius: 4px;
            overflow-x: auto;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🚀 Nebula Performance Dashboard</h1>
        <p class="timestamp">Generated: TIMESTAMP_PLACEHOLDER</p>
    </div>
EOF

# Replace timestamp
sed -i.bak "s/TIMESTAMP_PLACEHOLDER/${TIMESTAMP}/g" ${OUTPUT_FILE} && rm ${OUTPUT_FILE}.bak

# Extract key metrics from benchmark results
if [ -f "${RESULTS_DIR}/latest.txt" ]; then
    # Parse throughput metrics
    THROUGHPUT=$(grep -E "BenchmarkPipelineStages.*ops/s" ${RESULTS_DIR}/latest.txt | awk '{print $(NF-1)}' | head -1 || echo "0")
    LATENCY=$(grep -E "BenchmarkPipelineStages.*ns/op" ${RESULTS_DIR}/latest.txt | awk '{print $(NF-1)}' | head -1 || echo "0")
    ALLOCS=$(grep -E "BenchmarkPipelineStages.*allocs/op" ${RESULTS_DIR}/latest.txt | awk '{print $(NF-1)}' | head -1 || echo "0")
    
    cat >> ${OUTPUT_FILE} << EOF
    <div class="metric-grid">
        <div class="metric-card">
            <div class="metric-label">Pipeline Throughput</div>
            <div class="metric-value">${THROUGHPUT:-N/A}</div>
            <div class="metric-label">operations/sec</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Average Latency</div>
            <div class="metric-value">${LATENCY:-N/A}</div>
            <div class="metric-label">nanoseconds/op</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Allocations</div>
            <div class="metric-value">${ALLOCS:-N/A}</div>
            <div class="metric-label">allocs/op</div>
        </div>
    </div>

    <h2>Benchmark Results</h2>
    <div class="benchmark-table">
        <table>
            <thead>
                <tr>
                    <th>Benchmark</th>
                    <th>Iterations</th>
                    <th>ns/op</th>
                    <th>B/op</th>
                    <th>allocs/op</th>
                </tr>
            </thead>
            <tbody>
EOF

    # Parse all benchmark results
    grep "^Benchmark" ${RESULTS_DIR}/latest.txt | while read line; do
        BENCH_NAME=$(echo $line | awk '{print $1}')
        ITERATIONS=$(echo $line | awk '{print $2}')
        NS_OP=$(echo $line | awk '{print $3}')
        
        # Try to find memory stats on the same line or next line
        B_OP=$(echo $line | grep -o '[0-9]\+ B/op' | awk '{print $1}' || echo "-")
        ALLOCS_OP=$(echo $line | grep -o '[0-9]\+ allocs/op' | awk '{print $1}' || echo "-")
        
        # Determine performance class based on latency
        if [ "$NS_OP" != "-" ] && [ "$NS_OP" -lt 1000 ]; then
            CLASS="good"
        elif [ "$NS_OP" != "-" ] && [ "$NS_OP" -lt 10000 ]; then
            CLASS="warn"
        else
            CLASS="bad"
        fi
        
        cat >> ${OUTPUT_FILE} << EOF
                <tr>
                    <td>${BENCH_NAME}</td>
                    <td>${ITERATIONS}</td>
                    <td class="${CLASS}">${NS_OP}</td>
                    <td>${B_OP}</td>
                    <td>${ALLOCS_OP}</td>
                </tr>
EOF
    done
    
    cat >> ${OUTPUT_FILE} << EOF
            </tbody>
        </table>
    </div>

    <h2>Raw Benchmark Output</h2>
    <pre>$(cat ${RESULTS_DIR}/latest.txt)</pre>
EOF
fi

cat >> ${OUTPUT_FILE} << 'EOF'
    <h2>Performance Targets</h2>
    <div class="benchmark-table">
        <table>
            <thead>
                <tr>
                    <th>Metric</th>
                    <th>Current Target</th>
                    <th>Ultimate Goal</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>Throughput</td>
                    <td>10K records/sec</td>
                    <td>1M+ records/sec</td>
                    <td class="warn">Phase 1</td>
                </tr>
                <tr>
                    <td>P99 Latency</td>
                    <td><10ms</td>
                    <td><1ms</td>
                    <td class="warn">Phase 1</td>
                </tr>
                <tr>
                    <td>Memory/Million Records</td>
                    <td><500MB</td>
                    <td><50MB</td>
                    <td class="warn">Phase 1</td>
                </tr>
                <tr>
                    <td>GC Pause P99</td>
                    <td><10ms</td>
                    <td><1ms</td>
                    <td class="warn">Phase 1</td>
                </tr>
            </tbody>
        </table>
    </div>
</body>
</html>
EOF

echo "Performance dashboard generated: ${OUTPUT_FILE}"
echo "Open in browser: file://$(pwd)/${OUTPUT_FILE}"