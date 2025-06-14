#!/bin/bash
# Quick test script for Google Sheets 50K records/sec target

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== Google Sheets 50K Records/Sec Performance Test ===${NC}"
echo "Date: $(date)"
echo ""

# Create results directory
mkdir -p benchmark-results

# Run the 50K target benchmark
echo -e "${YELLOW}Running performance test...${NC}"
echo "Target: 50,000 records/second"
echo ""

# Run benchmark with custom binary
if command -v go &> /dev/null; then
    # Build benchmark binary
    echo "Building benchmark tool..."
    go build -o bin/benchmark ./cmd/benchmark
    
    # Run benchmark
    ./bin/benchmark -connector google_sheets -target -count 1 -duration 5s
else
    echo -e "${RED}Go is not installed. Running script-based test...${NC}"
    ./scripts/benchmark.sh --connector google_sheets --target
fi

echo ""
echo -e "${GREEN}Test complete!${NC}"
echo "Check benchmark-results/ directory for detailed results."