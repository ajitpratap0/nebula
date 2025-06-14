#!/bin/bash
# Quick benchmark check for pre-commit

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Running quick benchmark check...${NC}"

# Run only fast benchmarks (1 second time limit)
go test -bench=. -benchtime=1s -run=^$ ./tests/benchmarks/... > /tmp/bench-current.txt 2>&1

if [ $? -ne 0 ]; then
    echo -e "${RED}Benchmarks failed to run!${NC}"
    cat /tmp/bench-current.txt
    exit 1
fi

# Check for any benchmark that takes > 100ms (potential regression)
if grep -E "^\w+.*\s+[0-9]+\s+[0-9]{9,}" /tmp/bench-current.txt > /dev/null; then
    echo -e "${YELLOW}Warning: Some benchmarks are taking > 100ms${NC}"
    grep -E "^\w+.*\s+[0-9]+\s+[0-9]{9,}" /tmp/bench-current.txt
fi

# Check for excessive allocations (> 1000 allocs/op)
if grep -E "\s+[0-9]{4,}\s+allocs/op" /tmp/bench-current.txt > /dev/null; then
    echo -e "${YELLOW}Warning: High allocation count detected${NC}"
    grep -E "\s+[0-9]{4,}\s+allocs/op" /tmp/bench-current.txt
fi

echo -e "${GREEN}Benchmark check passed!${NC}"