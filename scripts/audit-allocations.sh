#!/bin/bash

# Allocation Audit Script for Nebula
# Finds memory allocation hotspots that should use pooling

echo "=== Nebula Memory Allocation Audit ==="
echo "Finding allocation patterns that should use pooling..."
echo ""

# Colors for output
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Audit batch slice allocations
echo -e "${YELLOW}1. Batch Slice Allocations (should use pool.GetBatchSlice)${NC}"
echo "------------------------------------------------"
rg -n "make\(\[\]\*models\.Record" --type go | grep -v "pool.go" | grep -v "_test.go" || echo "None found"
rg -n "make\(\[\]\*pool\.Record" --type go | grep -v "pool.go" | grep -v "_test.go" || echo "None found"
echo ""

# Audit map allocations
echo -e "${YELLOW}2. Map Allocations (should use pool.GetMap)${NC}"
echo "------------------------------------------------"
rg -n "make\(map\[string\]interface\{\}" --type go | grep -v "pool.go" | grep -v "_test.go" || echo "None found"
rg -n "map\[string\]interface\{\}\{" --type go | grep -v "pool.go" | grep -v "_test.go" || echo "None found"
echo ""

# Audit channel allocations
echo -e "${YELLOW}3. Channel Allocations (need new channel pool)${NC}"
echo "------------------------------------------------"
rg -n "make\(chan.*models\.Record" --type go | grep -v "_test.go" || echo "None found"
rg -n "make\(chan.*pool\.Record" --type go | grep -v "_test.go" || echo "None found"
rg -n "make\(chan.*\[\]\*.*Record" --type go | grep -v "_test.go" || echo "None found"
echo ""

# Audit byte slice allocations
echo -e "${YELLOW}4. Byte Slice Allocations (should use pool.GetByteSlice)${NC}"
echo "------------------------------------------------"
rg -n "make\(\[\]byte" --type go | grep -v "pool.go" | grep -v "_test.go" || echo "None found"
rg -n "var buf bytes\.Buffer" --type go | grep -v "_test.go" || echo "None found"
echo ""

# Audit string building
echo -e "${YELLOW}5. String Building (should use pool string builders)${NC}"
echo "------------------------------------------------"
rg -n "fmt\.Sprintf.*record" --type go | grep -v "_test.go" || echo "None found"
rg -n "strings\.Builder\{\}" --type go | grep -v "strings.go" | grep -v "_test.go" || echo "None found"
echo ""

# Check pool usage
echo -e "${GREEN}6. Current Pool Usage (good examples)${NC}"
echo "------------------------------------------------"
rg -n "pool\.Get" --type go | head -10 || echo "None found"
echo ""

# Summary statistics
echo -e "${RED}=== Summary ===${NC}"
echo -n "Total batch allocations: "
rg "make\(\[\]\*.*Record" --type go | grep -v "pool.go" | grep -v "_test.go" | wc -l
echo -n "Total map allocations: "
rg "make\(map\[string\]interface" --type go | grep -v "pool.go" | grep -v "_test.go" | wc -l
echo -n "Total channel allocations: "
rg "make\(chan.*Record" --type go | grep -v "_test.go" | wc -l
echo -n "Total byte slice allocations: "
rg "make\(\[\]byte" --type go | grep -v "pool.go" | grep -v "_test.go" | wc -l

echo ""
echo "Run 'make bench' before and after fixes to measure impact"