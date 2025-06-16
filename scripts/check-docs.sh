#!/bin/bash

# check-docs.sh - Verify Go documentation coverage
# This script checks for missing documentation on exported types, functions, and packages

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
TOTAL_PACKAGES=0
UNDOCUMENTED_PACKAGES=0
TOTAL_TYPES=0
UNDOCUMENTED_TYPES=0
TOTAL_FUNCTIONS=0
UNDOCUMENTED_FUNCTIONS=0
TOTAL_CONSTANTS=0
UNDOCUMENTED_CONSTANTS=0
TOTAL_VARS=0
UNDOCUMENTED_VARS=0

# Arrays to store findings
declare -a MISSING_PACKAGE_DOCS
declare -a MISSING_TYPE_DOCS
declare -a MISSING_FUNC_DOCS
declare -a MISSING_CONST_DOCS
declare -a MISSING_VAR_DOCS

# Directories to check (exclude vendor, test files, and generated code)
PACKAGES=$(go list ./... | grep -v vendor | grep -v /test | grep -v generated)

echo -e "${BLUE}=== Go Documentation Coverage Check ===${NC}"
echo ""

# Function to check if a line is a doc comment
is_doc_comment() {
    local line="$1"
    [[ "$line" =~ ^[[:space:]]*// ]] || [[ "$line" =~ ^[[:space:]]*/\* ]]
}

# Function to get the previous non-empty line
get_prev_line() {
    local file="$1"
    local line_num="$2"
    local prev_line=""
    local i=$((line_num - 1))
    
    while [ $i -gt 0 ]; do
        prev_line=$(sed -n "${i}p" "$file" | sed 's/^[[:space:]]*//')
        if [ -n "$prev_line" ]; then
            echo "$prev_line"
            return
        fi
        i=$((i - 1))
    done
}

# Check each package
for pkg in $PACKAGES; do
    TOTAL_PACKAGES=$((TOTAL_PACKAGES + 1))
    
    # Get package directory
    pkg_dir=$(go list -f '{{.Dir}}' "$pkg")
    pkg_name=$(basename "$pkg")
    
    # Check for package documentation (doc.go or package comment)
    has_doc=false
    
    # Check for doc.go file
    if [ -f "$pkg_dir/doc.go" ]; then
        if grep -q "^// Package" "$pkg_dir/doc.go" || grep -q "^/\* Package" "$pkg_dir/doc.go"; then
            has_doc=true
        fi
    fi
    
    # If no doc.go, check for package documentation in any .go file
    if [ "$has_doc" = false ]; then
        for file in "$pkg_dir"/*.go; do
            if [ -f "$file" ] && [[ ! "$file" =~ _test\.go$ ]]; then
                if grep -q "^// Package $pkg_name" "$file" || grep -q "^/\* Package $pkg_name" "$file"; then
                    has_doc=true
                    break
                fi
            fi
        done
    fi
    
    if [ "$has_doc" = false ]; then
        UNDOCUMENTED_PACKAGES=$((UNDOCUMENTED_PACKAGES + 1))
        MISSING_PACKAGE_DOCS+=("$pkg")
    fi
    
    # Check exported types, functions, constants, and variables in each file
    for file in "$pkg_dir"/*.go; do
        if [ -f "$file" ] && [[ ! "$file" =~ _test\.go$ ]] && [[ ! "$file" =~ generated ]] && [[ ! "$file" =~ \.pb\.go$ ]]; then
            # Process file line by line
            line_num=0
            while IFS= read -r line; do
                line_num=$((line_num + 1))
                
                # Skip empty lines and package declaration
                if [ -z "$line" ] || [[ "$line" =~ ^package[[:space:]] ]]; then
                    continue
                fi
                
                # Check for exported type
                if [[ "$line" =~ ^type[[:space:]]+([A-Z][a-zA-Z0-9_]*) ]]; then
                    TOTAL_TYPES=$((TOTAL_TYPES + 1))
                    type_name="${BASH_REMATCH[1]}"
                    
                    # Check if previous line is a doc comment
                    prev_line=$(get_prev_line "$file" "$line_num")
                    if ! is_doc_comment "$prev_line"; then
                        UNDOCUMENTED_TYPES=$((UNDOCUMENTED_TYPES + 1))
                        MISSING_TYPE_DOCS+=("$file:$line_num: type $type_name")
                    fi
                fi
                
                # Check for exported function or method
                if [[ "$line" =~ ^func[[:space:]]+([A-Z][a-zA-Z0-9_]*)[[:space:]]*\( ]] || \
                   [[ "$line" =~ ^func[[:space:]]+\([^\)]+\)[[:space:]]+([A-Z][a-zA-Z0-9_]*)[[:space:]]*\( ]]; then
                    TOTAL_FUNCTIONS=$((TOTAL_FUNCTIONS + 1))
                    func_name="${BASH_REMATCH[1]}"
                    
                    # Check if previous line is a doc comment
                    prev_line=$(get_prev_line "$file" "$line_num")
                    if ! is_doc_comment "$prev_line"; then
                        UNDOCUMENTED_FUNCTIONS=$((UNDOCUMENTED_FUNCTIONS + 1))
                        MISSING_FUNC_DOCS+=("$file:$line_num: func $func_name")
                    fi
                fi
                
                # Check for exported constants
                if [[ "$line" =~ ^const[[:space:]]+([A-Z][a-zA-Z0-9_]*) ]]; then
                    TOTAL_CONSTANTS=$((TOTAL_CONSTANTS + 1))
                    const_name="${BASH_REMATCH[1]}"
                    
                    # Check if previous line is a doc comment
                    prev_line=$(get_prev_line "$file" "$line_num")
                    if ! is_doc_comment "$prev_line"; then
                        UNDOCUMENTED_CONSTANTS=$((UNDOCUMENTED_CONSTANTS + 1))
                        MISSING_CONST_DOCS+=("$file:$line_num: const $const_name")
                    fi
                fi
                
                # Check for exported variables
                if [[ "$line" =~ ^var[[:space:]]+([A-Z][a-zA-Z0-9_]*) ]]; then
                    TOTAL_VARS=$((TOTAL_VARS + 1))
                    var_name="${BASH_REMATCH[1]}"
                    
                    # Check if previous line is a doc comment
                    prev_line=$(get_prev_line "$file" "$line_num")
                    if ! is_doc_comment "$prev_line"; then
                        UNDOCUMENTED_VARS=$((UNDOCUMENTED_VARS + 1))
                        MISSING_VAR_DOCS+=("$file:$line_num: var $var_name")
                    fi
                fi
            done < "$file"
        fi
    done
done

# Calculate percentages
if [ $TOTAL_PACKAGES -gt 0 ]; then
    PACKAGE_COVERAGE=$((100 - (UNDOCUMENTED_PACKAGES * 100 / TOTAL_PACKAGES)))
else
    PACKAGE_COVERAGE=100
fi

if [ $TOTAL_TYPES -gt 0 ]; then
    TYPE_COVERAGE=$((100 - (UNDOCUMENTED_TYPES * 100 / TOTAL_TYPES)))
else
    TYPE_COVERAGE=100
fi

if [ $TOTAL_FUNCTIONS -gt 0 ]; then
    FUNC_COVERAGE=$((100 - (UNDOCUMENTED_FUNCTIONS * 100 / TOTAL_FUNCTIONS)))
else
    FUNC_COVERAGE=100
fi

if [ $TOTAL_CONSTANTS -gt 0 ]; then
    CONST_COVERAGE=$((100 - (UNDOCUMENTED_CONSTANTS * 100 / TOTAL_CONSTANTS)))
else
    CONST_COVERAGE=100
fi

if [ $TOTAL_VARS -gt 0 ]; then
    VAR_COVERAGE=$((100 - (UNDOCUMENTED_VARS * 100 / TOTAL_VARS)))
else
    VAR_COVERAGE=100
fi

# Print summary
echo -e "${BLUE}=== Documentation Coverage Summary ===${NC}"
echo ""
echo -e "Packages:  ${GREEN}$((TOTAL_PACKAGES - UNDOCUMENTED_PACKAGES))${NC}/${TOTAL_PACKAGES} documented (${PACKAGE_COVERAGE}%)"
echo -e "Types:     ${GREEN}$((TOTAL_TYPES - UNDOCUMENTED_TYPES))${NC}/${TOTAL_TYPES} documented (${TYPE_COVERAGE}%)"
echo -e "Functions: ${GREEN}$((TOTAL_FUNCTIONS - UNDOCUMENTED_FUNCTIONS))${NC}/${TOTAL_FUNCTIONS} documented (${FUNC_COVERAGE}%)"
echo -e "Constants: ${GREEN}$((TOTAL_CONSTANTS - UNDOCUMENTED_CONSTANTS))${NC}/${TOTAL_CONSTANTS} documented (${CONST_COVERAGE}%)"
echo -e "Variables: ${GREEN}$((TOTAL_VARS - UNDOCUMENTED_VARS))${NC}/${TOTAL_VARS} documented (${VAR_COVERAGE}%)"
echo ""

# Print detailed findings if requested
if [ "${1:-}" = "-v" ] || [ "${1:-}" = "--verbose" ]; then
    if [ ${#MISSING_PACKAGE_DOCS[@]} -gt 0 ]; then
        echo -e "${YELLOW}=== Missing Package Documentation ===${NC}"
        for pkg in "${MISSING_PACKAGE_DOCS[@]}"; do
            echo "  - $pkg"
        done
        echo ""
    fi
    
    if [ ${#MISSING_TYPE_DOCS[@]} -gt 0 ]; then
        echo -e "${YELLOW}=== Missing Type Documentation ===${NC}"
        for item in "${MISSING_TYPE_DOCS[@]}"; do
            echo "  - $item"
        done
        echo ""
    fi
    
    if [ ${#MISSING_FUNC_DOCS[@]} -gt 0 ]; then
        echo -e "${YELLOW}=== Missing Function Documentation ===${NC}"
        for item in "${MISSING_FUNC_DOCS[@]}"; do
            echo "  - $item"
        done
        echo ""
    fi
    
    if [ ${#MISSING_CONST_DOCS[@]} -gt 0 ]; then
        echo -e "${YELLOW}=== Missing Constant Documentation ===${NC}"
        for item in "${MISSING_CONST_DOCS[@]}"; do
            echo "  - $item"
        done
        echo ""
    fi
    
    if [ ${#MISSING_VAR_DOCS[@]} -gt 0 ]; then
        echo -e "${YELLOW}=== Missing Variable Documentation ===${NC}"
        for item in "${MISSING_VAR_DOCS[@]}"; do
            echo "  - $item"
        done
        echo ""
    fi
fi

# Exit with error code if documentation is incomplete
TOTAL_UNDOCUMENTED=$((UNDOCUMENTED_PACKAGES + UNDOCUMENTED_TYPES + UNDOCUMENTED_FUNCTIONS + UNDOCUMENTED_CONSTANTS + UNDOCUMENTED_VARS))

if [ $TOTAL_UNDOCUMENTED -gt 0 ]; then
    echo -e "${RED}Documentation check failed!${NC} Found $TOTAL_UNDOCUMENTED undocumented items."
    echo ""
    echo "Run with -v or --verbose flag to see detailed findings:"
    echo "  ./scripts/check-docs.sh -v"
    echo ""
    echo "To enforce documentation standards in CI, add 'make docs-check' to your workflow."
    exit 1
else
    echo -e "${GREEN}All exported items are documented!${NC}"
    exit 0
fi