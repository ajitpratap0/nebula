#!/bin/bash
# Script to help replace fmt.Errorf with pooled error messages

echo "Finding all files with fmt.Errorf usage..."
files=$(grep -r "fmt\.Errorf" --include="*.go" . | cut -d: -f1 | sort | uniq)

total=$(echo "$files" | wc -l | tr -d ' ')
echo "Found $total files with fmt.Errorf usage"

# Group files by package/area for systematic replacement
echo -e "\n=== Files by area ==="

echo -e "\n[CDC Package]"
echo "$files" | grep "pkg/cdc/" || echo "None"

echo -e "\n[Connector Sources]"
echo "$files" | grep "pkg/connector/sources/" || echo "None"

echo -e "\n[Connector Destinations]"
echo "$files" | grep "pkg/connector/destinations/" || echo "None"

echo -e "\n[Performance Package]"
echo "$files" | grep "pkg/performance/" || echo "None"

echo -e "\n[Pipeline Internal]"
echo "$files" | grep "internal/pipeline/" || echo "None"

echo -e "\n[Schema Package]"
echo "$files" | grep "pkg/schema/" || echo "None"

echo -e "\n[Formats Package]"
echo "$files" | grep "pkg/formats/" || echo "None"

echo -e "\n[Config Package]"
echo "$files" | grep "pkg/config/" || echo "None"

echo -e "\n[Other Packages]"
echo "$files" | grep -v -E "pkg/(cdc|connector|performance|schema|formats|config)|internal/pipeline" || echo "None"

# Helper function to show fmt.Errorf patterns in a file
show_patterns() {
    local file=$1
    echo -e "\n=== $file ==="
    grep -n "fmt\.Errorf" "$file" | head -10
}

# Suggest error types based on context
suggest_error_type() {
    local context=$1
    case "$context" in
        *"file"*|*"open"*|*"read"*|*"write"*) echo "ErrorTypeFile" ;;
        *"connection"*|*"request"*|*"http"*|*"network"*) echo "ErrorTypeConnection" ;;
        *"parse"*|*"decode"*|*"unmarshal"*|*"convert"*) echo "ErrorTypeData" ;;
        *"auth"*|*"token"*|*"credential"*) echo "ErrorTypeAuthentication" ;;
        *"config"*|*"setting"*|*"parameter"*) echo "ErrorTypeConfig" ;;
        *"valid"*|*"check"*|*"verify"*) echo "ErrorTypeValidation" ;;
        *"timeout"*) echo "ErrorTypeTimeout" ;;
        *"not found"*) echo "ErrorTypeNotFound" ;;
        *) echo "ErrorTypeInternal" ;;
    esac
}

# Process high-priority files first
priority_files=(
    "pkg/cdc/mysql.go"
    "pkg/cdc/postgresql.go"
    "pkg/cdc/stream.go"
    "pkg/connector/sources/postgresql/postgresql_source.go"
    "pkg/connector/sources/postgresql_cdc/postgresql_cdc_source.go"
)

echo -e "\n=== High Priority Files (Hot Paths) ==="
for file in "${priority_files[@]}"; do
    if echo "$files" | grep -q "$file"; then
        echo "- $file"
        grep -c "fmt\.Errorf" "$file" | xargs echo "  fmt.Errorf count:"
    fi
done

echo -e "\n=== Next Steps ==="
echo "1. Start with high-priority files (CDC and database connectors)"
echo "2. For each file:"
echo "   - Check if fmt is used for anything other than Errorf"
echo "   - Add errors import if needed"
echo "   - Replace fmt.Errorf patterns appropriately"
echo "   - Compile to verify changes"
echo "3. Run tests after each package is complete"