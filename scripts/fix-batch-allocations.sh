#!/bin/bash

# Script to fix batch slice allocations to use pool.GetBatchSlice
# This will reduce memory allocations by ~250-300 bytes per record

echo "=== Fixing Batch Slice Allocations ==="
echo "Converting make([]*models.Record) to pool.GetBatchSlice()..."
echo ""

# Backup files before making changes
echo "Creating backups..."
find . -name "*.go" -path "./pkg/*" -o -path "./internal/*" | while read file; do
    if grep -q "make(\[\]\*models\.Record" "$file" 2>/dev/null || grep -q "make(\[\]\*pool\.Record" "$file" 2>/dev/null; then
        cp "$file" "$file.bak"
    fi
done

# Fix patterns in source files
echo "Fixing allocations..."

# Pattern 1: batch := make([]*models.Record, 0, batchSize)
# Replace with: batch := pool.GetBatchSlice(batchSize)
find . -name "*.go" -path "./pkg/*" -o -path "./internal/*" | xargs sed -i '' -E '
s/batch := make\(\[\]\*models\.Record, 0, ([^)]+)\)/batch := pool.GetBatchSlice(\1)/g
'

# Pattern 2: make([]*models.Record, 0, capacity) in assignments
find . -name "*.go" -path "./pkg/*" -o -path "./internal/*" | xargs sed -i '' -E '
s/= make\(\[\]\*models\.Record, 0, ([^)]+)\)/= pool.GetBatchSlice(\1)/g
'

# Pattern 3: make([]*pool.Record, ...) - update to use models.Record
find . -name "*.go" -path "./pkg/*" -o -path "./internal/*" | xargs sed -i '' -E '
s/make\(\[\]\*pool\.Record/make([]*models.Record/g
'

# Add defer pool.PutBatchSlice(batch) after batch creation
echo "Adding defer statements for cleanup..."

# Create a more sophisticated fix script
cat > /tmp/fix_batch_defer.py << 'EOF'
#!/usr/bin/env python3
import re
import sys

def fix_file(filename):
    with open(filename, 'r') as f:
        lines = f.readlines()
    
    modified = False
    i = 0
    while i < len(lines):
        line = lines[i]
        # Look for batch := pool.GetBatchSlice patterns
        if 'batch := pool.GetBatchSlice(' in line and 'defer pool.PutBatchSlice(batch)' not in lines[i+1] if i+1 < len(lines) else True:
            indent = len(line) - len(line.lstrip())
            defer_line = ' ' * indent + 'defer pool.PutBatchSlice(batch)\n'
            # Check if next line already has defer
            if i+1 < len(lines) and 'defer pool.PutBatchSlice' not in lines[i+1]:
                lines.insert(i+1, defer_line)
                modified = True
                i += 1
        i += 1
    
    if modified:
        with open(filename, 'w') as f:
            f.writelines(lines)
        print(f"Fixed {filename}")

if __name__ == "__main__":
    for filename in sys.argv[1:]:
        try:
            fix_file(filename)
        except Exception as e:
            print(f"Error processing {filename}: {e}")
EOF

chmod +x /tmp/fix_batch_defer.py

# Run the Python script on modified files
find . -name "*.go" -path "./pkg/*" -o -path "./internal/*" | while read file; do
    if grep -q "pool.GetBatchSlice" "$file" 2>/dev/null; then
        python3 /tmp/fix_batch_defer.py "$file"
    fi
done

# Add import for pool package where needed
echo "Adding pool imports..."
find . -name "*.go" -path "./pkg/*" -o -path "./internal/*" | while read file; do
    if grep -q "pool\.GetBatchSlice\|pool\.PutBatchSlice" "$file" 2>/dev/null; then
        # Check if pool import exists
        if ! grep -q '"github.com/nebula/nebula/pkg/pool"' "$file"; then
            # Add import after package declaration
            sed -i '' '/^package /a\
\
import (\
    "github.com/nebula/nebula/pkg/pool"\
)
' "$file"
            # Fix import formatting
            goimports -w "$file" 2>/dev/null || true
        fi
    fi
done

# Count changes
echo ""
echo "=== Summary ==="
echo -n "Files modified: "
find . -name "*.go.bak" | wc -l
echo -n "Batch allocations fixed: "
find . -name "*.go" -path "./pkg/*" -o -path "./internal/*" | xargs grep -l "pool.GetBatchSlice" 2>/dev/null | wc -l

# Clean up
rm -f /tmp/fix_batch_defer.py

echo ""
echo "Done! Run 'make test' to verify changes."
echo "Backup files created with .bak extension"