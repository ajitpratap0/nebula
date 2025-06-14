#!/bin/bash

# Comprehensive script to fix all memory allocations in Nebula codebase
# This script applies pooling to batch slices, maps, channels, and buffers

set -e

echo "=== Nebula Memory Optimization - Bulk Fix Script ==="
echo "Date: $(date)"
echo ""

# Create backup directory
BACKUP_DIR="backups/memory_opt_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

# Function to create backups
backup_files() {
    echo "Creating backups..."
    find . -name "*.go" \( -path "./pkg/*" -o -path "./internal/*" \) -type f | while read file; do
        if grep -q "make(\[\]\*models\.Record\|make(map\[string\]interface\{\}\|make(chan\|bytes\.Buffer" "$file" 2>/dev/null; then
            cp "$file" "$BACKUP_DIR/$(basename $file).bak"
        fi
    done
    echo "Backups created in $BACKUP_DIR"
}

# Function to fix batch slice allocations
fix_batch_slices() {
    echo ""
    echo "=== Fixing Batch Slice Allocations ==="
    
    # Count before
    COUNT_BEFORE=$(find . -name "*.go" \( -path "./pkg/*" -o -path "./internal/*" \) -type f -exec grep -l "make(\[\]\*models\.Record" {} \; 2>/dev/null | wc -l)
    echo "Files with batch allocations before: $COUNT_BEFORE"
    
    # Create Python script for intelligent batch slice fixes
    cat > /tmp/fix_batch_slices.py << 'EOF'
#!/usr/bin/env python3
import re
import sys
import os

def fix_batch_slices(filename):
    with open(filename, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Pattern 1: batch := make([]*models.Record, 0, capacity)
    content = re.sub(
        r'(\s*)(\w+)\s*:=\s*make\(\[\]\*models\.Record,\s*0,\s*([^)]+)\)',
        r'\1\2 := pool.GetBatchSlice(\3)\n\1defer pool.PutBatchSlice(\2)',
        content
    )
    
    # Pattern 2: variable = make([]*models.Record, 0, capacity)
    content = re.sub(
        r'(\s*)(\w+)\s*=\s*make\(\[\]\*models\.Record,\s*0,\s*([^)]+)\)',
        r'\1\2 = pool.GetBatchSlice(\3)',
        content
    )
    
    # Pattern 3: field: make([]*models.Record, 0, capacity) in struct initialization
    content = re.sub(
        r'(\s*)(\w+):\s*make\(\[\]\*models\.Record,\s*0,\s*([^)]+)\),',
        r'\1\2: pool.GetBatchSlice(\3),',
        content
    )
    
    # Pattern 4: Reset pattern - batch = make([]*models.Record, 0, capacity)
    # This should become batch = batch[:0] instead
    content = re.sub(
        r'(\s*)(\w+)\s*=\s*make\(\[\]\*models\.Record,\s*0,\s*[^)]+\)\s*//\s*[Rr]eset',
        r'\1\2 = \2[:0] // Reset without allocation',
        content
    )
    
    if content != original_content:
        # Add pool import if needed
        if 'pool.GetBatchSlice' in content and '"github.com/nebula/nebula/pkg/pool"' not in content:
            # Add import after package declaration
            content = re.sub(
                r'(package \w+\n\n)',
                r'\1import "github.com/nebula/nebula/pkg/pool"\n\n',
                content
            )
            
            # Fix if there's already an import block
            content = re.sub(
                r'(import \()',
                r'\1\n\t"github.com/nebula/nebula/pkg/pool"',
                content,
                count=1
            )
        
        with open(filename, 'w') as f:
            f.write(content)
        return True
    return False

if __name__ == "__main__":
    for filename in sys.argv[1:]:
        try:
            if fix_batch_slices(filename):
                print(f"Fixed: {filename}")
        except Exception as e:
            print(f"Error processing {filename}: {e}")
EOF
    
    chmod +x /tmp/fix_batch_slices.py
    
    # Apply fixes
    find . -name "*.go" \( -path "./pkg/*" -o -path "./internal/*" \) -type f | while read file; do
        if grep -q "make(\[\]\*models\.Record" "$file" 2>/dev/null; then
            python3 /tmp/fix_batch_slices.py "$file"
        fi
    done
    
    # Count after
    COUNT_AFTER=$(find . -name "*.go" \( -path "./pkg/*" -o -path "./internal/*" \) -type f -exec grep -l "make(\[\]\*models\.Record" {} \; 2>/dev/null | wc -l)
    echo "Files with batch allocations after: $COUNT_AFTER"
    echo "Fixed: $((COUNT_BEFORE - COUNT_AFTER)) files"
}

# Function to fix map allocations
fix_map_allocations() {
    echo ""
    echo "=== Fixing Map Allocations ==="
    
    # Count before
    COUNT_BEFORE=$(find . -name "*.go" \( -path "./pkg/*" -o -path "./internal/*" \) -type f -exec grep -l "make(map\[string\]interface{}" {} \; 2>/dev/null | wc -l)
    echo "Files with map allocations before: $COUNT_BEFORE"
    
    # Create Python script for map fixes
    cat > /tmp/fix_maps.py << 'EOF'
#!/usr/bin/env python3
import re
import sys

def should_skip_file(filename):
    # Skip certain files where map pooling might not be appropriate
    skip_patterns = [
        'pool.go',  # Pool implementation itself
        'errors.go',  # Error types might need persistent maps
        '_test.go',  # Test files
        'schema.go',  # Schema definitions
    ]
    for pattern in skip_patterns:
        if pattern in filename:
            return True
    return False

def fix_maps(filename):
    if should_skip_file(filename):
        return False
        
    with open(filename, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Pattern 1: data := make(map[string]interface{})
    content = re.sub(
        r'(\s*)(\w+)\s*:=\s*make\(map\[string\]interface\{\}\)',
        r'\1\2 := pool.GetMap()\n\1defer pool.PutMap(\2)',
        content
    )
    
    # Pattern 2: variable = make(map[string]interface{})
    content = re.sub(
        r'(\s*)(\w+)\s*=\s*make\(map\[string\]interface\{\}\)',
        r'\1\2 = pool.GetMap()',
        content
    )
    
    # Pattern 3: Don't pool maps in struct fields (they need longer lifetime)
    # Skip patterns like: field: make(map[string]interface{})
    
    if content != original_content:
        # Add pool import if needed
        if 'pool.GetMap' in content and '"github.com/nebula/nebula/pkg/pool"' not in content:
            content = re.sub(
                r'(package \w+\n\n)',
                r'\1import "github.com/nebula/nebula/pkg/pool"\n\n',
                content
            )
            
            content = re.sub(
                r'(import \()',
                r'\1\n\t"github.com/nebula/nebula/pkg/pool"',
                content,
                count=1
            )
        
        with open(filename, 'w') as f:
            f.write(content)
        return True
    return False

if __name__ == "__main__":
    for filename in sys.argv[1:]:
        try:
            if fix_maps(filename):
                print(f"Fixed: {filename}")
        except Exception as e:
            print(f"Error processing {filename}: {e}")
EOF
    
    chmod +x /tmp/fix_maps.py
    
    # Apply fixes to appropriate files
    find . -name "*.go" \( -path "./pkg/*" -o -path "./internal/*" \) -type f | while read file; do
        # Skip test files and pool implementation
        if [[ ! "$file" =~ "_test.go" ]] && [[ ! "$file" =~ "pool.go" ]]; then
            if grep -q "make(map\[string\]interface{}" "$file" 2>/dev/null; then
                python3 /tmp/fix_maps.py "$file"
            fi
        fi
    done
    
    # Count after
    COUNT_AFTER=$(find . -name "*.go" \( -path "./pkg/*" -o -path "./internal/*" \) -type f -exec grep -l "make(map\[string\]interface{}" {} \; 2>/dev/null | wc -l)
    echo "Files with map allocations after: $COUNT_AFTER"
    echo "Fixed: $((COUNT_BEFORE - COUNT_AFTER)) files"
}

# Function to fix buffer allocations
fix_buffer_allocations() {
    echo ""
    echo "=== Fixing Buffer Allocations ==="
    
    # Count before
    COUNT_BEFORE=$(find . -name "*.go" \( -path "./pkg/*" -o -path "./internal/*" \) -type f -exec grep -l "var buf bytes\.Buffer\|&bytes\.Buffer{}" {} \; 2>/dev/null | wc -l)
    echo "Files with buffer allocations before: $COUNT_BEFORE"
    
    # Create Python script for buffer fixes
    cat > /tmp/fix_buffers.py << 'EOF'
#!/usr/bin/env python3
import re
import sys

def fix_buffers(filename):
    with open(filename, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Pattern 1: var buf bytes.Buffer
    content = re.sub(
        r'(\s*)var\s+(\w+)\s+bytes\.Buffer',
        r'\1\2 := pool.GlobalBufferPool.Get()\n\1defer pool.GlobalBufferPool.Put(\2)',
        content
    )
    
    # Pattern 2: buf := &bytes.Buffer{}
    content = re.sub(
        r'(\s*)(\w+)\s*:=\s*&bytes\.Buffer\{\}',
        r'\1\2 := pool.GlobalBufferPool.Get()\n\1defer pool.GlobalBufferPool.Put(\2)',
        content
    )
    
    # Pattern 3: make([]byte, size)
    content = re.sub(
        r'(\s*)(\w+)\s*:=\s*make\(\[\]byte,\s*([^)]+)\)',
        r'\1\2 := pool.GetByteSlice()\n\1if cap(\2) < \3 {\n\1\t\2 = make([]byte, \3)\n\1}\n\1defer pool.PutByteSlice(\2)',
        content
    )
    
    if content != original_content:
        # Add pool import if needed
        if 'pool.GlobalBufferPool' in content and '"github.com/nebula/nebula/pkg/pool"' not in content:
            content = re.sub(
                r'(package \w+\n\n)',
                r'\1import "github.com/nebula/nebula/pkg/pool"\n\n',
                content
            )
            
            content = re.sub(
                r'(import \()',
                r'\1\n\t"github.com/nebula/nebula/pkg/pool"',
                content,
                count=1
            )
        
        with open(filename, 'w') as f:
            f.write(content)
        return True
    return False

if __name__ == "__main__":
    for filename in sys.argv[1:]:
        try:
            if fix_buffers(filename):
                print(f"Fixed: {filename}")
        except Exception as e:
            print(f"Error processing {filename}: {e}")
EOF
    
    chmod +x /tmp/fix_buffers.py
    
    # Apply fixes
    find . -name "*.go" \( -path "./pkg/*" -o -path "./internal/*" \) -type f | while read file; do
        if grep -q "var buf bytes\.Buffer\|&bytes\.Buffer{}\|make(\[\]byte" "$file" 2>/dev/null; then
            python3 /tmp/fix_buffers.py "$file"
        fi
    done
    
    # Count after
    COUNT_AFTER=$(find . -name "*.go" \( -path "./pkg/*" -o -path "./internal/*" \) -type f -exec grep -l "var buf bytes\.Buffer\|&bytes\.Buffer{}" {} \; 2>/dev/null | wc -l)
    echo "Files with buffer allocations after: $COUNT_AFTER"
    echo "Fixed: $((COUNT_BEFORE - COUNT_AFTER)) files"
}

# Function to run benchmarks
run_benchmark() {
    echo ""
    echo "=== Running Memory Benchmark ==="
    
    BENCH_FILE="benchmark_results/after_$1_$(date +%Y%m%d_%H%M%S).txt"
    mkdir -p benchmark_results
    
    echo "Running benchmark..."
    go test -bench=BenchmarkDirectPipelinePerformance -benchmem -run=^$ ./tests/benchmarks/direct_performance_benchmark_test.go 2>&1 | tee "$BENCH_FILE"
    
    # Extract memory per record
    if grep -q "B/op" "$BENCH_FILE"; then
        echo ""
        echo "Memory usage:"
        grep "Records_100000" "$BENCH_FILE" | grep -o "[0-9]* B/op" | head -1
        
        # Calculate bytes per record (100K records)
        BYTES=$(grep "Records_100000" "$BENCH_FILE" | grep -o "[0-9]* B/op" | grep -o "[0-9]*" | head -1)
        if [ -n "$BYTES" ]; then
            BYTES_PER_RECORD=$(echo "scale=2; $BYTES / 100000" | bc)
            echo "Estimated: $BYTES_PER_RECORD bytes/record"
        fi
    fi
}

# Main execution
echo "Starting comprehensive memory optimization..."

# Create initial backup
backup_files

# Run baseline benchmark
echo ""
echo "=== Baseline Benchmark ==="
run_benchmark "baseline"

# Fix batch slices
fix_batch_slices
run_benchmark "batch_fixes"

# Fix imports using goimports
echo ""
echo "=== Fixing imports ==="
find . -name "*.go" \( -path "./pkg/*" -o -path "./internal/*" \) -type f -exec goimports -w {} \; 2>/dev/null || true

# Fix map allocations
fix_map_allocations
run_benchmark "map_fixes"

# Fix buffer allocations
fix_buffer_allocations
run_benchmark "buffer_fixes"

# Clean up
rm -f /tmp/fix_*.py

echo ""
echo "=== Optimization Complete ==="
echo "Backup files saved in: $BACKUP_DIR"
echo "Benchmark results saved in: benchmark_results/"
echo ""
echo "Next steps:"
echo "1. Run 'make test' to ensure no regressions"
echo "2. Review changes with 'git diff'"
echo "3. Run full benchmark suite with 'make bench'"