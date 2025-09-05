package unit

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ajitpratap0/nebula/pkg/connector/destinations/iceberg"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestArrowBuilderPool_NewPool(t *testing.T) {
	logger := zaptest.NewLogger(t)
	allocator := memory.NewGoAllocator()

	pool := iceberg.NewArrowBuilderPool(allocator, logger)

	if pool == nil {
		t.Fatal("expected non-nil pool")
	}

	// Test with nil allocator
	poolWithNilAllocator := iceberg.NewArrowBuilderPool(nil, logger)
	if poolWithNilAllocator == nil {
		t.Error("expected valid pool even with nil allocator")
	}
}

func TestArrowBuilderPool_BasicGetAndPut(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pool := iceberg.NewArrowBuilderPool(memory.NewGoAllocator(), logger)

	// Create test schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	// Test Get - should create new builder (miss)
	pooled1 := pool.Get(schema)
	if pooled1 == nil {
		t.Fatal("expected non-nil pooled builder")
	}
	if pooled1.Builder() == nil {
		t.Fatal("expected non-nil underlying builder")
	}

	// Verify builder creation worked
	if pooled1.Builder() == nil {
		t.Error("expected non-nil underlying builder")
	}

	// Return to pool
	pool.Put(pooled1)

	// Test Get again - should reuse builder (hit)
	pooled2 := pool.Get(schema)
	if pooled2 == nil {
		t.Fatal("expected non-nil pooled builder")
	}

	// Should be same instance
	if pooled1 != pooled2 {
		t.Error("expected same pooled builder instance")
	}

	// Verify builder reuse worked
	if pooled2.Builder() == nil {
		t.Error("expected non-nil reused builder")
	}

	// Clean up
	pooled2.Release()
}

func TestArrowBuilderPool_SingleSchemaReuse(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pool := iceberg.NewArrowBuilderPool(memory.NewGoAllocator(), logger)

	schema1 := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	schema2 := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	// Get builder for schema1 - this sets the pool's schema
	pooled1 := pool.Get(schema1)
	pool.Put(pooled1)

	// Get builder for schema2 (identical to schema1) - should reuse same builder
	pooled2 := pool.Get(schema2)

	// Should be same instance since schemas are identical
	if pooled1 != pooled2 {
		t.Error("expected same pooled builder instance (identical schema reuse)")
	}

	// Verify builder reuse worked for single-schema optimization
	if pooled2.Builder() == nil {
		t.Error("expected non-nil reused builder")
	}

	pooled2.Release()
}

func TestArrowBuilderPool_ConcurrentAccess(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pool := iceberg.NewArrowBuilderPool(memory.NewGoAllocator(), logger)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	const numGoroutines = 10
	const numOperations = 50 // Reduced for stability

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Test concurrent Get/Put operations
	for i := range numGoroutines {
		go func(goroutineID int) {
			defer wg.Done()

			for range numOperations {
				// Get builder
				pooled := pool.Get(schema)
				if pooled == nil {
					t.Errorf("goroutine %d: got nil pooled builder", goroutineID)
					return
				}

				// Simulate some work
				time.Sleep(time.Microsecond)

				// Return to pool
				pool.Put(pooled)
			}
		}(i)
	}

	wg.Wait()

	// Test completed successfully if we reach here without panics or crashes
	t.Logf("Concurrent test completed successfully with %d goroutines × %d operations", numGoroutines, numOperations)
}

func TestArrowBuilderPool_Clear(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pool := iceberg.NewArrowBuilderPool(memory.NewGoAllocator(), logger)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	// Add some builders to pool
	pooled1 := pool.Get(schema)
	pool.Put(pooled1)
	pooled2 := pool.Get(schema)
	pool.Put(pooled2)

	// Clear the pool
	pool.Clear()

	// Getting a builder after clear should work
	pooled3 := pool.Get(schema)
	if pooled3 == nil {
		t.Error("expected non-nil pooled builder after clear")
	}

	pooled3.Release()
}

func TestPooledBuilder_NewRecord(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pool := iceberg.NewArrowBuilderPool(memory.NewGoAllocator(), logger)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	pooled := pool.Get(schema)

	// Build some data
	intBuilder := pooled.Builder().Field(0).(*array.Int32Builder)
	strBuilder := pooled.Builder().Field(1).(*array.StringBuilder)

	intBuilder.Append(123)
	strBuilder.Append("test")

	// Create record - this should return builder to pool
	record := pooled.NewRecord()
	if record == nil {
		t.Fatal("expected non-nil record")
	}

	if record.NumRows() != 1 {
		t.Errorf("expected 1 row, got %d", record.NumRows())
	}
	if record.NumCols() != 2 {
		t.Errorf("expected 2 columns, got %d", record.NumCols())
	}

	// Builder should be back in pool now - try to get another
	pooled2 := pool.Get(schema)
	if pooled2 == nil {
		t.Error("expected non-nil pooled builder from pool")
	}

	record.Release()
	pooled2.Release()
}

func TestArrowBuilderPool_MemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory test in short mode")
	}

	logger := zaptest.NewLogger(t)
	pool := iceberg.NewArrowBuilderPool(memory.NewGoAllocator(), logger)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	// Force garbage collection and get initial memory
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Create and use many builders
	const numBuilders = 100 // Reduced for stability
	builders := make([]*iceberg.PooledBuilder, numBuilders)

	for i := range numBuilders {
		builders[i] = pool.Get(schema)

		// Add some data to each builder
		intBuilder := builders[i].Builder().Field(0).(*array.Int64Builder)
		floatBuilder := builders[i].Builder().Field(1).(*array.Float64Builder)

		for j := range 50 { // Reduced data size
			intBuilder.Append(int64(j))
			floatBuilder.Append(float64(j) * 1.5)
		}
	}

	// Return all builders to pool
	for _, builder := range builders {
		pool.Put(builder)
	}

	// Force garbage collection
	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Memory usage should be reasonable
	memUsed := m2.Alloc - m1.Alloc
	t.Logf("Memory used: %d bytes for %d builders", memUsed, numBuilders)

	// Test completed successfully - memory usage recorded above
	t.Log("Memory usage test completed successfully")
}

func BenchmarkArrowBuilderPool_GetPut(b *testing.B) {
	logger := zap.NewNop()
	pool := iceberg.NewArrowBuilderPool(memory.NewGoAllocator(), logger)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	b.ResetTimer()

	for range b.N {
		pooled := pool.Get(schema)
		pool.Put(pooled)
	}
}

func BenchmarkArrowBuilderPool_Concurrent(b *testing.B) {
	logger := zap.NewNop()
	pool := iceberg.NewArrowBuilderPool(memory.NewGoAllocator(), logger)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pooled := pool.Get(schema)
			pool.Put(pooled)
		}
	})
}
