// Package pool implements a high-performance, type-safe object pooling system
// that is central to Nebula's zero-allocation architecture. It provides unified
// memory management for all reusable objects, significantly reducing garbage
// collection pressure and improving throughput.
//
// Architecture
//
// The pool package uses Go generics to provide type-safe pooling for any object
// type. It builds on sync.Pool but adds additional features like metrics,
// capacity limits, and warm-up capabilities.
//
// Core Types:
//
//   - Pool[T]: Generic pool implementation for any type T
//   - Record: The unified record type used throughout Nebula
//   - Global pools: Pre-configured pools for common types
//
// Memory Efficiency
//
// The pool system is crucial for achieving Nebula's memory targets:
//   - Reduces allocations by 99%+ in hot paths
//   - Enables zero-copy record passing between pipeline stages
//   - Supports the hybrid storage engine's 84 bytes/record efficiency
//   - Provides automatic cleanup via Release() methods
//
// Global Pools
//
// Pre-configured pools are available for common types:
//
//	var (
//		RecordPool      = NewPool[Record](...)      // Record objects
//		MapPool         = NewPool[map[string]any](...) // Generic maps
//		StringSlicePool = NewPool[[]string](...)    // String slices
//		ByteSlicePool   = NewPool[[]byte](...)      // Byte buffers
//	)
//
// Usage Patterns
//
// Basic pool usage:
//
//	// Get a record from the pool
//	record := pool.GetRecord()
//	defer record.Release() // Always release back to pool
//	
//	// Use the record
//	record.SetData("key", "value")
//	record.Metadata.Source = "postgresql"
//
// Creating a custom pool:
//
//	type MyObject struct {
//		data []byte
//		buffer *bytes.Buffer
//	}
//	
//	myPool := pool.NewPool[MyObject](
//		pool.WithNew(func() *MyObject {
//			return &MyObject{
//				data: make([]byte, 0, 1024),
//				buffer: bytes.NewBuffer(nil),
//			}
//		}),
//		pool.WithReset(func(obj *MyObject) {
//			obj.data = obj.data[:0]
//			obj.buffer.Reset()
//		}),
//		pool.WithCapacity(10000),
//	)
//
// Record Management
//
// The Record type is the primary data structure in Nebula:
//
//	// Creating records
//	record := pool.NewRecord("source", data)
//	cdcRecord := pool.NewCDCRecord("db", "table", "INSERT", before, after)
//	streamRecord := pool.NewStreamingRecord("stream", offset, data)
//	
//	// Always release records
//	defer record.Release()
//
// Performance Guidelines
//
// 1. Always release objects back to pools
// 2. Pre-warm pools for predictable performance
// 3. Set appropriate capacity limits
// 4. Reset objects properly to avoid data leaks
// 5. Use pool metrics to monitor efficiency
// 6. Avoid holding pool objects across goroutines
//
// Integration with Storage
//
// The pool system integrates seamlessly with the hybrid storage engine:
//   - Row mode: Records are pooled individually
//   - Columnar mode: Column buffers are pooled
//   - Automatic mode switching preserves pool efficiency
//
// Best Practices
//
// DO:
//   - Use GetRecord() and Release() consistently
//   - Implement proper reset functions for custom pools
//   - Monitor pool metrics in production
//   - Pre-allocate capacity for known workloads
//
// DON'T:
//   - Create records with new() - always use pools
//   - Hold pool objects longer than necessary
//   - Share pool objects between goroutines without sync
//   - Forget to release objects back to pools
//
// Metrics
//
// Pool metrics are exposed for monitoring:
//   - gets: Total objects retrieved
//   - puts: Total objects returned
//   - hits: Pool hits (reused objects)
//   - misses: Pool misses (new allocations)
//   - active: Currently active objects
//
// These metrics help identify pool efficiency and potential leaks.
package pool