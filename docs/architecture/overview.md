# Nebula Architecture Overview

Nebula is a high-performance, cloud-native Extract & Load (EL) data integration platform designed to achieve extreme throughput (1M+ records/second) with minimal resource usage.

## Design Principles

### 1. Zero-Copy Architecture
- Minimize memory allocations in hot paths
- Pass pointers, not values
- Reuse buffers through object pooling
- Direct memory access where safe

### 2. Unified Systems
- Single record type (`pool.Record`) instead of 15+ types
- Single pool system (`pool.Pool[T]`) instead of 4+ implementations
- Single configuration (`config.BaseConfig`) instead of 64+ types
- Reduces complexity and improves maintainability

### 3. Performance First
- Every design decision evaluated against performance impact
- Continuous benchmarking and profiling
- Performance gates at each development phase
- Target: 1M+ records/second throughput

### 4. Memory Efficiency
- Achieved 94% memory reduction (1,365→84 bytes/record)
- Hybrid storage engine with intelligent mode selection
- Object pooling for zero-allocation patterns
- Target: <100 bytes per record

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLI Interface                            │
│                        (cmd/nebula)                             │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────┴───────────────────────────────────────┐
│                    Pipeline Orchestrator                         │
│                   (internal/pipeline)                           │
├─────────────────────────────────────────────────────────────────┤
│ • Backpressure Management    • Error Recovery                   │
│ • Progress Tracking          • Resource Management               │
└─────────────┬───────────────────────────────────┬───────────────┘
              │                                   │
┌─────────────┴───────────────┐     ┌────────────┴────────────────┐
│     Source Connectors       │     │   Destination Connectors    │
│   (pkg/connector/sources)   │     │ (pkg/connector/destinations)│
├─────────────────────────────┤     ├─────────────────────────────┤
│ • CSV, JSON                 │     │ • CSV, JSON                 │
│ • PostgreSQL CDC            │     │ • Snowflake                 │
│ • MySQL CDC                 │     │ • BigQuery                  │
│ • Google Ads API            │     │ • S3, GCS                   │
│ • Meta Ads API              │     │ • PostgreSQL                │
└─────────────┬───────────────┘     └────────────┬────────────────┘
              │                                   │
┌─────────────┴───────────────────────────────────┴───────────────┐
│                    Connector Framework                           │
│                    (pkg/connector/base)                         │
├─────────────────────────────────────────────────────────────────┤
│ • Circuit Breakers          • Rate Limiting                     │
│ • Health Monitoring         • Retry Logic                       │
│ • Schema Evolution          • Metrics Collection                │
└─────────────────────────────────────────────────────────────────┘
                          │
┌─────────────────────────┴───────────────────────────────────────┐
│                    Storage Abstraction Layer                     │
│                      (pkg/pipeline)                             │
├─────────────────────────────────────────────────────────────────┤
│ • Hybrid Storage (Row/Columnar/Auto)                           │
│ • Intelligent Mode Selection                                    │
│ • Compression Integration                                       │
└─────────────┬───────────────────────────────────┬───────────────┘
              │                                   │
┌─────────────┴───────────────┐     ┌────────────┴────────────────┐
│   Columnar Storage Engine   │     │    Memory Pool System       │
│      (pkg/columnar)         │     │       (pkg/pool)           │
├─────────────────────────────┤     ├─────────────────────────────┤
│ • 84 bytes/record           │     │ • Zero allocations          │
│ • Type optimization         │     │ • Object reuse              │
│ • Dictionary encoding       │     │ • Global pools              │
└─────────────────────────────┘     └─────────────────────────────┘
```

## Data Flow

### 1. Record Creation
```go
// Records are obtained from pools, never created directly
record := pool.GetRecord()
defer record.Release()
```

### 2. Pipeline Processing
```
Source → Storage Adapter → Pipeline → Storage Adapter → Destination
         ↓                           ↓
         Auto Mode Selection         Auto Mode Selection
         (Row/Columnar)              (Row/Columnar)
```

### 3. Storage Mode Selection
- **< 10K records**: Row mode for low latency
- **> 10K records**: Columnar mode for memory efficiency
- **Streaming**: Force row mode
- **Batch**: Force columnar mode

## Component Architecture

### Connector Framework
- **Base Connector**: Common functionality (circuit breakers, rate limiting)
- **Source Interface**: Standard API for all data sources
- **Destination Interface**: Standard API for all data destinations
- **Registry**: Dynamic connector discovery and instantiation

### Storage Engine
- **Row Storage**: Traditional row-based storage (225 bytes/record)
- **Columnar Storage**: Column-oriented storage (84 bytes/record)
- **Hybrid Mode**: Automatic selection based on workload
- **Compression**: LZ4, Zstd, Snappy, Gzip support

### Memory Management
- **Unified Pool System**: Generic `Pool[T]` for all object types
- **Global Pools**: Pre-configured pools for common types
- **Zero-Copy Transfers**: Direct memory access between components
- **Automatic Cleanup**: Release() methods ensure proper cleanup

### Configuration System
- **BaseConfig**: Unified configuration structure
- **Environment Variables**: `${VAR_NAME}` substitution
- **Validation**: Automatic validation on load
- **Defaults**: Sensible production defaults

## Performance Architecture

### Concurrency Model
- **Worker Pools**: Configurable worker counts
- **Work Stealing**: Dynamic load balancing
- **CPU Affinity**: Pin workers to cores
- **Buffered Channels**: Reduce synchronization overhead

### Memory Optimization
- **Object Pooling**: Reuse instead of allocate
- **Zero-Copy**: Pass pointers, not values
- **Columnar Storage**: 94% memory reduction
- **Buffer Reuse**: Minimize GC pressure

### I/O Optimization
- **Batch Operations**: Reduce syscall overhead
- **Async I/O**: Non-blocking operations
- **Connection Pooling**: Reuse database connections
- **Compression**: Reduce network/disk I/O

## Scalability Architecture

### Horizontal Scaling
- **Partitioned Processing**: Split data by key
- **Distributed Coordination**: (Future) Multi-node support
- **Load Balancing**: Even distribution across workers
- **Stateless Design**: Easy to scale out

### Vertical Scaling
- **NUMA Awareness**: (Future) Optimize for NUMA architectures
- **Huge Pages**: Reduce TLB misses
- **CPU Cache**: Optimize for cache locality
- **SIMD Operations**: (Future) Vectorized processing

## Reliability Architecture

### Error Handling
- **Structured Errors**: Rich error context
- **Error Categories**: Retryable vs fatal
- **Circuit Breakers**: Automatic failure detection
- **Graceful Degradation**: Continue processing on partial failures

### Data Integrity
- **Exactly-Once Semantics**: Where supported
- **Transaction Support**: ACID compliance
- **Schema Evolution**: Automatic migration
- **Validation**: Input/output validation

### Monitoring
- **Health Checks**: Continuous health monitoring
- **Metrics Collection**: Prometheus-compatible
- **Distributed Tracing**: (Future) Request tracking
- **Alerting**: Configurable thresholds

## Security Architecture

### Authentication
- **Credential Management**: Secure storage
- **Token Refresh**: Automatic renewal
- **Multi-Factor**: Where supported
- **Service Accounts**: For production use

### Authorization
- **Role-Based Access**: Configurable permissions
- **Resource Isolation**: Tenant separation
- **Audit Logging**: Track all operations
- **Compliance**: GDPR, HIPAA ready

### Encryption
- **TLS Support**: In-transit encryption
- **At-Rest Encryption**: Where supported
- **Key Management**: Secure key storage
- **Certificate Validation**: Proper chain validation

## Future Architecture

### Planned Enhancements
1. **Distributed Processing**: Multi-node coordination
2. **Stream Processing**: Real-time transformations
3. **WASM Plugins**: Safe, sandboxed extensions
4. **GPU Acceleration**: For specific workloads
5. **Kubernetes Operators**: Native k8s integration

### Research Areas
1. **io_uring**: Linux kernel 5.10+ async I/O
2. **DPDK**: Kernel bypass networking
3. **RDMA**: Remote Direct Memory Access
4. **eBPF**: Kernel-level optimizations
5. **QUIC**: Next-gen transport protocol