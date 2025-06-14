# MySQL CDC Source Connector

## Overview

The MySQL CDC (Change Data Capture) Source Connector provides real-time streaming of changes from MySQL databases using binary log (binlog) replication. It captures INSERT, UPDATE, DELETE, and optionally DDL operations with support for both row-based and statement-based replication (row-based recommended).

## Features

### Core Capabilities
- **Binary Log Streaming**: Real-time change capture from MySQL binlog
- **GTID Support**: Global Transaction ID for reliable positioning
- **Initial Snapshot**: Consistent snapshot before streaming
- **Multi-Table Sync**: Replicate multiple tables concurrently
- **Position Persistence**: Save and resume from last position
- **DDL Capture**: Optional schema change tracking

### Advanced Features
- Master/slave failover support
- Parallel snapshot workers
- Chunked snapshot for large tables
- Heartbeat for lag monitoring
- Compressed binlog support
- MariaDB compatibility

## Prerequisites

### MySQL Configuration

1. **MySQL Version**: 5.6+ (5.7+ recommended for GTID)

2. **Binary Logging**: Must be enabled
   ```ini
   [mysqld]
   log_bin = mysql-bin
   binlog_format = ROW
   binlog_row_image = FULL
   expire_logs_days = 7
   ```

3. **Server ID**: Must be set (non-zero)
   ```ini
   [mysqld]
   server_id = 1
   ```

4. **GTID** (Recommended for MySQL 5.6+):
   ```ini
   [mysqld]
   gtid_mode = ON
   enforce_gtid_consistency = ON
   ```

5. **Restart MySQL** after configuration changes

### User Permissions

Create a user with replication privileges:

```sql
-- Create replication user
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'password';

-- Grant required privileges
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
GRANT SELECT ON mydb.* TO 'cdc_user'@'%';
GRANT RELOAD ON *.* TO 'cdc_user'@'%';

-- For GTID purged operations (optional)
GRANT SUPER ON *.* TO 'cdc_user'@'%';

FLUSH PRIVILEGES;
```

## Configuration

### Basic Configuration

```yaml
source:
  type: mysql_cdc
  properties:
    # Connection settings
    host: "localhost"
    port: 3306
    user: "cdc_user"
    password: "password"
    database: "mydb"
    
    # Replication settings
    server_id: 1001    # Must be unique in MySQL topology
    
    # Tables to replicate
    tables:
      - "users"
      - "orders"
      - "products"
    
    # Snapshot settings
    snapshot_mode: "initial"    # initial, never, schema_only
```

### Advanced Configuration

```yaml
source:
  type: mysql_cdc
  properties:
    # Connection
    host: "mysql-primary.example.com"
    port: 3306
    user: "replication_user"
    password: "secure_password"
    database: "production"
    
    # Replication settings
    server_id: 2001
    flavor: "mysql"             # mysql or mariadb
    
    # Start position (choose one)
    binlog_file: "mysql-bin.000042"
    binlog_pos: 154
    # OR use GTID
    gtid: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
    
    # Table filtering
    tables:
      - "customers"
      - "orders"
      - "order_items"
    exclude_tables:
      - "temp_*"
      - "log_*"
    
    # Snapshot configuration
    snapshot_mode: "initial"
    snapshot_parallel: 4        # Parallel workers
    snapshot_chunk_size: 10000  # Rows per chunk
    
    # Performance tuning
    batch_size: 5000
    flush_interval_ms: 500
    max_event_size: 16777216    # 16MB
    
    # Advanced options
    include_ddl: true
    heartbeat_period_seconds: 10
    read_timeout_seconds: 60
    
    # Position persistence
    save_interval_seconds: 10
    position_file: "/var/lib/mysql-cdc/position.json"
```

### Starting from Specific Position

#### Using Binlog Position
```yaml
binlog_file: "mysql-bin.000042"
binlog_pos: 154
snapshot_mode: "never"
```

#### Using GTID (Recommended)
```yaml
gtid: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
snapshot_mode: "never"
```

## Usage Examples

### Basic Usage

```go
import (
    "github.com/ajitpratap0/nebula/pkg/connector/sources"
    "github.com/ajitpratap0/nebula/pkg/connector/core"
)

// Create configuration
config := &core.Config{
    Type: "mysql_cdc",
    Properties: map[string]interface{}{
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "password",
        "database": "mydb",
        "server_id": 1001,
        "tables": []string{"users", "orders"},
    },
}

// Create and initialize source
source, err := sources.NewMySQLCDCSource("mysql-cdc", config)
if err != nil {
    log.Fatal(err)
}

ctx := context.Background()
if err := source.Initialize(ctx, config); err != nil {
    log.Fatal(err)
}

// Read changes
stream, err := source.Read(ctx)
if err != nil {
    log.Fatal(err)
}

// Process changes
for records := range stream.Records {
    for _, record := range records {
        processChange(record)
    }
}
```

### Processing Different Operations

```go
for records := range stream.Records {
    for _, record := range records {
        op := record.Data["_cdc_op"].(string)
        table := record.Data["_cdc_table"].(string)
        
        switch op {
        case "r": // Snapshot
            fmt.Printf("SNAPSHOT: %s - ID: %v\n", 
                table, record.Data["id"])
            
        case "c": // Insert
            fmt.Printf("INSERT: %s - New row: %+v\n", 
                table, record.Data)
            
        case "u": // Update
            fmt.Printf("UPDATE: %s - ID: %v\n", 
                table, record.Data["id"])
            // Check for old values
            for k, v := range record.Data {
                if strings.HasPrefix(k, "_old_") {
                    field := strings.TrimPrefix(k, "_old_")
                    fmt.Printf("  Changed %s: %v -> %v\n", 
                        field, v, record.Data[field])
                }
            }
            
        case "d": // Delete
            fmt.Printf("DELETE: %s - ID: %v\n", 
                table, record.Data["id"])
            
        case "DDL": // Schema change
            fmt.Printf("DDL: %s\n", record.Data["_cdc_query"])
        }
    }
}
```

## Data Format

### CDC Record Structure

```json
{
  "_cdc_op": "u",                    // Operation: r, c, u, d, DDL
  "_cdc_timestamp": "2024-01-15T10:30:00Z",
  "_cdc_binlog_file": "mysql-bin.000042",
  "_cdc_binlog_pos": 4567,
  "_cdc_gtid": "3E11FA47-71CA-11E1-9E33-C80AA9429562:123",
  "_cdc_schema": "mydb",
  "_cdc_table": "users",
  "_cdc_server_id": 1001,
  
  // Table columns
  "id": 123,
  "name": "John Doe",
  "email": "john@example.com",
  "updated_at": "2024-01-15 10:30:00",
  
  // For updates, old values prefixed with _old_
  "_old_name": "John",
  "_old_email": "john.old@example.com"
}
```

### Binlog Event Types

| Operation | Code | Description |
|-----------|------|-------------|
| Snapshot | r | Initial data read |
| Insert | c | Row inserted |
| Update | u | Row updated |
| Delete | d | Row deleted |
| DDL | DDL | Schema change |

## Performance Optimization

### Snapshot Optimization

1. **Parallel Workers**: Speed up initial load
   ```yaml
   snapshot_parallel: 8
   snapshot_chunk_size: 50000
   ```

2. **Skip Snapshot**: For incremental only
   ```yaml
   snapshot_mode: "never"
   gtid: "last-known-gtid"
   ```

3. **Chunked Reading**: For large tables
   ```yaml
   snapshot_chunk_size: 100000
   ```

### Streaming Optimization

1. **Batch Processing**: Reduce overhead
   ```yaml
   batch_size: 10000
   flush_interval_ms: 100
   ```

2. **Event Size**: Handle large transactions
   ```yaml
   max_event_size: 67108864  # 64MB
   ```

3. **Network Optimization**: Compress binlog transfer
   ```sql
   SET GLOBAL slave_compressed_protocol = 1;
   ```

## Monitoring

### Key Metrics

- `records_read`: Total records processed
- `events_processed`: Binlog events processed
- `ddl_count`: DDL operations captured
- `lag_seconds`: Replication lag
- `binlog_file`: Current file
- `binlog_pos`: Current position

### Monitoring Queries

1. **Check Binlog Position**
   ```sql
   SHOW MASTER STATUS;
   ```

2. **Monitor Binlog Size**
   ```sql
   SHOW BINARY LOGS;
   ```

3. **Check Replication Lag**
   ```sql
   SELECT 
     TIMESTAMPDIFF(SECOND, 
       FROM_UNIXTIME(UNIX_TIMESTAMP(NOW()) - @@global.heartbeat_period), 
       NOW()) AS lag_seconds;
   ```

### Health Monitoring

```go
// Monitor lag
if source.GetMetric("lag_seconds") > 60 {
    log.Warn("High replication lag detected")
}

// Check position
pos := source.GetPosition()
fmt.Printf("Current position: %s:%d\n", pos.File, pos.Position)
```

## GTID vs Binlog Position

### When to Use GTID

Advantages:
- Automatic failover support
- Easier position management
- No file/position tracking

Requirements:
- MySQL 5.6+ with GTID enabled
- All transactions must be GTID compatible

### When to Use Binlog Position

Advantages:
- Works with older MySQL versions
- Simple position tracking
- No GTID compatibility requirements

Limitations:
- Manual failover handling
- Position management complexity

## Troubleshooting

### Common Issues

1. **Access Denied**
   ```sql
   -- Verify permissions
   SHOW GRANTS FOR 'cdc_user'@'%';
   ```

2. **Binlog Not Enabled**
   ```sql
   SHOW VARIABLES LIKE 'log_bin';
   ```

3. **Wrong Binlog Format**
   ```sql
   SHOW VARIABLES LIKE 'binlog_format';
   -- Must be 'ROW'
   ```

4. **Server ID Conflict**
   ```sql
   -- Ensure unique server_id
   SHOW VARIABLES LIKE 'server_id';
   ```

### Recovery Procedures

1. **Reset Position**
   ```yaml
   # Delete position file and restart
   rm /var/lib/mysql-cdc/position.json
   snapshot_mode: "initial"
   ```

2. **Skip Corrupted Event**
   ```yaml
   # Start from next safe position
   binlog_file: "mysql-bin.000043"
   binlog_pos: 0
   ```

## Best Practices

1. **Position Management**
   - Save positions frequently
   - Use GTID when possible
   - Monitor position lag

2. **Table Selection**
   - Start with critical tables
   - Exclude temporary tables
   - Consider separate streams for different workloads

3. **Resource Management**
   - Monitor binlog disk usage
   - Set appropriate retention
   - Clean old binlog files

4. **High Availability**
   - Test failover procedures
   - Use GTID for easier recovery
   - Monitor all replicas

## Limitations

1. **Binlog Format**: ROW format required for full data
2. **Data Types**: Some types have limited support
3. **DDL Replication**: Limited to certain operations
4. **Performance Impact**: Can affect source database
5. **Network Bandwidth**: High-volume changes require bandwidth

## Security Considerations

1. **Network Security**
   - Use SSL/TLS for connections
   - Implement IP whitelisting
   - Use VPN for remote replication

2. **Authentication**
   - Use strong passwords
   - Limit user permissions
   - Regular credential rotation

3. **Data Privacy**
   - Exclude sensitive tables
   - Implement column filtering
   - Audit access logs

## MariaDB Compatibility

The connector supports MariaDB with some considerations:

```yaml
flavor: "mariadb"
# MariaDB-specific GTID format
gtid: "0-1-123"
```

MariaDB differences:
- Different GTID format
- Some event types vary
- Compatible with MariaDB 10.0+