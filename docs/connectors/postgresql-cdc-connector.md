# PostgreSQL CDC Source Connector

## Overview

Captures real-time changes from PostgreSQL databases using logical replication for INSERT, UPDATE, DELETE operations.

## Configuration

```yaml
source:
  type: postgresql_cdc
  properties:
    # Connection (Required)
    host: "localhost"
    port: 5432
    database: "mydb"
    username: "postgres"
    password: "password"
    
    # CDC Configuration
    slot_name: "nebula_slot"
    publication_name: "nebula_pub"
    tables: ["public.users", "public.orders"]
    
    # Optional: Initial snapshot
    initial_snapshot: true
    snapshot_mode: "initial"  # initial, never, when_needed
```

## Prerequisites

### PostgreSQL Setup

1. **Enable logical replication** in `postgresql.conf`:
   ```
   wal_level = logical
   max_replication_slots = 4
   max_wal_senders = 4
   ```

2. **Create publication** (as superuser):
   ```sql
   CREATE PUBLICATION nebula_pub FOR TABLE users, orders;
   ```

3. **Grant permissions**:
   ```sql
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO your_user;
   GRANT REPLICATION ON DATABASE mydb TO your_user;
   ```

## Features

- **Logical Replication**: Uses PostgreSQL's native CDC
- **Initial Snapshot**: Consistent point-in-time snapshot
- **Position Tracking**: LSN-based exactly-once delivery
- **Schema Evolution**: Handles DDL changes
- **Table Filtering**: Include/exclude specific tables

## Data Format

Changes are captured with metadata:
- **Operation**: INSERT, UPDATE, DELETE
- **Table**: Source table name
- **Before/After**: Row data before and after change
- **LSN**: Log sequence number for ordering
- **Timestamp**: Change timestamp

## Notes

- Requires PostgreSQL 10+ for pgoutput plugin
- Replication slot persists changes until consumed
- Monitor replication lag and disk usage
- DDL changes may require connector restart