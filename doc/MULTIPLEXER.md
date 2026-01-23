# Foreign Connection Multiplexer for PostgreSQL

## Overview

The Foreign Connection Multiplexer is a new feature that routes all foreign data wrapper (FDW) connections through a pool of background worker processes instead of establishing direct connections. This architecture provides:

- **Centralized Connection Management**: All foreign connections are managed by dedicated workers
- **Load Distribution**: Worker pool distributes connection load across multiple processes
- **Scalability**: Easily scale foreign connections by adjusting worker count
- **Monitoring**: Simplified monitoring of foreign connections through dedicated workers

## Architecture

```
┌──────────────┐
│  Backend 1   │───┐
└──────────────┘   │
                   │
┌──────────────┐   │    ┌─────────────────┐    ┌──────────────────┐
│  Backend 2   │───┼───>│  Worker Pool    │───>│  Foreign Server  │
└──────────────┘   │    │  (Multiplexer)  │    │                  │
                   │    └─────────────────┘    └──────────────────┘
┌──────────────┐   │
│  Backend N   │───┘
└──────────────┘
```

### Components

1. **Connection Multiplexer Module** (`src/backend/foreign/conn_multiplexer.c`)
   - Manages worker pool lifecycle
   - Tracks worker availability
   - Implements round-robin worker selection

2. **Background Workers**
   - Handle actual foreign server connections
   - Process connection requests from backends
   - Manage query forwarding and result routing

3. **postgres_fdw Integration** (`contrib/postgres_fdw/connection.c`)
   - Detects when multiplexer is enabled
   - Routes connections through workers
   - Maintains compatibility with direct connections

## Configuration

### GUC Parameters

#### `foreign_conn_multiplexer.workers`

- **Type**: Integer
- **Default**: 0 (disabled)
- **Range**: 0 to MAX_BACKENDS
- **Context**: POSTMASTER (requires restart)
- **Description**: Number of worker processes for foreign connection multiplexing

Set to 0 to disable the multiplexer. When set to a value greater than 0, that many background workers will be started at postmaster startup to handle foreign connections.

```sql
-- Enable with 4 workers
ALTER SYSTEM SET foreign_conn_multiplexer.workers = 4;
```

#### `foreign_conn_multiplexer.enabled`

- **Type**: Boolean
- **Default**: false
- **Context**: SIGHUP (reload without restart)
- **Description**: Enable connection multiplexer for foreign servers

When enabled along with `foreign_conn_multiplexer.workers > 0`, foreign connections will be routed through workers. This can be toggled without restarting PostgreSQL.

```sql
-- Enable the multiplexer
ALTER SYSTEM SET foreign_conn_multiplexer.enabled = true;
SELECT pg_reload_conf();
```

## Usage

### Basic Setup

1. **Configure the multiplexer in postgresql.conf:**

```ini
# Enable multiplexer with 4 workers
foreign_conn_multiplexer.workers = 4
foreign_conn_multiplexer.enabled = true
```

2. **Restart PostgreSQL** (for .workers parameter):

```bash
pg_ctl restart -D $PGDATA
```

3. **Verify workers are running:**

```sql
SELECT * FROM pg_stat_activity WHERE backend_type = 'conn_multiplexer worker';
```

### Dynamic Enable/Disable

You can enable or disable the multiplexer without restarting:

```sql
-- Disable temporarily
ALTER SYSTEM SET foreign_conn_multiplexer.enabled = false;
SELECT pg_reload_conf();

-- Re-enable
ALTER SYSTEM SET foreign_conn_multiplexer.enabled = true;
SELECT pg_reload_conf();
```

When disabled, connections fall back to direct connection mode automatically.

## Monitoring

### Check Worker Status

```sql
SELECT pid, backend_start, state, wait_event_type, wait_event
FROM pg_stat_activity
WHERE backend_type LIKE '%conn_multiplexer%';
```

### View Multiplexer Configuration

```sql
SHOW foreign_conn_multiplexer.workers;
SHOW foreign_conn_multiplexer.enabled;
```

## Implementation Details

### Connection Flow

1. Backend requests foreign connection via postgres_fdw
2. `GetConnection()` checks if multiplexer is enabled
3. If enabled, `MultiplexerConnect()` selects next available worker (round-robin)
4. Connection request is sent to worker
5. Worker establishes actual connection to foreign server
6. Worker handles query forwarding and result routing
7. Backend receives results as if directly connected

### Worker Pool Management

- Workers are registered as background processes during postmaster startup
- Each worker maintains its own set of foreign connections
- Round-robin algorithm distributes new connections across workers
- Workers restart automatically after 10 seconds if they crash

### Shared Memory

The multiplexer uses a small shared memory segment to track:
- Number of active workers
- Next worker for round-robin selection
- Initialization state

## Compatibility

### When Multiplexer is Used

- `foreign_conn_multiplexer.workers > 0` AND
- `foreign_conn_multiplexer.enabled = true` AND
- Workers have successfully initialized

### When Direct Connection is Used

- Multiplexer is disabled, OR
- Workers are not configured (workers = 0), OR
- Multiplexer connection fails (automatic fallback)

### Existing Applications

The multiplexer is transparent to applications. Queries work identically whether connections go through the multiplexer or directly to foreign servers.

## Performance Considerations

### Worker Count

- Start with 4-8 workers for most workloads
- Scale up if workers become bottlenecks
- Monitor worker CPU usage and connection counts

### Overhead

- Small overhead for message passing between backend and workers
- Offset by better connection pooling and management
- Most beneficial with many concurrent foreign connections

## Troubleshooting

### Workers Not Starting

Check logs for:
```
connection multiplexer worker N started
```

If missing, verify:
- `foreign_conn_multiplexer.workers` is > 0
- Postmaster was restarted after changing .workers
- System has available resources for workers

### Connections Not Using Multiplexer

Check that:
1. `foreign_conn_multiplexer.enabled = true`
2. Workers are running (check pg_stat_activity)
3. Log messages show "connection through multiplexer"

Enable debug logging:
```sql
SET client_min_messages = DEBUG1;
```

### Performance Issues

If multiplexer causes slowdowns:
- Increase worker count
- Consider disabling for low-connection workloads
- Check worker resource utilization

## Future Enhancements

Planned improvements include:
- Worker-to-worker communication for distributed foreign queries
- Connection pooling within workers
- Advanced load balancing algorithms
- Detailed statistics and monitoring views
- Integration with connection limits and resource management

## References

- Main Implementation: `src/backend/foreign/conn_multiplexer.c`
- postgres_fdw Integration: `contrib/postgres_fdw/connection.c`
- GUC Definitions: `src/backend/utils/misc/guc_tables.c`
- Worker Registration: `src/backend/postmaster/bgworker.c`
