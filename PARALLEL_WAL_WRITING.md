# Parallel WAL Writing Implementation

## Overview

This implementation addresses the bottleneck in PostgreSQL's Write-Ahead Logging (WAL) system where only one process at a time could write WAL data to disk under `WALWriteLock`. The new approach uses atomic operations to enable multiple processes to write to WAL concurrently.

## Key Changes

### 1. New Atomic Variables

Three new atomic variables were added to `XLogCtlData` structure in `xlog.c`:

- **`WALWriteReserveUpTo`**: Tracks the position up to which write slots have been reserved
- **`WALWriteSyncReserveUpTo`**: Tracks the position up to which fsync has been reserved
- **`WALWriteSyncDoneUpTo`**: Tracks the position up to which fsync has been completed

### 2. Parallel Write Reservation

The `XLogWrite` function now uses Compare-And-Swap (CAS) operations on `WALWriteReserveUpTo` to allow multiple processes to reserve write positions concurrently:

```c
do {
    expectedReserved = pg_atomic_read_u64(&XLogCtl->WALWriteReserveUpTo);
    
    if (expectedReserved >= WriteRqst.Write) {
        // Someone already reserved past our position
        return;
    }
    
    reservedUpTo = WriteRqst.Write;
} while (!pg_atomic_compare_exchange_u64(&XLogCtl->WALWriteReserveUpTo,
                                          &expectedReserved, reservedUpTo));
```

### 3. Parallel Fsync Coordination

Multiple processes coordinate fsync operations using CAS on `WALWriteSyncReserveUpTo`:

```c
do {
    expectedSyncReserved = pg_atomic_read_u64(&XLogCtl->WALWriteSyncReserveUpTo);
    syncReserved = LogwrtResult.Write;
} while (!pg_atomic_compare_exchange_u64(&XLogCtl->WALWriteSyncReserveUpTo,
                                          &expectedSyncReserved, syncReserved));

if (expectedSyncReserved < LogwrtResult.Write) {
    // We advanced the sync position, perform fsync
    issue_xlog_fsync(openLogFile, openLogSegNo, tli);
    pg_atomic_write_u64(&XLogCtl->WALWriteSyncDoneUpTo, LogwrtResult.Write);
}
```

### 4. Segment Boundary Handling

When a process writes the last page of a WAL segment, it performs fsync immediately to ensure data durability at segment boundaries. This is coordinated through the same atomic CAS mechanism.

### 5. Removed Lock Dependencies

The following functions no longer require `WALWriteLock`:
- `XLogWrite` - now uses atomic operations for coordination
- `XLogFlush` - directly calls `XLogWrite` without acquiring lock
- `XLogBackgroundFlush` - directly calls `XLogWrite` without acquiring lock
- `AdvanceXLInsertBuffer` - writes without acquiring lock when needed

### 6. Synchronization Protection

The `lastSegSwitchTime` and `lastSegSwitchLSN` fields, previously protected by `WALWriteLock`, are now protected by the existing `info_lck` spinlock to maintain thread safety with minimal overhead.

## Benefits

1. **Reduced Contention**: Multiple processes can write to WAL concurrently, eliminating the single-process bottleneck
2. **Better Throughput**: Parallel writes can better utilize disk I/O bandwidth
3. **Improved Scalability**: The system scales better with multiple concurrent transactions

## Concurrency Model

The implementation follows these principles:

1. **Write Reservation**: Each process reserves a write range using CAS, ensuring no overlapping writes
2. **Write Execution**: Processes write their reserved data independently and concurrently
3. **Fsync Coordination**: Only one process performs fsync for any given position range
4. **Boundary Handling**: Segment boundaries trigger immediate fsync by the process that writes the last page

## Compatibility

The implementation maintains compatibility with:
- All existing WAL sync methods (fsync, fdatasync, open_sync, etc.)
- Recovery and replication mechanisms
- Checkpoint and archiving processes
- All PostgreSQL synchronization primitives

## Future Considerations

- Performance benchmarking under various workloads
- Potential optimizations for fsync batching across segments
- Monitoring and statistics for parallel WAL write activity
