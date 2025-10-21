# Multiple WAL Write Locks Implementation

## Overview

This implementation adds infrastructure for multiple WAL write locks, similar to the existing WAL insertion locks (WALInsertLocks). The goal is to reduce contention on the single WALWriteLock by allowing multiple backends to write to disk concurrently.

## What Was Implemented

### 1. Configuration and Structure Definitions

- **GUC Parameter**: `log2_num_xlog_write_locks` (default: 3, giving 8 locks)
  - Added to `guc_tables.c` and `guc.h`
  - Controls `NUM_XLOG_WRITE_LOCKS = (1 << log2_num_xlog_write_locks)`

- **Data Structures** (`xlog.c`):
  - `WALWriteLockSlot`: Structure containing LWLock and atomic `writingAt` field
  - `WALWriteLockPadded`: Cache-line-aligned padded version
  - `WALWriteLocks`: Array of write locks in XLogCtlData
  - `SharedWritePtr`: Atomic pointer for chain reaction mechanism

### 2. Lock Management Functions

- **`WALWriteLockAcquire(int *lockno)`**
  - Acquires one of the write locks (round-robin with affinity)
  - Similar to `WALInsertLockAcquire`
  - Returns the lock number acquired

- **`WALWriteLockRelease(int lockno)`**
  - Releases the specified write lock

### 3. Chain Reaction Mechanism

- **`UpdateSharedWritePtr(XLogRecPtr writeStart, XLogRecPtr writeEnd, int lockno)`**
  - Implements the core chain reaction algorithm
  - Each process:
    1. Marks its write range in `writingAt`
    2. Waits until `SharedWritePtr` reaches its `writeStart` position
    3. Atomically updates `SharedWritePtr` to `writeEnd` using CAS
    4. This creates a chain where each writer enables the next

### 4. Infrastructure Setup

- **Shared Memory Allocation** (`XLOGShmemInit`):
  - Allocates array of `NUM_XLOG_WRITE_LOCKS` padded write locks
  - Initializes each lock with `LWTRANCHE_WAL_WRITE` tranche
  - Initializes `SharedWritePtr` atomic variable

- **Tranche Registration**:
  - Added `LWTRANCHE_WAL_WRITE` to `BuiltinTrancheIds` enum
  - Added "WALWrite" to `BuiltinTrancheNames` array

## Algorithm Design

The chain reaction mechanism is inspired by the lock-free algorithm used for `InitializedUpToAtomic` in buffer initialization:

1. **Write Phase**: Backend acquires a write lock, performs disk write
2. **Wait Phase**: Backend waits for `SharedWritePtr` to reach its write start position
3. **Update Phase**: Backend uses CAS to update `SharedWritePtr` from `writeStart` to `writeEnd`
4. **Chain Effect**: Updating `SharedWritePtr` allows the next backend to proceed

This ensures:
- **Ordering**: Writes are acknowledged in order
- **No Lost Updates**: CAS prevents race conditions
- **Progress**: Each backend that completes enables the next

## Integration Points

### Where to Integrate (Future Work)

The infrastructure is ready but needs integration into `XLogWrite`:

1. **Entry Point**: Replace `LWLockAcquire(WALWriteLock, ...)` with `WALWriteLockAcquire(&lockno)`

2. **Track Write Range**: Record:
   ```c
   XLogRecPtr myWriteStart = LogwrtResult.Write;
   // ... perform writes ...
   XLogRecPtr myWriteEnd = LogwrtResult.Write;
   ```

3. **Update Shared Pointer**: After writing:
   ```c
   UpdateSharedWritePtr(myWriteStart, myWriteEnd, lockno);
   ```

4. **Release Lock**: Replace `LWLockRelease(WALWriteLock)` with `WALWriteLockRelease(lockno)`

5. **Coordinate with Flush**: Ensure flush operations also use the mechanism correctly

### Challenges for Full Integration

- **XLogWrite Complexity**: The function has multiple exit paths and interacts with:
  - Segment switching
  - Fsync operations (WALFlushLock coordination)
  - Partial page writes
  - Recovery and backup states

- **Ordering Guarantees**: Must maintain:
  - Write ordering for durability
  - Coordination with `logWriteResult` atomic
  - Proper interaction with flush requests

- **Error Handling**: Critical sections and error recovery must be carefully handled

## Testing Strategy

1. **Unit Tests**: Test lock acquire/release and chain reaction in isolation
2. **Concurrency Tests**: Run with multiple concurrent writers
3. **Performance Tests**: Measure throughput improvement with pgbench
4. **Stress Tests**: Test under high contention scenarios
5. **Recovery Tests**: Ensure crash recovery still works correctly

## Configuration Tuning

- **Small Systems**: `log2_num_xlog_write_locks = 2` (4 locks)
- **Medium Systems**: `log2_num_xlog_write_locks = 3` (8 locks, default)
- **Large Systems**: `log2_num_xlog_write_locks = 4` (16 locks)

Monitor with `pg_stat_activity` and custom instrumentation to find optimal value.

## Files Modified

- `src/backend/access/transam/xlog.c`: Core implementation
- `src/backend/utils/misc/guc_tables.c`: GUC parameter
- `src/include/utils/guc.h`: GUC extern declaration
- `src/include/storage/lwlock.h`: Tranche ID
- `src/backend/storage/lmgr/lwlock.c`: Tranche name

## Build and Configuration

The code compiles successfully with:
```bash
./configure --enable-debug --enable-cassert --without-readline
make -j$(nproc)
```

Set in `postgresql.conf`:
```
log2_num_xlog_write_locks = 3  # 8 write locks
```

## Next Steps

1. **Phase 1**: Add instrumentation to track write lock contention
2. **Phase 2**: Integrate into a specific code path (e.g., `AdvanceXLInsertBuffer`)
3. **Phase 3**: Full integration into `XLogWrite`
4. **Phase 4**: Performance testing and tuning
5. **Phase 5**: Production readiness (error handling, edge cases)

## References

- Original patch: `reduce_walwritelock_contention.patch`
- WAL insertion locks: `WALInsertLock` implementation in `xlog.c`
- Lock-free algorithm: `InitializedUpToAtomic` mechanism
- Problem statement: Multiple write locks with chain reaction updates
