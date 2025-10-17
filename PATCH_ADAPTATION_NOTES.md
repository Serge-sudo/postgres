# Patch Adaptation Notes for PostgreSQL 17

## Overview
This patch (reduce_walwritelock_contention.patch) was originally written for an older version of PostgreSQL and requires significant adaptation for PG17.

## Key Architectural Differences in PG17

### 1. Atomic Operations for LogwrtResult
**Old (patch assumes)**:
- Uses `SpinLockAcquire(&XLogCtl->info_lck)` for LogwrtResult access
- Direct field access: `LogwrtResult = XLogCtl->LogwrtResult;`

**PG17**:
- Uses atomic operations: `pg_atomic_read_u64(&XLogCtl->logWriteResult)`
- Macro: `RefreshXLogWriteResult(LogwrtResult)` for safe reads
- Separate atomic variables for Write and Flush

### 2. TimeLineID Parameter
**Old (patch)**:
- `XLogWrite(WriteRqst, flexible)`

**PG17**:
- `XLogWrite(WriteRqst, tli, flexible)`
- TimeLineID must be tracked and passed

### 3. LWLock Names
**Old**:
- lwlocknames.txt file

**PG17**:
- lwlocklist.h with PG_LWLOCK() macros
- wait_event_names.txt for descriptions
- generate-lwlocknames.pl script generates header

## Changes Applied So Far

### ✅ Completed
1. Added WALFlushLock to lwlocklist.h (position 10)
2. Added WALFlush to wait_event_names.txt
3. Added PGPROC_LIST struct to proc.h
4. Added writeWAL, writePos, pendingWriteWALLinks to PGPROC
5. Added pendingWriteWALList to PROC_HDR
6. Initialized fields in proc.c (InitProcGlobal, InitProcess)
7. Updated XLogWrite and added XLogFsync function declarations in xlog.c

### ⏳ Remaining xlog.c Changes

#### Required Modifications

1. **Update XLogWrite() signature and all calls**:
   - Add PGPROC_LIST *wake_pendingWriteWALElem parameter
   - Pass TimeLineID (tli/insertTLI) appropriately
   - Update all call sites (4-5 locations)

2. **Adapt atomic operations**:
   - Replace spinlock-based LogwrtResult updates with atomic operations
   - Use pg_atomic_read_u64 and pg_atomic_write_u64
   - Maintain proper memory barriers

3. **Implement XLogWrite() changes** (~200 lines):
   - Add wake_pendingWriteWALElem parameter handling
   - Move lock release to allow early wakeup of waiting backends
   - Add spinlock protection for critical LogwrtResult updates
   - Handle finishing_seg case with intermediate lock release/reacquire
   - Update shared memory status with Write progress

4. **Implement XLogFlush() changes** (~180 lines):
   - Add group commit mechanism
   - Implement pendingWriteWALList management with atomic CAS operations
   - Add write leader selection logic
   - Batch WAL write requests
   - Wake up waiting backends after write completes
   - Handle flush-only case for backends that didn't write

5. **Implement XLogFsync()** (~70 lines):
   - New static function for flush-only operations
   - Similar to XLogWrite but only fsyncs
   - Must handle WALFlushLock properly

6. **Update XLogBackgroundFlush()**:
   - Pass NULL for wake_pendingWriteWALElem
   - Add conditional lock release

## Critical Considerations

### Memory Ordering
- The patch uses pg_write_barrier() and pg_read_barrier()
- These must be preserved for correctness of lock-free list operations

### Lock Ordering
- WALWriteLock must be acquired before WALFlushLock
- Intermediate releases require careful state management

### Backward Compatibility
- New fields in PGPROC/PROC_HDR change shared memory layout
- This breaks compatibility with existing clusters (requires initdb)

## Testing Requirements

1. **Build Test**: Verify compilation without errors/warnings
2. **Regression Tests**: Run make check to ensure no regressions
3. **Performance Tests**: Measure improvement in high-concurrency scenarios
4. **Crash Recovery**: Ensure WAL recovery still works correctly

## Next Steps

1. Carefully adapt XLogWrite() to PG17's atomic operations
2. Implement the group commit logic in XLogFlush()
3. Add XLogFsync() function
4. Update all XLogWrite() call sites
5. Test compilation
6. Fix any build errors
7. Run regression tests
8. Document performance characteristics

## Estimated Complexity

- **Lines to modify**: ~400-500 in xlog.c alone
- **Functions affected**: 3 major (XLogWrite, XLogFlush, XLogBackgroundFlush) + 1 new (XLogFsync)
- **Risk level**: High - core WAL functionality, must be correct
- **Testing burden**: High - need extensive testing for correctness and performance

