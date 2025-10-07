# Adaptive LWLock Implementation

## Overview

This implementation adds an Adaptive LWLock to PostgreSQL that reduces cache line contention for shared lock acquisitions. It has been applied to the ProcArrayLock, which is one of the most contended locks in high-concurrency workloads.

## Design

### Problem
Standard LWLocks have a significant performance issue: every shared lock acquisition causes cache line invalidation because the shared lock counter must be atomically incremented. This leads to excessive cache bouncing in read-heavy workloads.

### Solution
The Adaptive LWLock uses an array of cache-line-padded atomic counters:

- **Shared Lock Acquisition**: Randomly selects one of N atomic counters to increment
- **Exclusive Lock Acquisition**: Must acquire ALL N counters 
- **Adaptive Switching**: Based on workload statistics, exclusive lock holders can adjust the number of active counters

### Key Features

1. **Cache Line Padding**: Each counter sits in its own cache line (128 bytes) to prevent false sharing
2. **Memory Barriers**: Proper read/write barriers ensure consistency
3. **Retry Logic**: Validates that active counter count hasn't changed during acquisition
4. **Statistics**: Tracks acquisitions and contentions to guide adaptive behavior
5. **Workload Adaptation**:
   - High exclusive lock rate (>10%) + low contention (<1%) → reduce counters (faster exclusive)
   - High contention (>5%) → increase counters (less cache bouncing)

## Files

- `src/include/storage/adaptive_lwlock.h` - Header with structure definitions
- `src/backend/storage/lmgr/adaptive_lwlock.c` - Implementation
- `src/backend/storage/ipc/procarray.c` - Applied to ProcArrayLock
- `src/include/storage/procarray.h` - Macro wrappers for ProcArray

## Structure

```c
typedef struct AdaptiveLWLock
{
    uint16 tranche;                     // tranche ID
    pg_atomic_uint32 active_counters;   // number of active slots
    proclist_head waiters;              // wait queue
    AdaptiveLWLockCounter counters[8];  // cache-line-padded atomics
    AdaptiveLWLockStats *stats;         // statistics pointer
} AdaptiveLWLock;
```

## Usage Example

```c
// For ProcArrayLock, use the wrapper macros:
ProcArrayLockAcquire(LW_SHARED);      // shared lock
ProcArrayLockAcquire(LW_EXCLUSIVE);   // exclusive lock
ProcArrayLockRelease();                // release
```
## Performance Characteristics
### Read-Heavy Workloads
- **Benefit**: Significant reduction in cache line contention
- **Mechanism**: Multiple counters spread load across cache lines
- **Trade-off**: Slightly more complex acquisition logic
### Write-Heavy Workloads  
- **Benefit**: Adaptive reduction to single counter
- **Mechanism**: Statistics-driven adjustment by exclusive lock holders
- **Trade-off**: May not provide benefits over standard LWLock
### Mixed Workloads
- **Benefit**: Automatic adaptation to workload characteristics
- **Mechanism**: Continuous monitoring and adjustment
- **Trade-off**: Small overhead for statistics tracking
## Implementation Details
### Shared Lock Acquisition
1. Read current active_counters value (snapshot)
2. Randomly select counter slot based on process ID
3. Atomically increment selected counter
4. Verify active_counters hasn't changed (with read barrier)
5. If changed, decrement and retry
### Exclusive Lock Acquisition
1. Read current active_counters value
2. For each active counter:
   - Try to CAS from 0 to EXCLUSIVE_FLAG
   - If any fails, rollback and retry
3. After success, optionally adjust active_counters based on stats
### Adaptive Adjustment
Performed by exclusive lock holders after acquiring lock:
- Sample: Requires 1000+ total acquisitions
- Reduce counters if: exclusive_rate > 10% AND contention < 1%
- Increase counters if: contention > 5%
- Reset stats after adjustment to prevent oscillation
## Future Enhancements
Potential improvements to consider:
1. Apply to other heavily contended locks (e.g., WALWriteLock)
2. Per-NUMA-node counter affinity
3. More sophisticated adaptation algorithms
4. Runtime configuration of counter counts
5. Performance monitoring/instrumentation integration
## Testing
To test the implementation:
1. Build PostgreSQL: `make clean && make -j4`
2. Run regression tests: `make check`
3. Use pgbench for concurrency testing:
   ```bash
   pgbench -i test
   pgbench -c 64 -j 16 -T 60 test
   ```

## Commit History

1. **Add Adaptive LWLock implementation** - Core data structures and algorithms
2. **Apply Adaptive LWLock to ProcArrayLock** - Replace ProcArrayLock with adaptive version