# Per-Lock Semaphore Implementation

## Overview

This implementation adds separate semaphores for different LWLock types in PostgreSQL, controlled by the new GUC parameter `enable_per_lock_semaphore`.

## Changes Made

### 1. PGPROC Structure (src/include/storage/proc.h)

Added three new fields to the PGPROC struct:
- `lwSem`: Array of semaphores for different LWLock types (NUM_FIXED_LWLOCKS + 32 for user-defined tranches)
- `procArrayGroupSem`: Dedicated semaphore for ProcArray group operations
- `clogGroupSem`: Dedicated semaphore for CLOG group operations

### 2. Semaphore Allocation (src/backend/storage/lmgr/proc.c)

- **ProcGlobalSemas()**: Updated to calculate the total number of semaphores needed:
  - Original sem: 1 per process
  - lwSem array: PGPROC_LWSEM_ARRAY_SIZE (245 semaphores) per process
  - procArrayGroupSem: 1 per process
  - clogGroupSem: 1 per process
  - Total: (1 + 245 + 2) = 248 semaphores per process

- **InitProcGlobal()**: Allocates and creates all semaphores during initialization

- **InitProcess()** and **InitAuxiliaryProcess()**: Reset semaphores when processes start

### 3. Semaphore Selection (src/backend/storage/lmgr/lwlock.c)

Added `pangolin_semaphore_for_lwlock()` function that:
- For locks in MainLWLockArray (fixed locks): returns dedicated semaphore for that lock
- For user-defined tranches: returns one of 32 extra semaphores, capping at the last semaphore if tranche ID exceeds the range

### 4. LWLock Updates (src/backend/storage/lmgr/lwlock.c)

Updated all LWLock operations to use per-lock semaphores when `enable_per_lock_semaphore` is enabled:
- LWLockWakeup
- LWLockDequeueSelf
- LWLockAcquire
- LWLockAcquireOrWait
- LWLockWaitForVar
- LWLockUpdateVar

### 5. Group Operations (src/backend/storage/ipc/procarray.c, src/backend/access/transam/clog.c)

- **ProcArrayGroupClearXid()**: Uses `procArrayGroupSem` when enabled
- **TransactionGroupUpdateXidStatus()**: Uses `clogGroupSem` when enabled

### 6. GUC Parameter (src/backend/utils/misc/guc_tables.c)

Added `enable_per_lock_semaphore` boolean GUC parameter:
- Type: PGC_POSTMASTER (requires server restart to change)
- Default: false (off)
- Category: DEVELOPER_OPTIONS

## Design Rationale

### Why Per-Lock Semaphores?

The traditional single semaphore per process can become a bottleneck when multiple LWLocks are contended. By using separate semaphores for different locks:

1. **Reduced Contention**: Processes waiting on different locks use different semaphores
2. **Better Scalability**: Allows the OS scheduler to wake up the right processes for the right locks
3. **Improved Performance**: Can reduce unnecessary wake-ups and context switches

### Semaphore Array Sizing

- **NUM_FIXED_LWLOCKS (213)**: Covers all predefined LWLocks in MainLWLockArray
- **32 Extra Semaphores**: For user-defined tranches from extensions
- **Overflow Handling**: User-defined tranches beyond 32 share the last semaphore

### Backward Compatibility

When `enable_per_lock_semaphore = off`:
- Uses original single semaphore per process (`proc->sem`)
- No behavioral changes
- Fully compatible with existing code

## Testing

Successfully tested with:
- Basic table operations (CREATE, INSERT, SELECT)
- Concurrent transactions
- Both enabled and disabled GUC states
- Build completes without errors or warnings

## Performance Considerations

**Benefits:**
- Reduces semaphore contention on heavily loaded systems
- Better CPU cache locality for lock-specific operations
- More precise wakeup targeting

**Costs:**
- Increased semaphore usage (248x more semaphores per process)
- Slightly more complex lock acquisition logic
- Additional memory overhead in PGPROC structure

## Future Work

Potential improvements:
- Dynamic semaphore allocation based on actual lock usage
- Performance benchmarking under various workloads
- Tuning the number of user-defined tranche semaphores
- Consider making it runtime configurable (not requiring restart)
