# PostgreSQL Temporary Tables Fork Analysis Report

## Executive Summary

This report analyzes how PostgreSQL handles fork creation for temporary tables compared to permanent and unlogged relations. The key finding is that **temporary tables do NOT create init forks**, as hypothesized, because they do not need crash recovery and all data operations occur within a single backend session.

## Fork Types in PostgreSQL

PostgreSQL uses several relation forks for different purposes:

1. **MAIN_FORKNUM (main)** - Primary data storage
2. **FSM_FORKNUM (fsm)** - Free Space Map for tracking available space
3. **VISIBILITYMAP_FORKNUM (vm)** - Visibility Map for vacuum optimization
4. **INIT_FORKNUM (init)** - Initial state for unlogged relations crash recovery

## Analysis Findings

### 1. Main Fork Creation

**Location**: `src/backend/catalog/storage.c:RelationCreateStorage()`

All relation types (permanent, unlogged, and temporary) create the main fork at table creation time:

```c
srel = smgropen(rlocator, procNumber);
smgrcreate(srel, MAIN_FORKNUM, false);
```

### 2. Init Fork Creation

**Location**: `src/backend/access/heap/heapam_handler.c:heapam_relation_set_new_filelocator()`

**Key Finding**: Init forks are ONLY created for unlogged relations, NOT for temporary tables:

```c
/*
 * If required, set up an init fork for an unlogged table so that it can
 * be correctly reinitialized on restart.  Recovery may remove it while
 * replaying, for example, an XLOG_DBASE_CREATE* or XLOG_TBLSPC_CREATE
 * record.  Therefore, logging is necessary even if wal_level=minimal.
 */
if (persistence == RELPERSISTENCE_UNLOGGED)
{
    Assert(rel->rd_rel->relkind == RELKIND_RELATION ||
           rel->rd_rel->relkind == RELKIND_MATVIEW ||
           rel->rd_rel->relkind == RELKIND_TOASTVALUE);
    smgrcreate(srel, INIT_FORKNUM, false);
    log_smgrcreate(newrlocator, INIT_FORKNUM);
}
```

**Rationale**: Temporary tables don't need init forks because:
- They don't survive server crashes/restarts
- They are session-local and automatically cleaned up
- No crash recovery mechanism is needed

### 3. WAL Logging Behavior

**Location**: `src/backend/catalog/storage.c:RelationCreateStorage()`

```c
switch (relpersistence)
{
    case RELPERSISTENCE_TEMP:
        procNumber = ProcNumberForTempRelations();
        needs_wal = false;  // ← No WAL logging for temp tables
        break;
    case RELPERSISTENCE_UNLOGGED:
        procNumber = INVALID_PROC_NUMBER;
        needs_wal = false;  // ← No WAL logging for unlogged tables
        break;
    case RELPERSISTENCE_PERMANENT:
        procNumber = INVALID_PROC_NUMBER;
        needs_wal = true;   // ← WAL logging required
        break;
}
```

### 4. FSM and VM Fork Creation

**Pattern**: These forks are created **lazily** (on-demand) for all relation types, including temporary tables.

**Evidence**:
- `src/backend/storage/freespace/freespace.c`: Uses `smgrexists()` checks before operations
- `src/backend/access/heap/visibilitymap.c`: Similar lazy creation pattern

Example from FSM code:
```c
/*
 * If no FSM has been created yet for this relation, there's nothing to
 * truncate.
 */
if (!smgrexists(RelationGetSmgr(rel), FSM_FORKNUM))
    return InvalidBlockNumber;
```

## Code Locations Summary

| Fork Type | Creation Pattern | Key Files | Temporary Table Behavior |
|-----------|-----------------|-----------|-------------------------|
| **MAIN** | At table creation | `src/backend/catalog/storage.c:150` | ✅ Created |
| **INIT** | Only for unlogged | `src/backend/access/heap/heapam_handler.c:331-338` | ❌ Not created |
| **FSM** | Lazy/on-demand | `src/backend/storage/freespace/freespace.c` | ✅ Created when needed |
| **VM** | Lazy/on-demand | `src/backend/access/heap/visibilitymap.c` | ✅ Created when needed |

## Key Data Structures

### PendingRelDelete Structure
**Location**: `src/backend/catalog/storage.c:61-68`

```c
typedef struct PendingRelDelete
{
    RelFileLocator rlocator;    /* relation that may need to be deleted */
    ProcNumber  procNumber;     /* INVALID_PROC_NUMBER if not a temp rel */
    bool        atCommit;       /* T=delete at commit; F=delete at abort */
    int         nestLevel;      /* xact nesting level of request */
    struct PendingRelDelete *next;  /* linked-list link */
} PendingRelDelete;
```

Note: Temporary relations have a specific `procNumber` (not `INVALID_PROC_NUMBER`).

### RelationData Fields
**Location**: `src/include/utils/rel.h:60-61`

```c
ProcNumber  rd_backend;     /* owning backend's proc number, if temp rel */
bool        rd_islocaltemp; /* rel is a temp rel of this session */
```

## Important Macros

### RelationNeedsWAL
**Location**: `src/include/utils/rel.h:628-631`

```c
#define RelationNeedsWAL(relation)                                      \
    (RelationIsPermanent(relation) && (XLogIsNeeded() ||                \
      (relation->rd_createSubid == InvalidSubTransactionId &&           \
       relation->rd_firstRelfilelocatorSubid == InvalidSubTransactionId)))
```

This macro returns `false` for temporary tables since they are not permanent.

### RelationUsesLocalBuffers
**Location**: `src/include/utils/rel.h:637-638`

```c
#define RelationUsesLocalBuffers(relation) \
    ((relation)->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
```

## Conclusions

1. **Init Fork**: Temporary tables do NOT create init forks, confirming the hypothesis. Only unlogged relations create init forks for crash recovery purposes.

2. **FSM/VM Forks**: Temporary tables DO create FSM and VM forks when needed, using the same lazy creation pattern as other relation types.

3. **WAL Logging**: Temporary tables never write to WAL (`needs_wal = false`), which is consistent with their session-local, non-persistent nature.

4. **Storage Management**: Temporary tables use a special proc number and are tracked differently for cleanup purposes, but follow similar storage patterns for data management forks.

## Recommendations

1. The current implementation is correct and efficient - temporary tables don't need init forks.
2. The lazy creation of FSM/VM forks for temporary tables is appropriate since these improve performance even for session-local data.
3. The WAL exemption for temporary tables is a significant performance benefit for temporary data operations.

## Test Cases Needed

To verify this analysis, test cases should be created that:
1. Create temporary, unlogged, and permanent tables
2. Check filesystem for existence of different fork files
3. Verify that only unlogged tables have init forks
4. Confirm FSM/VM forks are created on-demand for all types