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

### 4. FSM Fork Creation

**Pattern**: FSM (Free Space Map) forks are created **lazily** (on-demand) for all relation types to track available space within pages.

**Evidence**:
- `src/backend/storage/freespace/freespace.c`: Uses `smgrexists()` checks before operations

Example from FSM code:
```c
/*
 * If no FSM has been created yet for this relation, there's nothing to
 * truncate.
 */
if (!smgrexists(RelationGetSmgr(rel), FSM_FORKNUM))
    return InvalidBlockNumber;
```

### 5. VM (Visibility Map) Fork Creation

**CORRECTED Analysis**: VM forks **CAN be created for temporary tables** during manual VACUUM operations.

**Key Findings**:

1. **Autovacuum behavior**: Autovacuum explicitly skips temporary tables, so VM forks are NOT created by autovacuum.

**Evidence from autovacuum.c:2086**:
```c
/*
 * We cannot safely process other backends' temp tables, so skip 'em.
 */
if (classForm->relpersistence == RELPERSISTENCE_TEMP)
    continue;
```

2. **Manual VACUUM behavior**: Manual VACUUM commands CAN run on temporary tables from the same backend and WILL create VM forks.

**Evidence from vacuum.c:2095-2100**:
```c
/*
 * Silently ignore tables that are temp tables of other backends ---
 * trying to vacuum these will lead to great unhappiness, since their
 * contents are probably not up-to-date on disk.
 */
if (RELATION_IS_OTHER_TEMP(rel))  // Only skips OTHER backends' temp tables
{
    relation_close(rel, lmode);
    PopActiveSnapshot();
    CommitTransactionCommand();
    return false;
}
```

**VM Fork Creation Code Path for Manual VACUUM**:

1. `vacuum.c:vacuum_rel()` → `table_relation_vacuum()` 
2. `vacuumlazy.c:heap_vacuum_rel()` → `lazy_scan_heap()`
3. `vacuumlazy.c:913: visibilitymap_pin(vacrel->rel, blkno, &vmbuffer)`
4. `visibilitymap.c:203: *vmbuf = vm_readbuf(rel, mapBlock, true)` ← `extend=true`
5. `visibilitymap.c:574: buf = vm_extend(rel, blkno + 1)` (if beyond current size)
6. `bufmgr.c:617: ExtendBufferedRelTo(..., EB_CREATE_FORK_IF_NEEDED, ...)`
7. `bufmgr.c:945: smgrcreate(bmr.smgr, fork, flags & EB_PERFORMING_RECOVERY)` ← **VM fork created**

**Practical Impact**:
- Temporary tables from the current backend can be manually vacuumed
- Manual VACUUM will create VM forks for temporary tables if they don't exist
- VM fork creation logic does NOT check relation persistence type
- This behavior is technically possible but rarely occurs in practice since:
  - Temporary tables are typically short-lived
  - Users rarely run manual VACUUM on temporary tables
  - The visibility benefits are minimal for single-backend access

## Code Locations Summary

| Fork Type | Creation Pattern | Key Files | Temporary Table Behavior |
|-----------|-----------------|-----------|-------------------------|
| **MAIN** | At table creation | `src/backend/catalog/storage.c:150` | ✅ Created |
| **INIT** | Only for unlogged | `src/backend/access/heap/heapam_handler.c:331-338` | ❌ Not created |
| **FSM** | Lazy/on-demand | `src/backend/storage/freespace/freespace.c` | ✅ Created when needed |
| **VM** | Lazy/on-demand during VACUUM | `src/backend/access/heap/visibilitymap.c` | ⚠️ Can be created during manual VACUUM |

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

2. **FSM Forks**: Temporary tables DO create FSM forks when needed for space management within the single backend session.

3. **VM Forks**: **CORRECTED**: Temporary tables CAN create VM forks during manual VACUUM operations, although this is rare in practice. Autovacuum skips temporary tables, but manual VACUUM can run on temporary tables from the same backend and will create VM forks if needed.

4. **WAL Logging**: Temporary tables never write to WAL (`needs_wal = false`), which is consistent with their session-local, non-persistent nature.

5. **Storage Management**: Temporary tables use a special proc number and are tracked differently for cleanup purposes, but follow similar storage patterns for data management forks (main, FSM, and potentially VM during manual vacuum only).

6. **Autovacuum Behavior**: Autovacuum explicitly skips temporary tables since cross-session optimization strategies are not applicable to session-local data.

## Recommendations

1. The current implementation is correct and efficient - temporary tables don't need init forks.
2. The lazy creation of FSM forks for temporary tables is appropriate since space management is still beneficial within a single backend session.
3. The WAL exemption for temporary tables is a significant performance benefit for temporary data operations.
4. **CORRECTED**: VM forks can be created for temporary tables during manual VACUUM, but this behavior is technically correct since the VM creation logic doesn't (and shouldn't need to) check persistence type. The practical impact is minimal since:
   - Temporary tables are rarely manually vacuumed
   - VM benefits are minimal for single-backend access
   - Autovacuum correctly skips temporary tables

## Test Cases Needed

To verify this analysis, test cases should be created that:
1. Create temporary, unlogged, and permanent tables
2. Check filesystem for existence of different fork files
3. Verify that only unlogged tables have init forks
4. Confirm FSM forks are created on-demand for all types when space management is needed
5. Verify that VM forks are NOT created for temporary tables, only for permanent and unlogged tables