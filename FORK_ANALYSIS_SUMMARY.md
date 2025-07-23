## Summary: PostgreSQL Temporary Tables Fork Creation Analysis

### Quick Answer
**Do temporary tables create init forks?** ❌ **NO**  
**Do temporary tables create VM forks?** ❌ **NO** (no MVCC needed for single backend)
**Do temporary tables create FSM forks?** ✅ **YES** (on-demand for space management)

### The Investigation

This analysis examined PostgreSQL's source code to understand fork creation patterns for temporary tables versus other relation types. The hypothesis was correct: temporary tables do not create init forks because they don't need crash recovery.

### Key Findings

| Table Type | Main Fork | Init Fork | FSM Fork | VM Fork | WAL Logged |
|------------|-----------|-----------|----------|---------|------------|
| **Temporary** | ✅ Always | ❌ Never | ✅ On-demand | ❌ Never | ❌ No |
| **Unlogged** | ✅ Always | ✅ Always | ✅ On-demand | ✅ On-demand | ❌ No |
| **Permanent** | ✅ Always | ❌ Never | ✅ On-demand | ✅ On-demand | ✅ Yes |

### Code Evidence

1. **RelationCreateStorage()** in `src/backend/catalog/storage.c:130-147`
   - Sets `needs_wal = false` for temporary tables
   - Only creates MAIN_FORKNUM initially

2. **heapam_relation_set_new_filelocator()** in `src/backend/access/heap/heapam_handler.c:331-338`
   - Init fork creation is conditional: `if (persistence == RELPERSISTENCE_UNLOGGED)`
   - Comment explicitly states purpose: "reinitialized on restart"

3. **Lazy Fork Creation** pattern for FSM and VM modules
   - FSM: Uses `smgrexists()` checks before operations, created when needed for all table types
   - VM: Created only for permanent and unlogged tables, not for temporary tables

### Why This Design Makes Sense

- **Temporary tables**: Session-local, don't survive crashes → no init fork needed, no VM fork needed (no MVCC)
- **Unlogged tables**: Persist across sessions but reset on crash → init fork for recovery, VM fork for multi-session visibility
- **Permanent tables**: Full crash recovery via WAL → no init fork needed, VM fork for multi-session visibility
- **FSM forks**: Space management optimization useful for all table types within their respective scopes

### Files Created in This Analysis

1. `TEMPORARY_TABLES_FORK_ANALYSIS.md` - Comprehensive technical report
2. `test_fork_creation.sh` - Test script to verify behavior
3. This summary document

### Verification

The analysis can be verified by:
1. Creating tables of different persistence types
2. Examining the data directory for fork files
3. Confirming only unlogged tables have `_init` files
4. Observing FSM files appear on-demand for space management
5. Verifying that VM files are NOT created for temporary tables

This investigation confirms that PostgreSQL's fork creation strategy is efficient and appropriate for each table type's persistence and concurrency requirements.