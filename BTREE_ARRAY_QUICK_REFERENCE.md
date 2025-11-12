# B-tree Array Optimization - Quick Reference

## TL;DR - The Answer to "Does IN do separate lookups?"

**NO.** PostgreSQL does NOT perform separate B-tree lookups for each element in an `IN` clause.

Instead, it:
1. Preprocesses the array (sort, deduplicate, merge if multiple arrays)
2. Performs ONE index scan that advances through array elements efficiently
3. Uses binary search to find the next matching array element when needed

## Key Numbers

For `WHERE id IN (v1, v2, ..., vK)` with index of size N:

| Operation | With Array Optimization | Without (K separate lookups) |
|-----------|------------------------|------------------------------|
| Tree descents | 1 | K |
| Time complexity | O(log N + M + K log K) | O(K × log N) |
| Page cache benefits | ✓ High (sequential) | ✗ Low (random) |

Where M = number of tuples scanned between first and last array value.

## File Locations

| Component | File | Key Function | Line |
|-----------|------|--------------|------|
| Array preprocessing | `src/backend/access/nbtree/nbtpreprocesskeys.c` | `_bt_preprocess_array_keys()` | 1116 |
| Binary search | `src/backend/access/nbtree/nbtutils.c` | `_bt_binsrch_array_skey()` | 251 |
| Array advancement | `src/backend/access/nbtree/nbtutils.c` | `_bt_advance_array_keys()` | 863 |
| Data structures | `src/include/access/nbtree.h` | `BTArrayKeyInfo` | 1028 |

## Preprocessing Optimizations

```
Input:  WHERE id IN (5, 3, 7, 3, NULL, 9)
         ↓
Step 1: Remove NULLs → [5, 3, 7, 3, 9]
Step 2: Sort         → [3, 3, 5, 7, 9]
Step 3: Deduplicate  → [3, 5, 7, 9]
         ↓
Result: Optimized array with 4 elements (down from 6)
```

## Scan Execution

```
Index:  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
Array:  [3, 7, 9]

Scan start at id≥3:
  Read id=3  → Match array[0]=3 ✓
  Read id=4  → Binary search → advance to array[1]=7
  Read id=5  → 5<7, continue
  Read id=6  → 6<7, continue  
  Read id=7  → Match array[1]=7 ✓
  Read id=8  → Binary search → advance to array[2]=9
  Read id=9  → Match array[2]=9 ✓
  Read id=10 → Array exhausted, stop scan
```

**Result:** 1 tree descent, ~7 tuples scanned, 3 matches found

## Array Merging Example

```sql
WHERE id IN (1,2,3,4,5) AND id IN (3,4,5,6,7)
```

**Before merge:** 10 total array elements  
**After merge:** 3 elements `[3,4,5]` (intersection only)  
**Benefit:** 70% reduction in work

## Contradictory Arrays Detection

```sql
WHERE id IN (1,2,3) AND id IN (10,20,30)
```

**Detection:** Intersection is empty  
**Result:** Zero rows returned immediately, no I/O performed  
**Saved:** Entire scan avoided

## Code Pattern: How to Check for Array Keys

```c
/* Check if scan has array keys */
BTScanOpaque so = (BTScanOpaque) scan->opaque;
if (so->numArrayKeys > 0)
{
    /* Scan uses array optimization */
    for (int i = 0; i < so->numArrayKeys; i++)
    {
        BTArrayKeyInfo *arrayKey = &so->arrayKeys[i];
        /* arrayKey->cur_elem is current position */
        /* arrayKey->elem_values[cur_elem] is current value */
    }
}
```

## Performance Tips

### ✓ Good Use Cases
- Small to medium arrays (< 1000 elements)
- Values relatively close in key space
- Multiple array conditions on same column (auto-merged)
- Repeated queries (benefits from page cache)

### ✗ Consider Alternatives
- Very large arrays (> 10000 elements) → use JOIN with temp table
- Extremely scattered values → might be better as separate queries
- Very frequent array changes → parsing/planning overhead

## Flags and Constants

| Flag | Meaning |
|------|---------|
| `SK_SEARCHARRAY` | This scan key contains an array |
| `SK_BT_REQFWD` | Key required for forward scan |
| `SK_BT_REQBKWD` | Key required for backward scan |
| `needPrimScan` | Need to start a new primitive scan |
| `scanBehind` | Last array advancement matched special value |

## Memory Context

All array-related data is allocated in a dedicated `arrayContext`:
- Created on first array scan
- Reset on rescan (efficient)
- Freed when scan completes
- Uses `ALLOCSET_SMALL_SIZES` (optimized for small frequent allocations)

## Debug/Trace Points

To understand array scan behavior:

```sql
-- See query plan
EXPLAIN (VERBOSE, COSTS) 
SELECT * FROM table WHERE id IN (1,2,3);

-- Look for:
-- - "Index Scan" or "Index Only Scan" (not multiple seeks)
-- - Filter condition with array
```

## Common Misconceptions

| Myth | Reality |
|------|---------|
| "IN does K separate lookups" | ❌ Single scan with array advancement |
| "Large arrays are always slow" | ⚠️ Depends on distribution; often faster than ORs |
| "Arrays don't benefit from index" | ❌ Highly optimized for indexed columns |
| "Must scan entire array for each tuple" | ❌ Binary search is O(log K) |
| "IN is same as multiple ORs" | ⚠️ Similar plan but arrays are more optimized |

## Related Features

- **Index Skip Scan:** Can jump over key ranges when array values are sparse
- **Bitmap Index Scan:** Alternative path for less selective conditions
- **Index Only Scan:** Works with arrays, returns tuple from index alone
- **Parallel Scans:** Multiple workers can share array key state

## Version History

This optimization has evolved over PostgreSQL versions:
- Early versions: Basic array support
- PostgreSQL 9.2+: Improved array preprocessing
- PostgreSQL 14+: Enhanced array merging and skip scan
- PostgreSQL 15+: Binary search optimization for array advancement

For the complete technical analysis, see: [BTREE_ARRAY_OPTIMIZATION_ANALYSIS.md](./BTREE_ARRAY_OPTIMIZATION_ANALYSIS.md)
