# PostgreSQL B-tree Array Lookup Optimizations

## Overview

This document analyzes the optimizations PostgreSQL uses for B-tree index lookups, particularly when handling queries with `IN` clauses like `SELECT * FROM table WHERE id IN (1, 2, 3)`.

**Key Question:** When looking up multiple keys in a single query, does PostgreSQL perform separate lookups for each item, or is there caching/optimization?

**Short Answer:** No, PostgreSQL does NOT perform separate lookups for each array element. Instead, it uses sophisticated optimizations that treat the array as a coordinated set of values, advancing through them efficiently during a single index scan.

## How IN Clauses are Represented

When you write:
```sql
SELECT * FROM table WHERE id IN (1, 2, 3)
```

PostgreSQL's query planner converts this into a **ScalarArrayOpExpr** which eventually becomes a scan key with the `SK_SEARCHARRAY` flag in the B-tree access method. The array is deconstructed and stored in a `BTArrayKeyInfo` structure.

### Key Data Structures

#### BTArrayKeyInfo (src/include/access/nbtree.h:1028-1034)
```c
typedef struct BTArrayKeyInfo
{
    int         scan_key;      /* index of associated key in keyData */
    int         cur_elem;      /* index of current element in elem_values */
    int         num_elems;     /* number of elems in current array value */
    Datum      *elem_values;   /* array of num_elems Datums */
} BTArrayKeyInfo;
```

#### BTScanOpaqueData (src/include/access/nbtree.h:1036-1050)
```c
typedef struct BTScanOpaqueData
{
    /* ... other fields ... */
    
    /* workspace for SK_SEARCHARRAY support */
    int         numArrayKeys;       /* number of equality-type array keys */
    bool        needPrimScan;       /* New prim scan to continue in current dir? */
    bool        scanBehind;         /* Last array advancement matched -inf attr? */
    bool        oppositeDirCheck;   /* explicit scanBehind recheck needed? */
    BTArrayKeyInfo *arrayKeys;      /* info about each equality-type array key */
    FmgrInfo   *orderProcs;         /* ORDER procs for required equality keys */
    MemoryContext arrayContext;     /* scan-lifespan context for array data */
    /* ... other fields ... */
}
```

## Array Preprocessing Optimizations

Before the actual scan begins, PostgreSQL performs extensive preprocessing on array keys in `_bt_preprocess_array_keys()` (src/backend/access/nbtree/nbtpreprocesskeys.c:1116-1449).

### Optimization 1: Null Elimination
All NULL elements are removed from the array since B-tree operators are strict (a NULL can never match).

**Code Location:** src/backend/access/nbtree/nbtpreprocesskeys.c:1256-1273
```c
/* Compress out any null elements */
num_nonnulls = 0;
for (j = 0; j < num_elems; j++)
{
    if (!elem_nulls[j])
        elem_values[num_nonnulls++] = elem_values[j];
}
```

### Optimization 2: Inequality Simplification
For inequality operators (`<`, `<=`, `>`, `>=`), PostgreSQL finds the single extreme element that satisfies the condition and replaces the entire array with that scalar value.

**Example:** `WHERE id < ANY(ARRAY[5, 3, 9])` becomes `WHERE id < 9` (the maximum).

**Code Location:** src/backend/access/nbtree/nbtpreprocesskeys.c:1290-1315
```c
switch (cur->sk_strategy)
{
    case BTLessStrategyNumber:
    case BTLessEqualStrategyNumber:
        cur->sk_argument =
            _bt_find_extreme_element(scan, cur, elemtype,
                                    BTGreaterStrategyNumber,
                                    elem_values, num_nonnulls);
        continue;
    /* ... similar for >= and > ... */
}
```

### Optimization 3: Sorting Array Elements
For equality operations, array elements are sorted in the same order as the index. This is critical because it allows the scan to advance through array elements in lockstep with the index scan.

**Code Location:** src/backend/access/nbtree/nbtpreprocesskeys.c:1329-1336
```c
/*
 * Sort the non-null elements and eliminate any duplicates. We must
 * sort in the same ordering used by the index column, so that the
 * arrays can be advanced in lockstep with the scan's progress through
 * the index's key space.
 */
reverse = (indoption[cur->sk_attno - 1] & INDOPTION_DESC) != 0;
num_elems = _bt_sort_array_elements(cur, sortprocp, reverse,
                                    elem_values, num_nonnulls);
```

### Optimization 4: Duplicate Elimination
During sorting, duplicates are removed using `qunique()`, reducing the number of elements to process.

### Optimization 5: Array Merging
When multiple equality array keys exist for the same index column (e.g., `WHERE id IN (1,2,3) AND id IN (2,3,4)`), PostgreSQL merges them by finding the intersection, resulting in `id IN (2,3)`.

**Code Location:** src/backend/access/nbtree/nbtpreprocesskeys.c:1338-1372

This can detect contradictory conditions early (e.g., `id IN (1,2) AND id IN (3,4)` results in an empty set, so the scan is marked unsatisfiable).

## How the Scan Works: NOT Separate Lookups

### Single Scan with Array Advancement

The key insight is that PostgreSQL performs a **single index scan** that processes all array elements efficiently, rather than multiple separate lookups.

#### Step 1: Initial Positioning (_bt_first)
The scan starts at the first array element. For `WHERE id IN (1, 5, 10)`, the scan positions itself at the first tuple with `id >= 1`.

#### Step 2: Reading Tuples
As tuples are read from the leaf pages, they are compared against the current array element.

#### Step 3: Array Advancement (_bt_advance_array_keys)
When a tuple doesn't match the current array element, the array elements are advanced efficiently using **binary search** rather than sequential iteration.

**Code Location:** src/backend/access/nbtree/nbtutils.c:863-1100

### Binary Search Optimization

The `_bt_binsrch_array_skey()` function (src/backend/access/nbtree/nbtutils.c:251-425) performs binary search to find the next matching array element.

**Key Optimization:** When array advancement is triggered by a required array scan key, the function uses an "optimistic comparison" strategy:

```c
if (ScanDirectionIsForward(dir))
{
    low_elem = array->cur_elem + 1; /* old cur_elem exhausted */
    
    /* Compare prospective new cur_elem (also the new lower bound) */
    if (high_elem >= low_elem)
    {
        arrdatum = array->elem_values[low_elem];
        result = _bt_compare_array_skey(orderproc, tupdatum, tupnull,
                                       arrdatum, cur);
        
        if (result <= 0)
        {
            /* Optimistic comparison optimization worked out */
            *set_elem_result = result;
            return low_elem;
        }
        /* ... continue with binary search ... */
    }
}
```

**What this means:** When scanning sequentially through the index, the next matching array element is often the very next one in the sorted array. The code checks this first before falling back to a full binary search. This makes the average case very fast.

### Lockstep Advancement

The array elements and index scan progress together in "lockstep":
- Array is sorted in the same order as the index
- As the scan moves forward in the index, array elements advance forward
- Required arrays can never go backward relative to scan progress

**Code Location:** src/backend/access/nbtree/nbtutils.c:439-505

```c
/*
 * _bt_advance_array_keys_increment() -- Advance to next set of array elements
 *
 * Advances the array keys by a single increment in the current scan
 * direction. When there are multiple array keys this can roll over from the
 * lowest order array to higher order arrays.
 */
```

## Caching Mechanisms

### Page-Level Caching
PostgreSQL's buffer manager caches index pages in shared memory. When scanning for multiple array values, if they fall on the same or nearby index pages, those pages remain cached, avoiding disk I/O.

### Tuple Position Tracking
The scan maintains state in `BTScanPosData` structures that track:
- Current page buffer
- Current position within the page
- Previously read tuples (for index-only scans)

**Code Location:** src/include/access/nbtree.h:960-1025

### Array Context Memory
A dedicated memory context (`arrayContext`) persists for the lifetime of the scan, holding array-related data without repeated allocation/deallocation.

## Performance Comparison: Array Scan vs Multiple Lookups

### Array Scan (What PostgreSQL Does)
```
For id IN (1, 5, 10, 15, 20):
1. Position at id=1 (one tree descent)
2. Scan forward, matching id=1
3. When encountering id>1, binary search array → advance to id=5
4. Continue scanning, match id=5
5. Advance to id=10, match, continue...
6. Single continuous scan with array advancement
```

**Complexity:** O(log N) tree descent + O(M) page scans + O(log K) per array advancement
- N = total index size
- M = number of leaf pages accessed
- K = number of array elements

### Multiple Separate Lookups (What PostgreSQL Does NOT Do)
```
For id IN (1, 5, 10, 15, 20):
1. Position at id=1 (tree descent)
2. Match id=1
3. Position at id=5 (ANOTHER tree descent)
4. Match id=5
5. Position at id=10 (ANOTHER tree descent)
... repeat for each element
```

**Complexity:** O(K × log N) tree descents
- Much more expensive due to repeated tree traversals

## Multiple Array Keys

When you have multiple array conditions:
```sql
WHERE col1 IN (1, 2) AND col2 IN (10, 20, 30)
```

PostgreSQL treats this as a 2D grid:
```
(1,10), (1,20), (1,30), (2,10), (2,20), (2,30)
```

The arrays advance in nested fashion, with the last array (rightmost column) advancing most frequently.

**Code Location:** src/backend/access/nbtree/nbtutils.c:460-485
```c
/*
 * We must advance the last array key most quickly, since it will
 * correspond to the lowest-order index column among the available
 * qualifications
 */
for (int i = so->numArrayKeys - 1; i >= 0; i--)
{
    /* ... advance array element ... */
    if (!rolled)
        return true;
    /* Need to advance next array key, if any */
}
```

## Summary

### What PostgreSQL Does:
1. ✅ **Preprocesses arrays:** Sorts, deduplicates, merges, eliminates NULLs
2. ✅ **Single index scan:** One tree descent, continuous leaf page scanning
3. ✅ **Binary search:** Efficiently finds next matching array element
4. ✅ **Lockstep advancement:** Arrays advance together with scan progress
5. ✅ **Page caching:** Buffer manager caches pages accessed during scan
6. ✅ **Optimistic checks:** Often matches next sequential array element immediately

### What PostgreSQL Does NOT Do:
1. ❌ **Separate lookups:** Does not perform K separate index searches for K array elements
2. ❌ **Sequential array scan:** Does not check each array element linearly against each tuple
3. ❌ **Duplicate tree descents:** Does not restart from root for each array value

### Performance Benefits:
- Dramatically fewer tree traversals (1 vs K)
- Better cache locality (sequential page access)
- Efficient CPU usage (binary search vs sequential)
- Early contradiction detection (merged arrays)
- Reduced I/O (cached pages)

## Code References

Key source files for understanding B-tree array optimizations:

1. **src/backend/access/nbtree/nbtpreprocesskeys.c**
   - `_bt_preprocess_array_keys()` - Main preprocessing logic
   - `_bt_sort_array_elements()` - Sorting and deduplication
   - `_bt_merge_arrays()` - Array intersection

2. **src/backend/access/nbtree/nbtutils.c**
   - `_bt_advance_array_keys()` - Main array advancement logic
   - `_bt_binsrch_array_skey()` - Binary search for next element
   - `_bt_advance_array_keys_increment()` - Simple increment advancement

3. **src/backend/access/nbtree/nbtsearch.c**
   - `_bt_first()` - Initial scan positioning
   - `_bt_next()` - Getting next tuple in scan

4. **src/include/access/nbtree.h**
   - Data structure definitions (BTArrayKeyInfo, BTScanOpaqueData)

## Example Walkthrough: SELECT WHERE id IN (3, 7, 15)

Let's trace through a concrete example with an index containing values: 1, 2, 3, 4, 5, 7, 8, 10, 12, 15, 18, 20

### Preprocessing Phase
```
Input array:  [3, 7, 15]
After sort:   [3, 7, 15]  (already sorted)
Duplicates:   none
NULLs:        none
Result:       [3, 7, 15]  stored in BTArrayKeyInfo
```

### Scan Execution Phase

```
Step 1: _bt_first() positions scan at first value >= 3
  Index: [1, 2, →3, 4, 5, 7, 8, 10, 12, 15, 18, 20]
  Array: [→3, 7, 15]  (cur_elem = 0)
  Match: ✓ (tuple with id=3 matches array[0])

Step 2: _bt_next() gets next tuple (id=4)
  Index: [1, 2, 3, →4, 5, 7, 8, 10, 12, 15, 18, 20]
  Array: [3, →7, 15]  (advanced via binary search)
  Match: ✗ (4 < 7, continue scan)

Step 3: _bt_next() gets next tuple (id=5)
  Index: [1, 2, 3, 4, →5, 7, 8, 10, 12, 15, 18, 20]
  Array: [3, →7, 15]  (no change, 5 < 7)
  Match: ✗ (5 < 7, continue scan)

Step 4: _bt_next() gets next tuple (id=7)
  Index: [1, 2, 3, 4, 5, →7, 8, 10, 12, 15, 18, 20]
  Array: [3, →7, 15]  (still at array[1])
  Match: ✓ (tuple with id=7 matches array[1])

Step 5: _bt_next() gets next tuple (id=8)
  Index: [1, 2, 3, 4, 5, 7, →8, 10, 12, 15, 18, 20]
  Array: [3, 7, →15]  (advanced via binary search)
  Match: ✗ (8 < 15, continue scan)

Steps 6-7: Scan through 10, 12 (no matches, 10 < 15, 12 < 15)

Step 8: _bt_next() gets next tuple (id=15)
  Index: [1, 2, 3, 4, 5, 7, 8, 10, 12, →15, 18, 20]
  Array: [3, 7, →15]  (still at array[2])
  Match: ✓ (tuple with id=15 matches array[2])

Step 9: _bt_next() gets next tuple (id=18)
  Index: [1, 2, 3, 4, 5, 7, 8, 10, 12, 15, →18, 20]
  Array: [3, 7, 15]  (exhausted, no more elements)
  Result: Scan terminates (no more array elements)
```

### What Happened
- **1 tree descent** (initial _bt_first positioning)
- **Scanned ~10 tuples** sequentially through leaf pages
- **3 binary searches** within array (small overhead, O(log K) where K=3)
- **3 matches** returned

Compare to 3 separate lookups which would require:
- **3 tree descents** (one per value)
- Potentially accessing same pages multiple times without cache benefit

## Visual Representation of Array Advancement

```
Scenario: WHERE id IN (5, 10, 15, 20, 25) with index scan

Time   Index Position    Array Position    Action
----   --------------    --------------    ------
T0     Root              [5,10,15,20,25]   Tree descent to id≥5
T1     Leaf: id=5        [→5,10,15,20,25]  Match! Return id=5
T2     Leaf: id=6        [5,→10,15,20,25]  Binary search: 6<10, continue
T3     Leaf: id=8        [5,→10,15,20,25]  8<10, continue  
T4     Leaf: id=10       [5,→10,15,20,25]  Match! Return id=10
T5     Leaf: id=11       [5,10,→15,20,25]  Binary search: 11<15, continue
...    ...               ...               ...
T10    Leaf: id=25       [5,10,15,20,→25]  Match! Return id=25
T11    Leaf: id=26       Array exhausted   Scan terminates

Total operations:
- Tree descents: 1
- Binary searches: ~5 (one per array advancement)
- Leaf page reads: ~15-20 (depending on page size and key distribution)
```

## Advanced Scenarios

### Scenario 1: Non-Contiguous Array Values
```sql
SELECT * FROM table WHERE id IN (1, 100, 200, 300)
```

**Optimization:** The scan can potentially "skip" over pages. If the index is at id=50 and the next array element is 100, the scan can jump directly rather than reading every page between 50 and 100. This is handled by the `needPrimScan` mechanism.

**Code Location:** src/backend/access/nbtree/nbtutils.c:816 (needPrimScan flag)

### Scenario 2: Composite Index with Arrays
```sql
-- Index on (dept_id, salary)
SELECT * FROM employees 
WHERE dept_id IN (10, 20, 30) 
  AND salary BETWEEN 50000 AND 100000
```

**Optimization:** The dept_id array advances as a higher-order key, with salary range checked for each dept_id value. This forms combinations like:
```
(10, 50000-100000)
(20, 50000-100000)
(30, 50000-100000)
```

Each dept_id causes a new "primitive scan" within that department's key range.

### Scenario 3: Array Merging Detection
```sql
SELECT * FROM table 
WHERE id IN (1, 2, 3, 4, 5) 
  AND id IN (3, 4, 5, 6, 7)
```

**Optimization:** Arrays are merged during preprocessing:
```
Array 1: [1, 2, 3, 4, 5]
Array 2: [3, 4, 5, 6, 7]
Merged:  [3, 4, 5]  (intersection only)
```

This dramatically reduces the scan workload from 12 potential values to just 3!

### Scenario 4: Contradictory Arrays
```sql
SELECT * FROM table 
WHERE id IN (1, 2, 3) 
  AND id IN (10, 20, 30)
```

**Optimization:** During array merging, PostgreSQL detects the intersection is empty. The scan is marked as `qual_ok = false` and never even starts - returning zero rows immediately without any I/O.

**Code Location:** src/backend/access/nbtree/nbtpreprocesskeys.c:1180

## Memory Management

### Array Context Lifecycle

The `arrayContext` is a dedicated memory context created when arrays are first encountered:

```c
if (so->arrayContext == NULL)
    so->arrayContext = AllocSetContextCreate(CurrentMemoryContext,
                                            "BTree array context",
                                            ALLOCSET_SMALL_SIZES);
else
    MemoryContextReset(so->arrayContext);
```

**Benefits:**
- All array-related allocations go into this context
- On rescan, the entire context is reset in one operation
- No per-element allocation/deallocation overhead
- Memory is automatically freed when scan completes

### What Gets Cached

1. **Deconstructed array elements:** The Datum array from the original array value
2. **Sorted array:** After sorting and deduplication
3. **ORDER procs:** Comparison functions for binary search
4. **Array metadata:** Current position, number of elements, etc.

## Performance Characteristics

### Time Complexity
- **Preprocessing:** O(K log K) for sorting K array elements
- **First positioning:** O(log N) tree descent (N = index size)
- **Per-tuple processing:** O(log K) for binary search if array advances
- **Total scan:** O(log N + M + A × log K) where:
  - N = total tuples in index
  - M = tuples scanned between first and last array value
  - A = number of array advancements (typically ≤ K)
  - K = number of array elements

### Space Complexity
- O(K) for storing deconstructed array elements
- O(K) for scan state (current positions, etc.)
- Index page buffers shared across all scans (not per-array overhead)

### Best Case
Array values are consecutive in the index: `WHERE id IN (100, 101, 102, 103)`
- Almost no binary search overhead (optimistic check succeeds)
- Single continuous page scan
- Performance similar to range scan `WHERE id BETWEEN 100 AND 103`

### Worst Case
Array values are spread across the entire index: `WHERE id IN (1, 1000000, 2000000, ...)`
- Must scan large portions of index
- Many primitive scans may be needed
- Still better than K separate lookups due to no repeated tree descents

## Comparison with Other Databases

PostgreSQL's approach is particularly sophisticated. Some other systems:

- **MySQL/InnoDB:** Traditionally converted `IN` to multiple OR conditions, potentially doing separate range scans
- **Oracle:** Uses similar array-based optimization with "inlist iterator" operation
- **SQL Server:** Uses "index seek" with multiple seek predicates

PostgreSQL's advantage: the tight integration between array preprocessing (merging, sorting, dedup) and the scan execution with binary search advancement.

## Conclusion

PostgreSQL's B-tree implementation for handling `IN` clauses is highly sophisticated. Rather than performing multiple independent lookups, it uses a single coordinated scan with intelligent array advancement. The preprocessing optimizations (sorting, deduplication, merging) combined with the binary search during scanning make array lookups very efficient, often approaching the performance of a single value lookup while handling multiple values.

### Key Takeaways

1. **No separate lookups:** A single scan handles all array values
2. **Preprocessing is crucial:** Sorting, dedup, and merging happen before scanning
3. **Binary search within arrays:** Finding the next matching element is O(log K), not O(K)
4. **Lockstep advancement:** Arrays move forward with the scan, never backward
5. **Memory efficient:** Dedicated context holds all array data for scan lifetime
6. **Page cache friendly:** Sequential scanning benefits from buffer cache
7. **Early termination:** Contradictory arrays detected before any I/O

### When Array Scans Excel

- **Many small values:** `IN (1,2,3,4,5)` is nearly as fast as `= 1`
- **Clustered values:** Values close together in key space
- **Multiple arrays on same attribute:** Automatic merging finds intersection
- **Hot index pages:** Repeated scans benefit from cached pages

### When to Consider Alternatives

- **Very large arrays:** Thousands of elements may be better as a JOIN
- **Highly scattered values:** Consider range queries if applicable
- **Non-indexed columns:** Array scans only work with indexes
