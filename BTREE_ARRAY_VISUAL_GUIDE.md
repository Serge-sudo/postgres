# Visual Guide: B-tree Array Scan Execution

## The Big Picture: One Scan, Not Multiple Lookups

```
❌ WRONG: What PostgreSQL Does NOT Do
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Query: WHERE id IN (5, 10, 15)

                ROOT
               /    \
              /      \
    Lookup id=5     Lookup id=10    Lookup id=15
         ↓               ↓                ↓
      LEAF            LEAF             LEAF
     Find 5          Find 10          Find 15

3 separate tree descents = EXPENSIVE


✓ CORRECT: What PostgreSQL Actually Does
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Query: WHERE id IN (5, 10, 15)

                ROOT
                 |
            Single Descent
                 ↓
              LEAF PAGE
    [... 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 ...]
         ↓      ↓               ↓
      Match 5   Skip to 10      Skip to 15

1 tree descent + sequential scan = FAST
```

## Detailed Execution Flow

```
STEP-BY-STEP: WHERE id IN (3, 7, 15)

Preprocessing Phase:
┌─────────────────────────────────────┐
│ Input:  [3, 7, 15]                  │
│   ↓                                 │
│ Sort:   [3, 7, 15] ✓ already sorted│
│   ↓                                 │
│ Dedup:  [3, 7, 15] ✓ no duplicates │
│   ↓                                 │
│ Store in BTArrayKeyInfo             │
└─────────────────────────────────────┘

Scan Execution Phase:

Time │ Index Position      │ Array State        │ Action
═════╪════════════════════╪═══════════════════╪═══════════════════════════
  T0 │ ROOT               │ [3, 7, 15]        │ Tree descent
     │                    │                   │
  T1 │ LEAF: → 3          │ [→3, 7, 15]       │ ✓ MATCH! Return (3)
     │                    │  ^                │
  T2 │ LEAF:   4 →        │ [3, →7, 15]       │ Binary search: 4 < 7
     │                    │     ^             │ Skip ahead in array
     │                    │                   │
  T3 │ LEAF:   5 →        │ [3, →7, 15]       │ 5 < 7, continue scan
     │                    │     ^             │
  T4 │ LEAF:   6 →        │ [3, →7, 15]       │ 6 < 7, continue scan
     │                    │     ^             │
     │                    │                   │
  T5 │ LEAF:   7 →        │ [3, →7, 15]       │ ✓ MATCH! Return (7)
     │                    │     ^             │
  T6 │ LEAF:   8 →        │ [3, 7, →15]       │ Binary search: 8 < 15
     │                    │        ^          │ Skip ahead in array
     │                    │                   │
 T7-9│ LEAF: 9,10,11... → │ [3, 7, →15]       │ Scan forward...
     │                    │        ^          │
     │                    │                   │
 T10 │ LEAF:     15 →     │ [3, 7, →15]       │ ✓ MATCH! Return (15)
     │                    │        ^          │
     │                    │                   │
 T11 │ LEAF:     16 →     │ [exhausted]       │ ✗ STOP: No more array
     │                    │                   │    elements
     
RESULT: 3 matches found with 1 tree descent and ~13 tuples scanned
```

## Binary Search Within Array

```
Scenario: Current tuple is id=12, need to find next array element

Array: [3, 5, 7, 9, 11, 13, 15, 17, 19]
Current position:        ^^ (was at 11, now need to advance)

Binary Search:
┌───────────────────────────────────────────────────┐
│ Range: [13, 15, 17, 19]  (elements after current)│
│                                                   │
│ Step 1: Check middle element                     │
│         [13, 15, ⓧ17, 19]                        │
│         Compare: 12 < 17 → Search left half      │
│                                                   │
│ Step 2: Check middle of left half                │
│         [13, ⓧ15]                                │
│         Compare: 12 < 15 → Search left half      │
│                                                   │
│ Step 3: Only one element left                    │
│         [ⓧ13]                                    │
│         Compare: 12 < 13 → Found next element!   │
│                                                   │
│ Result: Advanced to array[5] = 13               │
│         (Found in 3 comparisons vs 4 sequential) │
└───────────────────────────────────────────────────┘
```

## Optimistic Check Optimization

```
Common case: Array elements are close together in index

Array: [100, 101, 102, 103, 104]
Index: [..., 99, 100, 101, 102, 103, 104, 105, ...]

When at tuple id=100 (matches array[0]=100):
┌──────────────────────────────────────────────────┐
│ Next tuple is id=101                             │
│                                                  │
│ Optimistic check: Is 101 == array[1] ?          │
│                   Yes! ✓                         │
│                                                  │
│ No need for binary search!                      │
│ Cost: 1 comparison instead of log(K)            │
└──────────────────────────────────────────────────┘

This makes scanning consecutive values nearly as fast as a range scan!
```

## Array Merging Visualization

```
Query: WHERE id IN (1,2,3,4,5) AND id IN (3,4,5,6,7)

Before Merge:
┌─────────────────┐   ┌─────────────────┐
│ Array 1:        │   │ Array 2:        │
│ [1,2,3,4,5]     │ ∩ │ [3,4,5,6,7]     │
└─────────────────┘   └─────────────────┘
        ↓                       ↓
        └───────────┬───────────┘
                    ↓
            ┌─────────────┐
            │ Merged:     │
            │ [3,4,5]     │  ← Only intersection!
            └─────────────┘

Benefits:
- 10 elements → 3 elements (70% reduction)
- Faster preprocessing (sort fewer items)
- Faster scanning (fewer comparisons)
- Less memory usage
```

## Memory Layout

```
BTScanOpaqueData (per scan)
┌────────────────────────────────────────┐
│ numArrayKeys: 2                        │
│ arrayKeys: ────┐                       │
│ orderProcs: ───┼─┐                     │
│ arrayContext: ─┼─┼─┐                   │
└────────────────┼─┼─┼───────────────────┘
                 │ │ │
                 │ │ └──→ MemoryContext: "BTree array context"
                 │ │      ├─ elem_values arrays
                 │ │      ├─ sorted array copies  
                 │ │      └─ comparison functions
                 │ │
                 │ └─────→ FmgrInfo array (comparison functions)
                 │         for binary search operations
                 │
                 └───────→ BTArrayKeyInfo array:
                           ┌──────────────────────┐
                           │ [0] scan_key: 0      │
                           │     cur_elem: 2      │ ← Currently at 3rd element
                           │     num_elems: 5     │
                           │     elem_values: ─┐  │
                           ├──────────────────┼───┤
                           │ [1] scan_key: 1  │   │
                           │     cur_elem: 1  │   │
                           │     num_elems: 3 │   │
                           │     elem_values: ─┼─┐│
                           └──────────────────┼─┼┘
                                             │ │
                         ┌───────────────────┘ └─────────────────┐
                         │                                        │
                         ↓                                        ↓
                 [3, 5, 7, 9, 11]                         [10, 20, 30]
                 Datum array for col1                     Datum array for col2
```

## Page Cache Benefits

```
Scenario: Multiple queries with overlapping array values

Query 1: WHERE id IN (100, 200, 300, 400, 500)
┌────────────────────────────────────────────────────┐
│ Scan reads pages: [P10, P20, P30, P40, P50]       │
│ These pages are cached in shared_buffers           │
└────────────────────────────────────────────────────┘

Query 2: WHERE id IN (150, 250, 350, 450, 550)
┌────────────────────────────────────────────────────┐
│ Scan reads pages: [P15, P25, P35, P45, P55]       │
│                    ↑    ↑    ↑    ↑    ↑           │
│ Some overlap with cached pages from Query 1        │
│ = Faster execution (memory reads instead of disk)  │
└────────────────────────────────────────────────────┘

With separate lookups: Less cache benefit due to random access pattern
With array scan: Better cache benefit due to sequential access pattern
```

## Comparison: Array Scan vs Other Methods

```
╔══════════════════════╦════════════════╦═══════════════╦════════════════╗
║ Method               ║ Tree Descents  ║ Pages Scanned ║ Cache Friendly ║
╠══════════════════════╬════════════════╬═══════════════╬════════════════╣
║ Array Scan (optimal) ║ 1              ║ M*            ║ ✓✓✓            ║
╠══════════════════════╬════════════════╬═══════════════╬════════════════╣
║ K Separate Lookups   ║ K              ║ K             ║ ✓              ║
╠══════════════════════╬════════════════╬═══════════════╬════════════════╣
║ Multiple ORs         ║ 1-K**          ║ M             ║ ✓✓             ║
╠══════════════════════╬════════════════╬═══════════════╬════════════════╣
║ Range Scan           ║ 1              ║ M             ║ ✓✓✓            ║
╚══════════════════════╩════════════════╩═══════════════╩════════════════╝

*  M = pages between first and last matching value
** Depends on optimizer's ability to merge conditions
```

## When Array Scan Helps Most

```
Best Case: Consecutive Values
─────────────────────────────
WHERE id IN (100, 101, 102, 103, 104)

Index: [... 99, 100, 101, 102, 103, 104, 105 ...]
                ├──────────────────────┤
                  Single page read!

Optimistic checks succeed every time
Nearly identical performance to: WHERE id BETWEEN 100 AND 104


Good Case: Clustered Values  
───────────────────────────
WHERE id IN (100, 105, 110, 115, 120)

Index: [... 98, 99, 100, 101, ..., 105, ..., 110, ..., 115, ..., 120, 121 ...]
                    ├──────────────────────────────────────────────────┤
                              ~5 pages (assuming 5 vals/page)

Binary search finds next element quickly
Still much better than 5 separate lookups


Challenging Case: Scattered Values
──────────────────────────────────
WHERE id IN (1, 1000, 2000, 3000, 4000)

Index: [1, ..., 1000, ..., 2000, ..., 3000, ..., 4000, ...]
        ├─────────────────────────────────────────────────┤
                    ~100s of pages

Still only 1 tree descent
May benefit from "skip scan" optimization
Still better than 5 tree descents
```

## Debug: How to See Array Optimization in Action

```sql
-- Enable detailed query analysis
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) 
SELECT * FROM users WHERE id IN (1, 5, 10, 15, 20);

Expected output:
┌────────────────────────────────────────────────────────────────┐
│ Index Scan using users_pkey on users                           │
│   Output: id, name, email                                      │
│   Index Cond: (id = ANY ('{1,5,10,15,20}'::integer[]))  ← Array!│
│   Buffers: shared hit=3                    ← Only 3 pages read! │
│   Planning Time: 0.123 ms                                      │
│   Execution Time: 0.045 ms                                     │
└────────────────────────────────────────────────────────────────┘

What to look for:
✓ "= ANY" indicates array scan
✓ Low buffer count relative to array size
✓ Single "Index Scan" node (not multiple)
```

## Key Insights Summary

```
┌─────────────────────────────────────────────────────────────┐
│ 1. ONE SCAN, NOT K LOOKUPS                                  │
│    └─→ Single tree descent handles all array elements       │
│                                                              │
│ 2. BINARY SEARCH WITHIN ARRAY                               │
│    └─→ O(log K) to advance, not O(K)                        │
│                                                              │
│ 3. OPTIMISTIC CHECK OPTIMIZATION                            │
│    └─→ Usually finds next element in 1 comparison           │
│                                                              │
│ 4. PREPROCESSING REDUCES WORK                               │
│    └─→ Sort, dedup, merge can eliminate 30-70% of elements  │
│                                                              │
│ 5. CACHE-FRIENDLY SEQUENTIAL SCAN                           │
│    └─→ Better memory access pattern than random lookups     │
└─────────────────────────────────────────────────────────────┘
```

---

For complete technical details, see: [BTREE_ARRAY_OPTIMIZATION_ANALYSIS.md](./BTREE_ARRAY_OPTIMIZATION_ANALYSIS.md)
