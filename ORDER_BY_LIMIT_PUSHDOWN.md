# ORDER BY/LIMIT Pushdown Optimization for Outer Joins

## Overview

This implementation adds a new optimization to PostgreSQL that detects opportunities to push ORDER BY and LIMIT clauses down to the appropriate side of outer joins, potentially reducing the amount of data processed during the join operation.

## Implementation Details

### Location
- **File**: `src/backend/optimizer/plan/planner.c`
- **Function**: `optimize_outer_join_order_limit_pushdown()`
- **Integration Point**: Called in `subquery_planner()` after outer join reduction but before main planning

### Optimization Rules

1. **LEFT OUTER JOIN**: If ORDER BY references only left table columns, the ORDER BY and LIMIT can be pushed to the left side
2. **RIGHT OUTER JOIN**: If ORDER BY references only right table columns, the ORDER BY and LIMIT can be pushed to the right side

### Example Transformations

**Original Query:**
```sql
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 10;
```

**Optimized Equivalent:**
```sql
SELECT * FROM (SELECT * FROM t1 ORDER BY id LIMIT 10) t1_opt 
LEFT OUTER JOIN t2 ON t1_opt.id = t2.id;
```

## Current Implementation Status

The current implementation provides:
- ✅ **Pattern Detection**: Identifies when optimization is possible
- ✅ **Debug Logging**: Reports optimization opportunities via DEBUG1 messages  
- ✅ **Safety Checks**: Only processes simple base relation joins
- ✅ **Test Coverage**: Comprehensive regression tests
- ❌ **Query Transformation**: Not yet implemented (foundation only)

## Testing

### Running Tests
```bash
# Run specific regression test
make check TESTS=order_limit_pushdown

# Enable debug logging to see optimization detection
psql -c "SET client_min_messages = DEBUG1; 
         SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 10;"
```

### Expected Debug Output
```
DEBUG:  ORDER BY/LIMIT pushdown opportunity detected for LEFT outer join
```

## Future Enhancement Opportunities

### 1. Complete Query Tree Transformation

To implement actual pushdown, the `optimize_outer_join_order_limit_pushdown()` function would need to:

1. **Create Subquery**: Transform the base relation into a subquery with ORDER BY/LIMIT
2. **Update RTE**: Replace the base RangeTblEntry with a subquery RTE
3. **Adjust References**: Update variable references in the main query
4. **Remove Top-Level Clauses**: Clear ORDER BY/LIMIT from the main query

### 2. Enhanced Pattern Recognition

Extend the optimization to handle:
- Multiple join levels
- Subqueries in FROM clause
- Complex ORDER BY expressions
- Window functions

### 3. Cost-Based Decisions

Add cost analysis to determine when pushdown is beneficial:
- Estimate join selectivity
- Compare costs of sort-then-join vs join-then-sort
- Consider available indexes

### 4. Additional Join Types

Extend support to:
- FULL OUTER JOINs (with careful semantics consideration)
- Complex multi-table joins

## Code Structure

```c
static void optimize_outer_join_order_limit_pushdown(PlannerInfo *root)
{
    // 1. Validate query structure (single outer join)
    // 2. Analyze ORDER BY column references  
    // 3. Check optimization applicability
    // 4. Log detection (current) or transform query (future)
}
```

## Benefits

When fully implemented, this optimization can provide:
- **Reduced Memory Usage**: Smaller intermediate result sets
- **Faster Execution**: Less data to sort and join
- **Better Cache Locality**: Working with smaller datasets
- **Index Utilization**: Potential to use indexes for ORDER BY on base tables

## Limitations

Current limitations:
- Only handles simple two-table joins
- Requires base relations (not subqueries)
- No actual query transformation yet
- Limited to straightforward ORDER BY expressions

## Integration with PostgreSQL Planning

The optimization integrates cleanly with PostgreSQL's planning pipeline:
1. **Early Detection**: Runs in preprocessing phase before path generation
2. **Non-Intrusive**: Only analyzes and logs, doesn't modify critical structures
3. **Extensible**: Foundation allows for future enhancements
4. **Safe**: Conservative approach avoids breaking existing functionality