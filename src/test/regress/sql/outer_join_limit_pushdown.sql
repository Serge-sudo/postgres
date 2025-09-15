-- Test ORDER BY LIMIT pushdown optimization for outer joins

-- Create test tables
CREATE TABLE oj_t1 (
    id INTEGER,
    name TEXT
);

CREATE TABLE oj_t2 (
    id INTEGER,
    category TEXT
);

CREATE TABLE oj_t3 (
    id INTEGER,
    description TEXT
);

-- Insert test data
INSERT INTO oj_t1 VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five'), 
                         (6, 'six'), (7, 'seven'), (8, 'eight'), (9, 'nine'), (10, 'ten');
INSERT INTO oj_t2 VALUES (1, 'cat1'), (3, 'cat3'), (5, 'cat5'), (7, 'cat7'), (9, 'cat9');
INSERT INTO oj_t3 VALUES (2, 'desc2'), (4, 'desc4'), (6, 'desc6'), (8, 'desc8'), (10, 'desc10');

-- Demonstrate optimization with simple LEFT JOIN
\echo 'Testing LEFT JOIN optimization:'
SET enable_outer_join_limit_pushdown = on;
EXPLAIN (COSTS OFF, VERBOSE)
SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 
LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
ORDER BY t1.id LIMIT 3;

-- Show plan without optimization for comparison
\echo 'Same query without optimization:'
SET enable_outer_join_limit_pushdown = off;
EXPLAIN (COSTS OFF, VERBOSE)
SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 
LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
ORDER BY t1.id LIMIT 3;

-- Test nested outer joins
\echo 'Testing nested LEFT JOINs:'
SET enable_outer_join_limit_pushdown = on;
EXPLAIN (COSTS OFF, VERBOSE)
SELECT t1.id, t1.name, t2.category, t3.description 
FROM oj_t1 t1 
LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
LEFT JOIN oj_t3 t3 ON t1.id = t3.id 
ORDER BY t1.id LIMIT 4;

-- Test RIGHT JOIN  
\echo 'Testing RIGHT JOIN optimization:'
SET enable_outer_join_limit_pushdown = on;
EXPLAIN (COSTS OFF, VERBOSE)
SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 
RIGHT JOIN oj_t2 t2 ON t1.id = t2.id 
ORDER BY t2.id LIMIT 2;

-- Test cases that should not be optimized
\echo 'Testing ORDER BY non-preserved side (should not optimize):'
SET enable_outer_join_limit_pushdown = on;
EXPLAIN (COSTS OFF, VERBOSE)
SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 
LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
ORDER BY t2.category LIMIT 3;

-- Show actual results to verify correctness
\echo 'Verifying result correctness:'
SET enable_outer_join_limit_pushdown = on;
SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 
LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
ORDER BY t1.id LIMIT 5;

SET enable_outer_join_limit_pushdown = off;
SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 
LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
ORDER BY t1.id LIMIT 5;

-- Clean up
DROP TABLE oj_t1, oj_t2, oj_t3;