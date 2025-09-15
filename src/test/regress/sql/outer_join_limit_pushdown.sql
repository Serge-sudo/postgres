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

-- Insert test data
INSERT INTO oj_t1 VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five');
INSERT INTO oj_t2 VALUES (1, 'cat1'), (3, 'cat3'), (5, 'cat5');

-- Test basic LEFT JOIN with ORDER BY LIMIT
SET enable_outer_join_limit_pushdown = on;
EXPLAIN (COSTS OFF) 
SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 
LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
ORDER BY t1.id LIMIT 3;

-- Test with optimization disabled
SET enable_outer_join_limit_pushdown = off;
EXPLAIN (COSTS OFF) 
SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 
LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
ORDER BY t1.id LIMIT 3;

-- Test RIGHT JOIN
SET enable_outer_join_limit_pushdown = on;
EXPLAIN (COSTS OFF) 
SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 
RIGHT JOIN oj_t2 t2 ON t1.id = t2.id 
ORDER BY t2.id LIMIT 2;

-- Test that ORDER BY non-preserved side is not optimized
EXPLAIN (COSTS OFF) 
SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 
LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
ORDER BY t2.category LIMIT 3;

-- Clean up
DROP TABLE oj_t1, oj_t2;