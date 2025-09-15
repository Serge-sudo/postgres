-- Comprehensive test for ORDER BY LIMIT pushdown optimization for outer joins
-- This test validates that results are identical with optimization on/off

-- Create validation function to compare results with optimization on/off
CREATE OR REPLACE FUNCTION validate_query_results(query_text TEXT, test_description TEXT)
RETURNS TEXT AS $$
DECLARE
    result_count_on INTEGER;
    result_count_off INTEGER;
    diff_count INTEGER;
BEGIN
    -- Enable optimization and get results
    SET enable_outer_join_limit_pushdown = on;
    EXECUTE 'CREATE TEMP TABLE results_on AS ' || query_text;
    GET DIAGNOSTICS result_count_on = ROW_COUNT;
    
    -- Disable optimization and get results
    SET enable_outer_join_limit_pushdown = off;
    EXECUTE 'CREATE TEMP TABLE results_off AS ' || query_text;
    GET DIAGNOSTICS result_count_off = ROW_COUNT;
    
    -- Compare results using EXCEPT to find differences
    SELECT COUNT(*) INTO diff_count FROM (
        (SELECT * FROM results_on EXCEPT SELECT * FROM results_off)
        UNION ALL
        (SELECT * FROM results_off EXCEPT SELECT * FROM results_on)
    ) AS differences;
    
    -- Clean up temp tables
    DROP TABLE results_on;
    DROP TABLE results_off;
    
    -- Return validation result
    IF result_count_on = result_count_off AND diff_count = 0 THEN
        RETURN '✓ PASS: ' || test_description || ' (rows: ' || result_count_on || ')';
    ELSE
        RETURN '✗ FAIL: ' || test_description || ' (on: ' || result_count_on || ', off: ' || result_count_off || ', diff: ' || diff_count || ')';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create comprehensive test tables
CREATE TABLE oj_t1 (
    id INTEGER,
    name TEXT,
    value NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE oj_t2 (
    id INTEGER,
    category TEXT,
    priority INTEGER,
    active BOOLEAN DEFAULT true
);

CREATE TABLE oj_t3 (
    id INTEGER,
    description TEXT,
    tags TEXT[]
);

CREATE TABLE oj_t4 (
    t1_id INTEGER,
    t2_id INTEGER,
    relationship_type TEXT
);

-- Insert comprehensive test data
INSERT INTO oj_t1 VALUES 
    (1, 'first', 10.5, '2024-01-01'),
    (2, 'second', 20.0, '2024-01-02'),
    (3, 'third', 15.75, '2024-01-03'),
    (4, 'fourth', 30.25, '2024-01-04'),
    (5, 'fifth', 5.0, '2024-01-05'),
    (6, 'sixth', 25.5, '2024-01-06'),
    (7, 'seventh', 12.0, '2024-01-07'),
    (8, 'eighth', 35.0, '2024-01-08'),
    (9, 'ninth', 8.5, '2024-01-09'),
    (10, 'tenth', 40.0, '2024-01-10');

INSERT INTO oj_t2 VALUES 
    (1, 'cat1', 1, true),
    (3, 'cat3', 2, true),
    (5, 'cat5', 1, false),
    (7, 'cat7', 3, true),
    (9, 'cat9', 2, true);

INSERT INTO oj_t3 VALUES 
    (2, 'desc2', ARRAY['tag1', 'tag2']),
    (4, 'desc4', ARRAY['tag3']),
    (6, 'desc6', ARRAY['tag1', 'tag4']),
    (8, 'desc8', ARRAY['tag2', 'tag5']),
    (10, 'desc10', ARRAY['tag6']);

INSERT INTO oj_t4 VALUES 
    (1, 1, 'primary'),
    (2, 3, 'secondary'),
    (3, 5, 'primary'),
    (4, 7, 'tertiary'),
    (5, 9, 'secondary');

-- Run comprehensive validation tests
\echo
\echo '=== COMPREHENSIVE OUTER JOIN LIMIT PUSHDOWN VALIDATION ==='
\echo

-- Test 1: Basic LEFT JOIN with ORDER BY preserved side
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 5',
    'Basic LEFT JOIN - ORDER BY preserved side'
);

-- Test 2: Basic RIGHT JOIN with ORDER BY preserved side  
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 RIGHT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t2.id LIMIT 3',
    'Basic RIGHT JOIN - ORDER BY preserved side'
);

-- Test 3: LEFT JOIN with OFFSET
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 3 OFFSET 2',
    'LEFT JOIN with OFFSET'
);

-- Test 4: Multiple column ORDER BY
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t1.value FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.value DESC, t1.id LIMIT 4',
    'LEFT JOIN - Multiple column ORDER BY'
);

-- Test 5: Nested outer joins (2 levels)
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category, t3.description 
     FROM oj_t1 t1 
     LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
     LEFT JOIN oj_t3 t3 ON t1.id = t3.id 
     ORDER BY t1.id LIMIT 6',
    'Two-level nested LEFT JOINs'
);

-- Test 6: Mixed join types with ORDER BY preserved side
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category, t3.description 
     FROM oj_t1 t1 
     LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
     RIGHT JOIN oj_t3 t3 ON t1.id = t3.id 
     ORDER BY t3.id LIMIT 4',
    'Mixed LEFT and RIGHT JOINs'
);

-- Test 7: Complex expressions in ORDER BY
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t1.value * 2 as doubled_value 
     FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
     ORDER BY t1.value * 2 DESC, t1.name LIMIT 5',
    'LEFT JOIN - Complex expressions in ORDER BY'
);

-- Test 8: Date/timestamp ordering
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t1.created_at 
     FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
     ORDER BY t1.created_at DESC LIMIT 4',
    'LEFT JOIN - Timestamp ordering'
);

-- Test 9: Array column handling
SELECT validate_query_results(
    'SELECT t3.id, t3.description, t3.tags 
     FROM oj_t3 t3 LEFT JOIN oj_t1 t1 ON t3.id = t1.id 
     ORDER BY t3.id LIMIT 3',
    'LEFT JOIN with array columns'
);

-- Test 10: Boolean and numeric mixed ordering
SELECT validate_query_results(
    'SELECT t2.id, t2.category, t2.active, t2.priority 
     FROM oj_t2 t2 LEFT JOIN oj_t1 t1 ON t2.id = t1.id 
     ORDER BY t2.active DESC, t2.priority, t2.id LIMIT 4',
    'Mixed boolean and numeric ordering'
);

-- Test 11: Complex multi-level joins with LIMIT
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category, t3.description, t4.relationship_type 
     FROM oj_t1 t1 
     LEFT JOIN oj_t2 t2 ON t1.id = t2.id 
     LEFT JOIN oj_t3 t3 ON t1.id = t3.id 
     LEFT JOIN oj_t4 t4 ON t1.id = t4.t1_id 
     ORDER BY t1.id LIMIT 5',
    'Four-table nested LEFT JOINs'
);

-- Test 12: RIGHT JOIN with complex preserved side ordering
SELECT validate_query_results(
    'SELECT t1.name, t2.id, t2.category, t2.priority 
     FROM oj_t1 t1 RIGHT JOIN oj_t2 t2 ON t1.id = t2.id 
     ORDER BY t2.priority DESC, t2.category, t2.id LIMIT 3',
    'RIGHT JOIN - Complex preserved side ordering'
);

-- Test 13: Large LIMIT (larger than available rows)
SELECT validate_query_results(
    'SELECT t1.id, t1.name FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 100',
    'LIMIT larger than available rows'
);

-- Test 14: LIMIT 1 (minimal case)
SELECT validate_query_results(
    'SELECT t1.id, t1.name FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 1',
    'Minimal LIMIT 1 case'
);

-- Test 15: Text ordering with special characters
SELECT validate_query_results(
    'SELECT t1.id, t1.name FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.name LIMIT 4',
    'Text ordering'
);

-- Test cases that should NOT be optimized (negative tests)
\echo
\echo '=== NEGATIVE TEST CASES (should not be optimized) ==='
\echo

-- Test 16: ORDER BY non-preserved side (should not optimize)
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t2.category LIMIT 3',
    'ORDER BY non-preserved side (no optimization expected)'
);

-- Test 17: INNER JOIN (should not optimize)
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 INNER JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 3',
    'INNER JOIN (no optimization expected)'
);

-- Test 18: Mixed preserved and non-preserved columns in ORDER BY
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id, t2.category LIMIT 3',
    'Mixed preserved/non-preserved ORDER BY (no optimization expected)'
);

-- Test 19: No LIMIT clause
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id',
    'No LIMIT clause (no optimization expected)'
);

-- Test 20: No ORDER BY clause  
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id LIMIT 5',
    'No ORDER BY clause (no optimization expected)'
);

\echo
\echo '=== EDGE CASES ==='
\echo

-- Test 21: LIMIT 0
SELECT validate_query_results(
    'SELECT t1.id, t1.name FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 0',
    'LIMIT 0 edge case'
);

-- Test 22: Very large OFFSET
SELECT validate_query_results(
    'SELECT t1.id, t1.name FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 2 OFFSET 8',
    'Large OFFSET edge case'
);

-- Test 23: Self-join outer join
SELECT validate_query_results(
    'SELECT t1a.id, t1a.name, t1b.name as other_name 
     FROM oj_t1 t1a LEFT JOIN oj_t1 t1b ON t1a.id = t1b.id + 1 
     ORDER BY t1a.id LIMIT 5',
    'Self-join LEFT JOIN'
);

\echo
\echo '=== VALIDATION SUMMARY ==='
\echo 'All tests check that query results are identical with optimization enabled/disabled'
\echo

-- Clean up validation function and test tables
DROP FUNCTION validate_query_results(TEXT, TEXT);
DROP TABLE oj_t1, oj_t2, oj_t3, oj_t4;