--
-- OUTER_JOIN_LIMIT_PUSHDOWN
-- Test the ORDER BY LIMIT pushdown optimization for outer joins
-- Validates that the same results are returned with optimization on/off
--

-- Create test tables with more data for comprehensive testing
CREATE TABLE oj_t1 (id integer, name text, value integer, created_at timestamp);
CREATE TABLE oj_t2 (id integer, category text, score integer, amount numeric(10,2));
CREATE TABLE oj_t3 (id integer, status text, priority integer, active boolean);
CREATE TABLE oj_t4 (id integer, description text, flag boolean, data jsonb);
CREATE TABLE oj_t5 (id integer, type text, rank integer, tags text[]);

-- Insert comprehensive test data with various patterns
INSERT INTO oj_t1 VALUES 
  (1, 'one', 10, '2023-01-01'::timestamp), (2, 'two', 20, '2023-01-02'::timestamp), 
  (3, 'three', 30, '2023-01-03'::timestamp), (4, 'four', 40, '2023-01-04'::timestamp), 
  (5, 'five', 50, '2023-01-05'::timestamp), (6, 'six', 60, '2023-01-06'::timestamp), 
  (7, 'seven', 70, '2023-01-07'::timestamp), (8, 'eight', 80, '2023-01-08'::timestamp),
  (9, 'nine', 90, '2023-01-09'::timestamp), (10, 'ten', 100, '2023-01-10'::timestamp),
  (11, 'eleven', 15, '2023-01-11'::timestamp), (12, 'twelve', 25, '2023-01-12'::timestamp);

INSERT INTO oj_t2 VALUES 
  (1, 'cat1', 100, 10.50), (2, 'cat2', 200, 20.75), (3, 'cat3', 300, 30.25), 
  (5, 'cat5', 500, 50.00), (7, 'cat7', 700, 70.99), (9, 'cat9', 900, 90.10),
  (11, 'cat11', 110, 11.11), (13, 'cat13', 130, 13.13);

INSERT INTO oj_t3 VALUES 
  (1, 'active', 1, true), (3, 'pending', 3, false), (4, 'complete', 4, true), 
  (6, 'inactive', 6, false), (8, 'review', 8, true), (10, 'archived', 10, false),
  (12, 'new', 12, true), (14, 'expired', 14, false);

INSERT INTO oj_t4 VALUES 
  (2, 'desc2', true, '{"key": "value2"}'::jsonb), (4, 'desc4', false, '{"key": "value4"}'::jsonb), 
  (6, 'desc6', true, '{"key": "value6"}'::jsonb), (8, 'desc8', false, '{"key": "value8"}'::jsonb),
  (10, 'desc10', true, '{"key": "value10"}'::jsonb), (12, 'desc12', false, '{"key": "value12"}'::jsonb);

INSERT INTO oj_t5 VALUES 
  (1, 'type1', 1, ARRAY['tag1', 'tag2']), (3, 'type3', 3, ARRAY['tag3']), 
  (5, 'type5', 5, ARRAY['tag5', 'tag6', 'tag7']), (7, 'type7', 7, ARRAY['tag8']),
  (9, 'type9', 9, ARRAY['tag9', 'tag10']), (11, 'type11', 11, ARRAY['tag11']);

-- Create indexes
CREATE INDEX oj_t1_id_idx ON oj_t1(id);
CREATE INDEX oj_t1_value_idx ON oj_t1(value);
CREATE INDEX oj_t2_id_idx ON oj_t2(id);
CREATE INDEX oj_t2_score_idx ON oj_t2(score);
CREATE INDEX oj_t3_id_idx ON oj_t3(id);
CREATE INDEX oj_t4_id_idx ON oj_t4(id);
CREATE INDEX oj_t5_id_idx ON oj_t5(id);

-- Update statistics
ANALYZE oj_t1, oj_t2, oj_t3, oj_t4, oj_t5;

-- Create a function to validate query results with optimization on/off
CREATE OR REPLACE FUNCTION validate_query_results(
    query_text text,
    test_description text
) RETURNS text AS $$
DECLARE
    results_match boolean := false;
    enabled_count integer;
    disabled_count integer;
    difference_count integer;
BEGIN
    -- Create temporary tables for comparison
    EXECUTE 'CREATE TEMPORARY TABLE temp_results_enabled AS ' || query_text;
    
    -- Disable optimization and run again
    SET enable_outer_join_limit_pushdown = off;
    EXECUTE 'CREATE TEMPORARY TABLE temp_results_disabled AS ' || query_text;
    
    -- Re-enable optimization
    SET enable_outer_join_limit_pushdown = on;
    
    -- Check if row counts match
    SELECT INTO enabled_count count(*) FROM temp_results_enabled;
    SELECT INTO disabled_count count(*) FROM temp_results_disabled;
    
    -- Check for differences using EXCEPT
    EXECUTE 'SELECT count(*) FROM (
        (SELECT * FROM temp_results_enabled EXCEPT SELECT * FROM temp_results_disabled)
        UNION ALL
        (SELECT * FROM temp_results_disabled EXCEPT SELECT * FROM temp_results_enabled)
    ) AS differences' INTO difference_count;
    
    -- Determine if results match
    results_match := (enabled_count = disabled_count) AND (difference_count = 0);
    
    -- Clean up temporary tables
    DROP TABLE temp_results_enabled, temp_results_disabled;
    
    -- Return validation result
    IF results_match THEN
        RETURN '✓ PASS: ' || test_description;
    ELSE
        RETURN '✗ FAIL: ' || test_description || ' (enabled: ' || enabled_count || ', disabled: ' || disabled_count || ', differences: ' || difference_count || ')';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Test suite using the validation function
SELECT 'Running comprehensive ORDER BY LIMIT pushdown validation tests' as test_suite_header;

-- Basic join tests
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 3',
    'Simple LEFT JOIN with ORDER BY preserved side'
);

SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 RIGHT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t2.id LIMIT 4',
    'Simple RIGHT JOIN with ORDER BY preserved side'
);

-- Multiple join tests
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category, t3.status FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id LEFT JOIN oj_t3 t3 ON t1.id = t3.id ORDER BY t1.id LIMIT 5',
    'Two-level LEFT JOINs with ORDER BY preserved side'
);

SELECT validate_query_results(
    'SELECT t1.id, t2.category, t3.status, t4.description FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id LEFT JOIN oj_t3 t3 ON t1.id = t3.id LEFT JOIN oj_t4 t4 ON t1.id = t4.id ORDER BY t1.id LIMIT 6',
    'Three-level LEFT JOINs with ORDER BY preserved side'
);

SELECT validate_query_results(
    'SELECT t1.id, t2.category, t3.status, t4.description, t5.type FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id LEFT JOIN oj_t3 t3 ON t1.id = t3.id LEFT JOIN oj_t4 t4 ON t1.id = t4.id LEFT JOIN oj_t5 t5 ON t1.id = t5.id ORDER BY t1.id LIMIT 7',
    'Four-level LEFT JOINs with ORDER BY preserved side'
);

-- ORDER BY variations
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.score FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.value DESC, t1.id ASC LIMIT 4',
    'LEFT JOIN with multi-column ORDER BY'
);

SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.created_at DESC LIMIT 5',
    'LEFT JOIN with ORDER BY timestamp column'
);

-- OFFSET and LIMIT combinations
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 3 OFFSET 2',
    'LEFT JOIN with OFFSET and LIMIT'
);

SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 10 OFFSET 5',
    'LEFT JOIN with larger OFFSET and LIMIT'
);

-- Mixed join types
SELECT validate_query_results(
    'SELECT t1.id, t2.category, t3.status FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id RIGHT JOIN oj_t3 t3 ON t1.id = t3.id ORDER BY t3.id LIMIT 6',
    'LEFT JOIN followed by RIGHT JOIN'
);

SELECT validate_query_results(
    'SELECT t1.id, t2.category, t3.status FROM oj_t1 t1 RIGHT JOIN oj_t2 t2 ON t1.id = t2.id LEFT JOIN oj_t3 t3 ON t2.id = t3.id ORDER BY t2.id LIMIT 4',
    'RIGHT JOIN followed by LEFT JOIN'
);

-- Aggregate functions with GROUP BY
SELECT validate_query_results(
    'SELECT t1.id, COUNT(t2.id) as join_count, SUM(t2.score) as total_score FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id GROUP BY t1.id ORDER BY t1.id LIMIT 5',
    'LEFT JOIN with aggregates and GROUP BY'
);

-- Different data types and expressions
SELECT validate_query_results(
    'SELECT t1.id, t2.amount, t3.active, t4.data FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id LEFT JOIN oj_t3 t3 ON t1.id = t3.id LEFT JOIN oj_t4 t4 ON t1.id = t4.id ORDER BY t2.amount DESC NULLS LAST LIMIT 6',
    'Multi-join with numeric, boolean, and jsonb columns'
);

SELECT validate_query_results(
    'SELECT t1.id, t5.tags, array_length(t5.tags, 1) as tag_count FROM oj_t1 t1 LEFT JOIN oj_t5 t5 ON t1.id = t5.id ORDER BY t1.id LIMIT 8',
    'LEFT JOIN with array columns and functions'
);

-- Subqueries with outer joins
SELECT validate_query_results(
    'SELECT sub.id, sub.name, t2.category FROM (SELECT id, name, value FROM oj_t1 WHERE value > 30) sub LEFT JOIN oj_t2 t2 ON sub.id = t2.id ORDER BY sub.id LIMIT 4',
    'Subquery with LEFT JOIN'
);

SELECT validate_query_results(
    'SELECT t1.id, t1.name, sub.avg_score FROM oj_t1 t1 LEFT JOIN (SELECT id, AVG(score) as avg_score FROM oj_t2 GROUP BY id) sub ON t1.id = sub.id ORDER BY t1.id LIMIT 5',
    'LEFT JOIN with aggregated subquery'
);

-- Complex WHERE conditions
SELECT validate_query_results(
    'SELECT t1.id, t2.category, t3.status FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id LEFT JOIN oj_t3 t3 ON t1.id = t3.id WHERE t1.value BETWEEN 20 AND 80 ORDER BY t1.id LIMIT 6',
    'Multi-join with WHERE clause on preserved side'
);

SELECT validate_query_results(
    'SELECT t1.id, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id WHERE t2.score IS NULL OR t2.score > 300 ORDER BY t1.id LIMIT 5',
    'LEFT JOIN with complex WHERE condition'
);

-- CTE (Common Table Expression) tests
SELECT validate_query_results(
    'WITH ranked_t1 AS (SELECT id, name, value, ROW_NUMBER() OVER (ORDER BY value) as rn FROM oj_t1) SELECT r.id, r.name, t2.category FROM ranked_t1 r LEFT JOIN oj_t2 t2 ON r.id = t2.id ORDER BY r.id LIMIT 4',
    'CTE with LEFT JOIN and window function'
);

-- Different join conditions
SELECT validate_query_results(
    'SELECT t1.id, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.value / 10 = t2.id ORDER BY t1.id LIMIT 5',
    'LEFT JOIN with expression-based join condition'
);

-- Negative test cases (optimization should not apply)
SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id WHERE t2.category IS NOT NULL ORDER BY t2.category LIMIT 3',
    'ORDER BY non-preserved side (optimization disabled)'
);

SELECT validate_query_results(
    'SELECT t1.id, t1.name, t2.category FROM oj_t1 t1 INNER JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 3',
    'INNER JOIN (optimization disabled)'
);

SELECT validate_query_results(
    'SELECT t1.id, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t2.score DESC NULLS LAST LIMIT 4',
    'ORDER BY nullable column from non-preserved side (optimization disabled)'
);

-- Force different join algorithms to test all code paths
SET enable_nestloop = off;
SET enable_hashjoin = on;
SET enable_mergejoin = off;
SELECT validate_query_results(
    'SELECT t1.id, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 4',
    'LEFT JOIN with forced hash join algorithm'
);

SET enable_nestloop = off;
SET enable_hashjoin = off; 
SET enable_mergejoin = on;
SELECT validate_query_results(
    'SELECT t1.id, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 4',
    'LEFT JOIN with forced merge join algorithm'
);

SET enable_nestloop = on;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SELECT validate_query_results(
    'SELECT t1.id, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 4',
    'LEFT JOIN with forced nested loop algorithm'
);

-- Reset join settings
RESET enable_nestloop;
RESET enable_hashjoin;
RESET enable_mergejoin;

-- Test GUC parameter functionality
SELECT 'Testing GUC parameter functionality' as test_name;
SHOW enable_outer_join_limit_pushdown;

SET enable_outer_join_limit_pushdown = off;
SHOW enable_outer_join_limit_pushdown;

SET enable_outer_join_limit_pushdown = on; 
SHOW enable_outer_join_limit_pushdown;

-- Performance test with larger result set to ensure optimization is working
SELECT 'Performance validation with larger dataset' as test_name;
SELECT validate_query_results(
    'SELECT t1.id, t2.category, t3.status FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id LEFT JOIN oj_t3 t3 ON t1.id = t3.id ORDER BY t1.id LIMIT 20',
    'Large result set validation'
);

-- Edge case: LIMIT larger than available rows
SELECT validate_query_results(
    'SELECT t1.id, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 1000',
    'LIMIT larger than available rows'
);

-- Edge case: LIMIT 0
SELECT validate_query_results(
    'SELECT t1.id, t2.category FROM oj_t1 t1 LEFT JOIN oj_t2 t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 0',
    'LIMIT 0 edge case'
);

-- Clean up validation function and tables
DROP FUNCTION validate_query_results(text, text);
DROP TABLE oj_t1, oj_t2, oj_t3, oj_t4, oj_t5;

-- Reset settings
RESET enable_outer_join_limit_pushdown;