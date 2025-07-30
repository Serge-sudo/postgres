/*
 * Test virtual catalog functionality for temporary tables
 */

-- Create a temporary table
CREATE TEMP TABLE test_temp_table (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert some data
INSERT INTO test_temp_table (id, name) VALUES (1, 'Test record 1');
INSERT INTO test_temp_table (id, name) VALUES (2, 'Test record 2');

-- Test basic queries
SELECT * FROM test_temp_table;

-- Create a regular table for comparison
CREATE TABLE test_regular_table (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert some data
INSERT INTO test_regular_table (id, name) VALUES (1, 'Regular record 1');
INSERT INTO test_regular_table (id, name) VALUES (2, 'Regular record 2');

-- Test that we can query both tables
SELECT 'temp' as table_type, * FROM test_temp_table
UNION ALL
SELECT 'regular' as table_type, * FROM test_regular_table;

-- Check catalog visibility
-- This should show both tables
SELECT relname, relpersistence 
FROM pg_class 
WHERE relname IN ('test_temp_table', 'test_regular_table')
ORDER BY relname;

-- Test dropping temporary table
DROP TABLE test_temp_table;

-- Test dropping regular table
DROP TABLE test_regular_table;

-- Verify they're gone
SELECT COUNT(*) as remaining_test_tables
FROM pg_class 
WHERE relname LIKE 'test_%_table';