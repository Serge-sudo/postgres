-- Test for delayed temporary table placement feature
-- This test verifies that temporary tables can be extended properly
-- when delayed_temp_table_placement is enabled

-- Note: This test requires delayed_temp_table_placement to be enabled
-- which requires a server restart, so this is more of a functional test

-- Test 1: Create a small temporary table that should fit in temp_buffers
CREATE TEMP TABLE test_small_temp AS SELECT generate_series(1, 100) as id, 'data' as value;

-- Test 2: Create a larger temporary table to trigger buffer overflow
CREATE TEMP TABLE test_large_temp AS SELECT generate_series(1, 10000) as id, repeat('x', 100) as value;

-- Test 3: Insert more data to ensure proper block extension
INSERT INTO test_large_temp SELECT generate_series(10001, 20000), repeat('y', 100);

-- Test 4: Verify data integrity
SELECT COUNT(*) FROM test_small_temp;
SELECT COUNT(*) FROM test_large_temp;

-- Verify that the data is correct
SELECT COUNT(*) FROM test_small_temp WHERE id BETWEEN 1 AND 100;
SELECT COUNT(*) FROM test_large_temp WHERE id BETWEEN 1 AND 20000;

-- Test 5: Test JOIN operations to ensure both tables work correctly
SELECT COUNT(*) FROM test_small_temp s JOIN test_large_temp l ON s.id = l.id;

-- Clean up
DROP TABLE test_small_temp;
DROP TABLE test_large_temp;