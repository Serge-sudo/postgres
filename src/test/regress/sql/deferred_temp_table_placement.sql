--
-- DELAYED_TEMP_TABLE_PLACEMENT
-- Test delayed temporary table placement feature
--
-- This test exercises the deferred_temp_table_placement feature which delays
-- disk allocation for temporary tables until local buffers overflow.
-- The feature is controlled by a PGC_POSTMASTER parameter, so this test
-- focuses on functionality that should work regardless of the setting.

-- Check if the feature is available and show current setting
SELECT name, setting, context, short_desc FROM pg_settings WHERE name = 'deferred_temp_table_placement';

-- Test that the setting cannot be changed without restart
SET deferred_temp_table_placement = on;  -- should fail
RESET deferred_temp_table_placement;  -- should fail

-- ALTER SYSTEM should work for POSTMASTER parameters
ALTER SYSTEM SET deferred_temp_table_placement = on;
ALTER SYSTEM RESET deferred_temp_table_placement;

-- Save current temp_buffers setting and configure for testing
SELECT current_setting('temp_buffers') as original_temp_buffers;

-- Test that delayed temp tables work correctly regardless of the setting.
-- This ensures all operations function properly with or without the feature enabled.

-- Test basic functionality with different buffer sizes
SET temp_buffers TO 100;  -- Small buffer to test overflow scenarios

-- Create test tables to verify behavior
CREATE TEMP TABLE small_temp_test (id int, data text);
CREATE TEMP TABLE medium_temp_test (id int, data text);

-- Test 1: Basic insertion in small temp table
-- Insert small amount of data that should fit in temp_buffers
INSERT INTO small_temp_test SELECT i, 'test_data_' || i FROM generate_series(1, 50) i;

-- Check table size
SELECT pg_relation_size('small_temp_test') as small_temp_size;
SELECT COUNT(*) as row_count FROM small_temp_test;

-- Test 2: Index creation on temp table
CREATE INDEX small_temp_idx ON small_temp_test(id);
SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'small_temp_test';

-- Test 3: VACUUM operations
VACUUM small_temp_test;
VACUUM ANALYZE small_temp_test;

-- Test 4: ALTER TABLE operations
ALTER TABLE small_temp_test ADD COLUMN extra_col int DEFAULT 1;
ALTER TABLE small_temp_test DROP COLUMN extra_col;
ALTER TABLE small_temp_test ALTER COLUMN data TYPE varchar(100);
ALTER TABLE small_temp_test RENAME COLUMN data TO description;
ALTER TABLE small_temp_test RENAME COLUMN description TO data;
ALTER TABLE small_temp_test RENAME TO small_temp_renamed;
ALTER TABLE small_temp_renamed RENAME TO small_temp_test;

-- Test 5: Buffer overflow scenario
-- Insert enough data to exceed temp_buffers and force disk writes
INSERT INTO medium_temp_test 
  SELECT i, repeat('overflow_test_data_', 10) || i 
  FROM generate_series(1, 2000) i;

-- Check that we have substantial data
SELECT pg_relation_size('medium_temp_test') > 100000 as has_substantial_data;
SELECT COUNT(*) as row_count FROM medium_temp_test;

-- Test 6: Index operations on larger temp table
CREATE INDEX medium_temp_idx ON medium_temp_test(id);
CREATE INDEX medium_temp_data_idx ON medium_temp_test(data);

-- Test different index types
CREATE INDEX medium_temp_hash_idx ON medium_temp_test USING hash(id);
CREATE INDEX medium_temp_partial_idx ON medium_temp_test(id) WHERE id > 1000;

-- Test unique index
CREATE UNIQUE INDEX medium_temp_unique_idx ON medium_temp_test(id);

-- Test index usage
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM medium_temp_test WHERE id = 1000;
SELECT COUNT(*) FROM medium_temp_test WHERE id BETWEEN 100 AND 200;
RESET enable_seqscan;

-- Test 7: REINDEX operations
REINDEX INDEX medium_temp_idx;
REINDEX TABLE medium_temp_test;

-- Test 8: CLUSTER operation
CLUSTER medium_temp_test USING medium_temp_idx;
SELECT COUNT(*) FROM medium_temp_test; -- Verify data integrity

-- Test 9: VACUUM FULL operation
VACUUM FULL small_temp_test;
SELECT COUNT(*) FROM small_temp_test; -- Verify data integrity after VACUUM FULL

VACUUM FULL medium_temp_test;
SELECT COUNT(*) FROM medium_temp_test; -- Verify data integrity after VACUUM FULL

-- Test 10: TRUNCATE operation
CREATE TEMP TABLE truncate_test AS SELECT * FROM small_temp_test;
SELECT COUNT(*) as before_truncate FROM truncate_test;
TRUNCATE truncate_test;
SELECT COUNT(*) as after_truncate FROM truncate_test;

-- Test 11: CREATE TABLE AS with temp table
CREATE TEMP TABLE ctas_test AS SELECT * FROM medium_temp_test WHERE id <= 100;
SELECT COUNT(*) FROM ctas_test;

-- Test 12: Tablespace operations (if supported)
-- First check if we can create a tablespace for testing
DO $$
BEGIN
    -- Try to create a test tablespace, ignore if it fails
    BEGIN
        EXECUTE 'CREATE TABLESPACE temp_test_tblspace LOCATION ''/tmp/temp_test_tblspace''';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Could not create test tablespace: %', SQLERRM;
    END;
END
$$;

-- Test ALTER TABLE SET TABLESPACE if tablespace exists
DO $$
BEGIN
    IF EXISTS(SELECT 1 FROM pg_tablespace WHERE spcname = 'temp_test_tblspace') THEN
        EXECUTE 'ALTER TABLE small_temp_test SET TABLESPACE temp_test_tblspace';
        RAISE NOTICE 'Successfully moved temp table to test tablespace';
        -- Move it back
        EXECUTE 'ALTER TABLE small_temp_test SET TABLESPACE pg_default';
    ELSE
        RAISE NOTICE 'Test tablespace not available, skipping tablespace test';
    END IF;
END
$$;

-- Test 13: Constraint operations
ALTER TABLE small_temp_test ADD CONSTRAINT small_temp_pk PRIMARY KEY (id);
ALTER TABLE small_temp_test ADD CONSTRAINT data_not_null CHECK (data IS NOT NULL);

-- Test constraint functionality
INSERT INTO small_temp_test VALUES (999, 'constraint_test');
-- This should fail due to duplicate key
INSERT INTO small_temp_test VALUES (999, 'duplicate');  -- will show ERROR

-- Test 14: Complex queries and joins
CREATE TEMP TABLE join_test (id int PRIMARY KEY, value text);
INSERT INTO join_test SELECT i, 'join_value_' || i FROM generate_series(1, 100) i;

-- Test foreign key between temp tables
CREATE TEMP TABLE fk_test (
    id SERIAL PRIMARY KEY, 
    join_id int REFERENCES join_test(id), 
    description text
);
INSERT INTO fk_test (join_id, description) VALUES (1, 'fk_test_1'), (2, 'fk_test_2');

-- Test joins between temp tables
SELECT COUNT(*) as join_result 
FROM small_temp_test st 
JOIN join_test jt ON st.id = jt.id 
WHERE st.id <= 50;

-- Test three-way join
SELECT COUNT(*) as three_way_join
FROM small_temp_test st
JOIN join_test jt ON st.id = jt.id
JOIN fk_test fk ON jt.id = fk.join_id;

-- Test 15: View operations on temp tables
CREATE TEMP VIEW temp_view AS 
SELECT st.id, st.data, jt.value 
FROM small_temp_test st 
LEFT JOIN join_test jt ON st.id = jt.id;

SELECT COUNT(*) FROM temp_view;
DROP VIEW temp_view;

-- Test 16: Transaction rollback behavior
BEGIN;
    CREATE TEMP TABLE rollback_test (id int);
    INSERT INTO rollback_test VALUES (1), (2), (3);
    SELECT COUNT(*) as before_rollback FROM rollback_test;
ROLLBACK;
-- Table should not exist after rollback
SELECT COUNT(*) FROM pg_tables WHERE tablename = 'rollback_test' AND schemaname LIKE 'pg_temp%';

-- Test 17: Sequence operations with temp tables
CREATE TEMP SEQUENCE temp_seq;
CREATE TEMP TABLE seq_test (id int DEFAULT nextval('temp_seq'), data text);
INSERT INTO seq_test (data) VALUES ('seq1'), ('seq2'), ('seq3');
SELECT id, data FROM seq_test ORDER BY id;

-- Test 18: Trigger operations on temp tables
CREATE TEMP TABLE trigger_test (id int, data text, modified timestamp);

CREATE OR REPLACE FUNCTION temp_trigger_func() RETURNS trigger AS $$
BEGIN
    NEW.modified = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER temp_trigger 
    BEFORE INSERT OR UPDATE ON trigger_test 
    FOR EACH ROW EXECUTE FUNCTION temp_trigger_func();

INSERT INTO trigger_test (id, data) VALUES (1, 'trigger_test');
SELECT id, data, modified IS NOT NULL as has_timestamp FROM trigger_test;

-- Test 19: Large object operations (if applicable)
CREATE TEMP TABLE large_data_test (id int, large_text text);
INSERT INTO large_data_test 
SELECT i, repeat('large_text_content_', 100) || i 
FROM generate_series(1, 500) i;

SELECT pg_relation_size('large_data_test') > 50000 as has_large_data;

-- Test 20: Statistics and analyze
ANALYZE small_temp_test;
ANALYZE medium_temp_test;

-- Check that statistics are available
SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del 
FROM pg_stat_user_tables 
WHERE schemaname LIKE 'pg_temp%' AND tablename IN ('small_temp_test', 'medium_temp_test');

-- Test 21: Permissions and security (temp tables are session-private)
-- This should work in the same session
SELECT COUNT(*) FROM small_temp_test;

-- Test COMMENT operations
COMMENT ON TABLE small_temp_test IS 'Test temporary table';
COMMENT ON COLUMN small_temp_test.id IS 'ID column';

-- Test additional ALTER TABLE operations
ALTER TABLE small_temp_test ALTER COLUMN id SET NOT NULL;
ALTER TABLE small_temp_test ALTER COLUMN data SET DEFAULT 'default_value';

-- Test 22: DROP operations
DROP INDEX IF EXISTS small_temp_idx;
DROP TABLE IF EXISTS truncate_test;
DROP TABLE IF EXISTS ctas_test;
DROP TABLE IF EXISTS fk_test;
DROP TABLE IF EXISTS join_test;
DROP TABLE IF EXISTS seq_test;
DROP SEQUENCE IF EXISTS temp_seq;
DROP TABLE IF EXISTS trigger_test;
DROP FUNCTION IF EXISTS temp_trigger_func();
DROP TABLE IF EXISTS large_data_test;

-- Verify main test tables still exist
SELECT COUNT(*) as small_temp_count FROM small_temp_test;
SELECT COUNT(*) as medium_temp_count FROM medium_temp_test;

-- Final cleanup
DROP TABLE small_temp_test;
DROP TABLE medium_temp_test;

-- Clean up test tablespace if it was created
DO $$
BEGIN
    IF EXISTS(SELECT 1 FROM pg_tablespace WHERE spcname = 'temp_test_tblspace') THEN
        EXECUTE 'DROP TABLESPACE temp_test_tblspace';
        RAISE NOTICE 'Cleaned up test tablespace';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Could not clean up test tablespace: %', SQLERRM;
END
$$;

-- Test 23: Performance comparison test (basic)
-- This test creates many small temp tables to demonstrate the benefit
-- of delayed placement for small, short-lived temp tables

\timing on

-- Create and drop many small temp tables
DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 1..20 LOOP
        EXECUTE 'CREATE TEMP TABLE perf_test_' || i || ' (id int, data text)';
        EXECUTE 'INSERT INTO perf_test_' || i || ' SELECT j, ''data'' || j FROM generate_series(1, 10) j';
        EXECUTE 'SELECT COUNT(*) FROM perf_test_' || i;
        EXECUTE 'DROP TABLE perf_test_' || i;
    END LOOP;
END
$$;

\timing off

-- Test complete - all operations should work correctly
-- regardless of deferred_temp_table_placement setting
SELECT 'Delayed temp table placement tests completed successfully' as test_result;

-- Display final setting information
SELECT 
    name, 
    setting as current_value,
    boot_val as default_value,
    context,
    short_desc
FROM pg_settings 
WHERE name = 'deferred_temp_table_placement';

-- Reset temp_buffers to original value
-- Note: In practice, you would reset this, but in regression tests 
-- the session ends so this is not strictly necessary
RESET temp_buffers;