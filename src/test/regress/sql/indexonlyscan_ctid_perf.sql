-- Performance test for index-only scan with ctid feature
-- This test compares DELETE performance with and without the feature

-- Create a test table with an index
CREATE TABLE perf_test_table (
    id INTEGER PRIMARY KEY,
    data TEXT
);

-- Insert test data
INSERT INTO perf_test_table 
SELECT i, 'data_' || i 
FROM generate_series(1, 10000) i;

-- Make sure table is fully visible for index-only scans
VACUUM perf_test_table;

-- Create a table to store timing results
CREATE TABLE perf_results (
    test_name TEXT,
    feature_enabled BOOLEAN,
    execution_time_ms NUMERIC,
    test_timestamp TIMESTAMP DEFAULT NOW()
);

-- Test with feature ENABLED (default)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    i INTEGER;
BEGIN
    -- Warm up
    SET enable_seqscan = off;
    SET enable_indexonlyscan_ctid = on;
    
    start_time := clock_timestamp();
    
    -- Perform random deletes
    FOR i IN 1..100 LOOP
        DELETE FROM perf_test_table WHERE id = 1000 + i;
    END LOOP;
    
    end_time := clock_timestamp();
    
    INSERT INTO perf_results (test_name, feature_enabled, execution_time_ms)
    VALUES ('random_deletes', true, 
            EXTRACT(EPOCH FROM (end_time - start_time)) * 1000);
END $$;

-- Restore deleted data
INSERT INTO perf_test_table 
SELECT i, 'data_' || i 
FROM generate_series(1001, 1100) i;

VACUUM perf_test_table;

-- Test with feature DISABLED
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    i INTEGER;
BEGIN
    SET enable_seqscan = off;
    SET enable_indexonlyscan_ctid = off;
    
    start_time := clock_timestamp();
    
    -- Perform same random deletes
    FOR i IN 1..100 LOOP
        DELETE FROM perf_test_table WHERE id = 2000 + i;
    END LOOP;
    
    end_time := clock_timestamp();
    
    INSERT INTO perf_results (test_name, feature_enabled, execution_time_ms)
    VALUES ('random_deletes', false,
            EXTRACT(EPOCH FROM (end_time - start_time)) * 1000);
END $$;

-- Compare results
SELECT 
    feature_enabled,
    execution_time_ms,
    CASE 
        WHEN feature_enabled THEN 'With ctid feature'
        ELSE 'Without ctid feature'
    END as description,
    CASE 
        WHEN feature_enabled THEN 
            execution_time_ms / NULLIF((SELECT execution_time_ms FROM perf_results WHERE NOT feature_enabled AND test_name = 'random_deletes'), 0) * 100
        ELSE 100
    END as relative_performance_pct
FROM perf_results
WHERE test_name = 'random_deletes'
ORDER BY feature_enabled DESC;

-- Verify the feature improves performance
SELECT 
    CASE 
        WHEN (SELECT execution_time_ms FROM perf_results WHERE feature_enabled AND test_name = 'random_deletes') <
             (SELECT execution_time_ms FROM perf_results WHERE NOT feature_enabled AND test_name = 'random_deletes')
        THEN 'PASS: Feature enabled is faster'
        ELSE 'INFO: Feature enabled time: ' || 
             (SELECT execution_time_ms FROM perf_results WHERE feature_enabled AND test_name = 'random_deletes')::text ||
             'ms, Feature disabled time: ' ||
             (SELECT execution_time_ms FROM perf_results WHERE NOT feature_enabled AND test_name = 'random_deletes')::text ||
             'ms'
    END as performance_test_result;

-- Cleanup
DROP TABLE perf_test_table;
DROP TABLE perf_results;
