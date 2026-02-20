
-- Verify the GUC exists and defaults to off
SHOW enable_parallel_temp_table;

SET pax_parallel_workers = 7;
SET max_parallel_workers_per_gather = 7;

SET temp_buffers = '2GB';

CREATE OR REPLACE FUNCTION avg_select_time(
    q text,
    runs int DEFAULT 10
)
RETURNS double precision
LANGUAGE plpgsql
AS $$
DECLARE
    i int;
    t0 timestamptz;
    total_ms double precision := 0;
BEGIN
    IF runs <= 0 THEN
        RAISE EXCEPTION 'runs must be > 0';
    END IF;

    FOR i IN 1..runs LOOP
        t0 := clock_timestamp();
        EXECUTE q;
        total_ms := total_ms +
            EXTRACT(EPOCH FROM (clock_timestamp() - t0)) * 1000;
    END LOOP;

    RETURN total_ms / runs;
END;
$$;

CREATE TEMP TABLE parallel_temp_tbl (id int);
INSERT INTO parallel_temp_tbl SELECT i FROM generate_series(1,50000000) i;

VACUUM (ANALYZE) parallel_temp_tbl;

-- Without the flag, shows regular Seq Scan
SET enable_parallel_temp_table = off;
EXPLAIN (costs on) SELECT count(*) FROM parallel_temp_tbl WHERE id = 5;

SELECT avg_select_time('SELECT count(*) FROM parallel_temp_tbl WHERE id = 5;', 10);

-- With the flag, shows Local Parallel Seq Scan to indicate thread workers
SET enable_parallel_temp_table = on;
EXPLAIN (costs on) SELECT count(*) FROM parallel_temp_tbl WHERE id = 5;

-- Verify the result is correct when thread workers are active
SELECT avg_select_time('SELECT count(*) FROM parallel_temp_tbl WHERE id = 5;', 10);

DROP TABLE parallel_temp_tbl;

-- Reset GUC settings
RESET enable_parallel_temp_table;
RESET max_parallel_workers_per_gather;
