-- Test script for Foreign Connection Multiplexer
-- This script demonstrates the basic functionality of the multiplexer

-- Check if multiplexer GUC parameters exist
SHOW foreign_conn_multiplexer.workers;
SHOW foreign_conn_multiplexer.enabled;

-- Check if workers are running
SELECT count(*) as worker_count, 
       array_agg(pid) as worker_pids
FROM pg_stat_activity
WHERE backend_type LIKE '%conn_multiplexer%';

-- Display worker details if any are running
SELECT pid, 
       backend_start, 
       state,
       wait_event_type,
       wait_event
FROM pg_stat_activity
WHERE backend_type LIKE '%conn_multiplexer%'
ORDER BY pid;

-- Example: Enable the multiplexer (requires reload)
-- Note: Setting workers requires a restart
-- ALTER SYSTEM SET foreign_conn_multiplexer.workers = 4;
-- ALTER SYSTEM SET foreign_conn_multiplexer.enabled = true;
-- SELECT pg_reload_conf();

-- Example: Disable the multiplexer
-- ALTER SYSTEM SET foreign_conn_multiplexer.enabled = false;
-- SELECT pg_reload_conf();
