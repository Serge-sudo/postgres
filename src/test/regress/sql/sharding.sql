--
-- Test sharding DDL syntax
--
-- These tests verify that the basic sharding DDL parses correctly.
-- Full implementation is TODO.
--

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

SELECT * FROM pg_shardgroups;
SELECT * FROM pg_shardmembers;

\d+ pg_shardgroups
\d+ pg_shardmembers

-- CREATE SHARD GROUP 
CREATE SHARD GROUP sg_test;
SELECT * FROM pg_shardgroups;
-- -- Test error message
CREATE SHARD GROUP sg_test;
SELECT * FROM pg_shardgroups;

ALTER DATABASE regression SET DEFAULT SHARD GROUP sg_test;
SELECT * FROM pg_database;


-- ALTER DATABASE ... SET SHARD GROUP
CREATE SERVER server1
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'localhost', dbname 'regression', port '5432');
ALTER SHARD GROUP sg_test ADD MEMBER server1;
SELECT * FROM pg_foreign_server;
SELECT * FROM pg_shardmembers;
DROP SERVER server1;
ALTER SHARD GROUP sg_test DROP MEMBER server1;
SELECT * FROM pg_shardmembers;
ALTER SHARD GROUP sg_test ADD MEMBER server1;
SELECT * FROM pg_foreign_server;

-- check default shard group
CREATE WORLDWIDE TABLE test_t_worldwide (a int, b int);
SELECT * FROM pg_class WHERE relname like 'test_t_%';
DROP TABLE test_t_worldwide;

--check non-default shard group
CREATE SHARD GROUP sg_test_2;
SELECT * FROM pg_shardgroups;
CREATE WORLDWIDE TABLE test_t_worldwide (a int, b int) SHARD GROUP sg_test_2;
CREATE TABLE test_t_distribute (a int, b int) DISTRIBUTED BY LIST(a) SHARD GROUP sg_test_2;
CREATE TABLE test_t_distribute_1 PARTITION OF test_t_distribute FOR VALUES IN (1,2,3);
SELECT * FROM pg_class WHERE relname like 'test_t_%';

-- Test new features: DROP TABLE on shard group tables
-- This should replicate to all shard members
DROP TABLE IF EXISTS test_t_distribute_1;
SELECT * FROM pg_class WHERE relname = 'test_t_distribute_1';

-- Test CREATE INDEX CONCURRENTLY rejection on shard group tables
-- This should fail with an error message
CREATE INDEX CONCURRENTLY idx_test ON test_t_worldwide(a);

-- Test CREATE INDEX on shard group tables (non-concurrent)
-- This should replicate to all shard members
CREATE INDEX idx_test ON test_t_worldwide(a);
SELECT * FROM pg_class WHERE relname = 'idx_test';

-- Test REINDEX on shard group tables
-- This should replicate to all shard members
REINDEX TABLE test_t_worldwide;

-- Test REINDEX CONCURRENTLY rejection on shard group tables
-- This should fail with an error message
REINDEX TABLE CONCURRENTLY test_t_worldwide;

-- Clean up
DROP TABLE test_t_worldwide;
DROP TABLE test_t_distribute;

SELECT * FROM pg_shardgroups;
SELECT * FROM pg_shardmembers;
DROP SHARD GROUP sg_test_2;
SELECT * FROM pg_shardgroups;
SELECT * FROM pg_shardmembers;

