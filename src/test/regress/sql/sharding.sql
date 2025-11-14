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

SELECT * FROM pg_shardgroups;
SELECT * FROM pg_shardmembers;
DROP SHARD GROUP sg_test_2;
SELECT * FROM pg_shardgroups;
SELECT * FROM pg_shardmembers;

