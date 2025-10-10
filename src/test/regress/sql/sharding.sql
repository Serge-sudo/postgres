--
-- Test sharding DDL syntax
--
-- These tests verify that the basic sharding DDL parses correctly.
-- Full implementation is TODO.
--

-- CREATE SHARD GROUP (should error with "not yet implemented")
CREATE SHARD GROUP sg_test;

-- CREATE SHARD GROUP with options (should error with "not yet implemented")
CREATE SHARD GROUP sg_test2 WITH (routing_strategy='hash');

-- Test error message
CREATE SHARD GROUP sg_test;
