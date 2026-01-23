/* contrib/postgres_fdw/postgres_fdw--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION postgres_fdw UPDATE TO '1.2'" to load this file. \quit

CREATE FUNCTION postgres_fdw_get_locks(
    server_oid oid,
    OUT waiter_pid integer,
    OUT blocker_pid integer,
    OUT waiter_xid integer,
    OUT blocker_xid integer,
    OUT object_oid oid,
    OUT lock_mode text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'postgres_fdw_get_locks'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION postgres_connections(
    OUT cluster_name text,
    OUT local_pid integer,
    OUT remote_backend_pid integer)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'postgres_connections'
LANGUAGE C STRICT VOLATILE;

-- Function to query FDW connections from a remote server
CREATE FUNCTION postgres_fdw_connections(
    server_oid oid,
    OUT cluster_name text,
    OUT local_pid integer,
    OUT remote_backend_pid integer)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'postgres_fdw_connections'
LANGUAGE C STRICT VOLATILE;
