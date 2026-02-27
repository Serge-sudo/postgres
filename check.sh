#!/usr/bin/env bash
set -euo pipefail

# Two-node PostgreSQL setup with postgres_fdw
# Ports are FIXED: 5432 (node1) and 5433 (node2) and 5434 (node3)
# No shardgroups, no tables.

ROOT="${ROOT:-$(pwd)}"
WORKDIR="${WORKDIR:-$ROOT/tmp_fdw_2nodes}"
NODE1_DIR="$WORKDIR/node1"
NODE2_DIR="$WORKDIR/node2"
NODE3_DIR="$WORKDIR/node3"
PATH="$PATH:/workspaces/postgres/tmp_install/usr/local/pgsql/bin"

PORT1=5432
PORT2=5433
PORT3=5434
HOST=127.0.0.1

command -v initdb >/dev/null
command -v pg_ctl >/dev/null
command -v psql >/dev/null

psql_node() {
  local port="$1"
  local sql="$2"
  psql -h "$HOST" -p "$port" -d postgres -c "$sql"
}

# node1 -> node2
psql_node "$PORT1" "
CREATE SHARD GROUP group1;
select * from pg_shardgroups;
CREATE TABLE t1 (a INT) DISTRIBUTED BY RANGE(a) SHARD GROUP group1;
"

psql_node "$PORT1" "
ALTER SHARD GROUP group1 ADD MEMBER node2;
"

# psql_node "$PORT2" "
# select * from pg_shardgroups;
# "
