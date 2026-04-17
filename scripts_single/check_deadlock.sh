#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$(pwd)}"
WORKDIR="${WORKDIR:-$ROOT/tmp_fdw_deadlock}"
PATH="$PATH:/workspaces/postgres/tmp_install/usr/local/pgsql/bin"

HOST=127.0.0.1
PORT1=5432
PORT2=5433

command -v psql >/dev/null

psql_node() {
  local port="$1"
  local sql="$2"
  psql -X -v ON_ERROR_STOP=1 -h "$HOST" -p "$port" -d postgres -c "$sql"
}

echo "=== node1: create shard group and distributed table ==="
psql_node "$PORT1" "
CREATE SHARD GROUP group1;
SELECT * FROM pg_shardgroups;
CREATE TABLE t1 (a INT) DISTRIBUTED BY RANGE(a) SHARD GROUP group1;
"

echo "=== node1: add node2 to shard group ==="
psql_node "$PORT1" "
ALTER SHARD GROUP group1 ADD MEMBER node2;
"

echo "=== node1: create partitions and reshard ==="
psql -X -v ON_ERROR_STOP=1 -h "$HOST" -p "$PORT1" -d postgres <<'SQL'
CREATE TABLE t1_1 PARTITION OF t1 FOR VALUES FROM (0) TO (10);
CREATE TABLE t1_2 PARTITION OF t1 FOR VALUES FROM (10) TO (20);
CREATE TABLE t1_3 PARTITION OF t1 FOR VALUES FROM (20) TO (30);
CREATE TABLE t1_4 PARTITION OF t1 FOR VALUES FROM (30) TO (40);
ALTER SHARD GROUP group1 RESHARD;
\dt
SQL

echo "=== node2: list tables ==="
psql -X -v ON_ERROR_STOP=1 -h "$HOST" -p "$PORT2" -d postgres <<'SQL'
\dt
SQL

echo "=== preparing concurrent deadlock test ==="

mkdir -p "$WORKDIR"

cat > "$WORKDIR/node1_deadlock.sql" <<'SQL'
\set ON_ERROR_STOP 1
SET deadlock_timeout = '1s';
SET statement_timeout = '20s';

BEGIN;
SELECT 'node1 pid=' || pg_backend_pid();

LOCK TABLE t1_1 IN ACCESS EXCLUSIVE MODE;
SELECT 'node1 locked t1_1, sleeping before selecting t1_2';
SELECT pg_sleep(2);

SELECT 'node1 selecting from t1_2';
SELECT * FROM t1_2;

COMMIT;

SELECT 'node1 done';
SQL

cat > "$WORKDIR/node2_deadlock.sql" <<'SQL'
\set ON_ERROR_STOP 1
SET deadlock_timeout = '1s';
SET statement_timeout = '20s';

BEGIN;
SELECT 'node2 pid=' || pg_backend_pid();

LOCK TABLE t1_2 IN ACCESS EXCLUSIVE MODE;
SELECT 'node2 locked t1_2, sleeping before selecting t1_1';
SELECT pg_sleep(2);

SELECT 'node2 selecting from t1_1';
SELECT * FROM t1_1;

COMMIT;

SELECT 'node2 done';
SQL

echo "=== running both sessions in parallel ==="

psql -X -h "$HOST" -p "$PORT1" -d postgres -f "$WORKDIR/node1_deadlock.sql" \
  >"$WORKDIR/node1.out" 2>"$WORKDIR/node1.err" &
pid1=$!

psql -X -h "$HOST" -p "$PORT2" -d postgres -f "$WORKDIR/node2_deadlock.sql" \
  >"$WORKDIR/node2.out" 2>"$WORKDIR/node2.err" &
pid2=$!

rc1=0
rc2=0
wait "$pid1" || rc1=$?
wait "$pid2" || rc2=$?

echo
echo "=== node1 stdout ==="
cat "$WORKDIR/node1.out" || true
echo "=== node1 stderr ==="
cat "$WORKDIR/node1.err" || true

echo
echo "=== node2 stdout ==="
cat "$WORKDIR/node2.out" || true
echo "=== node2 stderr ==="
cat "$WORKDIR/node2.err" || true

echo
echo "=== result ==="
if grep -qi "deadlock detected" "$WORKDIR"/node*.err; then
  echo "Deadlock detected successfully."
  exit 0
fi

echo "No deadlock detected."
echo "node1 exit code: $rc1"
echo "node2 exit code: $rc2"
exit 1
