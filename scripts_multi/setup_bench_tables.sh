#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage:
  $0 --host HOST --port PORT --db DBNAME --user USER --nodes node2,node3,... [options]

Required:
  --host HOST
  --port PORT
  --db DBNAME
  --user USER
  --nodes LIST             Comma-separated shard-group member node names

Options:
  --partitions N           Number of partitions (default: 128)
  --scale N                Total number of rows in each table (default: 100000)
  --drop                   Drop old objects first
  --distributed-name T     Distributed table name (default: dist_accounts)
  --local-name T           Local partitioned table name (default: part_accounts)
  --shardgroup NAME        Shard group name (default: bench_group)
  --range-step N           Width of each range partition (default: auto from scale/partitions)

Schema:
  Both tables are pgbench_accounts-like:
    aid bigint not null
    bid integer not null
    abalance integer not null
    filler char(84) not null
EOF
}

HOST=""
PORT=""
DB=""
USER_NAME=""
NODES_CSV=""
PARTITIONS=128
SCALE=100000
DROP_FIRST=0
DIST_NAME="dist_accounts"
LOCAL_NAME="part_accounts"
SHARDGROUP="bench_group"
RANGE_STEP=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --db) DB="$2"; shift 2 ;;
    --user) USER_NAME="$2"; shift 2 ;;
    --nodes) NODES_CSV="$2"; shift 2 ;;
    --partitions) PARTITIONS="$2"; shift 2 ;;
    --scale) SCALE="$2"; shift 2 ;;
    --drop) DROP_FIRST=1; shift ;;
    --distributed-name) DIST_NAME="$2"; shift 2 ;;
    --local-name) LOCAL_NAME="$2"; shift 2 ;;
    --shardgroup) SHARDGROUP="$2"; shift 2 ;;
    --range-step) RANGE_STEP="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage >&2; exit 1 ;;
  esac
done

[[ -n "$HOST" ]] || { echo "Missing --host" >&2; exit 1; }
[[ -n "$PORT" ]] || { echo "Missing --port" >&2; exit 1; }
[[ -n "$DB" ]] || { echo "Missing --db" >&2; exit 1; }
[[ -n "$USER_NAME" ]] || { echo "Missing --user" >&2; exit 1; }

command -v psql >/dev/null || { echo "psql not found" >&2; exit 1; }

psql_cmd=(psql -X -v ON_ERROR_STOP=1 -h "$HOST" -p "$PORT" -U "$USER_NAME" -d "$DB")

log() {
  echo "[$(date +'%F %T')] $*"
}

IFS=',' read -r -a SHARD_NODES <<< "$NODES_CSV"

if [[ -z "$RANGE_STEP" ]]; then
  RANGE_STEP=$(( (SCALE + PARTITIONS - 1) / PARTITIONS ))
fi

if [[ "$RANGE_STEP" -le 0 ]]; then
  echo "--range-step must be > 0" >&2
  exit 1
fi

log "Parameters:"
echo "  host=$HOST"
echo "  port=$PORT"
echo "  db=$DB"
echo "  user=$USER_NAME"
echo "  shardgroup=$SHARDGROUP"
echo "  distributed table=$DIST_NAME"
echo "  local table=$LOCAL_NAME"
echo "  partitions=$PARTITIONS"
echo "  scale=$SCALE"
echo "  range_step=$RANGE_STEP"
echo "  shard members=${SHARD_NODES[*]}"
echo

if [[ "$DROP_FIRST" -eq 1 ]]; then
  log "Dropping old objects"
  "${psql_cmd[@]}" <<SQL
DROP TABLE IF EXISTS ${DIST_NAME} CASCADE;
DROP TABLE IF EXISTS ${LOCAL_NAME} CASCADE;
DROP SHARD GROUP IF EXISTS ${SHARDGROUP} CASCADE;
SQL
fi

log "Creating shard group ${SHARDGROUP}"
"${psql_cmd[@]}" <<SQL
CREATE SHARD GROUP ${SHARDGROUP};
SQL

for node in "${SHARD_NODES[@]}"; do
  log "Adding shard-group member: ${node}"
  "${psql_cmd[@]}" <<SQL
ALTER SHARD GROUP ${SHARDGROUP} ADD MEMBER ${node};
SQL
done

log "Current shard groups"
"${psql_cmd[@]}" <<SQL
SELECT * FROM pg_shardgroups;
SQL

log "Creating local partitioned table ${LOCAL_NAME}"
"${psql_cmd[@]}" <<SQL
CREATE TABLE ${LOCAL_NAME} (
  aid      bigint   NOT NULL,
  bid      integer  NOT NULL,
  abalance integer  NOT NULL,
  filler   char(84) NOT NULL,
  PRIMARY KEY (aid)
) PARTITION BY RANGE (aid);
SQL

log "Creating ${PARTITIONS} local partitions"
for ((i=0; i<PARTITIONS; i++)); do
  from=$(( i * RANGE_STEP + 1 ))
  to=$(( (i + 1) * RANGE_STEP + 1 ))

  "${psql_cmd[@]}" <<SQL
CREATE TABLE ${LOCAL_NAME}_p${i}
PARTITION OF ${LOCAL_NAME}
FOR VALUES FROM (${from}) TO (${to});
SQL
done

log "Creating distributed partitioned table ${DIST_NAME}"
"${psql_cmd[@]}" <<SQL
CREATE TABLE ${DIST_NAME} (
  aid      bigint   NOT NULL,
  bid      integer  NOT NULL,
  abalance integer  NOT NULL,
  filler   char(84) NOT NULL,
  PRIMARY KEY (aid)
) DISTRIBUTED BY RANGE(aid) SHARD GROUP ${SHARDGROUP};
SQL

log "Creating ${PARTITIONS} distributed partitions"
for ((i=0; i<PARTITIONS; i++)); do
  from=$(( i * RANGE_STEP + 1 ))
  to=$(( (i + 1) * RANGE_STEP + 1 ))

  "${psql_cmd[@]}" <<SQL
CREATE TABLE ${DIST_NAME}_p${i}
PARTITION OF ${DIST_NAME}
FOR VALUES FROM (${from}) TO (${to});
SQL
done

log "Loading ${SCALE} rows into ${LOCAL_NAME}"
"${psql_cmd[@]}" <<SQL
INSERT INTO ${LOCAL_NAME}(aid, bid, abalance, filler)
SELECT
  g AS aid,
  ((g - 1) / 100000)::integer + 1 AS bid,
  0 AS abalance,
  repeat('x', 84)::char(84) AS filler
FROM generate_series(1, ${SCALE}) AS g;
SQL

log "Loading ${SCALE} rows into ${DIST_NAME}"
"${psql_cmd[@]}" <<SQL
INSERT INTO ${DIST_NAME}(aid, bid, abalance, filler)
SELECT
  g AS aid,
  ((g - 1) / 100000)::integer + 1 AS bid,
  0 AS abalance,
  repeat('x', 84)::char(84) AS filler
FROM generate_series(1, ${SCALE}) AS g;
SQL

log "Resharding"
"${psql_cmd[@]}" <<SQL
ALTER SHARD GROUP group1 RESHARD;
SQL

log "Analyzing tables"
"${psql_cmd[@]}" <<SQL
ANALYZE ${LOCAL_NAME};
ANALYZE ${DIST_NAME};
SQL

log "Verifying counts"
"${psql_cmd[@]}" <<SQL
SELECT '${LOCAL_NAME}' AS table_name, count(*) FROM ${LOCAL_NAME}
UNION ALL
SELECT '${DIST_NAME}' AS table_name, count(*) FROM ${DIST_NAME};
SQL

cat <<EOF

Done.

Created shard group: ${SHARDGROUP}
Members: ${SHARD_NODES[*]}

Created local partitioned table:
  ${LOCAL_NAME}
  partitions: ${PARTITIONS}

Created distributed partitioned table:
  ${DIST_NAME}
  partitions: ${PARTITIONS}
  distributed by range(aid)
  shard group: ${SHARDGROUP}

Rows inserted into each table: ${SCALE}
Range step: ${RANGE_STEP}
EOF
