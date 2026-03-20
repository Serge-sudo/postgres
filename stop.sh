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
PORT1_MUX=6432
PORT2_MUX=6433
PORT3_MUX=6434
HOST=127.0.0.1

command -v initdb >/dev/null
command -v pg_ctl >/dev/null
command -v psql >/dev/null

mkdir -p "$WORKDIR"

log() { echo "[$(date +'%H:%M:%S')] $*"; }


stop_node() {
  local dir="$1"
  [[ -d "$dir" ]] && pg_ctl -D "$dir" -w stop -m immediate >/dev/null || true
}

cleanup() {
  log "stopping nodes"
  stop_node "$NODE1_DIR"
  stop_node "$NODE2_DIR"
  stop_node "$NODE3_DIR"
}

cleanup
