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

mkdir -p "$WORKDIR"

log() { echo "[$(date +'%H:%M:%S')] $*"; }

init_node() {
  local dir="$1"
  local port="$2"
  local name="$3"

  rm -rf "$dir"
  mkdir -p "$dir"

  log "initdb $name ($port)"
  initdb -D "$dir" --no-locale -E UTF8 >/dev/null

  cat >> "$dir/postgresql.conf" <<EOF
port = $port
listen_addresses = '$HOST'
cluster_name = '$name'
max_prepared_transactions = 10

# test-friendly defaults
fsync = off
synchronous_commit = off
full_page_writes = off
logging_collector = on
log_statement = 'all'
foreign_conn_multiplexer.enabled = on
foreign_conn_multiplexer.workers = 2
log_directory = 'log'
shared_preload_libraries = 'postgres_fdw'
log_filename = '${name}.log'
EOF

  cat > "$dir/pg_hba.conf" <<EOF
local   all   all                   trust
host    all   all   127.0.0.1/32     trust
host    all   all   ::1/128          trust
EOF

  log "starting $name"
  pg_ctl -D "$dir" -l "$dir/pgctl.log" -w start >/dev/null
}

stop_node() {
  local dir="$1"
  [[ -d "$dir" ]] && pg_ctl -D "$dir" -w stop -m fast >/dev/null || true
}

psql_node() {
  local port="$1"
  local sql="$2"
  psql -X -q -v ON_ERROR_STOP=1 -h "$HOST" -p "$port" -d postgres -c "$sql"
}

cleanup() {
  log "stopping nodes"
  stop_node "$NODE1_DIR"
  stop_node "$NODE2_DIR"
  stop_node "$NODE3_DIR"
}

cleanup

# ---- init & start nodes ----
init_node "$NODE1_DIR" "$PORT1" "node1"
init_node "$NODE2_DIR" "$PORT2" "node2"
init_node "$NODE3_DIR" "$PORT3" "node3"

# ---- install postgres_fdw ----
log "installing postgres_fdw"
psql_node "$PORT1" "CREATE EXTENSION IF NOT EXISTS postgres_fdw;"
psql_node "$PORT2" "CREATE EXTENSION IF NOT EXISTS postgres_fdw;"
psql_node "$PORT3" "CREATE EXTENSION IF NOT EXISTS postgres_fdw;"

# ---- create FDW connections ----
log "creating FDW servers and user mappings"

# node1 -> node2
psql_node "$PORT1" "
CREATE SERVER IF NOT EXISTS node2
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '$HOST', dbname 'postgres', port '$PORT2');
  
CREATE SERVER IF NOT EXISTS node3
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '$HOST', dbname 'postgres', port '$PORT3');

CREATE USER MAPPING IF NOT EXISTS
  FOR CURRENT_USER SERVER node2;

CREATE USER MAPPING IF NOT EXISTS
  FOR CURRENT_USER SERVER node3;
"

# node2 -> node1
psql_node "$PORT2" "
CREATE SERVER IF NOT EXISTS node1
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '$HOST', dbname 'postgres', port '$PORT1');
  
CREATE SERVER IF NOT EXISTS node3
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '$HOST', dbname 'postgres', port '$PORT3');

CREATE USER MAPPING IF NOT EXISTS
  FOR CURRENT_USER SERVER node1;
  
CREATE USER MAPPING IF NOT EXISTS
  FOR CURRENT_USER SERVER node3;
"

# node3 -> node1
psql_node "$PORT3" "
CREATE SERVER IF NOT EXISTS node1
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '$HOST', dbname 'postgres', port '$PORT1');

CREATE SERVER IF NOT EXISTS node2
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '$HOST', dbname 'postgres', port '$PORT2');

CREATE USER MAPPING IF NOT EXISTS
  FOR CURRENT_USER SERVER node1;
  
CREATE USER MAPPING IF NOT EXISTS
  FOR CURRENT_USER SERVER node2;
"

# ---- sanity check ----
log "sanity check"
psql_node "$PORT1" "SELECT srvname, srvoptions FROM pg_foreign_server;"
psql_node "$PORT2" "SELECT srvname, srvoptions FROM pg_foreign_server;"
psql_node "$PORT3" "SELECT srvname, srvoptions FROM pg_foreign_server;"

cat <<EOF

Done.

node1: port 5432, PGDATA=$NODE1_DIR
node2: port 5433, PGDATA=$NODE2_DIR
node3: port 5434, PGDATA=$NODE3_DIR

postgres_fdw installed
cross-node servers + user mappings created
NO shardgroups
NO tables

Connect:
  psql -p 5432 -d postgres
  psql -p 5433 -d postgres
  psql -p 5434 -d postgres
EOF
