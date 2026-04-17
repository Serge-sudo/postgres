#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage:
  $0 --config cluster.conf --node NODE_NAME [--init] [--start] [--fdw] [--all] [--stop] [--status]

Examples:
  $0 --config cluster.conf --node node1 --all
  $0 --config cluster.conf --node node2 --init --start --fdw
  $0 --config cluster.conf --node node3 --status

Description:
  Initializes and starts the local PostgreSQL node defined by NODE_NAME,
  installs postgres_fdw, and creates foreign servers/user mappings to all
  other nodes listed in the config file using their configured IP addresses.
EOF
}

CONFIG=""
SELF_NODE=""
DO_INIT=0
DO_START=0
DO_FDW=0
DO_STOP=0
DO_STATUS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG="$2"
      shift 2
      ;;
    --node)
      SELF_NODE="$2"
      shift 2
      ;;
    --init)
      DO_INIT=1
      shift
      ;;
    --start)
      DO_START=1
      shift
      ;;
    --fdw)
      DO_FDW=1
      shift
      ;;
    --all)
      DO_INIT=1
      DO_START=1
      DO_FDW=1
      shift
      ;;
    --stop)
      DO_STOP=1
      shift
      ;;
    --status)
      DO_STATUS=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

[[ -n "$CONFIG" ]] || { echo "Missing --config"; usage; exit 1; }
[[ -n "$SELF_NODE" ]] || { echo "Missing --node"; usage; exit 1; }
[[ -f "$CONFIG" ]] || { echo "Config file not found: $CONFIG"; exit 1; }

# shellcheck disable=SC1090
source "$CONFIG"

log() {
  echo "[$(date +'%F %T')] $*"
}

get_var() {
  local name="$1"
  eval "printf '%s' \"\${$name:-}\""
}

require_var() {
  local name="$1"
  local value
  value="$(get_var "$name")"
  [[ -n "$value" ]] || {
    echo "Required config variable is missing: $name" >&2
    exit 1
  }
}

# Validate global config
require_var PG_BIN
require_var DB_NAME
require_var DB_USER
require_var INITDB_ARGS
require_var MAX_PREPARED_TRANSACTIONS
require_var FSYNC
require_var SYNCHRONOUS_COMMIT
require_var FULL_PAGE_WRITES
require_var LOGGING_COLLECTOR
require_var SHARED_PRELOAD_LIBRARIES
require_var MUX_WORKER_COUNT

[[ ${#NODES[@]} -gt 0 ]] || {
  echo "NODES array is empty in config" >&2
  exit 1
}

if ! printf '%s\n' "${NODES[@]}" | grep -qx "$SELF_NODE"; then
  echo "Node '$SELF_NODE' is not listed in NODES=()" >&2
  exit 1
fi

SELF_HOST_VAR="HOST_${SELF_NODE}"
SELF_PORT_VAR="PORT_${SELF_NODE}"
SELF_MUX_PORT_VAR="MUX_PORT_${SELF_NODE}"
SELF_PGDATA_VAR="PGDATA_${SELF_NODE}"
SELF_CLUSTER_VAR="CLUSTER_NAME_${SELF_NODE}"

require_var "$SELF_HOST_VAR"
require_var "$SELF_PORT_VAR"
require_var "$SELF_MUX_PORT_VAR"
require_var "$SELF_PGDATA_VAR"
require_var "$SELF_CLUSTER_VAR"

SELF_HOST="127.0.0.1"
SELF_PORT="$(get_var "$SELF_PORT_VAR")"
SELF_MUX_PORT="$(get_var "$SELF_MUX_PORT_VAR")"
SELF_PGDATA="$(get_var "$SELF_PGDATA_VAR")"
SELF_CLUSTER_NAME="$(get_var "$SELF_CLUSTER_VAR")"

export PATH="$PG_BIN:$PATH"

command -v initdb >/dev/null || { echo "initdb not found in PG_BIN=$PG_BIN"; exit 1; }
command -v pg_ctl >/dev/null || { echo "pg_ctl not found in PG_BIN=$PG_BIN"; exit 1; }
command -v psql >/dev/null || { echo "psql not found in PG_BIN=$PG_BIN"; exit 1; }

psql_local() {
  local sql="$1"
  psql -X -v ON_ERROR_STOP=1 \
    -h "$SELF_HOST" \
    -p "$SELF_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -c "$sql"
}

init_node() {
  log "Initializing node $SELF_NODE in $SELF_PGDATA"

  stop_node || true
  rm -rf "$SELF_PGDATA"
  mkdir -p "$SELF_PGDATA"

  # shellcheck disable=SC2086
  initdb -D "$SELF_PGDATA" $INITDB_ARGS >/dev/null

  cat >> "$SELF_PGDATA/postgresql.conf" <<EOF
port = $SELF_PORT
listen_addresses = '*'
cluster_name = '$SELF_CLUSTER_NAME'
max_prepared_transactions = $MAX_PREPARED_TRANSACTIONS

fsync = $FSYNC
synchronous_commit = $SYNCHRONOUS_COMMIT
full_page_writes = $FULL_PAGE_WRITES

logging_collector = $LOGGING_COLLECTOR
log_directory = 'log'
log_filename = '${SELF_NODE}.log'

shared_preload_libraries = '$SHARED_PRELOAD_LIBRARIES'

mux_worker_count = $MUX_WORKER_COUNT
mux_tcp_port = $SELF_MUX_PORT
EOF

  cat > "$SELF_PGDATA/pg_hba.conf" <<EOF
local   all   all                     trust
host    all   all   127.0.0.1/32      trust
host    all   all   ::1/128           trust
host    all   all   0.0.0.0/0         trust
host    all   all   ::/0              trust
EOF

  log "Node $SELF_NODE initialized"
}

start_node() {
  log "Starting node $SELF_NODE"
  pg_ctl -D "$SELF_PGDATA" -l "$SELF_PGDATA/pgctl.log" -w start >/dev/null
  log "Node $SELF_NODE started on $SELF_HOST:$SELF_PORT"
}

stop_node() {
  log "Stopping node $SELF_NODE"
  pg_ctl -D "$SELF_PGDATA" -w stop -m fast >/dev/null
  log "Node $SELF_NODE stopped"
}

status_node() {
  pg_ctl -D "$SELF_PGDATA" status || true
}

setup_fdw() {
  log "Installing postgres_fdw on $SELF_NODE"
  psql_local "CREATE EXTENSION IF NOT EXISTS postgres_fdw;"
  log "Done install of postgres_fdw"

  for peer in "${NODES[@]}"; do
    if [[ "$peer" == "$SELF_NODE" ]]; then
      continue
    fi

    peer_host_var="HOST_${peer}"
    peer_port_var="PORT_${peer}"
    peer_mux_port_var="MUX_PORT_${peer}"

    require_var "$peer_host_var"
    require_var "$peer_port_var"
    require_var "$peer_mux_port_var"

    peer_host="$(get_var "$peer_host_var")"
    peer_port="$(get_var "$peer_port_var")"
    peer_mux_port="$(get_var "$peer_mux_port_var")"

    log "Creating FDW server $peer on $SELF_NODE -> $peer_host:$peer_port (mux $peer_mux_port)"

    psql_local "
      CREATE SERVER IF NOT EXISTS $peer
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (
          host '$peer_host',
          dbname '$DB_NAME',
          port '$peer_port',
          mux_port '$peer_mux_port'
        );

      CREATE USER MAPPING IF NOT EXISTS
        FOR CURRENT_USER SERVER $peer;
    "
  done

  log "FDW setup complete on $SELF_NODE"
}

show_fdw_servers() {
  log "Configured foreign servers on $SELF_NODE"
  psql_local "SELECT srvname, srvoptions FROM pg_foreign_server ORDER BY srvname;"
}

if [[ $DO_STOP -eq 1 ]]; then
  stop_node
fi

if [[ $DO_INIT -eq 1 ]]; then
  init_node
fi

if [[ $DO_START -eq 1 ]]; then
  start_node
fi

if [[ $DO_FDW -eq 1 ]]; then
  setup_fdw
  show_fdw_servers
fi

if [[ $DO_STATUS -eq 1 ]]; then
  status_node
fi

if [[ $DO_INIT -eq 0 && $DO_START -eq 0 && $DO_FDW -eq 0 && $DO_STOP -eq 0 && $DO_STATUS -eq 0 ]]; then
  usage
  exit 1
fi