#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage:
  $0 --host HOST --port PORT --db DBNAME --user USER [options]

Options:
  --clients N
  --jobs N
  --time SEC
  --transactions N
  --scale N
  --local-factor F        [0..1]
  --mode MODE             read | write | mixed   (default: write)
  --read-ratio F          [0..1] used in mixed mode (default: 0.5)
  --local-name T
  --distributed-name T
  --report-latencies
  --output FILE           Save pgbench output to file
  --append-output         Append to output file instead of overwriting
  --meta-output FILE      Save run metadata/header to a separate file

Examples:
  # write-only
  --mode write

  # read-only
  --mode read

  # 70% reads, 30% writes
  --mode mixed --read-ratio 0.7

  # save output
  --output results.txt

  # append output
  --output results.txt --append-output
EOF
}

HOST=""
PORT=""
DB=""
USER_NAME=""
CLIENTS=16
JOBS=16
TIME_SEC=""
TXNS=""
SCALE=100000
LOCAL_FACTOR=0.5
LOCAL_NAME="part_accounts"
DIST_NAME="dist_accounts"
REPORT_LATENCIES=0

MODE="write"
READ_RATIO=0.5

OUTPUT_FILE=""
APPEND_OUTPUT=0
META_OUTPUT_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --db) DB="$2"; shift 2 ;;
    --user) USER_NAME="$2"; shift 2 ;;
    --clients) CLIENTS="$2"; shift 2 ;;
    --jobs) JOBS="$2"; shift 2 ;;
    --time) TIME_SEC="$2"; shift 2 ;;
    --transactions) TXNS="$2"; shift 2 ;;
    --scale) SCALE="$2"; shift 2 ;;
    --local-factor) LOCAL_FACTOR="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --read-ratio) READ_RATIO="$2"; shift 2 ;;
    --local-name) LOCAL_NAME="$2"; shift 2 ;;
    --distributed-name) DIST_NAME="$2"; shift 2 ;;
    --report-latencies) REPORT_LATENCIES=1; shift ;;
    --output) OUTPUT_FILE="$2"; shift 2 ;;
    --append-output) APPEND_OUTPUT=1; shift ;;
    --meta-output) META_OUTPUT_FILE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage >&2; exit 1 ;;
  esac
done

[[ -n "$HOST" ]] || { echo "Missing --host" >&2; exit 1; }
[[ -n "$PORT" ]] || { echo "Missing --port" >&2; exit 1; }
[[ -n "$DB" ]] || { echo "Missing --db" >&2; exit 1; }
[[ -n "$USER_NAME" ]] || { echo "Missing --user" >&2; exit 1; }

if [[ -z "$TIME_SEC" && -z "$TXNS" ]]; then
  echo "Specify either --time or --transactions" >&2
  exit 1
fi

command -v pgbench >/dev/null || { echo "pgbench not found" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 not found" >&2; exit 1; }

python3 - <<PY
lf = float("${LOCAL_FACTOR}")
assert 0.0 <= lf <= 1.0, "local-factor must be in [0,1]"

rr = float("${READ_RATIO}")
assert 0.0 <= rr <= 1.0, "read-ratio must be in [0,1]"

mode = "${MODE}"
assert mode in ("read", "write", "mixed"), "mode must be read/write/mixed"
PY

TMPDIR_RUN="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_RUN"' EXIT

SCRIPT_FILE="$TMPDIR_RUN/workload.sql"

LOCAL_THRESHOLD="$(python3 - <<PY
print(int(float("${LOCAL_FACTOR}") * 1000000))
PY
)"

READ_THRESHOLD="$(python3 - <<PY
print(int(float("${READ_RATIO}") * 1000000))
PY
)"

cat > "$SCRIPT_FILE" <<EOF
\set aid random(1, ${SCALE})
\set delta random(-5000, 5000)
\set route random(1, 1000000)
\set rw random(1, 1000000)

BEGIN;
EOF

cat >> "$SCRIPT_FILE" <<EOF
\\if :route <= ${LOCAL_THRESHOLD}
  \\set target_local 1
\\else
  \\set target_local 0
\\endif
EOF

if [[ "$MODE" == "write" ]]; then

cat >> "$SCRIPT_FILE" <<EOF
\\if :target_local = 1
  UPDATE ${LOCAL_NAME}
     SET abalance = abalance + :delta
   WHERE aid = :aid;
\\else
  UPDATE ${DIST_NAME}
     SET abalance = abalance + :delta
   WHERE aid = :aid;
\\endif
EOF

elif [[ "$MODE" == "read" ]]; then

cat >> "$SCRIPT_FILE" <<EOF
\\if :target_local = 1
  SELECT abalance FROM ${LOCAL_NAME}
   WHERE aid = :aid;
\\else
  SELECT abalance FROM ${DIST_NAME}
   WHERE aid = :aid;
\\endif
EOF

elif [[ "$MODE" == "mixed" ]]; then

cat >> "$SCRIPT_FILE" <<EOF
\\if :rw <= ${READ_THRESHOLD}
  \\if :target_local = 1
    SELECT abalance FROM ${LOCAL_NAME}
     WHERE aid = :aid;
  \\else
    SELECT abalance FROM ${DIST_NAME}
     WHERE aid = :aid;
  \\endif
\\else
  \\if :target_local = 1
    UPDATE ${LOCAL_NAME}
       SET abalance = abalance + :delta
     WHERE aid = :aid;
  \\else
    UPDATE ${DIST_NAME}
       SET abalance = abalance + :delta
     WHERE aid = :aid;
  \\endif
\\endif
EOF
fi

cat >> "$SCRIPT_FILE" <<EOF
COMMIT;
EOF

make_run_header() {
  cat <<EOF
============================================================
timestamp=$(date +'%F %T')
host=$HOST
port=$PORT
db=$DB
user=$USER_NAME
clients=$CLIENTS
jobs=$JOBS
time=${TIME_SEC:-}
transactions=${TXNS:-}
scale=$SCALE
mode=$MODE
local_factor=$LOCAL_FACTOR
read_ratio=$READ_RATIO
local_table=$LOCAL_NAME
distributed_table=$DIST_NAME
report_latencies=$REPORT_LATENCIES
script_file=$SCRIPT_FILE
============================================================

=== Generated workload ===
$(cat "$SCRIPT_FILE")

=== Command ===
$(printf '%q ' "${cmd[@]}")
EOF
}

cmd=(
  pgbench
  -h "$HOST"
  -p "$PORT"
  -U "$USER_NAME"
  -d "$DB"
  -c "$CLIENTS"
  -j "$JOBS"
  --no-vacuum
  -P 10
  -f "$SCRIPT_FILE"
)

[[ -n "$TIME_SEC" ]] && cmd+=(-T "$TIME_SEC")
[[ -n "$TXNS" ]] && cmd+=(-t "$TXNS")
[[ "$REPORT_LATENCIES" -eq 1 ]] && cmd+=(-r)

echo "=== Generated workload ==="
cat "$SCRIPT_FILE"
echo

echo "Running:"
printf ' %q' "${cmd[@]}"
echo
echo "mode=$MODE local_factor=$LOCAL_FACTOR read_ratio=$READ_RATIO"
echo

if [[ -n "$META_OUTPUT_FILE" ]]; then
  mkdir -p "$(dirname "$META_OUTPUT_FILE")"
  make_run_header > "$META_OUTPUT_FILE"
fi

if [[ -n "$OUTPUT_FILE" ]]; then
  mkdir -p "$(dirname "$OUTPUT_FILE")"

  if [[ "$APPEND_OUTPUT" -eq 1 ]]; then
    {
      make_run_header
      echo
    } >> "$OUTPUT_FILE"
    "${cmd[@]}" 2>&1 | tee -a "$OUTPUT_FILE"
  else
    {
      make_run_header
      echo
    } > "$OUTPUT_FILE"
    "${cmd[@]}" 2>&1 | tee -a "$OUTPUT_FILE"
  fi
else
  "${cmd[@]}"
fi