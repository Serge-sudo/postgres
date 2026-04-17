#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage:
  $0 --host HOST --port PORT --db DBNAME --user USER [options]

Required:
  --host HOST
  --port PORT
  --db DBNAME
  --user USER

Options:
  --local-name T         Local partitioned table name (default: part_accounts)
  --distributed-name T   Distributed table name (default: dist_accounts)
  --repeats N            Number of runs per query per table (default: 5)
  --warmup N             Number of warmup runs per query per table (default: 1)
  --output FILE          Save raw results as TSV (optional)
  --query-set NAME       default | light | heavy   (default: default)

Description:
  Runs OLAP-style queries multiple times against both local and distributed
  pgbench-like tables, then reports average execution times in milliseconds.

Notes:
  Timing is measured using EXPLAIN (ANALYZE, FORMAT JSON).
EOF
}

HOST=""
PORT=""
DB=""
USER_NAME=""
LOCAL_NAME="part_accounts"
DIST_NAME="dist_accounts"
REPEATS=5
WARMUP=1
OUTPUT_FILE=""
QUERY_SET="default"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --db) DB="$2"; shift 2 ;;
    --user) USER_NAME="$2"; shift 2 ;;
    --local-name) LOCAL_NAME="$2"; shift 2 ;;
    --distributed-name) DIST_NAME="$2"; shift 2 ;;
    --repeats) REPEATS="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --output) OUTPUT_FILE="$2"; shift 2 ;;
    --query-set) QUERY_SET="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage >&2; exit 1 ;;
  esac
done

[[ -n "$HOST" ]] || { echo "Missing --host" >&2; exit 1; }
[[ -n "$PORT" ]] || { echo "Missing --port" >&2; exit 1; }
[[ -n "$DB" ]] || { echo "Missing --db" >&2; exit 1; }
[[ -n "$USER_NAME" ]] || { echo "Missing --user" >&2; exit 1; }

command -v psql >/dev/null || { echo "psql not found" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 not found" >&2; exit 1; }

psql_cmd=(psql -X -q -A -t -v ON_ERROR_STOP=1 -h "$HOST" -p "$PORT" -U "$USER_NAME" -d "$DB")

log() {
  echo "[$(date +'%F %T')] $*"
}

TMPDIR_RUN="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_RUN"' EXIT

RAW_FILE="${TMPDIR_RUN}/raw.tsv"
if [[ -n "$OUTPUT_FILE" ]]; then
  RAW_FILE="$OUTPUT_FILE"
fi

: > "$RAW_FILE"

# Format: query_id|description|sql_with_%TABLE%_placeholder
build_queries() {
  case "$QUERY_SET" in
    light)
      cat <<'EOF'
q1|count_all|SELECT count(*) FROM %TABLE%
q2|sum_balance|SELECT sum(abalance) FROM %TABLE%
q3|min_max_balance|SELECT min(abalance), max(abalance) FROM %TABLE%
q4|avg_balance|SELECT avg(abalance) FROM %TABLE%
EOF
      ;;
    heavy)
      cat <<'EOF'
q1|count_all|SELECT count(*) FROM %TABLE%
q2|sum_balance|SELECT sum(abalance) FROM %TABLE%
q3|min_max_balance|SELECT min(abalance), max(abalance) FROM %TABLE%
q4|avg_balance|SELECT avg(abalance) FROM %TABLE%
q5|group_by_bid|SELECT bid, count(*), sum(abalance), min(abalance), max(abalance) FROM %TABLE% GROUP BY bid ORDER BY bid
q6|group_by_bucket_100|SELECT (aid / 100000) AS bucket, count(*), sum(abalance), avg(abalance), min(abalance), max(abalance) FROM %TABLE% GROUP BY (aid / 100000) ORDER BY bucket
q7|filtered_agg_10pct|SELECT count(*), sum(abalance), avg(abalance), min(abalance), max(abalance) FROM %TABLE% WHERE aid % 10 = 0
q8|filtered_group|SELECT bid, count(*), sum(abalance) FROM %TABLE% WHERE aid % 4 = 0 GROUP BY bid ORDER BY bid
EOF
      ;;
    default)
      cat <<'EOF'
q1|count_all|SELECT count(*) FROM %TABLE%
q2|sum_balance|SELECT sum(abalance) FROM %TABLE%
q3|min_max_balance|SELECT min(abalance), max(abalance) FROM %TABLE%
q4|avg_balance|SELECT avg(abalance) FROM %TABLE%
q5|group_by_bid|SELECT bid, count(*), sum(abalance), min(abalance), max(abalance) FROM %TABLE% GROUP BY bid ORDER BY bid
q6|filtered_agg_10pct|SELECT count(*), sum(abalance), avg(abalance), min(abalance), max(abalance) FROM %TABLE% WHERE aid % 10 = 0
EOF
      ;;
    *)
      echo "Unknown --query-set: $QUERY_SET" >&2
      exit 1
      ;;
  esac
}

measure_query_ms() {
  local sql="$1"

  "${psql_cmd[@]}" <<SQL | python3 -c '
import sys, json
data = sys.stdin.read().strip()
obj = json.loads(data)
if isinstance(obj, list) and obj:
    print(obj[0]["Execution Time"])
else:
    raise SystemExit("Could not parse EXPLAIN JSON output")
'
EXPLAIN (ANALYZE, TIMING ON, SUMMARY ON, FORMAT JSON)
$sql;
SQL
}

run_one_table() {
  local table_kind="$1"
  local table_name="$2"

  while IFS='|' read -r query_id query_desc query_sql; do
    [[ -n "$query_id" ]] || continue
    sql="${query_sql//%TABLE%/$table_name}"

    log "Warmup: ${table_kind} ${query_id} (${query_desc})"
    for ((w=1; w<=WARMUP; w++)); do
      measure_query_ms "$sql" >/dev/null
    done

    for ((r=1; r<=REPEATS; r++)); do
      log "Run ${r}/${REPEATS}: ${table_kind} ${query_id} (${query_desc})"
      ms="$(measure_query_ms "$sql")"
      printf "%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$table_kind" "$table_name" "$query_id" "$query_desc" "$r" "$ms" >> "$RAW_FILE"
    done
  done < <(build_queries)
}

log "Running OLAP benchmark"
log "local table: $LOCAL_NAME"
log "distributed table: $DIST_NAME"
log "repeats: $REPEATS"
log "warmup: $WARMUP"
log "query set: $QUERY_SET"
echo

run_one_table "local" "$LOCAL_NAME"
run_one_table "distributed" "$DIST_NAME"

echo
log "Raw results saved to: $RAW_FILE"
echo

python3 - "$RAW_FILE" <<'PY'
import sys
from collections import defaultdict

path = sys.argv[1]

rows = []
with open(path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.rstrip("\n")
        if not line:
            continue
        kind, table, qid, qdesc, run_no, ms = line.split("\t")
        rows.append((kind, table, qid, qdesc, int(run_no), float(ms)))

if not rows:
    print("No results found.")
    sys.exit(1)

per_query = defaultdict(list)
per_kind = defaultdict(list)

for kind, table, qid, qdesc, run_no, ms in rows:
    per_query[(qid, qdesc, kind, table)].append(ms)
    per_kind[(kind, table)].append(ms)

print("Average execution time per query (ms)")
print("=" * 72)
print(f"{'query_id':<8} {'kind':<12} {'avg_ms':>12} {'runs':>8}  description")
print("-" * 72)

# stable order by qid then kind
keys = sorted(per_query.keys(), key=lambda x: (x[0], x[2]))
for qid, qdesc, kind, table in keys:
    vals = per_query[(qid, qdesc, kind, table)]
    avg = sum(vals) / len(vals)
    print(f"{qid:<8} {kind:<12} {avg:>12.3f} {len(vals):>8}  {qdesc}")

print()
print("Overall average across all OLAP queries (ms)")
print("=" * 72)
print(f"{'kind':<12} {'avg_ms':>12} {'runs':>8}")
print("-" * 72)

for kind, table in sorted(per_kind.keys()):
    vals = per_kind[(kind, table)]
    avg = sum(vals) / len(vals)
    print(f"{kind:<12} {avg:>12.3f} {len(vals):>8}")

print()
print("Comparison by query (distributed / local)")
print("=" * 72)
print(f"{'query_id':<8} {'local_ms':>12} {'dist_ms':>12} {'ratio':>12}  description")
print("-" * 72)

query_ids = sorted(set(qid for _, _, qid, _, _, _ in rows))
for qid in query_ids:
    local_key = None
    dist_key = None
    desc = None
    for key in per_query.keys():
        k_qid, k_desc, k_kind, k_table = key
        if k_qid == qid:
            desc = k_desc
            if k_kind == "local":
                local_key = key
            elif k_kind == "distributed":
                dist_key = key

    if local_key is None or dist_key is None:
        continue

    local_avg = sum(per_query[local_key]) / len(per_query[local_key])
    dist_avg = sum(per_query[dist_key]) / len(per_query[dist_key])
    ratio = dist_avg / local_avg if local_avg != 0 else float("inf")

    print(f"{qid:<8} {local_avg:>12.3f} {dist_avg:>12.3f} {ratio:>12.3f}  {desc}")
PY