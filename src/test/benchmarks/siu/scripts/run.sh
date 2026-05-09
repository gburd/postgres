#!/usr/bin/env bash
# A/B pgbench harness for SIU: master (upstream) vs tepid.
#
# Env vars:
#   SCALE       -- pgbench -s (also multiplier for siu_table row count = SCALE*100k)
#   CLIENTS     -- pgbench -c
#   THREADS     -- pgbench -j
#   DURATION    -- pgbench -T (seconds per workload)
#   WIDE_COLS   -- # of indexed columns in the wide_table (default 16)
#   WIDE_STEPS  -- comma-separated list of "updated columns" counts for
#                 the wide workload (default "0,1,4,8,WIDE_COLS")
#   PORT        -- postgres port (default 57480)
#
# For each variant in {master, tepid}:
#   initdb fresh pgdata, start postgres, create test objects,
#   run workloads (pgbench -N simple_update, siu_update, siu_mixed,
#   and wide_N for each value in WIDE_STEPS), collect TPS + HOT counts
#   + WAL delta + peak CPU/RSS sampled via pidstat.
# Emits CSV + Markdown summary under /scratch/siu-bench/results/.
set -euo pipefail

BENCH=${BENCH:-/scratch/siu-bench}
SCALE=${SCALE:-20}
CLIENTS=${CLIENTS:-16}
THREADS=${THREADS:-8}
DURATION=${DURATION:-120}
WIDE_COLS=${WIDE_COLS:-16}
WIDE_STEPS=${WIDE_STEPS:-0,1,4,8,16}
PORT=${PORT:-57480}

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUT=$BENCH/results/$TS.csv
LOGDIR=$BENCH/logs/$TS
mkdir -p "$LOGDIR"
echo "variant,workload,tps,latency_avg_ms,hot_updates,total_updates,wal_bytes,bloat_pages_before,bloat_pages_after,index_size_before,index_size_after,cpu_pct_peak,rss_mib_peak" > "$OUT"
echo "=== siu-bench A/B run $TS -> $OUT (scale=$SCALE clients=$CLIENTS threads=$THREADS duration=${DURATION}s)"

bin_of() {
  echo "$BENCH/$1/usr/local/pgsql/bin"
}

LD_of() {
  local base=$BENCH/$1/usr/local/pgsql
  # Linux distros that split 64-bit libs use lib64; most others use lib.
  if [ -d "$base/lib64" ]; then
    echo "$base/lib64"
  else
    echo "$base/lib"
  fi
}

psql_as() {
  local v=$1; shift
  LD_LIBRARY_PATH="$(LD_of "$v")" "$(bin_of "$v")/psql" -h /tmp -p "$PORT" -U postgres -X "$@"
}

pgbench_as() {
  local v=$1; shift
  LD_LIBRARY_PATH="$(LD_of "$v")" "$(bin_of "$v")/pgbench" -h /tmp -p "$PORT" -U postgres "$@"
}

start_pg() {
  local v=$1
  local datadir=$BENCH/_data_$v
  [ -d "$datadir" ] && find "$datadir" -mindepth 1 -delete && rmdir "$datadir"
  mkdir -p "$datadir"

  LD_LIBRARY_PATH="$(LD_of "$v")" "$(bin_of "$v")/initdb" -D "$datadir" -U postgres >"$LOGDIR/initdb_$v.log" 2>&1
  local sb=${SHARED_BUFFERS:-512MB}
  cat >> "$datadir/postgresql.conf" <<EOF
shared_buffers = $sb
work_mem = 32MB
max_wal_size = 4GB
synchronous_commit = on
checkpoint_timeout = 10min
wal_level = replica
log_destination = 'stderr'
logging_collector = off
port = $PORT
EOF
  LD_LIBRARY_PATH="$(LD_of "$v")" "$(bin_of "$v")/pg_ctl" -D "$datadir" \
    -o "-p $PORT" -l "$LOGDIR/pg_$v.log" start >/dev/null
  sleep 2
}

stop_pg() {
  local v=$1
  local datadir=$BENCH/_data_$v
  LD_LIBRARY_PATH="$(LD_of "$v")" "$(bin_of "$v")/pg_ctl" -D "$datadir" stop -m fast >/dev/null 2>&1 || true
}

postmaster_pid() {
  local v=$1
  head -1 "$BENCH/_data_$v/postmaster.pid" 2>/dev/null
}

setup_schemas() {
  local v=$1
  local rows=$((SCALE * 100000))
  # siu_table: the classic 4-col shape used in earlier runs.
  psql_as "$v" <<SQL
DROP TABLE IF EXISTS siu_table;
CREATE TABLE siu_table(a int PRIMARY KEY, b int, c int, d int, e text);
CREATE INDEX siu_b ON siu_table(b);
CREATE INDEX siu_c ON siu_table(c);
CREATE INDEX siu_d ON siu_table(d);
INSERT INTO siu_table
  SELECT i, i, i, i, repeat('x', 20) FROM generate_series(1, $rows) AS i;
VACUUM (ANALYZE) siu_table;
SQL
  # wide_table: id + WIDE_COLS integer indexed columns.
  local coldefs="" insertcols="" insertvals="" idxlist=""
  for i in $(seq 1 "$WIDE_COLS"); do
    coldefs+=", c$i int"
    insertcols+=", c$i"
    insertvals+=", i"
    idxlist+="CREATE INDEX wide_c$i ON wide_table(c$i); "
  done
  local wide_rows=$((SCALE * 1000))
  psql_as "$v" <<SQL
DROP TABLE IF EXISTS wide_table;
CREATE TABLE wide_table(id int PRIMARY KEY $coldefs);
$idxlist
INSERT INTO wide_table(id $insertcols) SELECT i $insertvals FROM generate_series(1, $wide_rows) AS i;
VACUUM (ANALYZE) wide_table;
SQL
  # pgbench schema for built-in simple_update.
  LD_LIBRARY_PATH="$(LD_of "$v")" "$(bin_of "$v")/pgbench" -h /tmp -p "$PORT" -U postgres \
    -i -s "$SCALE" -q postgres >"$LOGDIR/pgbench_init_$v.log" 2>&1
}

bloat_stats() {
  local v=$1 table=$2
  psql_as "$v" -Atc "SELECT pg_table_size('$table')/8192 || ',' || pg_indexes_size('$table')"
}

sample_peak() {
  # Sample CPU / RSS of the postmaster tree for $DURATION+5 seconds.
  # Writes "peak_cpu_pct,peak_rss_mib" to the given outfile.  Portable across
  # Linux / FreeBSD (falls back to pgrep + per-pid ps where --ppid isn't
  # available).  Returns 'NA,NA' if the sampler can't collect useful data.
  local outfile=$1 v=$2
  local leader
  leader=$(postmaster_pid "$v")
  [ -z "$leader" ] && { echo "NA,NA" > "$outfile"; return; }
  local dur=$(( DURATION + 5 ))
  (
    local max_cpu=0
    local max_rss=0
    local t0=$(date +%s)
    while :; do
      # Children of the leader + the leader itself.
      local pids
      pids=$( (pgrep -P "$leader" 2>/dev/null; echo "$leader") | tr '\n' ' ')
      local sample
      sample=$(ps -o pcpu=,rss= -p $pids 2>/dev/null | \
               awk '{cpu+=$1; rss+=$2} END{printf "%.1f %d\n", cpu+0, rss+0}')
      local c r
      read -r c r <<<"$sample"
      if [ -n "${c:-}" ] && [ -n "${r:-}" ]; then
        awk -v m="$max_cpu" -v c="$c" 'BEGIN{exit !(c>m)}' && max_cpu=$c
        [ "$r" -gt "$max_rss" ] 2>/dev/null && max_rss=$r
      fi
      local now=$(date +%s)
      [ $((now - t0)) -ge "$dur" ] && break
      sleep 1
    done
    local rss_mib=$(( max_rss / 1024 ))
    echo "$max_cpu,$rss_mib" > "$outfile"
  ) &
  echo $!
}

run_one() {
  local v=$1 workload=$2 script=$3 table=${4:-siu_table} extra_set=${5:-}

  local wal_start wal_end hot_start hot_end total_start total_end tps lat
  local bloat_before bloat_after idx_before idx_after
  read -r bloat_before idx_before <<<"$(bloat_stats "$v" "$table" | tr , ' ')"

  wal_start=$(psql_as "$v" -Atc "SELECT pg_current_wal_lsn()::text")
  hot_start=$(psql_as "$v" -Atc "SELECT coalesce(n_tup_hot_upd,0) FROM pg_stat_user_tables WHERE relname='$table'")
  total_start=$(psql_as "$v" -Atc "SELECT coalesce(n_tup_upd,0) FROM pg_stat_user_tables WHERE relname='$table'")

  local out="$LOGDIR/${v}_${workload}.log"
  local cpu_rss_file=$LOGDIR/${v}_${workload}.cpu
  local sampler_pid
  sampler_pid=$(sample_peak "$cpu_rss_file" "$v")

  set +e
  case "$workload" in
    simple_update)
      pgbench_as "$v" -N -c "$CLIENTS" -j "$THREADS" -T "$DURATION" \
        -n postgres >"$out" 2>&1
      ;;
    wide_*)
      # build the SET clause from extra_set which is "c1=:v,c2=:v,..."
      pgbench_as "$v" -f <(sed "s/:wide_set_clause/$extra_set/" "$script") \
        -c "$CLIENTS" -j "$THREADS" -T "$DURATION" \
        -D "scale=$SCALE" -n postgres >"$out" 2>&1
      ;;
    *)
      pgbench_as "$v" -f "$script" -c "$CLIENTS" -j "$THREADS" -T "$DURATION" \
        -n postgres >"$out" 2>&1
      ;;
  esac
  set -e

  wait "$sampler_pid" 2>/dev/null || true
  local cpu_rss
  cpu_rss=$(cat "$cpu_rss_file" 2>/dev/null || echo "NA,NA")

  tps=$(awk '/tps = /{print $3; exit}' "$out")
  lat=$(awk '/latency average = /{print $4; exit}' "$out")
  tps=${tps:-NA}
  lat=${lat:-NA}

  wal_end=$(psql_as "$v" -Atc "SELECT pg_current_wal_lsn()::text")
  hot_end=$(psql_as "$v" -Atc "SELECT coalesce(n_tup_hot_upd,0) FROM pg_stat_user_tables WHERE relname='$table'")
  total_end=$(psql_as "$v" -Atc "SELECT coalesce(n_tup_upd,0) FROM pg_stat_user_tables WHERE relname='$table'")

  local wal_bytes
  wal_bytes=$(psql_as "$v" -Atc "SELECT pg_wal_lsn_diff('$wal_end'::pg_lsn, '$wal_start'::pg_lsn)::bigint")

  read -r bloat_after idx_after <<<"$(bloat_stats "$v" "$table" | tr , ' ')"

  local hot=$((hot_end - hot_start))
  local tot=$((total_end - total_start))

  printf '%s,%s,%s,%s,%d,%d,%s,%s,%s,%s,%s,%s\n' \
    "$v" "$workload" "$tps" "$lat" "$hot" "$tot" \
    "$wal_bytes" \
    "$bloat_before" "$bloat_after" \
    "$idx_before" "$idx_after" \
    "$cpu_rss" >> "$OUT"
  printf '  %-8s %-14s tps=%10s lat=%6s hot=%8d/%-8d wal=%12s bloat=%s->%s idx=%s->%s cpu_rss=%s\n' \
    "$v" "$workload" "$tps" "$lat" "$hot" "$tot" "$wal_bytes" \
    "$bloat_before" "$bloat_after" "$idx_before" "$idx_after" "$cpu_rss"
}

build_wide_set_clause() {
  # emit e.g. "c1=:v,c2=:v,...,cN=:v" for first N cols.
  local n=$1
  if [ "$n" -eq 0 ]; then
    # No indexed-col update; touch a non-indexed column (id % 1 so it's a no-op)
    echo "id=id"
    return
  fi
  local clauses=""
  for i in $(seq 1 "$n"); do
    [ -n "$clauses" ] && clauses+=","
    clauses+="c$i=:v"
  done
  echo "$clauses"
}

for v in master tepid; do
  echo "--- variant: $v"
  stop_pg "$v" || true
  start_pg "$v"
  setup_schemas "$v"

  run_one "$v" simple_update ''                    pgbench_accounts
  run_one "$v" siu_update    "$BENCH/scripts/siu_update.sql"  siu_table
  run_one "$v" siu_mixed     "$BENCH/scripts/siu_mixed.sql"   siu_table

  for n in ${WIDE_STEPS//,/ }; do
    run_one "$v" "wide_${n}" "$BENCH/scripts/wide_update.sql" wide_table \
            "$(build_wide_set_clause "$n")"
  done

  stop_pg "$v"
done

echo "=== results: $OUT"
column -t -s, "$OUT" | head -50
