#!/usr/bin/env bash
# Honest HEAP-vs-RECNO benchmark driver (2-way, single undo-branch binary).
#
# "Honest" durability: fsync=on, full_page_writes=on, synchronous_commit=on.
# The two arms differ ONLY in default_table_access_method (heap vs recno);
# identical binary, identical GUCs, identical hardware. PGDATA lives on local
# NVMe (never tmpfs). Each data point runs for a fixed measurement window.
#
# Workloads:
#   tpcb    pgbench built-in TPC-B-like (--initialize per AM, then -T)
#   ro      pgbench read-only (-S) on the same tpcb dataset
#   copy    bulk COPY of N rows then VACUUM (timed), single-client only
# TPC-C (tprocc) is driven separately by the python harness (see runner note).
set -euo pipefail

# ---- knobs (env-overridable) ------------------------------------------------
BIN="${BIN:-/mnt/scratch/inst-undo/bin}"
PGDATA_BASE="${PGDATA_BASE:-/mnt/pgdata}"
OUT="${OUT:-/mnt/scratch/bench-results/honest-$(date +%Y%m%d_%H%M%S)}"
PORT="${PORT:-55432}"
SOCKDIR="${SOCKDIR:-$PGDATA_BASE}"
DURATION="${DURATION:-120}"          # measurement seconds per data point
WARMUP="${WARMUP:-15}"               # discarded warmup seconds (pgbench -T covers whole run; we drop first WARMUP via two-phase)
ITERATIONS="${ITERATIONS:-3}"        # A/B iterations per data point; median reported
CLIENTS="${CLIENTS:-1 2 4 8 16 32 64 128 192}"
SCALE="${SCALE:-300}"                # pgbench scale (300 => ~4.5GB, exceeds io-regime shared_buffers)
BIGLOAD_SCALE="${BIGLOAD_SCALE:-200}" # single-txn init scale for the large-load test (200 => 20M rows; broke RECNO pre-9.2-fix)
COPY_ROWS="${COPY_ROWS:-10000000}"   # rows for the bulk COPY workload
SHARED_BUFFERS="${SHARED_BUFFERS:-128GB}"
MAX_CONN="${MAX_CONN:-400}"
AMS="${AMS:-heap recno}"
WORKLOADS="${WORKLOADS:-bigload tpcb ro copy}"

PSQL="$BIN/psql -X -h $SOCKDIR -p $PORT -U postgres"
PGBENCH="$BIN/pgbench -h $SOCKDIR -p $PORT -U postgres"

mkdir -p "$OUT"
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT/driver.log"; }

# ---- cluster lifecycle ------------------------------------------------------
CLUSTER="$PGDATA_BASE/honest"

init_cluster() {
  log "initdb $CLUSTER"
  [ -d "$CLUSTER" ] && { "$BIN/pg_ctl" -D "$CLUSTER" stop -m immediate >/dev/null 2>&1 || true; rm -r "$CLUSTER"; }
  "$BIN/initdb" -D "$CLUSTER" -U postgres --no-sync >>"$OUT/driver.log" 2>&1
  cat >>"$CLUSTER/postgresql.conf" <<EOF
# --- honest durability (do NOT relax) ---
fsync = on
full_page_writes = on
synchronous_commit = on
wal_level = replica
# --- sizing for i7ie.metal-48xl (192 vCPU / 1.5TB / local NVMe) ---
shared_buffers = $SHARED_BUFFERS
max_connections = $MAX_CONN
max_wal_size = 64GB
min_wal_size = 4GB
checkpoint_timeout = 30min
checkpoint_completion_target = 0.9
effective_cache_size = 1200GB
work_mem = 128MB
maintenance_work_mem = 8GB
wal_compression = off
# --- LZ4-matched arms: both HEAP and RECNO compress overflow/TOAST with LZ4 ---
default_toast_compression = lz4
autovacuum = on
listen_addresses = ''
unix_socket_directories = '$SOCKDIR'
EOF
}

start_cluster() {
  "$BIN/pg_ctl" -D "$CLUSTER" -o "-p $PORT" -l "$OUT/postgres.log" start >>"$OUT/driver.log" 2>&1
  for _ in $(seq 1 30); do $PSQL -d postgres -Atc "SELECT 1" >/dev/null 2>&1 && return 0; sleep 1; done
  log "FATAL: cluster did not start"; tail -40 "$OUT/postgres.log" | tee -a "$OUT/driver.log"; exit 1
}
stop_cluster() { "$BIN/pg_ctl" -D "$CLUSTER" stop -m fast >>"$OUT/driver.log" 2>&1 || true; }

checkpoint() { $PSQL -d postgres -c "CHECKPOINT" >/dev/null 2>&1; }

# ---- per-AM database (isolates AM so pg_class relam is uniform) -------------
db_for() { echo "bench_$1"; }

create_db_for_am() {
  local am=$1 db; db=$(db_for "$am")
  $PSQL -d postgres -c "DROP DATABASE IF EXISTS $db" >/dev/null 2>&1
  $PSQL -d postgres -c "CREATE DATABASE $db" >/dev/null
  $PSQL -d "$db" -c "ALTER DATABASE $db SET default_table_access_method = $am" >/dev/null
}

# ---- pgbench TPC-B init (respects default_table_access_method of the db) ----
init_pgbench() {
  local am=$1 db; db=$(db_for "$am")
  log "pgbench --initialize am=$am scale=$SCALE db=$db"
  # -I dtgvp: drop, create tables (honors default_table_access_method), gen data, vacuum, primary keys
  $PGBENCH -i -I dtgvp -s "$SCALE" "$db" >>"$OUT/init_${am}.log" 2>&1
  # confirm the AM actually landed
  local got
  got=$($PSQL -d "$db" -Atc "SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE c.relname='pgbench_accounts'")
  [ "$got" = "$am" ] || { log "FATAL: pgbench_accounts AM=$got, expected $am"; exit 1; }
  log "  confirmed pgbench_accounts uses $got"
}

# ---- result capture ---------------------------------------------------------
CSV="$OUT/results.csv"
echo "workload,am,clients,tps,lat_avg_ms,duration_s,extra" > "$CSV"

run_pgbench_point() {
  local workload=$1 am=$2 clients=$3 flags=$4 db; db=$(db_for "$am")
  local jobs=$(( clients < 64 ? clients : 64 ))
  local tps_list="" lat_list=""
  local iter tps lat tps_ex
  for iter in $(seq 1 "$ITERATIONS"); do
    local raw="$OUT/raw_${workload}_${am}_c${clients}_i${iter}.txt"
    checkpoint
    # warmup (discarded); $flags is intentionally unquoted for word-splitting ('' vs '-S')
    # shellcheck disable=SC2086
    $PGBENCH $flags -c "$clients" -j "$jobs" -T "$WARMUP" "$db" >/dev/null 2>&1 || true
    # measurement
    # shellcheck disable=SC2086
    $PGBENCH $flags -c "$clients" -j "$jobs" -T "$DURATION" "$db" > "$raw" 2>&1 || {
      log "  pgbench FAILED workload=$workload am=$am c=$clients iter=$iter (see $raw)"; continue; }
    tps=$(awk '/including connections establishing/ {print $3}' "$raw" | tail -1)
    tps_ex=$(awk '/without initial connection|excluding connections/ {print $3}' "$raw" | tail -1)
    [ -n "$tps_ex" ] && tps="$tps_ex"
    lat=$(awk '/latency average/ {print $4}' "$raw" | tail -1)
    tps_list="$tps_list ${tps:-0}"
    lat_list="$lat_list ${lat:-0}"
    log "    iter=$iter $workload am=$am c=$clients TPS=${tps:-NA} lat=${lat:-NA}ms"
  done
  # median across iterations
  local mtps mlat
  mtps=$(echo $tps_list | tr ' ' '\n' | grep -v '^$' | sort -n | awk '{a[NR]=$1} END{print (NR? a[int((NR+1)/2)] : "NA")}')
  mlat=$(echo $lat_list | tr ' ' '\n' | grep -v '^$' | sort -n | awk '{a[NR]=$1} END{print (NR? a[int((NR+1)/2)] : "NA")}')
  echo "$workload,$am,$clients,${mtps:-NA},${mlat:-NA},$DURATION,iters=$ITERATIONS;tps_all=$(echo $tps_list|tr ' ' '/')" >> "$CSV"
  log "  MEDIAN $workload am=$am c=$clients TPS=${mtps:-NA} lat=${mlat:-NA}ms"
}

workload_tpcb() {
  local am=$1
  for c in $CLIENTS; do run_pgbench_point tpcb "$am" "$c" ""; done
}
workload_ro() {
  local am=$1
  for c in $CLIENTS; do run_pgbench_point ro "$am" "$c" "-S"; done
}

# bigload: time a single-transaction bulk init of BIGLOAD_SCALE (pgbench -i loads
# all pgbench_accounts rows in ONE txn).  This is the exact workload that
# aborted RECNO at commit before the MaxAllocSize (9.2) fix -- 200 => 20M rows.
# Reports load wall-time and confirms the commit succeeds on both AMs.
workload_bigload() {
  local am=$1 db="bigload_$1"
  log "bigload am=$am scale=$BIGLOAD_SCALE (single-txn init)"
  $PSQL -d postgres -c "DROP DATABASE IF EXISTS $db" >/dev/null 2>&1
  $PSQL -d postgres -c "CREATE DATABASE $db" >/dev/null
  $PSQL -d "$db" -c "ALTER DATABASE $db SET default_table_access_method = $am" >/dev/null
  checkpoint
  local t0 t1 load_s rc rows got
  t0=$(date +%s.%N)
  # -I dtg: drop, create, generate (single commit).  No vacuum/pk so the timed
  # window is the pure single-txn data load + commit.
  if $PGBENCH -i -I dtg -s "$BIGLOAD_SCALE" "$db" >>"$OUT/bigload_${am}.log" 2>&1; then
    rc=ok
  else
    rc=FAILED
  fi
  t1=$(date +%s.%N); load_s=$(awk "BEGIN{printf \"%.2f\", $t1-$t0}")
  rows=$($PSQL -d "$db" -Atc "SELECT count(*) FROM pgbench_accounts" 2>/dev/null || echo NA)
  got=$($PSQL -d "$db" -Atc "SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE c.relname='pgbench_accounts'" 2>/dev/null || echo NA)
  echo "bigload,$am,1,NA,NA,$load_s,scale=$BIGLOAD_SCALE;rows=$rows;am=$got;commit=$rc" >> "$CSV"
  log "  bigload am=$am rows=$rows load=${load_s}s commit=$rc (am=$got)"
  $PSQL -d postgres -c "DROP DATABASE IF EXISTS $db" >/dev/null 2>&1
}

# bulk COPY + VACUUM, single client, timed. Data file is generated once and
# shared by both AMs so the input is byte-identical across arms.
COPY_DATAFILE="$OUT/copydata.tsv"
gen_copy_data() {
  [ -f "$COPY_DATAFILE" ] && return 0
  log "generating copy data ($COPY_ROWS rows) -> $COPY_DATAFILE"
  seq 1 "$COPY_ROWS" | awk 'BEGIN{OFS="\t"} {print $1, ($1%1000), ($1%7), "payload_row_" $1}' > "$COPY_DATAFILE"
}
workload_copy() {
  local am=$1 db; db=$(db_for "$am")
  gen_copy_data
  log "copy workload am=$am rows=$COPY_ROWS"
  $PSQL -d "$db" -c "DROP TABLE IF EXISTS copytest" >/dev/null 2>&1
  $PSQL -d "$db" -c "CREATE TABLE copytest (id bigint, v1 int, v2 int, payload text)" >/dev/null
  local gen="$OUT/copy_${am}.log"
  checkpoint
  local t0 t1 copy_s vac_s
  t0=$(date +%s.%N)
  $PSQL -d "$db" -c "\copy copytest FROM '$COPY_DATAFILE' WITH (FORMAT text)" >>"$gen" 2>&1 || {
    log "  COPY FAILED am=$am (see $gen)"; return 0; }
  t1=$(date +%s.%N); copy_s=$(awk "BEGIN{printf \"%.2f\", $t1-$t0}")
  t0=$(date +%s.%N)
  $PSQL -d "$db" -c "VACUUM (ANALYZE) copytest" >>"$gen" 2>&1
  t1=$(date +%s.%N); vac_s=$(awk "BEGIN{printf \"%.2f\", $t1-$t0}")
  local rows sz
  rows=$($PSQL -d "$db" -Atc "SELECT count(*) FROM copytest")
  sz=$($PSQL -d "$db" -Atc "SELECT pg_total_relation_size('copytest')")
  echo "copy,$am,1,NA,NA,$copy_s,rows=$rows;copy_s=$copy_s;vacuum_s=$vac_s;total_bytes=$sz" >> "$CSV"
  log "  copy am=$am rows=$rows copy=${copy_s}s vacuum=${vac_s}s size=${sz}B"
}

# ---- main -------------------------------------------------------------------
log "=== honest HEAP vs RECNO ==="
log "BIN=$BIN CLUSTER=$CLUSTER OUT=$OUT"
log "DURATION=${DURATION}s WARMUP=${WARMUP}s SCALE=$SCALE CLIENTS=[$CLIENTS] COPY_ROWS=$COPY_ROWS"
log "shared_buffers=$SHARED_BUFFERS AMS=[$AMS] WORKLOADS=[$WORKLOADS]"

init_cluster
start_cluster
trap stop_cluster EXIT

for am in $AMS; do
  create_db_for_am "$am"
  # tpcb/ro share one pgbench dataset; only init if those workloads run
  case " $WORKLOADS " in *" tpcb "*|*" ro "*) init_pgbench "$am";; esac
done

for wl in $WORKLOADS; do
  for am in $AMS; do
    log "--- workload=$wl am=$am ---"
    case "$wl" in
      bigload) workload_bigload "$am";;
      tpcb) workload_tpcb "$am";;
      ro)   workload_ro "$am";;
      copy) workload_copy "$am";;
      *) log "unknown workload $wl";;
    esac
  done
done

log "=== DONE. results: $CSV ==="
tee -a "$OUT/driver.log" < "$CSV"
