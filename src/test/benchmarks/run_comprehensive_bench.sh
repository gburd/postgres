#!/usr/bin/env bash
#
# run_comprehensive_bench.sh - Comprehensive RECNO vs HEAP benchmark suite
#
# Runs a wide variety of workloads comparing RECNO and HEAP access methods,
# then outputs structured results.
#
# Usage:
#   ./run_comprehensive_bench.sh [options]
#
# Environment variables:
#   PG_BIN          - Path to PostgreSQL bin directory (auto-detected)
#   PG_LIB          - Path to PostgreSQL lib directory (auto-detected)
#   BENCH_BASE      - Base directory for temp data (default: /tmp/recno_bench)
#   SHARED_BUFFERS  - Shared buffer size (default: 512MB)
#   RESULTS_DIR     - Where to write result files (default: /tmp/recno_bench/results)

set -uo pipefail
# Note: not using -e because SQL errors (e.g. sLog OOM) should not abort the suite

###############################################################################
# Configuration
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Auto-detect PG_BIN
if [ -z "${PG_BIN:-}" ]; then
    for candidate in \
        "$REPO_ROOT/build/tmp_install/usr/local/pgsql/bin" \
        "$REPO_ROOT/build/tmp_install/install/bin" \
        "$REPO_ROOT/install/bin"; do
        if [ -x "$candidate/pgbench" ] && [ -x "$candidate/initdb" ]; then
            PG_BIN="$candidate"
            break
        fi
    done
    if [ -z "${PG_BIN:-}" ]; then
        echo "ERROR: Cannot find PG_BIN. Set PG_BIN=/path/to/pg/bin" >&2
        exit 1
    fi
fi

# Auto-detect PG_LIB
if [ -z "${PG_LIB:-}" ]; then
    BIN_PARENT="$(dirname "$PG_BIN")"
    for libdir in "$BIN_PARENT/lib64" "$BIN_PARENT/lib"; do
        if [ -f "$libdir/libpq.so" ] || [ -f "$libdir/libpq.so.5" ] || [ -f "$libdir/libpq.dylib" ]; then
            PG_LIB="$libdir"
            break
        fi
    done
fi

if [ -n "${PG_LIB:-}" ]; then
    export LD_LIBRARY_PATH="${PG_LIB}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    export DYLD_LIBRARY_PATH="${PG_LIB}${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
fi

BENCH_BASE="${BENCH_BASE:-/tmp/recno_bench}"
SHARED_BUFFERS="${SHARED_BUFFERS:-512MB}"
RESULTS_DIR="${RESULTS_DIR:-$BENCH_BASE/results}"
HOSTNAME_SHORT="$(hostname -s 2>/dev/null || hostname)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RESULT_FILE="$RESULTS_DIR/${HOSTNAME_SHORT}_${TIMESTAMP}.txt"

HEAP_PORT=54320
RECNO_PORT=54321
HEAP_PGDATA="$BENCH_BASE/heap_data"
RECNO_PGDATA="$BENCH_BASE/recno_data"

# Binaries
INITDB="$PG_BIN/initdb"
PG_CTL="$PG_BIN/pg_ctl"
PGBENCH="$PG_BIN/pgbench"
PSQL="$PG_BIN/psql"
PG_ISREADY="$PG_BIN/pg_isready"

for bin in "$INITDB" "$PG_CTL" "$PGBENCH" "$PSQL" "$PG_ISREADY"; do
    if [ ! -x "$bin" ]; then
        echo "ERROR: Missing binary: $bin" >&2
        exit 1
    fi
done

###############################################################################
# Cleanup
###############################################################################

cleanup() {
    echo ""
    echo "Cleaning up..."
    "$PG_CTL" stop -D "$HEAP_PGDATA" -m immediate 2>/dev/null || true
    "$PG_CTL" stop -D "$RECNO_PGDATA" -m immediate 2>/dev/null || true
}
trap cleanup EXIT

###############################################################################
# Helpers
###############################################################################

log() { echo ">>> $*" | tee -a "$RESULT_FILE"; }
out() { echo "$*" | tee -a "$RESULT_FILE"; }

init_cluster() {
    local pgdata="$1" port="$2" am="$3"
    log "Initializing $am cluster at $pgdata (port $port)"
    mkdir -p "$pgdata"
    "$INITDB" -D "$pgdata" --no-locale -E UTF8 -A trust >/dev/null 2>&1
    cat >> "$pgdata/postgresql.conf" <<EOF
port = $port
listen_addresses = '127.0.0.1'
unix_socket_directories = '$pgdata'
shared_buffers = $SHARED_BUFFERS
work_mem = 64MB
maintenance_work_mem = 256MB
wal_level = minimal
max_wal_senders = 0
fsync = off
synchronous_commit = off
full_page_writes = off
max_connections = 100
logging_collector = off
log_min_messages = warning
default_table_access_method = '$am'
max_logical_revert_workers = 0
EOF
}

start_cluster() {
    local pgdata="$1" label="$2" port="$3"
    log "Starting $label server on port $port"
    "$PG_CTL" start -D "$pgdata" -l "$pgdata/server.log" -w -t 60 >/dev/null 2>&1
    local retries=30
    while [ "$retries" -gt 0 ]; do
        if "$PG_ISREADY" -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then return 0; fi
        retries=$((retries - 1)); sleep 1
    done
    echo "ERROR: $label server on port $port not ready" >&2; return 1
}

stop_cluster() {
    local pgdata="$1" label="$2"
    "$PG_CTL" stop -D "$pgdata" -m fast 2>/dev/null || true
}

run_sql() {
    local port="$1" label="$2"
    shift 2
    "$PSQL" -h 127.0.0.1 -p "$port" -d postgres -X -q "$@"
}

# recno_batched_insert PORT TABLE_NAME ROWCOUNT SELECT_EXPR
#   Insert rows in per-txn batches of 1000 to avoid RECNO sLog overflow.
#   SELECT_EXPR uses 'i' as the loop variable, e.g. "i, md5(i::text)"
recno_batched_insert() {
    local port="$1" table="$2" rowcount="$3" select_expr="$4"
    local batch_size=1000
    local sqlfile="$BENCH_BASE/_batch_insert.sql"
    local batch=0 start end

    > "$sqlfile"
    while [ $((batch * batch_size)) -lt "$rowcount" ]; do
        start=$((batch * batch_size + 1))
        end=$(( (batch + 1) * batch_size ))
        [ "$end" -gt "$rowcount" ] && end="$rowcount"
        echo "INSERT INTO $table SELECT $select_expr FROM generate_series($start, $end) i;" >> "$sqlfile"
        batch=$((batch + 1))
    done

    "$PSQL" -h 127.0.0.1 -p "$port" -d postgres -X -q -f "$sqlfile" >/dev/null 2>&1
    rm -f "$sqlfile"
}

# ensure_server PORT PGDATA LABEL AM
#   Check if server is alive; if not, reinitialize and restart.
ensure_server() {
    local port="$1" pgdata="$2" label="$3" am="$4"
    if "$PG_ISREADY" -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then
        return 0
    fi
    echo "  [WARNING] $label server on port $port is down, reinitializing..." | tee -a "$RESULT_FILE"
    "$PG_CTL" stop -D "$pgdata" -m immediate 2>/dev/null || true
    rm -rf "$pgdata"
    init_cluster "$pgdata" "$port" "$am"
    start_cluster "$pgdata" "$label" "$port"
}

# time_sql PORT LABEL SQL -> prints elapsed ms
time_sql() {
    local port="$1" label="$2" sql="$3"
    local start_ns end_ns elapsed_ms
    start_ns=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
    run_sql "$port" "$label" -c "$sql" >/dev/null 2>&1
    end_ns=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
    elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    echo "$elapsed_ms"
}

# pgbench_tps PORT CLIENTS DURATION [CUSTOM_SCRIPT] -> prints TPS
pgbench_tps() {
    local port="$1" clients="$2" duration="$3" script="${4:-}"
    local args="-h 127.0.0.1 -p $port -c $clients -j $clients -T $duration --no-vacuum"
    if [ -n "$script" ]; then
        args="$args -f $script"
    fi
    local output
    output=$("$PGBENCH" $args postgres 2>&1) || true
    local tps
    tps=$(echo "$output" | grep -iE "without initial connection time|excluding connections establishing" \
         | sed 's/.*= *//' | sed 's/ .*//' || echo "0")
    echo "${tps:-0}"
}

fmt_ratio() {
    local heap="$1" recno="$2"
    echo "$recno $heap" | awk '{if ($2+0 > 0) printf "%.2fx", $1/$2; else print "N/A"}'
}

###############################################################################
# Banner
###############################################################################

mkdir -p "$RESULTS_DIR" "$BENCH_BASE"

out ""
out "================================================================"
out " Comprehensive RECNO vs HEAP Benchmark Suite"
out " Host: $HOSTNAME_SHORT  Date: $(date '+%Y-%m-%d %H:%M:%S')"
out " Git: $(cd "$REPO_ROOT" && git log --oneline -1 2>/dev/null || echo 'unknown')"
out " CPUs: $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo '?')"
out " RAM: $(free -h 2>/dev/null | awk '/^Mem:/{print $2}' || sysctl -n hw.physmem 2>/dev/null | awk '{printf "%.0fGB", $1/1073741824}' || echo '?')"
out " Shared buffers: $SHARED_BUFFERS"
out " PG_BIN: $PG_BIN"
out " Results: $RESULT_FILE"
out "================================================================"
out ""

###############################################################################
# Initialize clusters
###############################################################################

init_cluster "$HEAP_PGDATA" "$HEAP_PORT" "heap"
init_cluster "$RECNO_PGDATA" "$RECNO_PORT" "recno"
start_cluster "$HEAP_PGDATA" "HEAP" "$HEAP_PORT"
start_cluster "$RECNO_PGDATA" "RECNO" "$RECNO_PORT"

###############################################################################
# BENCHMARK 1: Bulk Insert Throughput
###############################################################################

out ""
out "================================================================"
out " BENCHMARK 1: Bulk Insert Throughput"
out "================================================================"

for rowcount in 1000 10000 50000 100000; do
    # HEAP
    run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_insert; CREATE TABLE bench_insert (id int, val text, data text) USING heap;" >/dev/null
    heap_ms=$(time_sql "$HEAP_PORT" "HEAP" "INSERT INTO bench_insert SELECT i, 'val'||i, md5(i::text) FROM generate_series(1,$rowcount) i")

    # RECNO (may hit sLog OOM at large row counts)
    run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_insert; CREATE TABLE bench_insert (id int, val text, data text) USING recno;" >/dev/null
    recno_ms=$(time_sql "$RECNO_PORT" "RECNO" "INSERT INTO bench_insert SELECT i, 'val'||i, md5(i::text) FROM generate_series(1,$rowcount) i") || recno_ms="OOM"

    if [ "$recno_ms" = "OOM" ]; then
        out "  ${rowcount} rows:  HEAP=${heap_ms}ms  RECNO=OOM (sLog exhausted)"
    else
        ratio=$(fmt_ratio "$heap_ms" "$recno_ms")
        out "  ${rowcount} rows:  HEAP=${heap_ms}ms  RECNO=${recno_ms}ms  ratio=${ratio}"
    fi

    # Clean up
    run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_insert;" >/dev/null
    run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_insert;" >/dev/null 2>&1 || true
done

###############################################################################
# BENCHMARK 2: Single-Row Insert Throughput (pgbench)
###############################################################################

out ""
out "================================================================"
out " BENCHMARK 2: Single-Row Insert Throughput (pgbench, 30s)"
out "================================================================"

# Create tables on both
BENCH2_SETUP="DROP TABLE IF EXISTS bench_insert_single; CREATE TABLE bench_insert_single (id serial, val text);"
run_sql "$HEAP_PORT" "HEAP" -c "$BENCH2_SETUP" >/dev/null
run_sql "$RECNO_PORT" "RECNO" -c "$BENCH2_SETUP" >/dev/null

# Custom pgbench script
BENCH2_SCRIPT="$BENCH_BASE/bench2_insert.sql"
echo "INSERT INTO bench_insert_single (val) VALUES (md5(random()::text));" > "$BENCH2_SCRIPT"

for clients in 1 2 4; do
    heap_tps=$(pgbench_tps "$HEAP_PORT" "$clients" 30 "$BENCH2_SCRIPT")
    recno_tps=$(pgbench_tps "$RECNO_PORT" "$clients" 30 "$BENCH2_SCRIPT")
    ratio=$(fmt_ratio "$heap_tps" "$recno_tps")
    out "  ${clients} client(s):  HEAP=${heap_tps} tps  RECNO=${recno_tps} tps  ratio=${ratio}"
done

run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_insert_single;" >/dev/null
run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_insert_single;" >/dev/null

###############################################################################
# BENCHMARK 3: Update Performance (in-place vs copy-on-write)
###############################################################################

out ""
out "================================================================"
out " BENCHMARK 3: Update Performance"
out "================================================================"

for rowcount in 10000 100000; do
    # Setup
    run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_update; CREATE TABLE bench_update (id int PRIMARY KEY, counter int DEFAULT 0, data text) USING heap; INSERT INTO bench_update SELECT i, 0, md5(i::text) FROM generate_series(1,$rowcount) i;" >/dev/null
    run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_update; CREATE TABLE bench_update (id int PRIMARY KEY, counter int DEFAULT 0, data text) USING recno;" >/dev/null
    recno_batched_insert "$RECNO_PORT" "bench_update" "$rowcount" "i, 0, md5(i::text)"

    # Single-column update (counter only — RECNO can do in-place)
    heap_ms=$(time_sql "$HEAP_PORT" "HEAP" "UPDATE bench_update SET counter = counter + 1")
    recno_ms=$(time_sql "$RECNO_PORT" "RECNO" "UPDATE bench_update SET counter = counter + 1")
    ratio=$(fmt_ratio "$heap_ms" "$recno_ms")
    out "  ${rowcount} rows single-col update:  HEAP=${heap_ms}ms  RECNO=${recno_ms}ms  ratio=${ratio}"

    # Full-row update (changes data column — bigger tuple change)
    heap_ms=$(time_sql "$HEAP_PORT" "HEAP" "UPDATE bench_update SET data = md5(counter::text)")
    recno_ms=$(time_sql "$RECNO_PORT" "RECNO" "UPDATE bench_update SET data = md5(counter::text)")
    ratio=$(fmt_ratio "$heap_ms" "$recno_ms")
    out "  ${rowcount} rows full-row update:  HEAP=${heap_ms}ms  RECNO=${recno_ms}ms  ratio=${ratio}"

    run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_update;" >/dev/null
    run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_update;" >/dev/null
done

###############################################################################
# BENCHMARK 4: Update-Heavy pgbench (TPC-B style)
###############################################################################

out ""
out "================================================================"
out " BENCHMARK 4: TPC-B Mixed Workload (pgbench, 60s)"
out "================================================================"

for scale in 10 50; do
    "$PGBENCH" -i -s "$scale" -h 127.0.0.1 -p "$HEAP_PORT" postgres >/dev/null 2>&1
    "$PGBENCH" -i -s "$scale" -h 127.0.0.1 -p "$RECNO_PORT" postgres >/dev/null 2>&1

    for clients in 1 4 8; do
        heap_tps=$(pgbench_tps "$HEAP_PORT" "$clients" 60)
        recno_tps=$(pgbench_tps "$RECNO_PORT" "$clients" 60)
        ratio=$(fmt_ratio "$heap_tps" "$recno_tps")
        out "  scale=${scale} ${clients} client(s):  HEAP=${heap_tps} tps  RECNO=${recno_tps} tps  ratio=${ratio}"
    done
done

###############################################################################
# BENCHMARK 5: Read-Only Sequential Scans
###############################################################################

# Health check: TPC-B deadlocks can crash the RECNO logical revert worker
ensure_server "$HEAP_PORT" "$HEAP_PGDATA" "HEAP" "heap"
ensure_server "$RECNO_PORT" "$RECNO_PGDATA" "RECNO" "recno"

out ""
out "================================================================"
out " BENCHMARK 5: Sequential Scan Performance"
out "================================================================"

for rowcount in 10000 50000 100000; do
    run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_scan; CREATE TABLE bench_scan (id int, val int, data text) USING heap; INSERT INTO bench_scan SELECT i, i%1000, md5(i::text) FROM generate_series(1,$rowcount) i;" >/dev/null

    # RECNO: batched inserts (1000 rows per txn) to stay within sLog capacity
    run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_scan; CREATE TABLE bench_scan (id int, val int, data text) USING recno;" >/dev/null
    recno_batched_insert "$RECNO_PORT" "bench_scan" "$rowcount" "i, i%1000, md5(i::text)"

    # Verify data loaded
    recno_count=$(run_sql "$RECNO_PORT" "RECNO" -t -c "SELECT count(*) FROM bench_scan;" 2>/dev/null | tr -d ' ')
    if [ "${recno_count:-0}" -lt "$((rowcount / 2))" ]; then
        out "  ${rowcount} rows: RECNO insert failed (got ${recno_count:-0} rows), skipping"
        run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_scan;" >/dev/null
        run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_scan;" >/dev/null 2>&1 || true
        continue
    fi

    # VACUUM to set PD_ALL_VISIBLE and VM flags (simulates autovacuum in production)
    run_sql "$HEAP_PORT" "HEAP" -c "VACUUM ANALYZE bench_scan;" >/dev/null 2>&1
    run_sql "$RECNO_PORT" "RECNO" -c "VACUUM ANALYZE bench_scan;" >/dev/null 2>&1

    # Full seq scan with aggregation
    heap_ms=$(time_sql "$HEAP_PORT" "HEAP" "SELECT count(*), sum(val), avg(val) FROM bench_scan")
    recno_ms=$(time_sql "$RECNO_PORT" "RECNO" "SELECT count(*), sum(val), avg(val) FROM bench_scan")
    ratio=$(fmt_ratio "$heap_ms" "$recno_ms")
    out "  ${rowcount} rows full scan+agg:  HEAP=${heap_ms}ms  RECNO=${recno_ms}ms  ratio=${ratio}"

    # Filtered scan
    heap_ms=$(time_sql "$HEAP_PORT" "HEAP" "SELECT count(*) FROM bench_scan WHERE val < 100")
    recno_ms=$(time_sql "$RECNO_PORT" "RECNO" "SELECT count(*) FROM bench_scan WHERE val < 100")
    ratio=$(fmt_ratio "$heap_ms" "$recno_ms")
    out "  ${rowcount} rows filtered scan (10%):  HEAP=${heap_ms}ms  RECNO=${recno_ms}ms  ratio=${ratio}"

    run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_scan;" >/dev/null
    run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_scan;" >/dev/null
done

###############################################################################
# BENCHMARK 6: Index Scan Performance
###############################################################################

out ""
out "================================================================"
out " BENCHMARK 6: Index Scan Performance (pgbench, 30s)"
out "================================================================"

ROWCOUNT=50000
run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_idx; CREATE TABLE bench_idx (id int PRIMARY KEY, val int, data text) USING heap; INSERT INTO bench_idx SELECT i, i%10000, md5(i::text) FROM generate_series(1,$ROWCOUNT) i; CREATE INDEX bench_idx_val ON bench_idx(val);" >/dev/null

# RECNO: batched insert to stay within sLog capacity
run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_idx; CREATE TABLE bench_idx (id int PRIMARY KEY, val int, data text) USING recno;" >/dev/null
recno_batched_insert "$RECNO_PORT" "bench_idx" "$ROWCOUNT" "i, i%10000, md5(i::text)"
run_sql "$RECNO_PORT" "RECNO" -c "CREATE INDEX bench_idx_val ON bench_idx(val);" >/dev/null

# Point lookup script (use \set to ensure planner sees constant → index scan)
BENCH6_PK="$BENCH_BASE/bench6_pk.sql"
printf '\\set id random(1, %d)\nSELECT * FROM bench_idx WHERE id = :id;\n' "$ROWCOUNT" > "$BENCH6_PK"

BENCH6_IDX="$BENCH_BASE/bench6_idx.sql"
printf '\\set val random(0, 9999)\nSELECT count(*) FROM bench_idx WHERE val = :val;\n' > "$BENCH6_IDX"

for clients in 1 4 8; do
    heap_tps=$(pgbench_tps "$HEAP_PORT" "$clients" 30 "$BENCH6_PK")
    recno_tps=$(pgbench_tps "$RECNO_PORT" "$clients" 30 "$BENCH6_PK")
    ratio=$(fmt_ratio "$heap_tps" "$recno_tps")
    out "  PK lookup ${clients} client(s):  HEAP=${heap_tps} tps  RECNO=${recno_tps} tps  ratio=${ratio}"
done

for clients in 1 4; do
    heap_tps=$(pgbench_tps "$HEAP_PORT" "$clients" 30 "$BENCH6_IDX")
    recno_tps=$(pgbench_tps "$RECNO_PORT" "$clients" 30 "$BENCH6_IDX")
    ratio=$(fmt_ratio "$heap_tps" "$recno_tps")
    out "  Index scan ${clients} client(s):  HEAP=${heap_tps} tps  RECNO=${recno_tps} tps  ratio=${ratio}"
done

run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_idx;" >/dev/null
run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_idx;" >/dev/null

###############################################################################
# BENCHMARK 7: Delete Performance
###############################################################################

ensure_server "$RECNO_PORT" "$RECNO_PGDATA" "RECNO" "recno"

out ""
out "================================================================"
out " BENCHMARK 7: Delete Performance"
out "================================================================"

for rowcount in 10000 100000; do
    run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_del; CREATE TABLE bench_del (id int, val text) USING heap; INSERT INTO bench_del SELECT i, md5(i::text) FROM generate_series(1,$rowcount) i;" >/dev/null
    run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_del; CREATE TABLE bench_del (id int, val text) USING recno;" >/dev/null
    recno_batched_insert "$RECNO_PORT" "bench_del" "$rowcount" "i, md5(i::text)"

    heap_ms=$(time_sql "$HEAP_PORT" "HEAP" "DELETE FROM bench_del WHERE id <= $((rowcount / 2))")
    recno_ms=$(time_sql "$RECNO_PORT" "RECNO" "DELETE FROM bench_del WHERE id <= $((rowcount / 2))")
    ratio=$(fmt_ratio "$heap_ms" "$recno_ms")
    out "  ${rowcount} rows delete 50%:  HEAP=${heap_ms}ms  RECNO=${recno_ms}ms  ratio=${ratio}"

    run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_del;" >/dev/null
    run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_del;" >/dev/null
done

###############################################################################
# BENCHMARK 8: Rollback / Abort Cost
###############################################################################

ensure_server "$RECNO_PORT" "$RECNO_PGDATA" "RECNO" "recno"

out ""
out "================================================================"
out " BENCHMARK 8: Rollback / Abort Cost"
out "================================================================"

# Use row counts within sLog capacity (max ~25K per txn with 100 connections)
for rowcount in 1000 10000 20000; do
    run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_rollback; CREATE TABLE bench_rollback (id int, val text) USING heap;" >/dev/null
    run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_rollback; CREATE TABLE bench_rollback (id int, val text) USING recno;" >/dev/null

    # Insert then rollback (single transaction — tests actual rollback cost)
    heap_ms=$(time_sql "$HEAP_PORT" "HEAP" "BEGIN; INSERT INTO bench_rollback SELECT i, md5(i::text) FROM generate_series(1,$rowcount) i; ROLLBACK;")
    recno_ms=$(time_sql "$RECNO_PORT" "RECNO" "BEGIN; INSERT INTO bench_rollback SELECT i, md5(i::text) FROM generate_series(1,$rowcount) i; ROLLBACK;")
    ratio=$(fmt_ratio "$heap_ms" "$recno_ms")
    out "  ${rowcount} rows insert+rollback:  HEAP=${heap_ms}ms  RECNO=${recno_ms}ms  ratio=${ratio}"

    run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_rollback;" >/dev/null
    run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_rollback;" >/dev/null
done

###############################################################################
# BENCHMARK 9: Storage Bloat After Updates
###############################################################################

ensure_server "$RECNO_PORT" "$RECNO_PGDATA" "RECNO" "recno"

out ""
out "================================================================"
out " BENCHMARK 9: Storage Bloat After Repeated Updates"
out "================================================================"

ROWCOUNT=50000
run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_bloat; CREATE TABLE bench_bloat (id int, counter int DEFAULT 0, data text) USING heap; INSERT INTO bench_bloat SELECT i, 0, md5(i::text) FROM generate_series(1,$ROWCOUNT) i;" >/dev/null
run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_bloat; CREATE TABLE bench_bloat (id int, counter int DEFAULT 0, data text) USING recno;" >/dev/null
recno_batched_insert "$RECNO_PORT" "bench_bloat" "$ROWCOUNT" "i, 0, md5(i::text)"

heap_init=$(run_sql "$HEAP_PORT" "HEAP" -t -c "SELECT pg_total_relation_size('bench_bloat');" | tr -d ' ')
recno_init=$(run_sql "$RECNO_PORT" "RECNO" -t -c "SELECT pg_total_relation_size('bench_bloat');" | tr -d ' ')
out "  Initial size (${ROWCOUNT} rows):  HEAP=$(echo "$heap_init" | awk '{printf "%.1fMB", $1/1048576}')  RECNO=$(echo "$recno_init" | awk '{printf "%.1fMB", $1/1048576}')"

for round in 1 2 3 4 5; do
    run_sql "$HEAP_PORT" "HEAP" -c "UPDATE bench_bloat SET counter = counter + 1;" >/dev/null
    run_sql "$RECNO_PORT" "RECNO" -c "UPDATE bench_bloat SET counter = counter + 1;" >/dev/null
done

heap_after=$(run_sql "$HEAP_PORT" "HEAP" -t -c "SELECT pg_total_relation_size('bench_bloat');" | tr -d ' ')
recno_after=$(run_sql "$RECNO_PORT" "RECNO" -t -c "SELECT pg_total_relation_size('bench_bloat');" | tr -d ' ')
heap_bloat=$(echo "$heap_after $heap_init" | awk '{printf "%.2f", $1/$2}')
recno_bloat=$(echo "$recno_after $recno_init" | awk '{printf "%.2f", $1/$2}')
out "  After 5 update rounds:  HEAP=$(echo "$heap_after" | awk '{printf "%.1fMB", $1/1048576}') (${heap_bloat}x)  RECNO=$(echo "$recno_after" | awk '{printf "%.1fMB", $1/1048576}') (${recno_bloat}x)"

# VACUUM and measure again
run_sql "$HEAP_PORT" "HEAP" -c "VACUUM bench_bloat;" >/dev/null
heap_vacuumed=$(run_sql "$HEAP_PORT" "HEAP" -t -c "SELECT pg_total_relation_size('bench_bloat');" | tr -d ' ')
out "  After VACUUM (HEAP):  $(echo "$heap_vacuumed" | awk '{printf "%.1fMB", $1/1048576}')  (RECNO needs no VACUUM for this)"

run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_bloat;" >/dev/null
run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_bloat;" >/dev/null

###############################################################################
# BENCHMARK 10: Mixed Read-Write (pgbench custom)
###############################################################################

ensure_server "$RECNO_PORT" "$RECNO_PGDATA" "RECNO" "recno"

out ""
out "================================================================"
out " BENCHMARK 10: Mixed Read-Write Workload (pgbench, 60s)"
out "================================================================"

ROWCOUNT=100000
run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_mixed; CREATE TABLE bench_mixed (id int PRIMARY KEY, counter int DEFAULT 0, data text) USING heap; INSERT INTO bench_mixed SELECT i, 0, md5(i::text) FROM generate_series(1,$ROWCOUNT) i;" >/dev/null
run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_mixed; CREATE TABLE bench_mixed (id int PRIMARY KEY, counter int DEFAULT 0, data text) USING recno;" >/dev/null
recno_batched_insert "$RECNO_PORT" "bench_mixed" "$ROWCOUNT" "i, 0, md5(i::text)"

# 80% reads, 20% writes
BENCH10_SCRIPT="$BENCH_BASE/bench10_mixed.sql"
cat > "$BENCH10_SCRIPT" <<'EOSQL'
\set id random(1, 100000)
\set do_write random(1, 5)
BEGIN;
SELECT * FROM bench_mixed WHERE id = :id;
SELECT count(*) FROM bench_mixed WHERE id BETWEEN :id AND :id + 100;
\if :do_write = 1
UPDATE bench_mixed SET counter = counter + 1 WHERE id = :id;
\endif
END;
EOSQL

for clients in 1 4 8; do
    heap_tps=$(pgbench_tps "$HEAP_PORT" "$clients" 60 "$BENCH10_SCRIPT")
    recno_tps=$(pgbench_tps "$RECNO_PORT" "$clients" 60 "$BENCH10_SCRIPT")
    ratio=$(fmt_ratio "$heap_tps" "$recno_tps")
    out "  ${clients} client(s):  HEAP=${heap_tps} tps  RECNO=${recno_tps} tps  ratio=${ratio}"
done

run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_mixed;" >/dev/null
run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_mixed;" >/dev/null

###############################################################################
# BENCHMARK 11: Wide Table Performance
###############################################################################

ensure_server "$RECNO_PORT" "$RECNO_PGDATA" "RECNO" "recno"

out ""
out "================================================================"
out " BENCHMARK 11: Wide Table (many columns)"
out "================================================================"

ROWCOUNT=50000
WIDE_CREATE="DROP TABLE IF EXISTS bench_wide; CREATE TABLE bench_wide (id int PRIMARY KEY, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int, t1 text, t2 text, t3 text, t4 text, t5 text)"
WIDE_INSERT_EXPR="i, i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9, md5(i::text), md5((i+1)::text), md5((i+2)::text), md5((i+3)::text), md5((i+4)::text)"

run_sql "$HEAP_PORT" "HEAP" -c "$WIDE_CREATE USING heap; INSERT INTO bench_wide SELECT $WIDE_INSERT_EXPR FROM generate_series(1,$ROWCOUNT) i;" >/dev/null
run_sql "$RECNO_PORT" "RECNO" -c "$WIDE_CREATE USING recno;" >/dev/null
recno_batched_insert "$RECNO_PORT" "bench_wide" "$ROWCOUNT" "$WIDE_INSERT_EXPR"

# VACUUM to set PD_ALL_VISIBLE flags
run_sql "$HEAP_PORT" "HEAP" -c "VACUUM ANALYZE bench_wide;" >/dev/null 2>&1
run_sql "$RECNO_PORT" "RECNO" -c "VACUUM ANALYZE bench_wide;" >/dev/null 2>&1

# Narrow projection
heap_ms=$(time_sql "$HEAP_PORT" "HEAP" "SELECT sum(c1), avg(c2) FROM bench_wide")
recno_ms=$(time_sql "$RECNO_PORT" "RECNO" "SELECT sum(c1), avg(c2) FROM bench_wide")
ratio=$(fmt_ratio "$heap_ms" "$recno_ms")
out "  Narrow projection (2 int cols):  HEAP=${heap_ms}ms  RECNO=${recno_ms}ms  ratio=${ratio}"

# Full scan all columns
heap_ms=$(time_sql "$HEAP_PORT" "HEAP" "SELECT count(*) FROM bench_wide WHERE t1 LIKE 'a%' OR t5 LIKE 'b%'")
recno_ms=$(time_sql "$RECNO_PORT" "RECNO" "SELECT count(*) FROM bench_wide WHERE t1 LIKE 'a%' OR t5 LIKE 'b%'")
ratio=$(fmt_ratio "$heap_ms" "$recno_ms")
out "  Full row filter:  HEAP=${heap_ms}ms  RECNO=${recno_ms}ms  ratio=${ratio}"

# Single-column update on wide table
heap_ms=$(time_sql "$HEAP_PORT" "HEAP" "UPDATE bench_wide SET c1 = c1 + 1")
recno_ms=$(time_sql "$RECNO_PORT" "RECNO" "UPDATE bench_wide SET c1 = c1 + 1")
ratio=$(fmt_ratio "$heap_ms" "$recno_ms")
out "  Single-col update (wide table):  HEAP=${heap_ms}ms  RECNO=${recno_ms}ms  ratio=${ratio}"

run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_wide;" >/dev/null
run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_wide;" >/dev/null

###############################################################################
# BENCHMARK 12: COPY (Bulk Load) Performance
###############################################################################

ensure_server "$RECNO_PORT" "$RECNO_PGDATA" "RECNO" "recno"

out ""
out "================================================================"
out " BENCHMARK 12: COPY Bulk Load"
out "================================================================"

for ROWCOUNT in 10000 50000; do
    # Generate CSV data to a temp file
    CSVFILE="$BENCH_BASE/bench_copy.csv"
    run_sql "$HEAP_PORT" "HEAP" -t -c "COPY (SELECT i, md5(i::text), md5((i*2)::text) FROM generate_series(1,$ROWCOUNT) i) TO STDOUT WITH CSV" > "$CSVFILE"

    for am_port in "$HEAP_PORT:heap" "$RECNO_PORT:recno"; do
        port="${am_port%%:*}"
        am="${am_port##*:}"
        run_sql "$port" "$am" -c "DROP TABLE IF EXISTS bench_copy; CREATE TABLE bench_copy (id int, val text, data text) USING $am;" >/dev/null
    done

    heap_ms=$(time_sql "$HEAP_PORT" "HEAP" "\\copy bench_copy FROM '$CSVFILE' WITH CSV")
    recno_ms=$(time_sql "$RECNO_PORT" "RECNO" "\\copy bench_copy FROM '$CSVFILE' WITH CSV")
    ratio=$(fmt_ratio "$heap_ms" "$recno_ms")
    out "  COPY ${ROWCOUNT} rows:  HEAP=${heap_ms}ms  RECNO=${recno_ms}ms  ratio=${ratio}"

    run_sql "$HEAP_PORT" "HEAP" -c "DROP TABLE IF EXISTS bench_copy;" >/dev/null
    run_sql "$RECNO_PORT" "RECNO" -c "DROP TABLE IF EXISTS bench_copy;" >/dev/null
done

###############################################################################
# Stop servers and print summary
###############################################################################

stop_cluster "$HEAP_PGDATA" "HEAP"
stop_cluster "$RECNO_PGDATA" "RECNO"

out ""
out "================================================================"
out " Benchmark suite complete."
out " Results written to: $RESULT_FILE"
out "================================================================"
