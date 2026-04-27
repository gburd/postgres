#!/usr/bin/env bash
#
# RECNO vs HEAP Comprehensive Benchmark Suite
#
# Designed to stress every dimension where RECNO's timestamp-based MVCC
# with per-relation UNDO differs from HEAP's xmin/xmax MVCC:
#
#   1. OLTP mixed workload (TPC-B style) at varying concurrency
#   2. Update-heavy workload (in-place updates, RECNO's sweet spot)
#   3. Read-heavy under concurrent writes (reader-writer contention)
#   4. Point lookup latency (index scan with concurrent modifications)
#   5. Bulk insert throughput
#   6. Sequential scan with concurrent DML
#   7. Table bloat and VACUUM overhead
#   8. Long-running transactions (snapshot isolation cost)
#
# Each test runs for >=5 minutes.  Results written to $RESULTS_DIR as CSV.
#
set -euo pipefail

# ── Configuration ───────────────────────────────────────────────────
BASEDIR="${BASEDIR:-/scratch/recno-bench}"
PGINSTALL="${BASEDIR}/install"
PGBIN="${PGINSTALL}/bin"
PGDATA_HEAP="${BASEDIR}/data-heap"
PGDATA_RECNO="${BASEDIR}/data-recno"
RESULTS_DIR="${BASEDIR}/results/$(date +%Y%m%d_%H%M%S)"
LOGDIR="${BASEDIR}/logs"

# pgbench parameters
SCALE=100           # 10M rows in accounts table (~1.5 GB per AM)
DURATION=300        # 5 minutes per test
WARMUP=30           # 30 second warmup before measurement
CLIENTS_LIST="1 4 12 24 48"  # concurrency levels to sweep

# Large dataset for cache-pressure tests
LARGE_ROWS=20000000  # 20M rows for targeted tests

# Port allocation
PORT_HEAP=15432
PORT_RECNO=15433

export PATH="${PGBIN}:${PATH}"

mkdir -p "${RESULTS_DIR}" "${LOGDIR}"

# ── Utility functions ───────────────────────────────────────────────

log() { echo "[$(date '+%H:%M:%S')] $*"; }

start_pg() {
    local pgdata="$1"
    local port="$2"
    local label="$3"
    log "Starting PostgreSQL ($label) on port $port..."
    pg_ctl -D "$pgdata" -l "${LOGDIR}/${label}.log" \
        -o "-p $port -c listen_addresses=localhost" start -w
    # Wait for ready
    for i in $(seq 1 30); do
        pg_isready -h localhost -p "$port" -q && break
        sleep 1
    done
}

stop_pg() {
    local pgdata="$1"
    local label="$2"
    log "Stopping PostgreSQL ($label)..."
    pg_ctl -D "$pgdata" stop -m fast -w 2>/dev/null || true
}

init_cluster() {
    local pgdata="$1"
    local port="$2"
    local label="$3"
    if [ -d "$pgdata" ]; then
        stop_pg "$pgdata" "$label" 2>/dev/null || true
        rm -rf "$pgdata"
    fi
    log "Initializing cluster ($label)..."
    initdb -D "$pgdata" --no-locale -E UTF8 -A trust >/dev/null

    # Optimized configuration for benchmarking
    cat >> "${pgdata}/postgresql.conf" <<PGCONF
# Memory
shared_buffers = '4GB'
work_mem = '64MB'
maintenance_work_mem = '1GB'
effective_cache_size = '32GB'

# WAL / Checkpoints
wal_level = minimal
max_wal_senders = 0
checkpoint_timeout = '30min'
max_wal_size = '8GB'
min_wal_size = '2GB'

# Background writer
bgwriter_lru_maxpages = 400
bgwriter_lru_multiplier = 4.0

# Planner
random_page_cost = 1.1
effective_io_concurrency = 200
default_statistics_target = 200

# Logging (minimal for benchmarks)
log_min_messages = warning
log_checkpoints = on

# Connections
max_connections = 100
max_locks_per_transaction = 256

# Autovacuum (disable for controlled tests, enable selectively)
autovacuum = off

# RECNO / UNDO specific
undo_instant_abort_threshold = 65536
PGCONF

    start_pg "$pgdata" "$port" "$label"
}

run_sql() {
    local port="$1"
    local db="$2"
    shift 2
    psql -h localhost -p "$port" -d "$db" -X -q "$@"
}

run_pgbench() {
    local port="$1"
    local db="$2"
    local clients="$3"
    local duration="$4"
    local script="$5"
    local label="$6"
    pgbench -h localhost -p "$port" -d "$db" \
        -c "$clients" -j "$(( clients < 12 ? clients : 12 ))" \
        -T "$duration" -P 10 \
        -f "$script" \
        2>&1
}

# ── Schema creation ─────────────────────────────────────────────────

create_tpcb_schema() {
    local port="$1"
    local am="$2"
    local db="bench_${am}"

    log "Creating TPC-B schema ($am, scale=$SCALE)..."
    run_sql "$port" postgres -c "DROP DATABASE IF EXISTS ${db};"
    run_sql "$port" postgres -c "CREATE DATABASE ${db};"

    # Create tables with the specified access method
    run_sql "$port" "$db" <<SQL
CREATE TABLE pgbench_branches (
    bid int NOT NULL PRIMARY KEY,
    bbalance int,
    filler char(88)
) USING ${am};

CREATE TABLE pgbench_tellers (
    tid int NOT NULL PRIMARY KEY,
    bid int REFERENCES pgbench_branches(bid),
    tbalance int,
    filler char(84)
) USING ${am};

CREATE TABLE pgbench_accounts (
    aid int NOT NULL PRIMARY KEY,
    bid int REFERENCES pgbench_branches(bid),
    abalance int,
    filler char(84)
) USING ${am};

CREATE TABLE pgbench_history (
    tid int,
    bid int,
    aid int,
    delta int,
    mtime timestamp,
    filler char(22)
) USING ${am};
SQL

    # Populate branches and tellers
    run_sql "$port" "$db" <<SQL
INSERT INTO pgbench_branches
SELECT i, 0, '' FROM generate_series(1, ${SCALE}) i;

INSERT INTO pgbench_tellers
SELECT i, (i-1)/${SCALE}*10+1, 0, ''
FROM generate_series(1, ${SCALE}*10) i;
SQL

    # Populate accounts in batches of 1M to avoid UNDO overflow on RECNO
    local total_accounts=$(( SCALE * 100000 ))
    local batch_size=1000000
    local offset=1
    while [ "$offset" -le "$total_accounts" ]; do
        local batch_end=$(( offset + batch_size - 1 ))
        [ "$batch_end" -gt "$total_accounts" ] && batch_end=$total_accounts
        log "    Loading accounts ${offset}..${batch_end}"
        run_sql "$port" "$db" -c \
            "INSERT INTO pgbench_accounts SELECT i, (i-1)/(${total_accounts}/${SCALE})+1, 0, '' FROM generate_series(${offset}, ${batch_end}) i;"
        offset=$(( batch_end + 1 ))
    done

    run_sql "$port" "$db" -c "ANALYZE;"
    log "  Populated ${am}: ${total_accounts} accounts"
}

# ── Benchmark 1: OLTP Mixed Workload (TPC-B) ───────────────────────
# Tests: transaction throughput, latency, concurrency scaling
# RECNO advantage: in-place updates avoid tuple chaining

bench_oltp() {
    local port="$1"
    local am="$2"
    local db="bench_${am}"
    local script_file="${RESULTS_DIR}/pgbench_${am}_oltp.sql"

    cat > "$script_file" <<SQL
\set aid random(1, ${SCALE} * 100000)
\set bid random(1, ${SCALE})
\set tid random(1, ${SCALE} * 10)
\set delta random(-5000, 5000)
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
    VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
COMMIT;
SQL

    for clients in $CLIENTS_LIST; do
        log "  OLTP ($am): clients=$clients, duration=${DURATION}s"
        local outfile="${RESULTS_DIR}/oltp_${am}_c${clients}.txt"
        run_pgbench "$port" "$db" "$clients" "$DURATION" "$script_file" \
            "oltp_${am}_c${clients}" > "$outfile" 2>&1
        # Extract TPS
        local tps=$(grep "without initial connection time" "$outfile" | awk '{print $3}' || echo "0")
        local lat=$(grep "latency average" "$outfile" | awk -F'= ' '{print $2}' | awk '{print $1}' || echo "0")
        echo "oltp,${am},${clients},tps,${tps}" >> "${RESULTS_DIR}/summary.csv"
        echo "oltp,${am},${clients},lat_avg_ms,${lat}" >> "${RESULTS_DIR}/summary.csv"
        log "    TPS=$tps  Avg lat=${lat}ms"
    done
}

# ── Benchmark 2: Update-Heavy (Non-indexed Column) ─────────────────
# Tests: in-place update efficiency, table bloat
# RECNO advantage: in-place updates don't create dead tuples

bench_update_heavy() {
    local port="$1"
    local am="$2"
    local db="bench_${am}"
    local script_file="${RESULTS_DIR}/pgbench_${am}_update.sql"

    cat > "$script_file" <<SQL
\set aid random(1, ${SCALE} * 100000)
\set delta random(-5000, 5000)
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SQL

    for clients in $CLIENTS_LIST; do
        log "  Update-heavy ($am): clients=$clients, duration=${DURATION}s"
        local outfile="${RESULTS_DIR}/update_${am}_c${clients}.txt"

        # Record size before
        local size_before=$(run_sql "$port" "$db" -t -A \
            -c "SELECT pg_total_relation_size('pgbench_accounts');" 2>/dev/null)

        run_pgbench "$port" "$db" "$clients" "$DURATION" "$script_file" \
            "update_${am}_c${clients}" > "$outfile" 2>&1

        # Record size after
        local size_after=$(run_sql "$port" "$db" -t -A \
            -c "SELECT pg_total_relation_size('pgbench_accounts');" 2>/dev/null)

        local tps=$(grep "without initial connection time" "$outfile" | awk '{print $3}' || echo "0")
        local lat=$(grep "latency average" "$outfile" | awk -F'= ' '{print $2}' | awk '{print $1}' || echo "0")
        local bloat_mb=$(( (size_after - size_before) / 1024 / 1024 ))
        echo "update_heavy,${am},${clients},tps,${tps}" >> "${RESULTS_DIR}/summary.csv"
        echo "update_heavy,${am},${clients},lat_avg_ms,${lat}" >> "${RESULTS_DIR}/summary.csv"
        echo "update_heavy,${am},${clients},bloat_mb,${bloat_mb}" >> "${RESULTS_DIR}/summary.csv"
        log "    TPS=$tps  Avg lat=${lat}ms  Bloat=${bloat_mb}MB"
    done
}

# ── Benchmark 3: Read-Heavy Under Write Load ───────────────────────
# Tests: reader-writer contention, snapshot isolation overhead
# RECNO advantage: timestamp comparison vs CLOG lookup for visibility

bench_read_write() {
    local port="$1"
    local am="$2"
    local db="bench_${am}"
    local read_script="${RESULTS_DIR}/pgbench_${am}_read.sql"
    local write_script="${RESULTS_DIR}/pgbench_${am}_write.sql"

    cat > "$read_script" <<SQL
\set aid random(1, ${SCALE} * 100000)
SELECT aid, abalance FROM pgbench_accounts WHERE aid = :aid;
SQL

    cat > "$write_script" <<SQL
\set aid random(1, ${SCALE} * 100000)
\set delta random(-5000, 5000)
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SQL

    # 80% readers, 20% writers across various concurrency levels
    for clients in 12 24 48; do
        local read_clients=$(( clients * 4 / 5 ))
        local write_clients=$(( clients - read_clients ))
        [ "$write_clients" -lt 1 ] && write_clients=1

        log "  Read-write ($am): ${read_clients}R + ${write_clients}W, duration=${DURATION}s"
        local outfile="${RESULTS_DIR}/readwrite_${am}_c${clients}.txt"

        pgbench -h localhost -p "$port" -d "$db" \
            -c "$clients" -j "$(( clients < 12 ? clients : 12 ))" \
            -T "$DURATION" -P 10 \
            -f "${read_script}@$((read_clients * 100 / clients))" \
            -f "${write_script}@$((write_clients * 100 / clients))" \
            > "$outfile" 2>&1

        local tps=$(grep "without initial connection time" "$outfile" | awk '{print $3}' || echo "0")
        local lat=$(grep "latency average" "$outfile" | awk -F'= ' '{print $2}' | awk '{print $1}' || echo "0")
        echo "read_write,${am},${clients},tps,${tps}" >> "${RESULTS_DIR}/summary.csv"
        echo "read_write,${am},${clients},lat_avg_ms,${lat}" >> "${RESULTS_DIR}/summary.csv"
        log "    TPS=$tps  Avg lat=${lat}ms"
    done
}

# ── Benchmark 4: Point Lookup Latency ───────────────────────────────
# Tests: index scan performance, visibility check overhead
# Measures 99th percentile latency under load

bench_point_lookup() {
    local port="$1"
    local am="$2"
    local db="bench_${am}"
    local script_file="${RESULTS_DIR}/pgbench_${am}_point.sql"

    cat > "$script_file" <<SQL
\set aid random(1, ${SCALE} * 100000)
SELECT aid, abalance, filler FROM pgbench_accounts WHERE aid = :aid;
SQL

    for clients in 1 4 12 24; do
        log "  Point lookup ($am): clients=$clients, duration=${DURATION}s"
        local outfile="${RESULTS_DIR}/point_${am}_c${clients}.txt"

        pgbench -h localhost -p "$port" -d "$db" \
            -c "$clients" -j "$(( clients < 12 ? clients : 12 ))" \
            -T "$DURATION" -P 10 --latency-limit=100 \
            -f "$script_file" \
            > "$outfile" 2>&1

        local tps=$(grep "without initial connection time" "$outfile" | awk '{print $3}' || echo "0")
        local lat=$(grep "latency average" "$outfile" | awk -F'= ' '{print $2}' | awk '{print $1}' || echo "0")
        echo "point_lookup,${am},${clients},tps,${tps}" >> "${RESULTS_DIR}/summary.csv"
        echo "point_lookup,${am},${clients},lat_avg_ms,${lat}" >> "${RESULTS_DIR}/summary.csv"
        log "    TPS=$tps  Avg lat=${lat}ms"
    done
}

# ── Benchmark 5: Bulk Insert ────────────────────────────────────────
# Tests: insert throughput, UNDO overhead on inserts

bench_bulk_insert() {
    local port="$1"
    local am="$2"
    local db="bench_${am}"

    log "  Bulk insert ($am): ${LARGE_ROWS} rows..."

    run_sql "$port" "$db" <<SQL
DROP TABLE IF EXISTS bench_bulk;
CREATE TABLE bench_bulk (
    id bigint PRIMARY KEY,
    val1 int,
    val2 int,
    data text
) USING ${am};
SQL

    local start_time=$(date +%s%N)
    # Insert in batches of 1M to avoid UNDO overflow on RECNO
    local batch_size=1000000
    local offset=1
    while [ "$offset" -le "$LARGE_ROWS" ]; do
        local batch_end=$(( offset + batch_size - 1 ))
        [ "$batch_end" -gt "$LARGE_ROWS" ] && batch_end=$LARGE_ROWS
        run_sql "$port" "$db" -c \
            "INSERT INTO bench_bulk SELECT i, (random()*1000000)::int, (random()*1000000)::int, 'payload_' || lpad(i::text, 20, '0') FROM generate_series(${offset}, ${batch_end}) i;"
        offset=$(( batch_end + 1 ))
    done

    local end_time=$(date +%s%N)
    local elapsed_ms=$(( (end_time - start_time) / 1000000 ))
    local rows_per_sec=$(( LARGE_ROWS * 1000 / elapsed_ms ))

    local table_size=$(run_sql "$port" "$db" -t -A \
        -c "SELECT pg_size_pretty(pg_total_relation_size('bench_bulk'));" 2>/dev/null)

    echo "bulk_insert,${am},1,rows_per_sec,${rows_per_sec}" >> "${RESULTS_DIR}/summary.csv"
    echo "bulk_insert,${am},1,elapsed_ms,${elapsed_ms}" >> "${RESULTS_DIR}/summary.csv"
    echo "bulk_insert,${am},1,table_size,${table_size}" >> "${RESULTS_DIR}/summary.csv"
    log "    ${rows_per_sec} rows/sec  Elapsed=${elapsed_ms}ms  Size=${table_size}"
}

# ── Benchmark 6: Sequential Scan Under DML ─────────────────────────
# Tests: scan performance with concurrent modifications
# RECNO advantage: no CLOG lookups, timestamp comparison is cheaper

bench_seqscan_dml() {
    local port="$1"
    local am="$2"
    local db="bench_${am}"
    local scan_script="${RESULTS_DIR}/pgbench_${am}_scan.sql"
    local dml_script="${RESULTS_DIR}/pgbench_${am}_dml.sql"

    cat > "$scan_script" <<SQL
SELECT count(*), sum(abalance), avg(abalance)
FROM pgbench_accounts
WHERE aid BETWEEN 1 AND 1000000;
SQL

    cat > "$dml_script" <<SQL
\set aid random(1, ${SCALE} * 100000)
\set delta random(-5000, 5000)
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SQL

    # 4 scanners + 8 writers
    log "  SeqScan+DML ($am): 4 scanners + 8 writers, duration=${DURATION}s"
    local outfile="${RESULTS_DIR}/seqscan_dml_${am}.txt"

    pgbench -h localhost -p "$port" -d "$db" \
        -c 12 -j 12 -T "$DURATION" -P 10 \
        -f "${scan_script}@33" \
        -f "${dml_script}@67" \
        > "$outfile" 2>&1

    local tps=$(grep "without initial connection time" "$outfile" | awk '{print $3}' || echo "0")
    local lat=$(grep "latency average" "$outfile" | awk -F'= ' '{print $2}' | awk '{print $1}' || echo "0")
    echo "seqscan_dml,${am},12,tps,${tps}" >> "${RESULTS_DIR}/summary.csv"
    echo "seqscan_dml,${am},12,lat_avg_ms,${lat}" >> "${RESULTS_DIR}/summary.csv"
    log "    TPS=$tps  Avg lat=${lat}ms"
}

# ── Benchmark 7: VACUUM Overhead ───────────────────────────────────
# Tests: dead tuple accumulation, VACUUM speed, post-VACUUM performance
# RECNO advantage: in-place updates = less dead tuple bloat

bench_vacuum() {
    local port="$1"
    local am="$2"
    local db="bench_${am}"

    log "  VACUUM overhead ($am)..."

    # Create dedicated table for vacuum test
    run_sql "$port" "$db" <<SQL
DROP TABLE IF EXISTS bench_vacuum;
CREATE TABLE bench_vacuum (
    id int PRIMARY KEY,
    counter int DEFAULT 0,
    data text DEFAULT repeat('x', 100)
) USING ${am};
SQL
    # Insert in batches of 1M to avoid UNDO overflow on RECNO
    local offset=1
    while [ "$offset" -le 5000000 ]; do
        local batch_end=$(( offset + 999999 ))
        [ "$batch_end" -gt 5000000 ] && batch_end=5000000
        run_sql "$port" "$db" -c \
            "INSERT INTO bench_vacuum SELECT i, 0, repeat('x', 100) FROM generate_series(${offset}, ${batch_end}) i;"
        offset=$(( batch_end + 1 ))
    done
    run_sql "$port" "$db" -c "ANALYZE bench_vacuum;"

    local size_initial=$(run_sql "$port" "$db" -t -A \
        -c "SELECT pg_total_relation_size('bench_vacuum');" 2>/dev/null)

    # Generate dead tuples: update every row once (non-indexed column)
    log "    Generating dead tuples (5M updates)..."
    local start_time=$(date +%s%N)
    run_sql "$port" "$db" -c \
        "UPDATE bench_vacuum SET counter = counter + 1;"
    local update_time=$(( ($(date +%s%N) - start_time) / 1000000 ))

    local size_after_updates=$(run_sql "$port" "$db" -t -A \
        -c "SELECT pg_total_relation_size('bench_vacuum');" 2>/dev/null)

    # VACUUM timing
    log "    Running VACUUM..."
    local vac_start=$(date +%s%N)
    run_sql "$port" "$db" -c "VACUUM bench_vacuum;"
    local vacuum_time=$(( ($(date +%s%N) - vac_start) / 1000000 ))

    local size_after_vacuum=$(run_sql "$port" "$db" -t -A \
        -c "SELECT pg_total_relation_size('bench_vacuum');" 2>/dev/null)

    local bloat_pct=$(( (size_after_updates - size_initial) * 100 / size_initial ))
    local reclaim_pct=0
    if [ "$size_after_updates" -gt "$size_initial" ]; then
        reclaim_pct=$(( (size_after_updates - size_after_vacuum) * 100 / (size_after_updates - size_initial) ))
    fi

    echo "vacuum,${am},1,update_5m_ms,${update_time}" >> "${RESULTS_DIR}/summary.csv"
    echo "vacuum,${am},1,bloat_pct,${bloat_pct}" >> "${RESULTS_DIR}/summary.csv"
    echo "vacuum,${am},1,vacuum_ms,${vacuum_time}" >> "${RESULTS_DIR}/summary.csv"
    echo "vacuum,${am},1,reclaim_pct,${reclaim_pct}" >> "${RESULTS_DIR}/summary.csv"
    echo "vacuum,${am},1,size_initial_mb,$(( size_initial / 1024 / 1024 ))" >> "${RESULTS_DIR}/summary.csv"
    echo "vacuum,${am},1,size_after_upd_mb,$(( size_after_updates / 1024 / 1024 ))" >> "${RESULTS_DIR}/summary.csv"
    echo "vacuum,${am},1,size_after_vac_mb,$(( size_after_vacuum / 1024 / 1024 ))" >> "${RESULTS_DIR}/summary.csv"

    log "    Update: ${update_time}ms  Bloat: ${bloat_pct}%  VACUUM: ${vacuum_time}ms  Reclaimed: ${reclaim_pct}%"
}

# ── Benchmark 8: Long-Running Transaction Impact ───────────────────
# Tests: how a long-running read transaction affects write throughput
# RECNO advantage: old versions live in UNDO, not main table

bench_long_txn() {
    local port="$1"
    local am="$2"
    local db="bench_${am}"
    local write_script="${RESULTS_DIR}/pgbench_${am}_longtxn_write.sql"

    cat > "$write_script" <<SQL
\set aid random(1, ${SCALE} * 100000)
\set delta random(-5000, 5000)
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SQL

    log "  Long-txn impact ($am): 24 writers + 1 long reader, ${DURATION}s"
    local outfile="${RESULTS_DIR}/longtxn_${am}.txt"

    # Start a long-running read transaction in the background
    run_sql "$port" "$db" -c "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM pgbench_accounts; SELECT pg_sleep(${DURATION});" &
    local long_pid=$!

    sleep 2  # Let the snapshot establish

    # Run concurrent writes
    pgbench -h localhost -p "$port" -d "$db" \
        -c 24 -j 12 -T "$DURATION" -P 10 \
        -f "$write_script" \
        > "$outfile" 2>&1

    # Kill the long transaction
    kill "$long_pid" 2>/dev/null || true
    wait "$long_pid" 2>/dev/null || true

    local tps=$(grep "without initial connection time" "$outfile" | awk '{print $3}' || echo "0")
    local lat=$(grep "latency average" "$outfile" | awk -F'= ' '{print $2}' | awk '{print $1}' || echo "0")

    local size_after=$(run_sql "$port" "$db" -t -A \
        -c "SELECT pg_total_relation_size('pgbench_accounts');" 2>/dev/null)

    echo "long_txn,${am},24,tps,${tps}" >> "${RESULTS_DIR}/summary.csv"
    echo "long_txn,${am},24,lat_avg_ms,${lat}" >> "${RESULTS_DIR}/summary.csv"
    echo "long_txn,${am},24,table_size_mb,$(( size_after / 1024 / 1024 ))" >> "${RESULTS_DIR}/summary.csv"
    log "    TPS=$tps  Avg lat=${lat}ms  Table size=$(( size_after / 1024 / 1024 ))MB"
}

# ── Benchmark 9: Cache Pressure (Large Working Set) ────────────────
# Tests: performance when data doesn't fit in shared_buffers
# shared_buffers=4GB, but table is ~3GB+, forcing evictions

bench_cache_pressure() {
    local port="$1"
    local am="$2"
    local db="bench_${am}"
    local script_file="${RESULTS_DIR}/pgbench_${am}_cache.sql"

    # Use the large bulk table (if it exists) or the accounts table
    # With 20M rows and ~150 bytes each, ~3GB of data
    run_sql "$port" "$db" -c "SELECT count(*) FROM bench_bulk;" 2>/dev/null || {
        log "    Skipping cache pressure (no bench_bulk table)"
        return
    }

    cat > "$script_file" <<SQL
\set id random(1, ${LARGE_ROWS})
SELECT id, val1, val2, data FROM bench_bulk WHERE id = :id;
SQL

    for clients in 12 24; do
        log "  Cache pressure ($am): clients=$clients, duration=${DURATION}s"
        local outfile="${RESULTS_DIR}/cache_${am}_c${clients}.txt"

        pgbench -h localhost -p "$port" -d "$db" \
            -c "$clients" -j 12 -T "$DURATION" -P 10 \
            -f "$script_file" \
            > "$outfile" 2>&1

        local tps=$(grep "without initial connection time" "$outfile" | awk '{print $3}' || echo "0")
        local lat=$(grep "latency average" "$outfile" | awk -F'= ' '{print $2}' | awk '{print $1}' || echo "0")
        echo "cache_pressure,${am},${clients},tps,${tps}" >> "${RESULTS_DIR}/summary.csv"
        echo "cache_pressure,${am},${clients},lat_avg_ms,${lat}" >> "${RESULTS_DIR}/summary.csv"
        log "    TPS=$tps  Avg lat=${lat}ms"
    done
}

# ── Results Summary ─────────────────────────────────────────────────

print_summary() {
    log ""
    log "═══════════════════════════════════════════════════════════════"
    log "                 RECNO vs HEAP Benchmark Results"
    log "═══════════════════════════════════════════════════════════════"
    log ""

    if [ ! -f "${RESULTS_DIR}/summary.csv" ]; then
        log "No results found."
        return
    fi

    # Print side-by-side comparison for TPS metrics
    log "TPS Comparison (higher is better):"
    log "─────────────────────────────────────────────────────────────"
    printf "%-20s %6s %10s %10s %8s\n" "Benchmark" "Conc" "HEAP" "RECNO" "Ratio"
    log "─────────────────────────────────────────────────────────────"

    local benchmarks=$(awk -F, '{print $1}' "${RESULTS_DIR}/summary.csv" | sort -u)
    for bench in $benchmarks; do
        local clients_list=$(grep "^${bench}," "${RESULTS_DIR}/summary.csv" | grep ",tps," | awk -F, '{print $3}' | sort -un)
        for c in $clients_list; do
            local heap_tps=$(grep "^${bench},heap,${c},tps," "${RESULTS_DIR}/summary.csv" | awk -F, '{print $5}' | head -1)
            local recno_tps=$(grep "^${bench},recno,${c},tps," "${RESULTS_DIR}/summary.csv" | awk -F, '{print $5}' | head -1)
            if [ -n "$heap_tps" ] && [ -n "$recno_tps" ] && [ "$heap_tps" != "0" ]; then
                local ratio=$(echo "$recno_tps $heap_tps" | awk '{printf "%.2f", $1/$2}')
                printf "%-20s %6s %10s %10s %8sx\n" "$bench" "$c" "$heap_tps" "$recno_tps" "$ratio"
            fi
        done
    done

    log ""
    log "Bloat & VACUUM Comparison:"
    log "─────────────────────────────────────────────────────────────"
    for metric in bloat_pct vacuum_ms reclaim_pct size_initial_mb size_after_upd_mb size_after_vac_mb; do
        local heap_val=$(grep "^vacuum,heap,1,${metric}," "${RESULTS_DIR}/summary.csv" | awk -F, '{print $5}' | head -1)
        local recno_val=$(grep "^vacuum,recno,1,${metric}," "${RESULTS_DIR}/summary.csv" | awk -F, '{print $5}' | head -1)
        if [ -n "$heap_val" ] && [ -n "$recno_val" ]; then
            printf "  %-25s  HEAP=%-12s  RECNO=%-12s\n" "$metric" "$heap_val" "$recno_val"
        fi
    done

    log ""
    log "Full results: ${RESULTS_DIR}/summary.csv"
    log "═══════════════════════════════════════════════════════════════"
}

# ── Main ────────────────────────────────────────────────────────────

main() {
    log "RECNO vs HEAP Benchmark Suite"
    log "Machine: $(hostname) ($(nproc) cores, $(free -h | awk '/Mem:/{print $2}') RAM)"
    log "Duration per test: ${DURATION}s  Scale: ${SCALE}  Clients: ${CLIENTS_LIST}"
    log "Results: ${RESULTS_DIR}"
    log ""

    echo "benchmark,am,clients,metric,value" > "${RESULTS_DIR}/summary.csv"

    # ── Initialize both clusters ──
    init_cluster "$PGDATA_HEAP" "$PORT_HEAP" "heap"
    init_cluster "$PGDATA_RECNO" "$PORT_RECNO" "recno"

    # ── Create schemas ──
    create_tpcb_schema "$PORT_HEAP" "heap"
    create_tpcb_schema "$PORT_RECNO" "recno"

    # ── Run benchmarks ──

    log ""
    log "━━━ Benchmark 1: OLTP Mixed Workload ━━━"
    bench_oltp "$PORT_HEAP" "heap"
    bench_oltp "$PORT_RECNO" "recno"

    log ""
    log "━━━ Benchmark 2: Update-Heavy Workload ━━━"
    bench_update_heavy "$PORT_HEAP" "heap"
    bench_update_heavy "$PORT_RECNO" "recno"

    log ""
    log "━━━ Benchmark 3: Read-Heavy Under Write Load ━━━"
    bench_read_write "$PORT_HEAP" "heap"
    bench_read_write "$PORT_RECNO" "recno"

    log ""
    log "━━━ Benchmark 4: Point Lookup Latency ━━━"
    bench_point_lookup "$PORT_HEAP" "heap"
    bench_point_lookup "$PORT_RECNO" "recno"

    log ""
    log "━━━ Benchmark 5: Bulk Insert ━━━"
    bench_bulk_insert "$PORT_HEAP" "heap"
    bench_bulk_insert "$PORT_RECNO" "recno"

    log ""
    log "━━━ Benchmark 6: Sequential Scan Under DML ━━━"
    bench_seqscan_dml "$PORT_HEAP" "heap"
    bench_seqscan_dml "$PORT_RECNO" "recno"

    log ""
    log "━━━ Benchmark 7: VACUUM Overhead ━━━"
    bench_vacuum "$PORT_HEAP" "heap"
    bench_vacuum "$PORT_RECNO" "recno"

    log ""
    log "━━━ Benchmark 8: Long-Running Transaction Impact ━━━"
    bench_long_txn "$PORT_HEAP" "heap"
    bench_long_txn "$PORT_RECNO" "recno"

    log ""
    log "━━━ Benchmark 9: Cache Pressure ━━━"
    bench_cache_pressure "$PORT_HEAP" "heap"
    bench_cache_pressure "$PORT_RECNO" "recno"

    # ── Results ──
    print_summary

    # ── Cleanup ──
    stop_pg "$PGDATA_HEAP" "heap"
    stop_pg "$PGDATA_RECNO" "recno"

    log ""
    log "Done. Full results in: ${RESULTS_DIR}"
}

main "$@" 2>&1 | tee "${RESULTS_DIR}/benchmark.log"
