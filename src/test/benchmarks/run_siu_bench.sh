#!/usr/bin/env bash
#
# run_siu_bench.sh - Selective Index Updates (SIU) benchmark suite
#
# Compares PostgreSQL performance between master (no SIU) and tepid (SIU)
# branches across 8 workloads measuring overhead, benefit, bloat, reads,
# HOT chain length, vacuum behavior, and standard TPC-B.
#
# Usage:
#   ./run_siu_bench.sh [options]
#
# Options (via environment variables):
#   BENCH_BASE     - Working directory (default: /scratch/siu-bench)
#   REPO_DIR       - Source repo (default: auto-detect from script location)
#   SCALE          - pgbench scale factor for W1/W8 (default: 50)
#   DURATION       - Test duration in seconds per run (default: 120)
#   WARMUP         - Warmup duration in seconds (default: 30)
#   ITERATIONS     - Measurement iterations per config (default: 3)
#   ROW_COUNT      - Rows for SIU-specific tables (default: 500000)
#   WORKLOADS      - Comma-separated list of workloads to run (default: all)
#                    e.g., WORKLOADS=W1,W2,W3
#   SKIP_BUILD     - Set to 1 to skip build phase (default: 0)
#   SKIP_SETUP     - Set to 1 to skip worktree/build setup (default: 0)
#   USE_TASKSET    - Set to 1 to pin server to CPUs 0-11 (default: 0)
#   PERF_STAT      - Set to 1 to collect perf stat for W1 (default: 0)
#
# Designed for host "meh": 24 cores, 125GB RAM, /scratch available.
#

set -euo pipefail

###############################################################################
# Defaults
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

BENCH_BASE="${BENCH_BASE:-/scratch/siu-bench}"
REPO_DIR="${REPO_DIR:-$REPO_ROOT}"
SCALE="${SCALE:-50}"
DURATION="${DURATION:-120}"
WARMUP="${WARMUP:-30}"
ITERATIONS="${ITERATIONS:-3}"
ROW_COUNT="${ROW_COUNT:-500000}"
WORKLOADS="${WORKLOADS:-W1,W2,W3,W4,W5,W6,W7,W8}"
SKIP_BUILD="${SKIP_BUILD:-0}"
SKIP_SETUP="${SKIP_SETUP:-0}"
USE_TASKSET="${USE_TASKSET:-0}"
PERF_STAT="${PERF_STAT:-0}"

RESULTS_DIR="${BENCH_BASE}/results"
LOGFILE="${RESULTS_DIR}/siu_bench_$(date +%Y%m%d_%H%M%S).log"

# Ports for the two instances
MASTER_PORT=54330
TEPID_PORT=54331

# Client count lists per workload
W1_CLIENTS="1 2 4 8 16"
W2_CLIENTS="1 2 4 8"
W3_CLIENTS="1 4 8"
W5_CLIENTS="1 4"
W8_CLIENTS="1 2 4 8 16"

###############################################################################
# Paths derived from BENCH_BASE
###############################################################################

MASTER_SRC="${BENCH_BASE}/master"
TEPID_SRC="${BENCH_BASE}/tepid"
MASTER_INSTALL="${BENCH_BASE}/install-master"
TEPID_INSTALL="${BENCH_BASE}/install-tepid"

###############################################################################
# Logging
###############################################################################

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOGFILE}"
}

log_header() {
    log "============================================================"
    log "$*"
    log "============================================================"
}

###############################################################################
# Helper: find installed PG binaries for a branch
###############################################################################

# pg_bindir INSTALL_PREFIX
# Returns the bin directory under a DESTDIR-style install
pg_bindir() {
    local prefix="$1"
    for candidate in \
        "$prefix/usr/local/pgsql/bin" \
        "$prefix/usr/local/bin" \
        "$prefix/bin"; do
        if [ -x "$candidate/initdb" ]; then
            echo "$candidate"
            return 0
        fi
    done
    echo "ERROR: Cannot find PG binaries under $prefix" >&2
    return 1
}

# pg_libdir INSTALL_PREFIX
pg_libdir() {
    local prefix="$1"
    for candidate in \
        "$prefix/usr/local/pgsql/lib64" \
        "$prefix/usr/local/pgsql/lib" \
        "$prefix/usr/local/lib64" \
        "$prefix/usr/local/lib" \
        "$prefix/lib64" \
        "$prefix/lib"; do
        if [ -f "$candidate/libpq.so" ] || [ -f "$candidate/libpq.dylib" ]; then
            echo "$candidate"
            return 0
        fi
    done
    echo "ERROR: Cannot find PG libraries under $prefix" >&2
    return 1
}

###############################################################################
# Cluster lifecycle
###############################################################################

# init_cluster PGDATA PORT LABEL
init_cluster() {
    local pgdata="$1" port="$2" label="$3"
    local bindir="$4"

    log "Initializing $label cluster at $pgdata (port $port)"
    rm -rf "$pgdata"
    mkdir -p "$pgdata"
    "$bindir/initdb" -D "$pgdata" --no-locale -E UTF8 -A trust >/dev/null 2>&1

    cat >> "$pgdata/postgresql.conf" <<PGCONF
port = ${port}
listen_addresses = '127.0.0.1'
unix_socket_directories = '${pgdata}'
shared_buffers = '4GB'
effective_cache_size = '96GB'
work_mem = '64MB'
maintenance_work_mem = '1GB'
max_connections = 100
max_wal_size = '16GB'
checkpoint_timeout = '30min'
wal_level = minimal
max_wal_senders = 0
fsync = off
synchronous_commit = off
full_page_writes = off
random_page_cost = 1.1
effective_io_concurrency = 200
log_min_messages = warning
logging_collector = off
PGCONF
}

# start_cluster PGDATA LABEL BINDIR
start_cluster() {
    local pgdata="$1" label="$2" bindir="$3"
    local pg_ctl="$bindir/pg_ctl"

    log "Starting $label server"
    if [ "$USE_TASKSET" = "1" ] && command -v taskset >/dev/null 2>&1; then
        # Pin to CPUs 0-11 (first 12 of 24 cores)
        taskset -c 0-11 "$pg_ctl" start -D "$pgdata" -l "$pgdata/server.log" -w -t 30 >/dev/null 2>&1
    else
        "$pg_ctl" start -D "$pgdata" -l "$pgdata/server.log" -w -t 30 >/dev/null 2>&1
    fi
}

# stop_cluster PGDATA LABEL BINDIR
stop_cluster() {
    local pgdata="$1" label="$2" bindir="$3"
    local pg_ctl="$bindir/pg_ctl"

    log "Stopping $label server"
    "$pg_ctl" stop -D "$pgdata" -m fast 2>/dev/null || true
}

# wait_for_ready PORT LABEL BINDIR
wait_for_ready() {
    local port="$1" label="$2" bindir="$3"
    local retries=30
    while [ "$retries" -gt 0 ]; do
        if "$bindir/pg_isready" -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then
            return 0
        fi
        retries=$((retries - 1))
        sleep 1
    done
    echo "ERROR: $label server on port $port did not become ready" >&2
    return 1
}

# run_sql PORT BINDIR SQL [DBNAME]
run_sql() {
    local port="$1" bindir="$2" sql="$3" dbname="${4:-postgres}"
    "$bindir/psql" -h 127.0.0.1 -p "$port" -X -q -d "$dbname" -c "$sql"
}

# run_sql_file PORT BINDIR SQLFILE [DBNAME]
run_sql_file() {
    local port="$1" bindir="$2" sqlfile="$3" dbname="${4:-postgres}"
    "$bindir/psql" -h 127.0.0.1 -p "$port" -X -q -d "$dbname" -f "$sqlfile"
}

# fetch_val PORT BINDIR SQL [DBNAME] -> stdout
fetch_val() {
    local port="$1" bindir="$2" sql="$3" dbname="${4:-postgres}"
    "$bindir/psql" -h 127.0.0.1 -p "$port" -X -t -A -d "$dbname" -c "$sql"
}

###############################################################################
# pgbench helpers
###############################################################################

# run_pgbench_tps PORT BINDIR CLIENTS DURATION [SCRIPT_FILE]
# Sets RESULT_TPS and RESULT_LAT
run_pgbench_tps() {
    local port="$1" bindir="$2" clients="$3" dur="$4"
    local script_file="${5:-}"
    local output

    RESULT_TPS=""
    RESULT_LAT=""

    local pgbench_args=(-h 127.0.0.1 -p "$port" -c "$clients" -j "$clients"
                        -T "$dur" --no-vacuum)
    if [ -n "$script_file" ]; then
        pgbench_args+=(-f "$script_file")
    fi
    pgbench_args+=(postgres)

    output=$("$bindir/pgbench" "${pgbench_args[@]}" 2>&1) || true

    # PG19+ says "without initial connection time"; older said "excluding connections establishing"
    RESULT_TPS=$(echo "$output" | grep -i -E "without initial connection time|excluding connections establishing" \
                 | sed 's/.*= *//' | sed 's/ .*//' || echo "")
    RESULT_LAT=$(echo "$output" | grep -i "latency average" \
                 | sed 's/.*= *//' | sed 's/ .*//' || echo "")

    if [ -z "$RESULT_TPS" ]; then RESULT_TPS="FAIL"; fi
    if [ -z "$RESULT_LAT" ]; then RESULT_LAT="FAIL"; fi
}

###############################################################################
# Stats collection
###############################################################################

# collect_table_stats PORT BINDIR TABLE -> prints CSV line
# Format: table_size,index_size,total_size,n_live_tup,n_dead_tup,n_tup_hot_upd,n_tup_newpage_upd
collect_table_stats() {
    local port="$1" bindir="$2" table="$3"
    fetch_val "$port" "$bindir" "
        SELECT pg_relation_size('${table}')
            || ',' || pg_indexes_size('${table}')
            || ',' || pg_total_relation_size('${table}')
            || ',' || COALESCE(n_live_tup, 0)
            || ',' || COALESCE(n_dead_tup, 0)
            || ',' || COALESCE(n_tup_hot_upd, 0)
            || ',' || COALESCE(n_tup_newpage_upd, 0)
        FROM pg_stat_user_tables
        WHERE relname = '${table}';"
}

# collect_index_stats PORT BINDIR TABLE -> prints CSV lines
# Format: index_name,idx_scan,idx_tup_read,idx_tup_fetch,index_size
collect_index_stats() {
    local port="$1" bindir="$2" table="$3"
    fetch_val "$port" "$bindir" "
        SELECT i.indexrelname
            || ',' || COALESCE(i.idx_scan, 0)
            || ',' || COALESCE(i.idx_tup_read, 0)
            || ',' || COALESCE(i.idx_tup_fetch, 0)
            || ',' || pg_relation_size(i.indexrelid)
        FROM pg_stat_user_indexes i
        WHERE i.relname = '${table}'
        ORDER BY i.indexrelname;"
}

# reset_stats PORT BINDIR
reset_stats() {
    local port="$1" bindir="$2"
    run_sql "$port" "$bindir" "SELECT pg_stat_reset();"
}

###############################################################################
# Median calculation (from sorted values)
###############################################################################

# median VAL1 VAL2 VAL3 ...
median() {
    local sorted
    sorted=$(printf '%s\n' "$@" | sort -g)
    local count=$#
    local mid=$((count / 2))
    if [ $((count % 2)) -eq 1 ]; then
        echo "$sorted" | sed -n "$((mid + 1))p"
    else
        local a b
        a=$(echo "$sorted" | sed -n "${mid}p")
        b=$(echo "$sorted" | sed -n "$((mid + 1))p")
        echo "$a $b" | awk '{printf "%.1f", ($1 + $2) / 2}'
    fi
}

# p5 and p95 approximation for 3 samples: min and max
p5() { printf '%s\n' "$@" | sort -g | head -1; }
p95() { printf '%s\n' "$@" | sort -g | tail -1; }

###############################################################################
# Cleanup
###############################################################################

MASTER_PGDATA="${BENCH_BASE}/pgdata-master"
TEPID_PGDATA="${BENCH_BASE}/pgdata-tepid"

cleanup_clusters() {
    if [ -n "${MASTER_BINDIR:-}" ]; then
        "$MASTER_BINDIR/pg_ctl" stop -D "$MASTER_PGDATA" -m immediate 2>/dev/null || true
    fi
    if [ -n "${TEPID_BINDIR:-}" ]; then
        "$TEPID_BINDIR/pg_ctl" stop -D "$TEPID_PGDATA" -m immediate 2>/dev/null || true
    fi
}

trap cleanup_clusters EXIT

###############################################################################
# Build phase
###############################################################################

setup_and_build() {
    log_header "SETUP: Creating worktrees and building"

    mkdir -p "$BENCH_BASE"

    if [ ! -d "$MASTER_SRC" ]; then
        log "Creating master worktree..."
        cd "$REPO_DIR"
        git worktree add "$MASTER_SRC" master 2>/dev/null || \
            git worktree add "$MASTER_SRC" origin/master 2>/dev/null || \
            { log "ERROR: Cannot create master worktree"; exit 1; }
    fi

    if [ ! -d "$TEPID_SRC" ]; then
        log "Creating tepid worktree..."
        cd "$REPO_DIR"
        git worktree add "$TEPID_SRC" tepid 2>/dev/null || \
            { log "ERROR: Cannot create tepid worktree"; exit 1; }
    fi

    # Build master
    if [ ! -d "$MASTER_INSTALL" ] || [ "$SKIP_BUILD" != "1" ]; then
        log "Building master branch..."
        cd "$MASTER_SRC"
        if [ ! -d build ]; then
            meson setup build -Dcassert=false -Doptimization=2 \
                --prefix=/usr/local/pgsql 2>&1 | tail -5 | tee -a "$LOGFILE"
        fi
        ninja -C build 2>&1 | tail -3 | tee -a "$LOGFILE"
        rm -rf "$MASTER_INSTALL"
        DESTDIR="$MASTER_INSTALL" ninja -C build install 2>&1 | tail -3 | tee -a "$LOGFILE"
        log "Master build complete."
    fi

    # Build tepid
    if [ ! -d "$TEPID_INSTALL" ] || [ "$SKIP_BUILD" != "1" ]; then
        log "Building tepid branch..."
        cd "$TEPID_SRC"
        if [ ! -d build ]; then
            meson setup build -Dcassert=false -Doptimization=2 \
                --prefix=/usr/local/pgsql 2>&1 | tail -5 | tee -a "$LOGFILE"
        fi
        ninja -C build 2>&1 | tail -3 | tee -a "$LOGFILE"
        rm -rf "$TEPID_INSTALL"
        DESTDIR="$TEPID_INSTALL" ninja -C build install 2>&1 | tail -3 | tee -a "$LOGFILE"
        log "Tepid build complete."
    fi

    cd "$BENCH_BASE"
}

###############################################################################
# CSV output helpers
###############################################################################

csv_file() {
    echo "${RESULTS_DIR}/results_${1}.csv"
}

csv_header() {
    local workload="$1"
    shift
    echo "$@" > "$(csv_file "$workload")"
}

csv_append() {
    local workload="$1"
    shift
    echo "$@" >> "$(csv_file "$workload")"
}

###############################################################################
# Workload W1: Non-SIU HOT overhead
###############################################################################

run_w1() {
    log_header "W1: Non-SIU HOT overhead (code-present but not triggered)"
    csv_header "W1" "branch,clients,iteration,tps,latency_ms"

    for branch in master tepid; do
        local bindir port pgdata
        if [ "$branch" = "master" ]; then
            bindir="$MASTER_BINDIR"; port=$MASTER_PORT; pgdata="$MASTER_PGDATA"
        else
            bindir="$TEPID_BINDIR"; port=$TEPID_PORT; pgdata="$TEPID_PGDATA"
        fi

        init_cluster "$pgdata" "$port" "$branch" "$bindir"
        start_cluster "$pgdata" "$branch" "$bindir"
        wait_for_ready "$port" "$branch" "$bindir"

        log "  Initializing pgbench (scale=$SCALE)..."
        "$bindir/pgbench" -h 127.0.0.1 -p "$port" -i -s "$SCALE" postgres >/dev/null 2>&1

        for clients in $W1_CLIENTS; do
            log "  Warmup: $branch, $clients clients, ${WARMUP}s"
            run_pgbench_tps "$port" "$bindir" "$clients" "$WARMUP"

            local tps_vals=()
            local lat_vals=()
            for iter in $(seq 1 "$ITERATIONS"); do
                log "  Run $iter/$ITERATIONS: $branch, $clients clients, ${DURATION}s"
                run_pgbench_tps "$port" "$bindir" "$clients" "$DURATION"
                csv_append "W1" "$branch,$clients,$iter,$RESULT_TPS,$RESULT_LAT"
                if [ "$RESULT_TPS" != "FAIL" ]; then tps_vals+=("$RESULT_TPS"); fi
                if [ "$RESULT_LAT" != "FAIL" ]; then lat_vals+=("$RESULT_LAT"); fi
            done

            if [ ${#tps_vals[@]} -gt 0 ]; then
                local med_tps
                med_tps=$(median "${tps_vals[@]}")
                log "  Median TPS ($branch, $clients clients): $med_tps"
            fi
        done

        # Collect perf stat for 1 client if requested
        if [ "$PERF_STAT" = "1" ] && [ "$branch" = "tepid" ] && command -v perf >/dev/null 2>&1; then
            log "  Collecting perf stat (1 client, 30s)..."
            local perf_out="${RESULTS_DIR}/perf_stat_W1_${branch}.txt"
            perf stat -d -p "$(head -1 "$pgdata/postmaster.pid")" \
                -- sleep 30 > "$perf_out" 2>&1 &
            local perf_pid=$!
            run_pgbench_tps "$port" "$bindir" 1 30
            wait "$perf_pid" 2>/dev/null || true
            log "  perf stat saved to $perf_out"
        fi

        stop_cluster "$pgdata" "$branch" "$bindir"
    done

    log "W1 results written to $(csv_file W1)"
}

###############################################################################
# Schema creation helpers for W2-W7
###############################################################################

# create_siu_single PORT BINDIR ROW_COUNT
create_siu_single() {
    local port="$1" bindir="$2" rows="$3"
    run_sql "$port" "$bindir" "
        DROP TABLE IF EXISTS siu_single;
        CREATE TABLE siu_single (
            id integer PRIMARY KEY,
            val integer,
            payload text DEFAULT repeat('x', 100)
        );
        CREATE INDEX idx_siu_val ON siu_single(val);
        INSERT INTO siu_single (id, val)
        SELECT i, i FROM generate_series(1, ${rows}) i;
        ANALYZE siu_single;"
}

# create_siu_multi PORT BINDIR ROW_COUNT
create_siu_multi() {
    local port="$1" bindir="$2" rows="$3"
    run_sql "$port" "$bindir" "
        DROP TABLE IF EXISTS siu_multi;
        CREATE TABLE siu_multi (
            id integer PRIMARY KEY,
            col_a integer,
            col_b integer,
            col_c text,
            col_d timestamp DEFAULT now(),
            payload text DEFAULT repeat('x', 80)
        );
        CREATE INDEX idx_a ON siu_multi(col_a);
        CREATE INDEX idx_b ON siu_multi(col_b);
        CREATE INDEX idx_c ON siu_multi(col_c);
        CREATE INDEX idx_d ON siu_multi(col_d);
        INSERT INTO siu_multi (id, col_a, col_b, col_c)
        SELECT i, i, i * 2, 'val_' || i
        FROM generate_series(1, ${rows}) i;
        ANALYZE siu_multi;"
}

###############################################################################
# Workload W2: SIU benefit — single indexed column update
###############################################################################

run_w2() {
    log_header "W2: SIU benefit - single indexed column update"
    csv_header "W2" "branch,clients,iteration,tps,latency_ms"

    # Write pgbench custom script
    local script="${BENCH_BASE}/w2_update.sql"
    cat > "$script" <<'SQL'
\set aid random(1, 500000)
UPDATE siu_single SET val = val + 1 WHERE id = :aid;
SQL

    for branch in master tepid; do
        local bindir port pgdata
        if [ "$branch" = "master" ]; then
            bindir="$MASTER_BINDIR"; port=$MASTER_PORT; pgdata="$MASTER_PGDATA"
        else
            bindir="$TEPID_BINDIR"; port=$TEPID_PORT; pgdata="$TEPID_PGDATA"
        fi

        init_cluster "$pgdata" "$port" "$branch" "$bindir"
        start_cluster "$pgdata" "$branch" "$bindir"
        wait_for_ready "$port" "$branch" "$bindir"

        log "  Creating siu_single table ($ROW_COUNT rows)..."
        create_siu_single "$port" "$bindir" "$ROW_COUNT"
        reset_stats "$port" "$bindir"

        for clients in $W2_CLIENTS; do
            log "  Warmup: $branch, $clients clients, ${WARMUP}s"
            run_pgbench_tps "$port" "$bindir" "$clients" "$WARMUP" "$script"

            local tps_vals=()
            for iter in $(seq 1 "$ITERATIONS"); do
                log "  Run $iter/$ITERATIONS: $branch, $clients clients, ${DURATION}s"
                reset_stats "$port" "$bindir"
                run_pgbench_tps "$port" "$bindir" "$clients" "$DURATION" "$script"
                csv_append "W2" "$branch,$clients,$iter,$RESULT_TPS,$RESULT_LAT"
                if [ "$RESULT_TPS" != "FAIL" ]; then tps_vals+=("$RESULT_TPS"); fi
            done

            if [ ${#tps_vals[@]} -gt 0 ]; then
                log "  Median TPS ($branch, $clients): $(median "${tps_vals[@]}")"
            fi
        done

        # Collect HOT stats after runs
        log "  Table stats ($branch):"
        local stats
        stats=$(collect_table_stats "$port" "$bindir" "siu_single")
        log "    table_size,index_size,total_size,live,dead,hot_upd,newpage_upd = $stats"
        echo "# ${branch}_table_stats: $stats" >> "$(csv_file W2)"

        stop_cluster "$pgdata" "$branch" "$bindir"
    done

    log "W2 results written to $(csv_file W2)"
}

###############################################################################
# Workload W3: SIU benefit — multi-index, partial column update
###############################################################################

run_w3() {
    log_header "W3: SIU benefit - multi-index table, partial column update"
    csv_header "W3" "branch,clients,iteration,tps,latency_ms"

    local script="${BENCH_BASE}/w3_update.sql"
    cat > "$script" <<'SQL'
\set aid random(1, 500000)
UPDATE siu_multi SET col_a = col_a + 1 WHERE id = :aid;
SQL

    for branch in master tepid; do
        local bindir port pgdata
        if [ "$branch" = "master" ]; then
            bindir="$MASTER_BINDIR"; port=$MASTER_PORT; pgdata="$MASTER_PGDATA"
        else
            bindir="$TEPID_BINDIR"; port=$TEPID_PORT; pgdata="$TEPID_PGDATA"
        fi

        init_cluster "$pgdata" "$port" "$branch" "$bindir"
        start_cluster "$pgdata" "$branch" "$bindir"
        wait_for_ready "$port" "$branch" "$bindir"

        log "  Creating siu_multi table ($ROW_COUNT rows)..."
        create_siu_multi "$port" "$bindir" "$ROW_COUNT"
        reset_stats "$port" "$bindir"

        for clients in $W3_CLIENTS; do
            log "  Warmup: $branch, $clients clients, ${WARMUP}s"
            run_pgbench_tps "$port" "$bindir" "$clients" "$WARMUP" "$script"

            local tps_vals=()
            for iter in $(seq 1 "$ITERATIONS"); do
                log "  Run $iter/$ITERATIONS: $branch, $clients clients, ${DURATION}s"
                reset_stats "$port" "$bindir"
                run_pgbench_tps "$port" "$bindir" "$clients" "$DURATION" "$script"
                csv_append "W3" "$branch,$clients,$iter,$RESULT_TPS,$RESULT_LAT"
                if [ "$RESULT_TPS" != "FAIL" ]; then tps_vals+=("$RESULT_TPS"); fi
            done

            if [ ${#tps_vals[@]} -gt 0 ]; then
                log "  Median TPS ($branch, $clients): $(median "${tps_vals[@]}")"
            fi
        done

        # Collect per-index stats
        log "  Index stats ($branch):"
        local idx_stats
        idx_stats=$(collect_index_stats "$port" "$bindir" "siu_multi")
        log "    $idx_stats"
        echo "# ${branch}_index_stats:" >> "$(csv_file W3)"
        echo "$idx_stats" | while IFS= read -r line; do
            echo "# ${branch}_idx: $line" >> "$(csv_file W3)"
        done

        local stats
        stats=$(collect_table_stats "$port" "$bindir" "siu_multi")
        log "    table_stats = $stats"
        echo "# ${branch}_table_stats: $stats" >> "$(csv_file W3)"

        stop_cluster "$pgdata" "$branch" "$bindir"
    done

    log "W3 results written to $(csv_file W3)"
}

###############################################################################
# Workload W4: Storage bloat comparison
###############################################################################

run_w4() {
    log_header "W4: Storage bloat comparison"
    csv_header "W4" "branch,phase,table_size,index_size,total_size,n_live_tup,n_dead_tup,n_tup_hot_upd"

    for branch in master tepid; do
        local bindir port pgdata
        if [ "$branch" = "master" ]; then
            bindir="$MASTER_BINDIR"; port=$MASTER_PORT; pgdata="$MASTER_PGDATA"
        else
            bindir="$TEPID_BINDIR"; port=$TEPID_PORT; pgdata="$TEPID_PGDATA"
        fi

        init_cluster "$pgdata" "$port" "$branch" "$bindir"
        start_cluster "$pgdata" "$branch" "$bindir"
        wait_for_ready "$port" "$branch" "$bindir"

        log "  Creating siu_single table ($ROW_COUNT rows)..."
        create_siu_single "$port" "$bindir" "$ROW_COUNT"

        # Record initial sizes
        local stats_before
        stats_before=$(collect_table_stats "$port" "$bindir" "siu_single")
        csv_append "W4" "$branch,before,$stats_before"
        log "  Before updates ($branch): $stats_before"

        # 10 rounds of full-table update
        log "  Running 10 full-table update rounds ($branch)..."
        for round in $(seq 1 10); do
            run_sql "$port" "$bindir" "UPDATE siu_single SET val = val + 1;"
            if [ $((round % 5)) -eq 0 ]; then
                log "    Round $round/10 complete"
            fi
        done

        # Record post-update sizes
        local stats_after
        stats_after=$(collect_table_stats "$port" "$bindir" "siu_single")
        csv_append "W4" "$branch,after_updates,$stats_after"
        log "  After 10 update rounds ($branch): $stats_after"

        stop_cluster "$pgdata" "$branch" "$bindir"
    done

    log "W4 results written to $(csv_file W4)"
}

###############################################################################
# Workload W5: Read-side overhead — bitmap scan with SIU tuples
###############################################################################

run_w5() {
    log_header "W5: Read-side overhead - bitmap scan with SIU tuples"
    csv_header "W5" "branch,clients,iteration,tps,latency_ms"

    local script="${BENCH_BASE}/w5_read.sql"
    cat > "$script" <<'SQL'
\set start random(1, 499000)
SELECT count(*) FROM siu_single WHERE val BETWEEN :start AND :start + 1000;
SQL

    for branch in master tepid; do
        local bindir port pgdata
        if [ "$branch" = "master" ]; then
            bindir="$MASTER_BINDIR"; port=$MASTER_PORT; pgdata="$MASTER_PGDATA"
        else
            bindir="$TEPID_BINDIR"; port=$TEPID_PORT; pgdata="$TEPID_PGDATA"
        fi

        init_cluster "$pgdata" "$port" "$branch" "$bindir"
        start_cluster "$pgdata" "$branch" "$bindir"
        wait_for_ready "$port" "$branch" "$bindir"

        # Create table and do updates to produce SIU tuples
        log "  Creating siu_single table and generating SIU tuples..."
        create_siu_single "$port" "$bindir" "$ROW_COUNT"
        for round in $(seq 1 10); do
            run_sql "$port" "$bindir" "UPDATE siu_single SET val = val + 1;"
        done
        run_sql "$port" "$bindir" "ANALYZE siu_single;"

        # Capture EXPLAIN ANALYZE for one query
        log "  Capturing query plan ($branch)..."
        local plan
        plan=$(fetch_val "$port" "$bindir" \
            "EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM siu_single WHERE val BETWEEN 1000 AND 2000;")
        echo "# ${branch}_plan:" >> "$(csv_file W5)"
        echo "$plan" | while IFS= read -r line; do
            echo "# $line" >> "$(csv_file W5)"
        done
        log "    Plan captured"

        for clients in $W5_CLIENTS; do
            log "  Warmup: $branch, $clients clients, ${WARMUP}s"
            run_pgbench_tps "$port" "$bindir" "$clients" "$WARMUP" "$script"

            local tps_vals=()
            for iter in $(seq 1 "$ITERATIONS"); do
                log "  Run $iter/$ITERATIONS: $branch, $clients clients, 60s"
                run_pgbench_tps "$port" "$bindir" "$clients" 60 "$script"
                csv_append "W5" "$branch,$clients,$iter,$RESULT_TPS,$RESULT_LAT"
                if [ "$RESULT_TPS" != "FAIL" ]; then tps_vals+=("$RESULT_TPS"); fi
            done

            if [ ${#tps_vals[@]} -gt 0 ]; then
                log "  Median TPS ($branch, $clients): $(median "${tps_vals[@]}")"
            fi
        done

        stop_cluster "$pgdata" "$branch" "$bindir"
    done

    log "W5 results written to $(csv_file W5)"
}

###############################################################################
# Workload W6: HOT chain length under sustained updates
###############################################################################

run_w6() {
    log_header "W6: HOT chain length under sustained updates"
    csv_header "W6" "branch,round,n_live_tup,n_dead_tup,n_tup_hot_upd,table_size,max_ctid_block"

    for branch in master tepid; do
        local bindir port pgdata
        if [ "$branch" = "master" ]; then
            bindir="$MASTER_BINDIR"; port=$MASTER_PORT; pgdata="$MASTER_PGDATA"
        else
            bindir="$TEPID_BINDIR"; port=$TEPID_PORT; pgdata="$TEPID_PGDATA"
        fi

        init_cluster "$pgdata" "$port" "$branch" "$bindir"
        start_cluster "$pgdata" "$branch" "$bindir"
        wait_for_ready "$port" "$branch" "$bindir"

        log "  Creating siu_single table ($ROW_COUNT rows)..."
        create_siu_single "$port" "$bindir" "$ROW_COUNT"

        # Baseline stats
        local stats
        stats=$(fetch_val "$port" "$bindir" "
            SELECT COALESCE(n_live_tup, 0) || ',' || COALESCE(n_dead_tup, 0)
                   || ',' || COALESCE(n_tup_hot_upd, 0)
                   || ',' || pg_relation_size('siu_single')
            FROM pg_stat_user_tables WHERE relname = 'siu_single';")
        local max_block
        max_block=$(fetch_val "$port" "$bindir" \
            "SELECT COALESCE(max((ctid::text::point)[0]::bigint), 0) FROM siu_single;")
        csv_append "W6" "$branch,0,$stats,$max_block"
        log "  Baseline ($branch): $stats, max_block=$max_block"

        # 50 rounds of full-table update (no vacuum)
        log "  Running 50 update rounds without vacuum ($branch)..."
        for round in $(seq 1 50); do
            run_sql "$port" "$bindir" "UPDATE siu_single SET val = val + 1;"

            # Collect stats every 10 rounds
            if [ $((round % 10)) -eq 0 ]; then
                # Force stats flush
                run_sql "$port" "$bindir" "SELECT pg_stat_force_next_flush();" 2>/dev/null || true
                sleep 1

                stats=$(fetch_val "$port" "$bindir" "
                    SELECT COALESCE(n_live_tup, 0) || ',' || COALESCE(n_dead_tup, 0)
                           || ',' || COALESCE(n_tup_hot_upd, 0)
                           || ',' || pg_relation_size('siu_single')
                    FROM pg_stat_user_tables WHERE relname = 'siu_single';")
                max_block=$(fetch_val "$port" "$bindir" \
                    "SELECT COALESCE(max((ctid::text::point)[0]::bigint), 0) FROM siu_single;")
                csv_append "W6" "$branch,$round,$stats,$max_block"
                log "    Round $round ($branch): $stats, max_block=$max_block"
            fi
        done

        stop_cluster "$pgdata" "$branch" "$bindir"
    done

    log "W6 results written to $(csv_file W6)"
}

###############################################################################
# Workload W7: Vacuum interaction
###############################################################################

run_w7() {
    log_header "W7: Vacuum interaction"
    csv_header "W7" "branch,phase,table_size,index_size,total_size,n_live_tup,n_dead_tup,n_tup_hot_upd"

    for branch in master tepid; do
        local bindir port pgdata
        if [ "$branch" = "master" ]; then
            bindir="$MASTER_BINDIR"; port=$MASTER_PORT; pgdata="$MASTER_PGDATA"
        else
            bindir="$TEPID_BINDIR"; port=$TEPID_PORT; pgdata="$TEPID_PGDATA"
        fi

        init_cluster "$pgdata" "$port" "$branch" "$bindir"
        start_cluster "$pgdata" "$branch" "$bindir"
        wait_for_ready "$port" "$branch" "$bindir"

        log "  Creating siu_single table ($ROW_COUNT rows)..."
        create_siu_single "$port" "$bindir" "$ROW_COUNT"

        # Phase 1: Initial sizes
        local stats
        stats=$(collect_table_stats "$port" "$bindir" "siu_single")
        csv_append "W7" "$branch,initial,$stats"
        log "  Initial ($branch): $stats"

        # Phase 2: 10 rounds of updates
        log "  Running 10 update rounds ($branch)..."
        for round in $(seq 1 10); do
            run_sql "$port" "$bindir" "UPDATE siu_single SET val = val + 1;"
        done

        # Force stats flush
        run_sql "$port" "$bindir" "SELECT pg_stat_force_next_flush();" 2>/dev/null || true
        sleep 1

        stats=$(collect_table_stats "$port" "$bindir" "siu_single")
        csv_append "W7" "$branch,after_updates,$stats"
        log "  After 10 updates ($branch): $stats"

        # Phase 3: VACUUM
        log "  Running VACUUM ($branch)..."
        local vacuum_start vacuum_end vacuum_dur
        vacuum_start=$(date +%s%N)
        run_sql "$port" "$bindir" "VACUUM siu_single;"
        vacuum_end=$(date +%s%N)
        vacuum_dur=$(( (vacuum_end - vacuum_start) / 1000000 ))

        run_sql "$port" "$bindir" "SELECT pg_stat_force_next_flush();" 2>/dev/null || true
        sleep 1

        stats=$(collect_table_stats "$port" "$bindir" "siu_single")
        csv_append "W7" "$branch,after_vacuum,$stats"
        log "  After VACUUM ($branch): $stats (took ${vacuum_dur}ms)"
        echo "# ${branch}_vacuum_ms: $vacuum_dur" >> "$(csv_file W7)"

        # Phase 4: VACUUM FULL
        log "  Running VACUUM FULL ($branch)..."
        vacuum_start=$(date +%s%N)
        run_sql "$port" "$bindir" "VACUUM FULL siu_single;"
        vacuum_end=$(date +%s%N)
        vacuum_dur=$(( (vacuum_end - vacuum_start) / 1000000 ))

        run_sql "$port" "$bindir" "SELECT pg_stat_force_next_flush();" 2>/dev/null || true
        sleep 1

        stats=$(collect_table_stats "$port" "$bindir" "siu_single")
        csv_append "W7" "$branch,after_vacuum_full,$stats"
        log "  After VACUUM FULL ($branch): $stats (took ${vacuum_dur}ms)"
        echo "# ${branch}_vacuum_full_ms: $vacuum_dur" >> "$(csv_file W7)"

        stop_cluster "$pgdata" "$branch" "$bindir"
    done

    log "W7 results written to $(csv_file W7)"
}

###############################################################################
# Workload W8: pgbench TPC-B (standard workload)
###############################################################################

run_w8() {
    log_header "W8: pgbench TPC-B (standard workload)"
    csv_header "W8" "branch,clients,iteration,tps,latency_ms"

    for branch in master tepid; do
        local bindir port pgdata
        if [ "$branch" = "master" ]; then
            bindir="$MASTER_BINDIR"; port=$MASTER_PORT; pgdata="$MASTER_PGDATA"
        else
            bindir="$TEPID_BINDIR"; port=$TEPID_PORT; pgdata="$TEPID_PGDATA"
        fi

        init_cluster "$pgdata" "$port" "$branch" "$bindir"
        start_cluster "$pgdata" "$branch" "$bindir"
        wait_for_ready "$port" "$branch" "$bindir"

        log "  Initializing pgbench (scale=$SCALE)..."
        "$bindir/pgbench" -h 127.0.0.1 -p "$port" -i -s "$SCALE" postgres >/dev/null 2>&1

        for clients in $W8_CLIENTS; do
            log "  Warmup: $branch, $clients clients, ${WARMUP}s"
            run_pgbench_tps "$port" "$bindir" "$clients" "$WARMUP"

            local tps_vals=()
            for iter in $(seq 1 "$ITERATIONS"); do
                log "  Run $iter/$ITERATIONS: $branch, $clients clients, ${DURATION}s"
                run_pgbench_tps "$port" "$bindir" "$clients" "$DURATION"
                csv_append "W8" "$branch,$clients,$iter,$RESULT_TPS,$RESULT_LAT"
                if [ "$RESULT_TPS" != "FAIL" ]; then tps_vals+=("$RESULT_TPS"); fi
            done

            if [ ${#tps_vals[@]} -gt 0 ]; then
                log "  Median TPS ($branch, $clients): $(median "${tps_vals[@]}")"
            fi
        done

        stop_cluster "$pgdata" "$branch" "$bindir"
    done

    log "W8 results written to $(csv_file W8)"
}

###############################################################################
# Summary report
###############################################################################

print_summary() {
    log_header "SUMMARY REPORT"

    # For TPS-based workloads (W1, W2, W3, W5, W8), compute median TPS per
    # branch+clients and print a comparison table.
    for w in W1 W2 W3 W5 W8; do
        local csvf
        csvf="$(csv_file "$w")"
        if [ ! -f "$csvf" ]; then continue; fi

        log ""
        log "--- $w ---"
        printf "%-8s | %-8s | %12s | %12s | %8s\n" \
            "Wkload" "Clients" "Master TPS" "Tepid TPS" "Ratio" | tee -a "$LOGFILE"
        printf "%-8s-+-%-8s-+-%12s-+-%12s-+-%8s\n" \
            "--------" "--------" "------------" "------------" "--------" | tee -a "$LOGFILE"

        # Get unique client counts from CSV (skip comments)
        local client_list
        client_list=$(grep -v '^#' "$csvf" | tail -n +2 | cut -d, -f2 | sort -un)

        for c in $client_list; do
            # Compute median TPS for each branch
            local m_vals t_vals m_med t_med ratio
            m_vals=$(grep -v '^#' "$csvf" | awk -F, -v b="master" -v c="$c" \
                '$1==b && $2==c && $4!="FAIL" {print $4}')
            t_vals=$(grep -v '^#' "$csvf" | awk -F, -v b="tepid" -v c="$c" \
                '$1==b && $2==c && $4!="FAIL" {print $4}')

            if [ -z "$m_vals" ] || [ -z "$t_vals" ]; then continue; fi

            m_med=$(echo "$m_vals" | sort -g | awk '{a[NR]=$1} END {
                if (NR%2) print a[(NR+1)/2]; else printf "%.1f", (a[NR/2]+a[NR/2+1])/2}')
            t_med=$(echo "$t_vals" | sort -g | awk '{a[NR]=$1} END {
                if (NR%2) print a[(NR+1)/2]; else printf "%.1f", (a[NR/2]+a[NR/2+1])/2}')

            ratio=$(echo "$m_med $t_med" | awk '{if ($1 > 0) printf "%.3f", $2/$1; else print "N/A"}')

            printf "%-8s | %8s | %12s | %12s | %8sx\n" \
                "$w" "$c" "$m_med" "$t_med" "$ratio" | tee -a "$LOGFILE"
        done
    done

    # W4 storage comparison
    local w4f
    w4f="$(csv_file W4)"
    if [ -f "$w4f" ]; then
        log ""
        log "--- W4: Storage Bloat ---"
        printf "%-8s | %-14s | %12s | %12s | %12s\n" \
            "Branch" "Phase" "Table Size" "Index Size" "Total Size" | tee -a "$LOGFILE"
        printf "%-8s-+-%-14s-+-%12s-+-%12s-+-%12s\n" \
            "--------" "--------------" "------------" "------------" "------------" | tee -a "$LOGFILE"

        grep -v '^#' "$w4f" | tail -n +2 | while IFS=, read -r branch phase tsize isize total rest; do
            printf "%-8s | %-14s | %12s | %12s | %12s\n" \
                "$branch" "$phase" "$tsize" "$isize" "$total" | tee -a "$LOGFILE"
        done
    fi

    # W6 HOT chain summary
    local w6f
    w6f="$(csv_file W6)"
    if [ -f "$w6f" ]; then
        log ""
        log "--- W6: HOT Chain Length ---"
        printf "%-8s | %6s | %10s | %10s | %12s | %12s\n" \
            "Branch" "Round" "Dead Tups" "HOT Upd" "Table Size" "Max Block" | tee -a "$LOGFILE"
        printf "%-8s-+-%6s-+-%10s-+-%10s-+-%12s-+-%12s\n" \
            "--------" "------" "----------" "----------" "------------" "------------" | tee -a "$LOGFILE"

        grep -v '^#' "$w6f" | tail -n +2 | while IFS=, read -r branch round live dead hot tsize maxblk; do
            printf "%-8s | %6s | %10s | %10s | %12s | %12s\n" \
                "$branch" "$round" "$dead" "$hot" "$tsize" "$maxblk" | tee -a "$LOGFILE"
        done
    fi

    # W7 vacuum summary
    local w7f
    w7f="$(csv_file W7)"
    if [ -f "$w7f" ]; then
        log ""
        log "--- W7: Vacuum Interaction ---"
        printf "%-8s | %-18s | %12s | %12s | %10s | %10s\n" \
            "Branch" "Phase" "Table Size" "Total Size" "Dead Tups" "HOT Upd" | tee -a "$LOGFILE"
        printf "%-8s-+-%-18s-+-%12s-+-%12s-+-%10s-+-%10s\n" \
            "--------" "------------------" "------------" "------------" "----------" "----------" | tee -a "$LOGFILE"

        grep -v '^#' "$w7f" | tail -n +2 | while IFS=, read -r branch phase tsize isize total live dead hot rest; do
            printf "%-8s | %-18s | %12s | %12s | %10s | %10s\n" \
                "$branch" "$phase" "$tsize" "$total" "$dead" "$hot" | tee -a "$LOGFILE"
        done

        # Print vacuum timings
        log ""
        grep '^# .*vacuum.*ms:' "$w7f" | sed 's/^# /  /' | tee -a "$LOGFILE"
    fi

    log ""
    log "All CSV results in: $RESULTS_DIR"
    log "Log file: $LOGFILE"
}

###############################################################################
# Main
###############################################################################

mkdir -p "$RESULTS_DIR"

log_header "SIU BENCHMARK SUITE"
log "Host: $(hostname)"
log "Date: $(date)"
log "BENCH_BASE: $BENCH_BASE"
log "REPO_DIR:   $REPO_DIR"
log "SCALE:      $SCALE"
log "DURATION:   ${DURATION}s"
log "WARMUP:     ${WARMUP}s"
log "ITERATIONS: $ITERATIONS"
log "ROW_COUNT:  $ROW_COUNT"
log "WORKLOADS:  $WORKLOADS"

# Setup and build unless skipped
if [ "$SKIP_SETUP" != "1" ]; then
    setup_and_build
fi

# Resolve binary directories
MASTER_BINDIR=$(pg_bindir "$MASTER_INSTALL")
TEPID_BINDIR=$(pg_bindir "$TEPID_INSTALL")
MASTER_LIBDIR=$(pg_libdir "$MASTER_INSTALL") || MASTER_LIBDIR=""
TEPID_LIBDIR=$(pg_libdir "$TEPID_INSTALL") || TEPID_LIBDIR=""

# Export library paths
if [ -n "$MASTER_LIBDIR" ]; then
    export LD_LIBRARY_PATH="${MASTER_LIBDIR}:${TEPID_LIBDIR:-}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    export DYLD_LIBRARY_PATH="${MASTER_LIBDIR}:${TEPID_LIBDIR:-}${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
fi

log "MASTER_BINDIR: $MASTER_BINDIR"
log "TEPID_BINDIR:  $TEPID_BINDIR"
log ""

# Verify binaries
"$MASTER_BINDIR/postgres" --version 2>&1 | tee -a "$LOGFILE"
"$TEPID_BINDIR/postgres" --version 2>&1 | tee -a "$LOGFILE"

# Run selected workloads
IFS=',' read -ra WL_ARRAY <<< "$WORKLOADS"
for w in "${WL_ARRAY[@]}"; do
    case "$w" in
        W1) run_w1 ;;
        W2) run_w2 ;;
        W3) run_w3 ;;
        W4) run_w4 ;;
        W5) run_w5 ;;
        W6) run_w6 ;;
        W7) run_w7 ;;
        W8) run_w8 ;;
        *)  log "WARNING: Unknown workload '$w', skipping." ;;
    esac
done

# Print summary
print_summary

log_header "SIU BENCHMARK SUITE COMPLETE"
log "Date: $(date)"
