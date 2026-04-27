#!/usr/bin/env bash
#
# run_heap_vs_recno.sh - Compare pgbench TPC-B performance between heap and recno AMs
#
# Usage:
#   ./run_heap_vs_recno.sh [options]
#
# Options (via environment variables):
#   PG_BIN        - Path to PostgreSQL bin directory (default: auto-detect from build tree)
#   PG_LIB        - Path to PostgreSQL lib directory (default: auto-detect from PG_BIN)
#   PGDATA_BASE   - Base directory for test databases (default: /tmp/pgbench_compare)
#   SCALE         - pgbench scale factor (default: 10)
#   DURATION      - Test duration in seconds per run (default: 60)
#   MAX_CLIENTS   - Maximum client count to test (default: 4)
#
# Portable: works on Linux and FreeBSD.

set -euo pipefail

###############################################################################
# Defaults
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Auto-detect PG_BIN from the meson build tree's tmp_install
if [ -z "${PG_BIN:-}" ]; then
    for candidate in \
        "$REPO_ROOT/build/tmp_install/install/bin" \
        "$REPO_ROOT/build/tmp_install/home/"*"/bin" \
        "$REPO_ROOT/install/bin"; do
        if [ -x "$candidate/pgbench" ] && [ -x "$candidate/initdb" ]; then
            PG_BIN="$candidate"
            break
        fi
    done
    if [ -z "${PG_BIN:-}" ]; then
        echo "ERROR: Cannot auto-detect PG_BIN. Set PG_BIN=/path/to/pg/bin" >&2
        exit 1
    fi
fi

# Resolve PG_LIB from PG_BIN's sibling lib or lib64 directory
if [ -z "${PG_LIB:-}" ]; then
    BIN_PARENT="$(dirname "$PG_BIN")"
    for libdir in "$BIN_PARENT/lib64" "$BIN_PARENT/lib"; do
        if [ -f "$libdir/libpq.so" ] || [ -f "$libdir/libpq.dylib" ]; then
            PG_LIB="$libdir"
            break
        fi
    done
    # Also look for in-tree build lib paths
    if [ -z "${PG_LIB:-}" ]; then
        for libdir in \
            "$REPO_ROOT/build/tmp_install/install/lib64" \
            "$REPO_ROOT/build/tmp_install/install/lib" \
            "$REPO_ROOT/build/src/interfaces/libpq"; do
            if [ -f "$libdir/libpq.so" ] || [ -f "$libdir/libpq.dylib" ]; then
                PG_LIB="$libdir"
                break
            fi
        done
    fi
fi

PGDATA_BASE="${PGDATA_BASE:-/tmp/pgbench_compare}"
SCALE="${SCALE:-10}"
DURATION="${DURATION:-60}"
MAX_CLIENTS="${MAX_CLIENTS:-4}"

# Build the list of client counts: 1, 2, 4, ... up to MAX_CLIENTS
CLIENT_COUNTS=()
c=1
while [ "$c" -le "$MAX_CLIENTS" ]; do
    CLIENT_COUNTS+=("$c")
    c=$((c * 2))
done

# Ports for the two instances (pick high ports unlikely to conflict)
HEAP_PORT=54320
RECNO_PORT=54321

###############################################################################
# Binaries
###############################################################################

INITDB="$PG_BIN/initdb"
PG_CTL="$PG_BIN/pg_ctl"
PGBENCH="$PG_BIN/pgbench"
PSQL="$PG_BIN/psql"
PG_ISREADY="$PG_BIN/pg_isready"

for bin in "$INITDB" "$PG_CTL" "$PGBENCH" "$PSQL" "$PG_ISREADY"; do
    if [ ! -x "$bin" ]; then
        echo "ERROR: Required binary not found or not executable: $bin" >&2
        exit 1
    fi
done

# Export LD_LIBRARY_PATH / DYLD_LIBRARY_PATH so the binaries find libpq
if [ -n "${PG_LIB:-}" ]; then
    export LD_LIBRARY_PATH="${PG_LIB}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    # macOS / FreeBSD may also need DYLD_LIBRARY_PATH
    export DYLD_LIBRARY_PATH="${PG_LIB}${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
fi

###############################################################################
# Cleanup on exit
###############################################################################

HEAP_PGDATA="$PGDATA_BASE/heap_data"
RECNO_PGDATA="$PGDATA_BASE/recno_data"

cleanup() {
    echo ""
    echo "Cleaning up..."
    # Stop servers if running (ignore errors)
    "$PG_CTL" stop -D "$HEAP_PGDATA" -m immediate 2>/dev/null || true
    "$PG_CTL" stop -D "$RECNO_PGDATA" -m immediate 2>/dev/null || true
    rm -rf "$PGDATA_BASE"
    echo "Done."
}

trap cleanup EXIT

###############################################################################
# Helper functions
###############################################################################

log() {
    echo ">>> $*"
}

# init_cluster PGDATA PORT ACCESS_METHOD
init_cluster() {
    local pgdata="$1" port="$2" am="$3"

    log "Initializing $am cluster at $pgdata"
    mkdir -p "$pgdata"
    "$INITDB" -D "$pgdata" --no-locale -E UTF8 -A trust >/dev/null 2>&1

    # Configure the cluster
    {
        echo "port = $port"
        echo "listen_addresses = '127.0.0.1'"
        echo "unix_socket_directories = '$pgdata'"
        echo "shared_buffers = 256MB"
        echo "wal_level = minimal"
        echo "max_wal_senders = 0"
        echo "fsync = off"
        echo "synchronous_commit = off"
        echo "full_page_writes = off"
        echo "max_connections = 100"
        echo "logging_collector = off"
        echo "log_min_messages = warning"
        echo "default_table_access_method = '$am'"
    } >> "$pgdata/postgresql.conf"
}

# start_cluster PGDATA LABEL
start_cluster() {
    local pgdata="$1" label="$2"
    log "Starting $label server"
    "$PG_CTL" start -D "$pgdata" -l "$pgdata/server.log" -w -t 30 >/dev/null 2>&1
}

# stop_cluster PGDATA LABEL
stop_cluster() {
    local pgdata="$1" label="$2"
    log "Stopping $label server"
    "$PG_CTL" stop -D "$pgdata" -m fast 2>/dev/null || true
}

# wait_for_ready PORT LABEL
wait_for_ready() {
    local port="$1" label="$2"
    local retries=30
    while [ "$retries" -gt 0 ]; do
        if "$PG_ISREADY" -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then
            return 0
        fi
        retries=$((retries - 1))
        sleep 1
    done
    echo "ERROR: $label server on port $port did not become ready" >&2
    return 1
}

# run_pgbench_init PORT
run_pgbench_init() {
    local port="$1"
    log "Initializing pgbench tables (scale=$SCALE) on port $port"
    "$PGBENCH" -i -s "$SCALE" -h 127.0.0.1 -p "$port" postgres >/dev/null 2>&1
}

# run_pgbench PORT CLIENTS -> sets RESULT_TPS and RESULT_LAT
run_pgbench_test() {
    local port="$1" clients="$2"
    local output
    RESULT_TPS=""
    RESULT_LAT=""

    output=$("$PGBENCH" -h 127.0.0.1 -p "$port" -c "$clients" -j "$clients" \
                        -T "$DURATION" --no-vacuum postgres 2>&1) || true

    # Extract TPS (excluding connections establishing)
    RESULT_TPS=$(echo "$output" | grep -i "excluding connections establishing" \
                 | sed 's/.*= *//' | sed 's/ .*//' || echo "")

    # Extract latency average
    RESULT_LAT=$(echo "$output" | grep -i "latency average" \
                 | sed 's/.*= *//' | sed 's/ .*//' || echo "")

    # If we couldn't parse, mark as failed
    if [ -z "$RESULT_TPS" ]; then
        RESULT_TPS="FAIL"
    fi
    if [ -z "$RESULT_LAT" ]; then
        RESULT_LAT="FAIL"
    fi
}

###############################################################################
# Arrays to hold results
###############################################################################

declare -a HEAP_TPS_RESULTS=()
declare -a HEAP_LAT_RESULTS=()
declare -a RECNO_TPS_RESULTS=()
declare -a RECNO_LAT_RESULTS=()

###############################################################################
# Main
###############################################################################

echo ""
echo "============================================================"
echo " HEAP vs RECNO pgbench Comparison"
echo " Scale: $SCALE  Duration: ${DURATION}s  Max clients: $MAX_CLIENTS"
echo " PG_BIN: $PG_BIN"
echo "============================================================"
echo ""

mkdir -p "$PGDATA_BASE"

# ── Run heap benchmarks ────────────────────────────────────────
init_cluster "$HEAP_PGDATA" "$HEAP_PORT" "heap"
start_cluster "$HEAP_PGDATA" "heap"
wait_for_ready "$HEAP_PORT" "heap"
run_pgbench_init "$HEAP_PORT"

for clients in "${CLIENT_COUNTS[@]}"; do
    log "Running heap benchmark with $clients client(s) for ${DURATION}s"
    run_pgbench_test "$HEAP_PORT" "$clients"
    HEAP_TPS_RESULTS+=("$RESULT_TPS")
    HEAP_LAT_RESULTS+=("$RESULT_LAT")
    log "  TPS=$RESULT_TPS  Latency=$RESULT_LAT ms"
done

stop_cluster "$HEAP_PGDATA" "heap"

# ── Run recno benchmarks ───────────────────────────────────────
init_cluster "$RECNO_PGDATA" "$RECNO_PORT" "recno"
start_cluster "$RECNO_PGDATA" "recno"
wait_for_ready "$RECNO_PORT" "recno"
run_pgbench_init "$RECNO_PORT"

for i in "${!CLIENT_COUNTS[@]}"; do
    clients="${CLIENT_COUNTS[$i]}"
    log "Running recno benchmark with $clients client(s) for ${DURATION}s"
    run_pgbench_test "$RECNO_PORT" "$clients"
    RECNO_TPS_RESULTS+=("$RESULT_TPS")
    RECNO_LAT_RESULTS+=("$RESULT_LAT")
    log "  TPS=$RESULT_TPS  Latency=$RESULT_LAT ms"
done

stop_cluster "$RECNO_PGDATA" "recno"

###############################################################################
# Print comparison table
###############################################################################

# Column widths
CW=9   # client column
TW=12  # TPS columns
LW=14  # latency columns

pad_right() {
    printf "%-${2}s" "$1"
}

pad_left() {
    printf "%${2}s" "$1"
}

echo ""
echo "==========================================================================="
printf " HEAP vs RECNO Benchmark Results\n"
printf " Scale: %s, Duration: %ss\n" "$SCALE" "$DURATION"
echo "==========================================================================="
printf " %-${CW}s | %-${TW}s | %-${TW}s | %-${LW}s | %-${LW}s\n" \
       "Clients" "Heap TPS" "Recno TPS" "Heap Lat(ms)" "Recno Lat(ms)"
echo "-----------+--------------+--------------+----------------+----------------"

for i in "${!CLIENT_COUNTS[@]}"; do
    clients="${CLIENT_COUNTS[$i]}"
    h_tps="${HEAP_TPS_RESULTS[$i]:-FAIL}"
    r_tps="${RECNO_TPS_RESULTS[$i]:-FAIL}"
    h_lat="${HEAP_LAT_RESULTS[$i]:-FAIL}"
    r_lat="${RECNO_LAT_RESULTS[$i]:-FAIL}"

    printf " %${CW}s | %${TW}s | %${TW}s | %${LW}s | %${LW}s\n" \
           "$clients" "$h_tps" "$r_tps" "$h_lat" "$r_lat"
done

echo "==========================================================================="

# Print ratio summary if we have numeric results
echo ""
echo "Ratio (Recno / Heap):"
for i in "${!CLIENT_COUNTS[@]}"; do
    clients="${CLIENT_COUNTS[$i]}"
    h_tps="${HEAP_TPS_RESULTS[$i]:-}"
    r_tps="${RECNO_TPS_RESULTS[$i]:-}"

    if [ "$h_tps" != "FAIL" ] && [ "$r_tps" != "FAIL" ] && \
       [ -n "$h_tps" ] && [ -n "$r_tps" ]; then
        # Use awk for portable floating point division
        ratio=$(echo "$r_tps $h_tps" | awk '{if ($2 > 0) printf "%.3f", $1/$2; else print "N/A"}')
        printf "  %s client(s): %sx\n" "$clients" "$ratio"
    else
        printf "  %s client(s): N/A (one or both runs failed)\n" "$clients"
    fi
done

echo ""
echo "Done. Results above are TPS excluding connection establishment time."
