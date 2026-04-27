#!/usr/bin/env bash
#
# run_hammerdb.sh -- Orchestrate HammerDB TPC-C/TPC-H benchmarks across
# all buffer replacement algorithms (clock, arc, car, lirs, lru, osic).
#
# Prerequisites:
#   - HammerDB installed (https://www.hammerdb.com/download.html)
#   - HAMMERDB_HOME set to the HammerDB installation directory
#   - PostgreSQL running with buffer pool extensions installed
#   - pg_bp_arc, pg_bp_car, pg_bp_lirs, pg_bp_lru, pg_bp_osic in shared_preload_libraries
#
# Usage:
#   ./run_hammerdb.sh [OPTIONS]
#
# Options:
#   --pghost HOST          PostgreSQL host (default: localhost)
#   --pgport PORT          PostgreSQL port (default: 5432)
#   --pguser USER          PostgreSQL user (default: $USER)
#   --pgdatabase DB        Database name (default: hammerdb)
#   --tpcc-duration SEC    TPC-C test duration in seconds (default: 1800 = 30min)
#   --tpch-duration SEC    TPC-H test duration in seconds (default: 1800)
#   --warehouses N         TPC-C warehouse count (default: 10)
#   --scale-factor N       TPC-H scale factor (default: 1)
#   --pool-size BYTES      Buffer pool size (default: 134217728 = 128MB)
#   --strategies LIST      Comma-separated strategies (default: clock,arc,car,lirs,lru,osic)
#   --output-dir DIR       Results directory (default: benchmark_results/hammerdb_YYYYMMDD_HHMMSS)
#   --skip-build           Skip TPC-C/H schema build (reuse existing)
#   --tpcc-only            Run only TPC-C
#   --tpch-only            Run only TPC-H
#   --with-profiling       Enable profiling during runs
#   --help                 Show this help

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROFILING_DIR="$BENCH_DIR/profiling"

# Defaults
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-$USER}"
PGDATABASE="hammerdb"
TPCC_DURATION=1800
TPCH_DURATION=1800
WAREHOUSES=10
SCALE_FACTOR=1
POOL_SIZE=134217728  # 128MB
STRATEGIES="clock,arc,car,lirs,lru,osic"
OUTPUT_DIR=""
SKIP_BUILD=0
TPCC_ONLY=0
TPCH_ONLY=0
WITH_PROFILING=0

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --pghost)      PGHOST="$2"; shift 2 ;;
        --pgport)      PGPORT="$2"; shift 2 ;;
        --pguser)      PGUSER="$2"; shift 2 ;;
        --pgdatabase)  PGDATABASE="$2"; shift 2 ;;
        --tpcc-duration)  TPCC_DURATION="$2"; shift 2 ;;
        --tpch-duration)  TPCH_DURATION="$2"; shift 2 ;;
        --warehouses)  WAREHOUSES="$2"; shift 2 ;;
        --scale-factor) SCALE_FACTOR="$2"; shift 2 ;;
        --pool-size)   POOL_SIZE="$2"; shift 2 ;;
        --strategies)  STRATEGIES="$2"; shift 2 ;;
        --output-dir)  OUTPUT_DIR="$2"; shift 2 ;;
        --skip-build)  SKIP_BUILD=1; shift ;;
        --tpcc-only)   TPCC_ONLY=1; shift ;;
        --tpch-only)   TPCH_ONLY=1; shift ;;
        --with-profiling) WITH_PROFILING=1; shift ;;
        --help)
            head -30 "$0" | tail -20
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ -z "$OUTPUT_DIR" ]]; then
    OUTPUT_DIR="benchmark_results/hammerdb_$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$OUTPUT_DIR"

# Detect HAMMERDB_HOME
if [[ -z "${HAMMERDB_HOME:-}" ]]; then
    # Try common locations
    for d in /opt/HammerDB* "$HOME/HammerDB"* /usr/local/HammerDB*; do
        if [[ -d "$d" && -f "$d/hammerdbcli" ]]; then
            HAMMERDB_HOME="$d"
            break
        fi
    done
    if [[ -z "${HAMMERDB_HOME:-}" ]]; then
        echo "ERROR: HAMMERDB_HOME not set and HammerDB not found in common locations."
        echo "Set HAMMERDB_HOME to your HammerDB installation directory."
        exit 1
    fi
fi
echo "Using HammerDB at: $HAMMERDB_HOME"

# Detect platform for profiling
detect_platform() {
    case "$(uname -s)" in
        Linux)
            if command -v bpftrace &>/dev/null; then
                echo "linux-bpftrace"
            elif command -v perf &>/dev/null; then
                echo "linux-perf"
            else
                echo "linux-none"
            fi
            ;;
        FreeBSD)
            if command -v dtrace &>/dev/null; then
                echo "freebsd-dtrace"
            else
                echo "freebsd-none"
            fi
            ;;
        Darwin)
            if command -v dtrace &>/dev/null; then
                echo "darwin-dtrace"
            else
                echo "darwin-none"
            fi
            ;;
        *) echo "unknown" ;;
    esac
}
PLATFORM=$(detect_platform)
echo "Platform: $PLATFORM"

# PostgreSQL connection helper
psql_cmd() {
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$1" -tAc "$2"
}

psql_cmd_db() {
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -tAc "$1"
}

# Strategy handler map
handler_for_strategy() {
    case "$1" in
        clock) echo "" ;;
        arc)   echo "arc_pool_handler" ;;
        car)   echo "car_pool_handler" ;;
        lirs)  echo "lirs_pool_handler" ;;
        lru)   echo "lru_pool_handler" ;;
        osic)  echo "osic_pool_handler" ;;
        *)     echo "ERROR: unknown strategy $1" >&2; exit 1 ;;
    esac
}

extension_for_strategy() {
    case "$1" in
        clock) echo "" ;;
        arc)   echo "pg_bp_arc" ;;
        car)   echo "pg_bp_car" ;;
        lirs)  echo "pg_bp_lirs" ;;
        lru)   echo "pg_bp_lru" ;;
        osic)  echo "pg_bp_osic" ;;
    esac
}

stat_extension_for_strategy() {
    case "$1" in
        clock) echo "" ;;
        arc)   echo "pg_stat_arc" ;;
        car)   echo "pg_stat_car" ;;
        lirs)  echo "pg_stat_lirs" ;;
        lru)   echo "pg_stat_lru" ;;
        osic)  echo "pg_stat_osic" ;;
    esac
}

# Create database if needed
setup_database() {
    echo "--- Setting up database: $PGDATABASE ---"
    if ! psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -tAc \
         "SELECT 1 FROM pg_database WHERE datname='$PGDATABASE'" | grep -q 1; then
        psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -c \
             "CREATE DATABASE $PGDATABASE"
    fi

    # Install base extensions
    psql_cmd_db "CREATE EXTENSION IF NOT EXISTS pg_stat_statements"
    psql_cmd_db "CREATE EXTENSION IF NOT EXISTS pg_buffercache"
    psql_cmd_db "CREATE EXTENSION IF NOT EXISTS pg_stat_bufferpool"
}

# Set up buffer pool for a strategy
setup_pool() {
    local strategy="$1"
    local handler
    handler=$(handler_for_strategy "$strategy")

    if [[ "$strategy" == "clock" ]]; then
        echo "  Using DEFAULT pool (clock sweep) -- no pool creation needed"
        return
    fi

    local ext
    ext=$(extension_for_strategy "$strategy")
    local stat_ext
    stat_ext=$(stat_extension_for_strategy "$strategy")

    psql_cmd_db "CREATE EXTENSION IF NOT EXISTS $ext"
    if [[ -n "$stat_ext" ]]; then
        psql_cmd_db "CREATE EXTENSION IF NOT EXISTS $stat_ext"
    fi

    # Drop if exists from previous run
    psql_cmd_db "DROP BUFFER POOL IF EXISTS hammerdb_${strategy}" 2>/dev/null || true

    psql_cmd_db "CREATE BUFFER POOL hammerdb_${strategy} SIZE '$POOL_SIZE' HANDLER $handler"
    echo "  Created buffer pool hammerdb_${strategy} (${POOL_SIZE} bytes, handler: $handler)"
}

# Tear down buffer pool for a strategy
teardown_pool() {
    local strategy="$1"
    if [[ "$strategy" != "clock" ]]; then
        psql_cmd_db "DROP BUFFER POOL IF EXISTS hammerdb_${strategy}" 2>/dev/null || true
    fi
}

# Snapshot pool stats before/after
snapshot_pool_stats() {
    local strategy="$1"
    local label="$2"
    local outfile="$OUTPUT_DIR/${strategy}_${label}_stats.json"

    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -tA <<SQL > "$outfile"
SELECT json_build_object(
    'timestamp', now(),
    'strategy', '$strategy',
    'label', '$label',
    'bufferpool', (SELECT json_agg(row_to_json(s)) FROM pg_stat_bufferpool s),
    'pg_stat_statements_total', (
        SELECT json_build_object(
            'total_calls', sum(calls),
            'total_time_ms', sum(total_exec_time),
            'shared_blks_hit', sum(shared_blks_hit),
            'shared_blks_read', sum(shared_blks_read)
        )
        FROM pg_stat_statements
    )
);
SQL
}

# Start profiling for a run
start_profiling() {
    local strategy="$1"
    local workload="$2"
    local profile_dir="$OUTPUT_DIR/profiles/${strategy}_${workload}"
    mkdir -p "$profile_dir"

    local pg_pid
    pg_pid=$(psql_cmd_db "SELECT pg_backend_pid()")

    case "$PLATFORM" in
        linux-perf)
            # Record perf data for all postgres processes
            perf record -g -o "$profile_dir/perf.data" \
                -p "$(pgrep -d, -f 'postgres')" &
            echo $! > "$profile_dir/perf.pid"
            ;;
        linux-bpftrace)
            if [[ -f "$PROFILING_DIR/spinlock_hold_times.bt" ]]; then
                bpftrace "$PROFILING_DIR/spinlock_hold_times.bt" \
                    > "$profile_dir/spinlock_hold_times.txt" 2>&1 &
                echo $! > "$profile_dir/bpftrace_spinlock.pid"
            fi
            ;;
        freebsd-dtrace|darwin-dtrace)
            if [[ -f "$PROFILING_DIR/spinlock_hold_times.d" ]]; then
                dtrace -s "$PROFILING_DIR/spinlock_hold_times.d" \
                    -p "$(pgrep -f 'postgres: checkpointer')" \
                    > "$profile_dir/spinlock_hold_times.txt" 2>&1 &
                echo $! > "$profile_dir/dtrace_spinlock.pid"
            fi
            ;;
    esac
    echo "  Profiling started in $profile_dir"
}

# Stop profiling
stop_profiling() {
    local strategy="$1"
    local workload="$2"
    local profile_dir="$OUTPUT_DIR/profiles/${strategy}_${workload}"

    for pidfile in "$profile_dir"/*.pid; do
        if [[ -f "$pidfile" ]]; then
            local pid
            pid=$(cat "$pidfile")
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
            rm -f "$pidfile"
        fi
    done

    # Generate flamegraph if perf data exists
    if [[ -f "$profile_dir/perf.data" ]]; then
        if command -v perf &>/dev/null; then
            perf script -i "$profile_dir/perf.data" > "$profile_dir/perf.script" 2>/dev/null || true
            # If FlameGraph tools available, generate SVG
            if command -v stackcollapse-perf.pl &>/dev/null; then
                stackcollapse-perf.pl "$profile_dir/perf.script" \
                    | flamegraph.pl > "$profile_dir/flamegraph.svg" 2>/dev/null || true
            fi
        fi
    fi
    echo "  Profiling stopped for $strategy/$workload"
}

# Generate HammerDB TCL script for TPC-C
generate_tpcc_tcl() {
    local strategy="$1"
    local outfile="$OUTPUT_DIR/${strategy}_tpcc.tcl"
    local pool_name=""
    if [[ "$strategy" != "clock" ]]; then
        pool_name="hammerdb_${strategy}"
    fi

    cat > "$outfile" <<TCLEOF
#!/usr/bin/env tclsh
# HammerDB TPC-C benchmark for buffer strategy: $strategy
# Auto-generated by run_hammerdb.sh

puts "=== TPC-C Benchmark: strategy=$strategy ==="

dbset db pg
dbset bm TPC-C

# Connection
diset connection pg_host $PGHOST
diset connection pg_port $PGPORT
diset connection pg_sslmode disable

# TPC-C schema
diset tpcc pg_count_ware $WAREHOUSES
diset tpcc pg_num_vu 4
diset tpcc pg_superuser $PGUSER
diset tpcc pg_superuserpass ""
diset tpcc pg_defaultdbase postgres
diset tpcc pg_dbase $PGDATABASE
diset tpcc pg_driver timed
diset tpcc pg_rampup 2
diset tpcc pg_duration [expr {$TPCC_DURATION / 60}]
diset tpcc pg_timeprofile true
diset tpcc pg_allwarehouse true
diset tpcc pg_storedprocs false

# Build schema (if needed)
TCLEOF

    if [[ $SKIP_BUILD -eq 0 ]]; then
        cat >> "$outfile" <<'TCLEOF'
puts "Building TPC-C schema..."
buildschema
waittocomplete
TCLEOF
    fi

    cat >> "$outfile" <<TCLEOF

# Run benchmark
puts "Starting TPC-C test for strategy $strategy..."
loadscript
vuset vu 8
vucreate
vurun
runtimer $TPCC_DURATION
vudestroy
waittocomplete

puts "TPC-C test complete for strategy $strategy"
TCLEOF

    echo "$outfile"
}

# Generate HammerDB TCL script for TPC-H
generate_tpch_tcl() {
    local strategy="$1"
    local outfile="$OUTPUT_DIR/${strategy}_tpch.tcl"

    cat > "$outfile" <<TCLEOF
#!/usr/bin/env tclsh
# HammerDB TPC-H benchmark for buffer strategy: $strategy
# Auto-generated by run_hammerdb.sh

puts "=== TPC-H Benchmark: strategy=$strategy ==="

dbset db pg
dbset bm TPC-H

# Connection
diset connection pg_host $PGHOST
diset connection pg_port $PGPORT
diset connection pg_sslmode disable

# TPC-H schema
diset tpch pg_tpch_superuser $PGUSER
diset tpch pg_tpch_superuserpass ""
diset tpch pg_tpch_defaultdbase postgres
diset tpch pg_tpch_dbase ${PGDATABASE}_tpch
diset tpch pg_scale_fact $SCALE_FACTOR
diset tpch pg_num_tpch_threads 4
diset tpch pg_tpch_driver timed
diset tpch pg_total_querysets 3
diset tpch pg_raise_query_error false
diset tpch pg_verbose false

TCLEOF

    if [[ $SKIP_BUILD -eq 0 ]]; then
        cat >> "$outfile" <<'TCLEOF'
# Build schema
puts "Building TPC-H schema..."
buildschema
waittocomplete
TCLEOF
    fi

    cat >> "$outfile" <<TCLEOF

# Run benchmark
puts "Starting TPC-H test for strategy $strategy..."
loadscript
vuset vu 4
vucreate
vurun
runtimer $TPCH_DURATION
vudestroy
waittocomplete

puts "TPC-H test complete for strategy $strategy"
TCLEOF

    echo "$outfile"
}

# Run a single HammerDB benchmark
run_hammerdb_bench() {
    local tcl_script="$1"
    local strategy="$2"
    local workload="$3"
    local log_file="$OUTPUT_DIR/${strategy}_${workload}.log"

    echo "  Running HammerDB $workload for $strategy..."
    echo "  Script: $tcl_script"
    echo "  Log: $log_file"

    # Reset pg_stat_statements
    psql_cmd_db "SELECT pg_stat_statements_reset()" 2>/dev/null || true

    # Snapshot before
    snapshot_pool_stats "$strategy" "before_${workload}"

    # Start profiling if requested
    if [[ $WITH_PROFILING -eq 1 ]]; then
        start_profiling "$strategy" "$workload"
    fi

    # Run HammerDB
    (cd "$HAMMERDB_HOME" && ./hammerdbcli auto "$tcl_script") > "$log_file" 2>&1 || {
        echo "  WARNING: HammerDB exited with non-zero status"
    }

    # Stop profiling
    if [[ $WITH_PROFILING -eq 1 ]]; then
        stop_profiling "$strategy" "$workload"
    fi

    # Snapshot after
    snapshot_pool_stats "$strategy" "after_${workload}"

    # Extract key metrics from HammerDB output
    extract_metrics "$log_file" "$strategy" "$workload"
}

# Extract metrics from HammerDB log
extract_metrics() {
    local log_file="$1"
    local strategy="$2"
    local workload="$3"
    local metrics_file="$OUTPUT_DIR/${strategy}_${workload}_metrics.csv"

    echo "timestamp,strategy,workload,metric,value" > "$metrics_file"

    local ts
    ts=$(date +%Y-%m-%dT%H:%M:%S)

    # TPC-C: look for NOPM and TPM
    if [[ "$workload" == "tpcc" ]]; then
        local nopm tpm
        nopm=$(grep -oP 'System achieved \K[0-9]+(?= NOPM)' "$log_file" 2>/dev/null || echo "0")
        tpm=$(grep -oP 'from \K[0-9]+(?= PostgreSQL TPM)' "$log_file" 2>/dev/null || echo "0")
        echo "$ts,$strategy,$workload,nopm,$nopm" >> "$metrics_file"
        echo "$ts,$strategy,$workload,tpm,$tpm" >> "$metrics_file"
        echo "  TPC-C Results: NOPM=$nopm TPM=$tpm"
    fi

    # TPC-H: look for query times
    if [[ "$workload" == "tpch" ]]; then
        # Extract per-query times
        grep -oP 'query \K[0-9]+ completed in [0-9.]+ seconds' "$log_file" 2>/dev/null | \
        while IFS=' ' read -r qnum _ _ secs _; do
            echo "$ts,$strategy,$workload,query_${qnum}_seconds,$secs" >> "$metrics_file"
        done
        local total_time
        total_time=$(grep -oP 'Total execution time \K[0-9.]+' "$log_file" 2>/dev/null || echo "0")
        echo "$ts,$strategy,$workload,total_time_seconds,$total_time" >> "$metrics_file"
        echo "  TPC-H Results: total_time=${total_time}s"
    fi
}

# Generate summary report
generate_summary() {
    local summary_file="$OUTPUT_DIR/summary.csv"
    echo "strategy,workload,metric,value" > "$summary_file"

    for f in "$OUTPUT_DIR"/*_metrics.csv; do
        if [[ -f "$f" ]]; then
            tail -n +2 "$f" | cut -d, -f2- >> "$summary_file"
        fi
    done

    echo ""
    echo "============================================"
    echo "  HammerDB Benchmark Summary"
    echo "============================================"
    echo ""

    if [[ $TPCC_ONLY -ne 1 ]] && [[ $TPCH_ONLY -ne 1 ]] || [[ $TPCC_ONLY -eq 1 ]]; then
        echo "TPC-C Results (NOPM = New Orders Per Minute):"
        echo "  Strategy          NOPM         TPM"
        echo "  --------          ----         ---"
        IFS=',' read -ra strat_arr <<< "$STRATEGIES"
        for s in "${strat_arr[@]}"; do
            local mf="$OUTPUT_DIR/${s}_tpcc_metrics.csv"
            if [[ -f "$mf" ]]; then
                local nopm tpm
                nopm=$(grep ",nopm," "$mf" | cut -d, -f5)
                tpm=$(grep ",tpm," "$mf" | cut -d, -f5)
                printf "  %-18s %-12s %s\n" "$s" "$nopm" "$tpm"
            fi
        done
        echo ""
    fi

    if [[ $TPCC_ONLY -ne 1 ]] && [[ $TPCH_ONLY -ne 1 ]] || [[ $TPCH_ONLY -eq 1 ]]; then
        echo "TPC-H Results (total execution time in seconds):"
        echo "  Strategy          Time (s)"
        echo "  --------          --------"
        IFS=',' read -ra strat_arr <<< "$STRATEGIES"
        for s in "${strat_arr[@]}"; do
            local mf="$OUTPUT_DIR/${s}_tpch_metrics.csv"
            if [[ -f "$mf" ]]; then
                local total
                total=$(grep ",total_time_seconds," "$mf" | cut -d, -f5)
                printf "  %-18s %s\n" "$s" "$total"
            fi
        done
        echo ""
    fi

    echo "Full results in: $OUTPUT_DIR"
    echo "============================================"
}

# Main execution
main() {
    echo "============================================"
    echo "  HammerDB Benchmark Runner"
    echo "============================================"
    echo "  Host:       $PGHOST:$PGPORT"
    echo "  Database:   $PGDATABASE"
    echo "  Strategies: $STRATEGIES"
    echo "  Warehouses: $WAREHOUSES (TPC-C)"
    echo "  Scale:      $SCALE_FACTOR (TPC-H)"
    echo "  Pool size:  $POOL_SIZE bytes"
    echo "  TPC-C:      ${TPCC_DURATION}s"
    echo "  TPC-H:      ${TPCH_DURATION}s"
    echo "  Profiling:  $([[ $WITH_PROFILING -eq 1 ]] && echo "ON ($PLATFORM)" || echo "OFF")"
    echo "  Output:     $OUTPUT_DIR"
    echo "============================================"
    echo ""

    # Setup
    setup_database

    IFS=',' read -ra strat_arr <<< "$STRATEGIES"

    for strategy in "${strat_arr[@]}"; do
        echo ""
        echo "=== Strategy: $strategy ==="
        echo ""

        # Create buffer pool
        setup_pool "$strategy"

        # Note: For non-clock strategies, HammerDB tables won't automatically use the
        # custom pool. We need to ALTER the TPC tables to use the pool after schema build.
        # This is done by running a post-build SQL script.
        if [[ "$strategy" != "clock" ]]; then
            # After schema build, move tables to the custom pool
            local post_build="$OUTPUT_DIR/${strategy}_post_build.sql"
            cat > "$post_build" <<SQL
-- Move TPC-C tables to buffer pool hammerdb_${strategy}
DO \$\$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename IN ('warehouse', 'district', 'customer', 'history',
                          'orders', 'new_order', 'order_line', 'item', 'stock')
    LOOP
        EXECUTE format('ALTER TABLE %I SET (buffer_pool = ''hammerdb_${strategy}'')', tbl);
    END LOOP;
END
\$\$;
SQL
        fi

        # TPC-C
        if [[ $TPCH_ONLY -ne 1 ]]; then
            local tpcc_script
            tpcc_script=$(generate_tpcc_tcl "$strategy")

            run_hammerdb_bench "$tpcc_script" "$strategy" "tpcc"

            # Run post-build SQL to move tables into pool
            if [[ "$strategy" != "clock" ]] && [[ -f "$post_build" ]]; then
                psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
                    -f "$post_build" 2>/dev/null || true
            fi
        fi

        # TPC-H
        if [[ $TPCC_ONLY -ne 1 ]]; then
            local tpch_script
            tpch_script=$(generate_tpch_tcl "$strategy")

            run_hammerdb_bench "$tpch_script" "$strategy" "tpch"
        fi

        # Tear down pool
        teardown_pool "$strategy"
    done

    # Summary
    generate_summary
}

main "$@"
