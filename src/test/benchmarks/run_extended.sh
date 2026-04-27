#!/usr/bin/env bash
#
# run_extended.sh -- Run the full extended benchmark suite.
#
# Designed for long-running performance comparison on a dedicated machine
# (e.g., Intel NUC at /scratch/pg-arc-extended). Tests all buffer replacement
# algorithms across multiple dimensions:
#
#   - 6 algorithms: clock, arc, car, lirs, lru, osic
#   - 3 pool sizes: 8MB, 32MB, 128MB
#   - 3 row counts: 10K, 100K, 1M
#   - 19 query patterns (all patterns from the benchmark suite)
#   - HammerDB TPC-C (30 min per algorithm)
#   - Profiling: perf/bpftrace on Linux, DTrace on FreeBSD/macOS
#
# Usage:
#   ./run_extended.sh [OPTIONS]
#
# Options:
#   --pghost HOST       PostgreSQL host (default: localhost)
#   --pgport PORT       PostgreSQL port (default: 5432)
#   --pguser USER       PostgreSQL user (default: $USER)
#   --pgdatabase DB     Database for Python benchmarks (default: benchmark_db)
#   --build-dir DIR     PostgreSQL build directory (default: auto-detect)
#   --output-dir DIR    Results directory (default: benchmark_results/extended_YYYYMMDD_HHMMSS)
#   --skip-python       Skip Python benchmark suite
#   --skip-hammerdb     Skip HammerDB TPC-C/H
#   --skip-profiling    Skip profiling
#   --quick             Quick mode: 1 pool size, 2 row counts, 5 patterns
#   --help              Show this help

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HAMMERDB_DIR="$SCRIPT_DIR/hammerdb"
PROFILING_DIR="$SCRIPT_DIR/profiling"

# Defaults
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-$USER}"
PGDATABASE="benchmark_db"
BUILD_DIR=""
OUTPUT_DIR=""
SKIP_PYTHON=0
SKIP_HAMMERDB=0
SKIP_PROFILING=0
QUICK_MODE=0

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --pghost)      PGHOST="$2"; shift 2 ;;
        --pgport)      PGPORT="$2"; shift 2 ;;
        --pguser)      PGUSER="$2"; shift 2 ;;
        --pgdatabase)  PGDATABASE="$2"; shift 2 ;;
        --build-dir)   BUILD_DIR="$2"; shift 2 ;;
        --output-dir)  OUTPUT_DIR="$2"; shift 2 ;;
        --skip-python) SKIP_PYTHON=1; shift ;;
        --skip-hammerdb) SKIP_HAMMERDB=1; shift ;;
        --skip-profiling) SKIP_PROFILING=1; shift ;;
        --quick)       QUICK_MODE=1; shift ;;
        --help)
            head -30 "$0" | tail -22
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
if [[ -z "$OUTPUT_DIR" ]]; then
    OUTPUT_DIR="benchmark_results/extended_${TIMESTAMP}"
fi
mkdir -p "$OUTPUT_DIR"

# Logging
LOG_FILE="$OUTPUT_DIR/run_extended.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "============================================================"
echo "  Extended Buffer Pool Benchmark Suite"
echo "  Started: $(date)"
echo "============================================================"
echo ""
echo "  Host:         $PGHOST:$PGPORT"
echo "  Database:     $PGDATABASE"
echo "  Output:       $OUTPUT_DIR"
echo "  Quick mode:   $([[ $QUICK_MODE -eq 1 ]] && echo "YES" || echo "NO")"
echo "  Skip Python:  $([[ $SKIP_PYTHON -eq 1 ]] && echo "YES" || echo "NO")"
echo "  Skip HammerDB:$([[ $SKIP_HAMMERDB -eq 1 ]] && echo "YES" || echo "NO")"
echo "  Skip Profile: $([[ $SKIP_PROFILING -eq 1 ]] && echo "YES" || echo "NO")"
echo ""

# Detect platform
detect_platform() {
    case "$(uname -s)" in
        Linux)
            if command -v bpftrace &>/dev/null; then echo "linux-bpftrace"
            elif command -v perf &>/dev/null; then echo "linux-perf"
            else echo "linux-none"; fi ;;
        FreeBSD)
            if command -v dtrace &>/dev/null; then echo "freebsd-dtrace"
            else echo "freebsd-none"; fi ;;
        Darwin)
            if command -v dtrace &>/dev/null; then echo "darwin-dtrace"
            else echo "darwin-none"; fi ;;
        *) echo "unknown" ;;
    esac
}
PLATFORM=$(detect_platform)
echo "Platform: $PLATFORM"
echo ""

# Find Python (prefer python3)
PYTHON=$(command -v python3 || command -v python)
if [[ -z "$PYTHON" ]]; then
    echo "WARNING: Python not found; skipping Python benchmarks"
    SKIP_PYTHON=1
fi

# Ensure database exists
ensure_database() {
    local db="$1"
    if ! psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -tAc \
         "SELECT 1 FROM pg_database WHERE datname='$db'" 2>/dev/null | grep -q 1; then
        echo "Creating database: $db"
        psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -c "CREATE DATABASE $db"
    fi
}

# Collect system info
collect_sysinfo() {
    local info_file="$OUTPUT_DIR/system_info.txt"
    echo "=== System Information ===" > "$info_file"
    echo "Date: $(date)" >> "$info_file"
    echo "Hostname: $(hostname)" >> "$info_file"
    echo "Kernel: $(uname -a)" >> "$info_file"
    echo "" >> "$info_file"

    if [[ -f /proc/cpuinfo ]]; then
        echo "=== CPU ===" >> "$info_file"
        grep -m1 'model name' /proc/cpuinfo >> "$info_file" 2>/dev/null || true
        echo "Cores: $(nproc)" >> "$info_file"
        echo "" >> "$info_file"
    fi

    if [[ -f /proc/meminfo ]]; then
        echo "=== Memory ===" >> "$info_file"
        head -5 /proc/meminfo >> "$info_file"
        echo "" >> "$info_file"
    fi

    echo "=== PostgreSQL ===" >> "$info_file"
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -c "SELECT version()" \
        >> "$info_file" 2>/dev/null || true
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -tAc \
        "SHOW shared_buffers" >> "$info_file" 2>/dev/null || true
    echo "" >> "$info_file"

    echo "System info collected: $info_file"
}

#######################################################################
# Phase 1: Python benchmark suite
#######################################################################
run_python_benchmarks() {
    echo ""
    echo "============================================================"
    echo "  Phase 1: Python Benchmark Suite"
    echo "============================================================"
    echo ""

    ensure_database "$PGDATABASE"

    # Define test dimensions
    local pool_sizes row_counts strategies patterns
    if [[ $QUICK_MODE -eq 1 ]]; then
        pool_sizes=("33554432")       # 32MB only
        row_counts=("10000 100000")   # 10K, 100K
        strategies=("clock arc car")
        patterns="repeated_hotset zipfian_access working_set_shift capacity_pressure sequential_then_random"
    else
        pool_sizes=("8388608" "33554432" "134217728")  # 8MB, 32MB, 128MB
        row_counts=("10000 100000 1000000")            # 10K, 100K, 1M
        strategies=("all")
        patterns="all"
    fi

    for pool_size in "${pool_sizes[@]}"; do
        local size_label
        case "$pool_size" in
            8388608)   size_label="8MB" ;;
            33554432)  size_label="32MB" ;;
            134217728) size_label="128MB" ;;
            *) size_label="${pool_size}B" ;;
        esac

        local run_dir="$OUTPUT_DIR/python_${size_label}"
        mkdir -p "$run_dir"

        echo "--- Pool size: $size_label ($pool_size bytes) ---"

        for strat in ${strategies[@]}; do
            echo "  Strategy: $strat"

            $PYTHON -m src.test.benchmarks \
                --host "$PGHOST" \
                --port "$PGPORT" \
                --database "$PGDATABASE" \
                --mode bufpool \
                --strategy "$strat" \
                --pool-size "$pool_size" \
                --schema all \
                --rows ${row_counts[@]} \
                --distribution all \
                --pattern $patterns \
                --warmup 2 \
                --iterations 5 \
                --output-dir "$run_dir/${strat}" \
                -v 2>&1 || {
                    echo "  WARNING: benchmark failed for pool=$size_label strategy=$strat"
                }
        done
    done

    echo ""
    echo "Python benchmarks complete."
}

#######################################################################
# Phase 2: HammerDB TPC-C/H
#######################################################################
run_hammerdb_benchmarks() {
    echo ""
    echo "============================================================"
    echo "  Phase 2: HammerDB TPC-C / TPC-H"
    echo "============================================================"
    echo ""

    if [[ ! -f "$HAMMERDB_DIR/run_hammerdb.sh" ]]; then
        echo "WARNING: HammerDB runner not found at $HAMMERDB_DIR/run_hammerdb.sh"
        echo "Skipping HammerDB benchmarks."
        return
    fi

    if [[ -z "${HAMMERDB_HOME:-}" ]]; then
        echo "WARNING: HAMMERDB_HOME not set. Skipping HammerDB benchmarks."
        echo "Set HAMMERDB_HOME to your HammerDB installation directory."
        return
    fi

    local tpcc_duration pool_size strategies
    if [[ $QUICK_MODE -eq 1 ]]; then
        tpcc_duration=300      # 5 min
        pool_size=33554432     # 32MB
        strategies="clock,arc"
    else
        tpcc_duration=1800     # 30 min
        pool_size=134217728    # 128MB
        strategies="clock,arc,car,lirs,lru,osic"
    fi

    local profiling_flag=""
    if [[ $SKIP_PROFILING -eq 0 ]]; then
        profiling_flag="--with-profiling"
    fi

    bash "$HAMMERDB_DIR/run_hammerdb.sh" \
        --pghost "$PGHOST" \
        --pgport "$PGPORT" \
        --pguser "$PGUSER" \
        --tpcc-duration "$tpcc_duration" \
        --tpch-duration "$tpcc_duration" \
        --pool-size "$pool_size" \
        --strategies "$strategies" \
        --output-dir "$OUTPUT_DIR/hammerdb" \
        $profiling_flag \
        2>&1 || {
            echo "WARNING: HammerDB benchmarks had errors"
        }

    echo ""
    echo "HammerDB benchmarks complete."
}

#######################################################################
# Phase 3: Profiling
#######################################################################
run_profiling() {
    echo ""
    echo "============================================================"
    echo "  Phase 3: Profiling"
    echo "============================================================"
    echo ""

    local profile_dir="$OUTPUT_DIR/profiling"
    mkdir -p "$profile_dir"

    local duration
    if [[ $QUICK_MODE -eq 1 ]]; then
        duration=30
    else
        duration=120
    fi

    case "$PLATFORM" in
        linux-perf|linux-bpftrace)
            if [[ -f "$PROFILING_DIR/perf_equivalent.sh" ]]; then
                echo "Running perf + bpftrace profiling for ${duration}s..."

                # Run a workload in the background for profiling
                echo "  Starting background workload for profiling..."
                $PYTHON -m src.test.benchmarks \
                    --host "$PGHOST" \
                    --port "$PGPORT" \
                    --database "$PGDATABASE" \
                    --mode bufpool \
                    --strategy arc \
                    --schema medium \
                    --rows 100000 \
                    --distribution random \
                    --pattern repeated_hotset \
                    --iterations 20 \
                    --output-dir "$profile_dir/workload_output" \
                    -v &
                local workload_pid=$!

                sleep 2  # Let workload start

                # Run profiling
                bash "$PROFILING_DIR/perf_equivalent.sh" all \
                    -d "$duration" \
                    -o "$profile_dir" \
                    2>&1 || true

                # Wait for workload
                wait "$workload_pid" 2>/dev/null || true
            fi
            ;;
        freebsd-dtrace|darwin-dtrace)
            echo "DTrace profiling available."
            echo "Run manually:"
            echo "  dtrace -s $PROFILING_DIR/spinlock_hold_times.d -p <postgres_pid>"
            echo "  dtrace -s $PROFILING_DIR/list_traversal.d -p <postgres_pid>"
            echo "  dtrace -s $PROFILING_DIR/ghost_ops.d -p <postgres_pid>"
            echo "  dtrace -s $PROFILING_DIR/hit_rate_over_time.d -p <postgres_pid>"
            ;;
        *)
            echo "No profiling tools available on this platform."
            ;;
    esac

    echo ""
    echo "Profiling complete."
}

#######################################################################
# Phase 4: Generate combined report
#######################################################################
generate_report() {
    echo ""
    echo "============================================================"
    echo "  Phase 4: Generating Combined Report"
    echo "============================================================"
    echo ""

    local report_file="$OUTPUT_DIR/report.txt"

    cat > "$report_file" <<EOF
============================================================
  Extended Buffer Pool Benchmark Report
  Generated: $(date)
  Host: $(hostname)
============================================================

Platform: $PLATFORM
Quick mode: $([[ $QUICK_MODE -eq 1 ]] && echo "YES" || echo "NO")

--- Contents ---
EOF

    if [[ $SKIP_PYTHON -eq 0 ]]; then
        echo "Python benchmark results:" >> "$report_file"
        for d in "$OUTPUT_DIR"/python_*/; do
            if [[ -d "$d" ]]; then
                echo "  $(basename "$d"):" >> "$report_file"
                for f in "$d"/*/*.csv; do
                    if [[ -f "$f" ]]; then
                        echo "    $(basename "$f")" >> "$report_file"
                    fi
                done
            fi
        done
        echo "" >> "$report_file"
    fi

    if [[ $SKIP_HAMMERDB -eq 0 ]] && [[ -f "$OUTPUT_DIR/hammerdb/summary.csv" ]]; then
        echo "HammerDB results:" >> "$report_file"
        cat "$OUTPUT_DIR/hammerdb/summary.csv" >> "$report_file"
        echo "" >> "$report_file"
    fi

    if [[ $SKIP_PROFILING -eq 0 ]] && [[ -d "$OUTPUT_DIR/profiling" ]]; then
        echo "Profiling artifacts:" >> "$report_file"
        ls -la "$OUTPUT_DIR/profiling/" >> "$report_file" 2>/dev/null || true
        echo "" >> "$report_file"
    fi

    echo "" >> "$report_file"
    echo "Full results tree:" >> "$report_file"
    find "$OUTPUT_DIR" -type f | sort >> "$report_file"

    echo "Report: $report_file"
}

#######################################################################
# Main
#######################################################################
main() {
    collect_sysinfo

    if [[ $SKIP_PYTHON -eq 0 ]]; then
        run_python_benchmarks
    fi

    if [[ $SKIP_HAMMERDB -eq 0 ]]; then
        run_hammerdb_benchmarks
    fi

    if [[ $SKIP_PROFILING -eq 0 ]]; then
        run_profiling
    fi

    generate_report

    echo ""
    echo "============================================================"
    echo "  Extended Benchmark Suite Complete"
    echo "  Finished: $(date)"
    echo "  Results:  $OUTPUT_DIR"
    echo "============================================================"
}

main "$@"
