#!/usr/bin/env bash
#
# perf_equivalent.sh -- Linux perf/bpftrace equivalents of the DTrace profiling scripts.
#
# Provides the same measurements as the .d scripts using Linux-native tools:
#   perf stat      -- hardware counters (cache misses, branch mispredictions)
#   perf record    -- sampling profiler for flamegraphs
#   perf probe     -- dynamic tracepoints on PostgreSQL functions
#   bpftrace       -- eBPF-based tracing (DTrace equivalent)
#
# Usage:
#   ./perf_equivalent.sh <command> [options]
#
# Commands:
#   stat           Collect hardware counters during a workload
#   record         Record profile for flamegraph generation
#   flamegraph     Generate flamegraph SVG from perf.data
#   spinlocks      Trace spinlock hold times (uses bpftrace)
#   traversal      Trace list traversal lengths (uses bpftrace)
#   ghost          Trace ghost list operations (uses bpftrace)
#   hitrate        Sample buffer hit rates over time (uses bpftrace)
#   all            Run all profiling simultaneously
#
# Options:
#   -p PID         PostgreSQL backend PID to trace
#   -d DURATION    Duration in seconds (default: 60)
#   -o DIR         Output directory (default: ./profile_output)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Defaults
PG_PID=""
DURATION=60
OUTPUT_DIR="./profile_output"
COMMAND=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        stat|record|flamegraph|spinlocks|traversal|ghost|hitrate|all)
            COMMAND="$1"; shift ;;
        -p) PG_PID="$2"; shift 2 ;;
        -d) DURATION="$2"; shift 2 ;;
        -o) OUTPUT_DIR="$2"; shift 2 ;;
        -h|--help)
            head -25 "$0" | tail -20
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ -z "$COMMAND" ]]; then
    echo "Usage: $0 <command> [options]"
    echo "Run '$0 --help' for details."
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

# Find PostgreSQL PIDs if not specified
find_pg_pids() {
    if [[ -n "$PG_PID" ]]; then
        echo "$PG_PID"
    else
        pgrep -f 'postgres:' | head -20 | tr '\n' ',' | sed 's/,$//'
    fi
}

# Find the postgres binary
find_pg_binary() {
    local pid
    if [[ -n "$PG_PID" ]]; then
        pid="$PG_PID"
    else
        pid=$(pgrep -f 'postgres' | head -1)
    fi
    if [[ -n "$pid" ]]; then
        readlink -f "/proc/$pid/exe" 2>/dev/null || which postgres
    else
        which postgres
    fi
}

#######################################################################
# perf stat -- Hardware counters
#######################################################################
cmd_stat() {
    local pids
    pids=$(find_pg_pids)
    if [[ -z "$pids" ]]; then
        echo "No PostgreSQL processes found. Start postgres first."
        exit 1
    fi

    echo "Recording hardware counters for ${DURATION}s..."
    echo "PIDs: $pids"

    perf stat \
        -e cache-misses,cache-references,instructions,cycles \
        -e branch-misses,branch-instructions \
        -e L1-dcache-load-misses,L1-dcache-loads \
        -e LLC-load-misses,LLC-loads \
        -e context-switches,cpu-migrations \
        -p "$pids" \
        --timeout "$((DURATION * 1000))" \
        2>&1 | tee "$OUTPUT_DIR/perf_stat.txt"

    echo ""
    echo "Results saved to $OUTPUT_DIR/perf_stat.txt"
}

#######################################################################
# perf record -- Sampling profiler
#######################################################################
cmd_record() {
    local pids
    pids=$(find_pg_pids)
    if [[ -z "$pids" ]]; then
        echo "No PostgreSQL processes found."
        exit 1
    fi

    echo "Recording profile for ${DURATION}s..."
    echo "PIDs: $pids"

    perf record \
        -g --call-graph dwarf \
        -F 99 \
        -p "$pids" \
        -o "$OUTPUT_DIR/perf.data" \
        -- sleep "$DURATION"

    echo "Profile saved to $OUTPUT_DIR/perf.data"
    echo "Generate report: perf report -i $OUTPUT_DIR/perf.data"
    echo "Generate flamegraph: $0 flamegraph -o $OUTPUT_DIR"
}

#######################################################################
# flamegraph -- Generate SVG from perf.data
#######################################################################
cmd_flamegraph() {
    local perf_data="$OUTPUT_DIR/perf.data"
    if [[ ! -f "$perf_data" ]]; then
        echo "No perf.data found at $perf_data"
        echo "Run '$0 record' first."
        exit 1
    fi

    echo "Generating flamegraph from $perf_data..."

    # Generate perf script output
    perf script -i "$perf_data" > "$OUTPUT_DIR/perf.script"

    # Check for FlameGraph tools
    local collapse_script=""
    local flamegraph_script=""

    for dir in /usr/local/share/FlameGraph "$HOME/FlameGraph" /opt/FlameGraph; do
        if [[ -f "$dir/stackcollapse-perf.pl" ]]; then
            collapse_script="$dir/stackcollapse-perf.pl"
            flamegraph_script="$dir/flamegraph.pl"
            break
        fi
    done

    if command -v stackcollapse-perf.pl &>/dev/null; then
        collapse_script="stackcollapse-perf.pl"
        flamegraph_script="flamegraph.pl"
    fi

    if [[ -z "$collapse_script" ]]; then
        echo "FlameGraph tools not found."
        echo "Install from: https://github.com/brendangregg/FlameGraph"
        echo "Script output saved to $OUTPUT_DIR/perf.script"
        return
    fi

    "$collapse_script" "$OUTPUT_DIR/perf.script" > "$OUTPUT_DIR/perf.folded"
    "$flamegraph_script" "$OUTPUT_DIR/perf.folded" > "$OUTPUT_DIR/flamegraph.svg"

    echo "Flamegraph: $OUTPUT_DIR/flamegraph.svg"

    # Also generate a buffer-pool-focused flamegraph
    grep -E 'Buffer|Strategy|Victim|freelist|bufmgr|arc_|car_|lirs_|lru_|osic_' \
        "$OUTPUT_DIR/perf.folded" > "$OUTPUT_DIR/perf_bufpool.folded" || true

    if [[ -s "$OUTPUT_DIR/perf_bufpool.folded" ]]; then
        "$flamegraph_script" \
            --title "Buffer Pool Functions" \
            --colors hot \
            "$OUTPUT_DIR/perf_bufpool.folded" > "$OUTPUT_DIR/flamegraph_bufpool.svg"
        echo "Buffer pool flamegraph: $OUTPUT_DIR/flamegraph_bufpool.svg"
    fi
}

#######################################################################
# bpftrace-based commands
#######################################################################
check_bpftrace() {
    if ! command -v bpftrace &>/dev/null; then
        echo "bpftrace not found. Install bpftrace for eBPF tracing."
        exit 1
    fi
}

cmd_spinlocks() {
    check_bpftrace
    local bt_script="$SCRIPT_DIR/spinlock_hold_times.bt"
    if [[ ! -f "$bt_script" ]]; then
        echo "Missing $bt_script"
        exit 1
    fi

    local args=()
    if [[ -n "$PG_PID" ]]; then
        args+=(-p "$PG_PID")
    fi

    echo "Tracing spinlock hold times for ${DURATION}s..."
    timeout "$DURATION" bpftrace "${args[@]}" "$bt_script" \
        2>&1 | tee "$OUTPUT_DIR/spinlock_hold_times.txt" || true

    echo "Results: $OUTPUT_DIR/spinlock_hold_times.txt"
}

cmd_traversal() {
    check_bpftrace
    local bt_script="$SCRIPT_DIR/list_traversal.bt"
    if [[ ! -f "$bt_script" ]]; then
        echo "Missing $bt_script"
        exit 1
    fi

    local args=()
    if [[ -n "$PG_PID" ]]; then
        args+=(-p "$PG_PID")
    fi

    echo "Tracing list traversal for ${DURATION}s..."
    timeout "$DURATION" bpftrace "${args[@]}" "$bt_script" \
        2>&1 | tee "$OUTPUT_DIR/list_traversal.txt" || true

    echo "Results: $OUTPUT_DIR/list_traversal.txt"
}

cmd_ghost() {
    check_bpftrace
    local pg_binary
    pg_binary=$(find_pg_binary)

    local args=()
    if [[ -n "$PG_PID" ]]; then
        args+=(-p "$PG_PID")
    fi

    echo "Tracing ghost list operations for ${DURATION}s..."
    echo "(Using inline bpftrace script)"

    timeout "$DURATION" bpftrace "${args[@]}" -e "
BEGIN { printf(\"Tracing ghost list ops... Ctrl-C to stop.\\n\"); }

uprobe:${pg_binary}:*ghost*,
uprobe:${pg_binary}:*Ghost*
{
    @ghost_ops[probe] = count();
    @ghost_ops_per_sec = count();
}

interval:s:1
{
    printf(\"Ghost ops/s: \"); print(@ghost_ops_per_sec);
    clear(@ghost_ops_per_sec);
}

END
{
    printf(\"\\n=== Ghost Operation Totals ===\\n\");
    print(@ghost_ops);
}
" 2>&1 | tee "$OUTPUT_DIR/ghost_ops.txt" || true

    echo "Results: $OUTPUT_DIR/ghost_ops.txt"
}

cmd_hitrate() {
    check_bpftrace
    local pg_binary
    pg_binary=$(find_pg_binary)

    local args=()
    if [[ -n "$PG_PID" ]]; then
        args+=(-p "$PG_PID")
    fi

    echo "Sampling buffer hit rates for ${DURATION}s..."

    timeout "$DURATION" bpftrace "${args[@]}" -e "
BEGIN
{
    printf(\"%-20s  %10s  %10s  %8s\\n\",
           \"TIMESTAMP\", \"HITS\", \"MISSES\", \"HIT_PCT\");
}

uprobe:${pg_binary}:ReadBuffer_common
{
    @in_readbuf[tid] = 1;
}

uretprobe:${pg_binary}:ReadBuffer_common
/@in_readbuf[tid]/
{
    delete(@in_readbuf[tid]);
}

/* Track shared buffer hits via pg_stat counters if available,
   or via BufferAlloc found_in_buffer return value */
uprobe:${pg_binary}:BufferAlloc
{
    @buf_alloc_entry[tid] = nsecs;
}

uretprobe:${pg_binary}:BufferAlloc
/@buf_alloc_entry[tid]/
{
    /* retval != InvalidBuffer means found in pool (hit) */
    if (retval > 0) {
        @hits++;
    } else {
        @misses++;
    }
    delete(@buf_alloc_entry[tid]);
}

interval:s:1
{
    time(\"%-20H:%M:%S  \");
    print(@hits); print(@misses);
    clear(@hits); clear(@misses);
}

END
{
    printf(\"\\n=== Overall Buffer Stats ===\\n\");
    print(@hits); print(@misses);
    clear(@in_readbuf); clear(@buf_alloc_entry);
}
" 2>&1 | tee "$OUTPUT_DIR/hit_rate_over_time.txt" || true

    echo "Results: $OUTPUT_DIR/hit_rate_over_time.txt"
}

#######################################################################
# all -- run everything in parallel
#######################################################################
cmd_all() {
    echo "Starting all profiling for ${DURATION}s..."
    echo "Output: $OUTPUT_DIR"
    echo ""

    # perf stat in background
    cmd_stat &
    local stat_pid=$!

    # perf record in background
    cmd_record &
    local record_pid=$!

    # bpftrace scripts if available
    if command -v bpftrace &>/dev/null; then
        cmd_spinlocks &
        local spinlock_pid=$!

        cmd_traversal &
        local traversal_pid=$!
    fi

    # Wait for all
    wait "$stat_pid" 2>/dev/null || true
    wait "$record_pid" 2>/dev/null || true
    if command -v bpftrace &>/dev/null; then
        wait "$spinlock_pid" 2>/dev/null || true
        wait "$traversal_pid" 2>/dev/null || true
    fi

    # Generate flamegraph from recorded data
    cmd_flamegraph

    echo ""
    echo "=== All profiling complete ==="
    echo "Results in: $OUTPUT_DIR/"
    ls -la "$OUTPUT_DIR/"
}

#######################################################################
# Dispatch
#######################################################################
case "$COMMAND" in
    stat)       cmd_stat ;;
    record)     cmd_record ;;
    flamegraph) cmd_flamegraph ;;
    spinlocks)  cmd_spinlocks ;;
    traversal)  cmd_traversal ;;
    ghost)      cmd_ghost ;;
    hitrate)    cmd_hitrate ;;
    all)        cmd_all ;;
    *)          echo "Unknown command: $COMMAND"; exit 1 ;;
esac
