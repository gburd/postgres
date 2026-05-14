#!/bin/bash
#
# run_all.sh - Run all RECNO performance benchmarks and plot results.
#
# Usage:
#   ./run_all.sh [--plot-only] [--skip-plot]
#
# This script runs each Perl benchmark script in sequence, then
# optionally generates plots from the resulting CSV files.
#
# Prerequisites:
#   - PostgreSQL built and installed with RECNO support
#   - Perl with PostgreSQL::Test::Cluster module
#   - Python3 with matplotlib and pandas (for plotting)
#
# The Perl scripts use PostgreSQL::Test::Cluster to start their own
# temporary PostgreSQL instances, so no running server is required.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"

PLOT_ONLY=0
SKIP_PLOT=0

for arg in "$@"; do
    case "$arg" in
        --plot-only)  PLOT_ONLY=1 ;;
        --skip-plot)  SKIP_PLOT=1 ;;
        --help|-h)
            echo "Usage: $0 [--plot-only] [--skip-plot]"
            echo ""
            echo "  --plot-only   Only generate plots from existing CSV results"
            echo "  --skip-plot   Run benchmarks but skip plot generation"
            echo ""
            echo "Benchmarks:"
            echo "  bulk_insert.pl      - Bulk insert throughput (1M, 10M rows)"
            echo "  update_workload.pl  - Update performance and bloat"
            echo "  sequential_scan.pl  - Full table scan performance"
            echo "  index_scan.pl       - Index lookup latency"
            exit 0
            ;;
    esac
done

echo "============================================"
echo "RECNO Performance Benchmark Suite"
echo "============================================"
echo "Script dir:  ${SCRIPT_DIR}"
echo "Results dir: ${RESULTS_DIR}"
echo "Date:        $(date)"
echo "============================================"

mkdir -p "${RESULTS_DIR}"

if [ "$PLOT_ONLY" -eq 0 ]; then
    BENCHMARKS=(
        "bulk_insert.pl"
        "update_workload.pl"
        "sequential_scan.pl"
        "index_scan.pl"
    )

    FAILED=0
    for bench in "${BENCHMARKS[@]}"; do
        bench_path="${SCRIPT_DIR}/${bench}"
        if [ ! -f "$bench_path" ]; then
            echo ""
            echo "WARNING: ${bench} not found, skipping."
            continue
        fi

        echo ""
        echo "--------------------------------------------"
        echo "Running: ${bench}"
        echo "--------------------------------------------"

        if perl "$bench_path"; then
            echo "  ${bench}: PASSED"
        else
            echo "  ${bench}: FAILED (exit code $?)"
            FAILED=$((FAILED + 1))
        fi
    done

    echo ""
    echo "============================================"
    if [ "$FAILED" -gt 0 ]; then
        echo "${FAILED} benchmark(s) failed."
    else
        echo "All benchmarks completed successfully."
    fi
    echo "============================================"
fi

# Generate plots
if [ "$SKIP_PLOT" -eq 0 ]; then
    echo ""
    echo "--------------------------------------------"
    echo "Generating plots..."
    echo "--------------------------------------------"

    if command -v python3 &>/dev/null; then
        if python3 -c "import matplotlib, pandas" 2>/dev/null; then
            python3 "${SCRIPT_DIR}/plot_results.py" "${RESULTS_DIR}"
        else
            echo "WARNING: matplotlib or pandas not installed."
            echo "Install with: pip install matplotlib pandas"
            echo "Skipping plot generation."
        fi
    else
        echo "WARNING: python3 not found. Skipping plot generation."
    fi
fi

echo ""
echo "============================================"
echo "Results in: ${RESULTS_DIR}/"
echo ""
ls -lh "${RESULTS_DIR}/"*.csv 2>/dev/null || echo "  (no CSV files yet)"
ls -lh "${RESULTS_DIR}/"*.png 2>/dev/null || echo "  (no PNG files yet)"
echo "============================================"
