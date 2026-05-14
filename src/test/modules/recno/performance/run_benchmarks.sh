#!/bin/bash
#
# run_benchmarks.sh - Comprehensive RECNO performance benchmark suite
#
# Usage: ./run_benchmarks.sh [PGHOST] [PGPORT] [DBNAME]
#
# Output: CSV files in ./results/ directory
#
# This script runs all RECNO benchmarks and produces comparison data
# between RECNO and HEAP access methods.
#

set -e

PGHOST="${1:-localhost}"
PGPORT="${2:-5432}"
DBNAME="${3:-recno_bench}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create results directory
mkdir -p "${RESULTS_DIR}"

PSQL="psql -h ${PGHOST} -p ${PGPORT} -d ${DBNAME} -X -q"

echo "============================================"
echo "RECNO Performance Benchmark Suite"
echo "============================================"
echo "Host:     ${PGHOST}"
echo "Port:     ${PGPORT}"
echo "Database: ${DBNAME}"
echo "Results:  ${RESULTS_DIR}"
echo "Run ID:   ${TIMESTAMP}"
echo "============================================"

# Create the benchmark database if it doesn't exist
createdb -h "${PGHOST}" -p "${PGPORT}" "${DBNAME}" 2>/dev/null || true

# Write CSV header for main results
RESULTS_CSV="${RESULTS_DIR}/benchmark_${TIMESTAMP}.csv"
echo "benchmark,am,rows,metric,value,unit" > "${RESULTS_CSV}"

append_result() {
    echo "$1,$2,$3,$4,$5,$6" >> "${RESULTS_CSV}"
}

# ---------------------------------------------------------------------------
# Benchmark 1: Compression Effectiveness
# ---------------------------------------------------------------------------
echo ""
echo "--- Benchmark 1: Compression Effectiveness ---"

${PSQL} -f "${SCRIPT_DIR}/bench_compression.sql" \
    -o "${RESULTS_DIR}/compression_${TIMESTAMP}.txt" 2>&1

# Extract compression results into CSV
${PSQL} -At -F',' <<'SQL' >> "${RESULTS_CSV}"
-- This query is run after bench_compression.sql has created the tables
SELECT
    'compression',
    'recno',
    (SELECT count(*) FROM recno_comp_int),
    'table_size_bytes',
    pg_relation_size('recno_comp_int'),
    'bytes'
UNION ALL
SELECT
    'compression',
    'heap',
    (SELECT count(*) FROM heap_comp_int),
    'table_size_bytes',
    pg_relation_size('heap_comp_int'),
    'bytes'
UNION ALL
SELECT
    'compression_text',
    'recno',
    (SELECT count(*) FROM recno_comp_text),
    'table_size_bytes',
    pg_relation_size('recno_comp_text'),
    'bytes'
UNION ALL
SELECT
    'compression_text',
    'heap',
    (SELECT count(*) FROM heap_comp_text),
    'table_size_bytes',
    pg_relation_size('heap_comp_text'),
    'bytes'
UNION ALL
SELECT
    'compression_numeric',
    'recno',
    (SELECT count(*) FROM recno_comp_numeric),
    'table_size_bytes',
    pg_relation_size('recno_comp_numeric'),
    'bytes'
UNION ALL
SELECT
    'compression_numeric',
    'heap',
    (SELECT count(*) FROM heap_comp_numeric),
    'table_size_bytes',
    pg_relation_size('heap_comp_numeric'),
    'bytes';
SQL

echo "  Compression benchmarks complete."

# ---------------------------------------------------------------------------
# Benchmark 2: Bulk Insert Performance
# ---------------------------------------------------------------------------
echo ""
echo "--- Benchmark 2: Bulk Insert Performance ---"

${PSQL} -f "${SCRIPT_DIR}/bench_bulk_insert.sql" \
    -o "${RESULTS_DIR}/bulk_insert_${TIMESTAMP}.txt" 2>&1

echo "  Bulk insert benchmarks complete."

# ---------------------------------------------------------------------------
# Benchmark 3: Update Performance (In-place vs Bloat)
# ---------------------------------------------------------------------------
echo ""
echo "--- Benchmark 3: Update Performance ---"

${PSQL} -f "${SCRIPT_DIR}/bench_update.sql" \
    -o "${RESULTS_DIR}/update_${TIMESTAMP}.txt" 2>&1

echo "  Update benchmarks complete."

# ---------------------------------------------------------------------------
# Benchmark 4: Sequential Scan Performance
# ---------------------------------------------------------------------------
echo ""
echo "--- Benchmark 4: Sequential Scan Performance ---"

${PSQL} -f "${SCRIPT_DIR}/bench_seqscan.sql" \
    -o "${RESULTS_DIR}/seqscan_${TIMESTAMP}.txt" 2>&1

echo "  Sequential scan benchmarks complete."

# ---------------------------------------------------------------------------
# Benchmark 5: Concurrent Workload (pgbench)
# ---------------------------------------------------------------------------
echo ""
echo "--- Benchmark 5: Concurrent Workload ---"

# Setup tables for pgbench
${PSQL} -f "${SCRIPT_DIR}/bench_pgbench_setup.sql" 2>&1

# Run pgbench with HEAP tables
echo "  Running pgbench with HEAP tables (60s, 4 clients)..."
pgbench -h "${PGHOST}" -p "${PGPORT}" -d "${DBNAME}" \
    -f "${SCRIPT_DIR}/pgbench_heap_workload.sql" \
    -c 4 -j 2 -T 60 -P 10 \
    > "${RESULTS_DIR}/pgbench_heap_${TIMESTAMP}.txt" 2>&1 || true

# Run pgbench with RECNO tables
echo "  Running pgbench with RECNO tables (60s, 4 clients)..."
pgbench -h "${PGHOST}" -p "${PGPORT}" -d "${DBNAME}" \
    -f "${SCRIPT_DIR}/pgbench_recno_workload.sql" \
    -c 4 -j 2 -T 60 -P 10 \
    > "${RESULTS_DIR}/pgbench_recno_${TIMESTAMP}.txt" 2>&1 || true

echo "  Concurrent workload benchmarks complete."

# ---------------------------------------------------------------------------
# Final Summary
# ---------------------------------------------------------------------------
echo ""
echo "--- Final Summary ---"

${PSQL} -f "${SCRIPT_DIR}/bench_summary.sql" \
    -o "${RESULTS_DIR}/summary_${TIMESTAMP}.txt" 2>&1

# Cleanup
${PSQL} -f "${SCRIPT_DIR}/bench_cleanup.sql" 2>&1

echo ""
echo "============================================"
echo "Benchmarks complete!"
echo "Results written to: ${RESULTS_DIR}/"
echo "  Main CSV:    benchmark_${TIMESTAMP}.csv"
echo "  Details:     *_${TIMESTAMP}.txt"
echo "============================================"
