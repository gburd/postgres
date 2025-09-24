#!/usr/bin/env bash

set -euo pipefail
#set -x

PSQL="$PG_INSTALL_DIR/bin/psql -X -h $PG_DATA_DIR postgres"
PGBENCH="$PG_INSTALL_DIR/bin/pgbench -h $PG_DATA_DIR"

PGBENCH_SCALE=50
PGBENCH_CLIENTS=8
PGBENCH_JOBS=4
PGBENCH_TIME=120

echo "=== Setting up test environment ==="
$PSQL -f setup.sql

echo "=== Setup the benchmark ==="
$PGBENCH -i -s $PGBENCH_SCALE postgres

echo "=== Post-setup configuration ==="
$PSQL -f post_setup.sql

echo "=== Pre-loading data to create page pressure ==="
$PSQL -c "
-- Create more update activity to fill pages with large tuples
UPDATE pgbench_accounts
SET notes = 'preload_' || (random() * 10000)::int || '_' || repeat('large_data_chunk', 50),
    update_count = update_count + 1,
    last_updated = now()
WHERE aid IN (
    SELECT (random() * (100000 * $PGBENCH_SCALE))::int + 1
    FROM generate_series(1, 20000)
);

-- Create some dead tuples by updating again
UPDATE pgbench_accounts
SET notes = 'dead_preload_' || (random() * 10000)::int || '_' || repeat('dead_data_chunk', 60),
    update_count = update_count + 2
WHERE aid IN (
    SELECT (random() * (100000 * $PGBENCH_SCALE))::int + 1
    FROM generate_series(1, 10000)
);
"

echo "=== Capturing baseline statistics ==="
$PSQL -c "SELECT 'BASELINE' as phase, * FROM capture_prune_stats();" >results.log
$PSQL -c "SELECT 'BASELINE_EXIT_ANALYSIS' as phase, * FROM analyze_exit_reasons();" >>results.log

echo "=== Running pgbench HOT update workload ==="
$PGBENCH -c $PGBENCH_CLIENTS -j $PGBENCH_JOBS -T $PGBENCH_TIME -f prune_test.sql -P 10 postgres

echo "=== Capturing mid-test statistics ==="
$PSQL -c "SELECT 'MID_TEST' as phase, * FROM capture_prune_stats();" >>results.log
$PSQL -c "SELECT 'MID_TEST_EXIT_ANALYSIS' as phase, * FROM analyze_exit_reasons();" >>results.log

echo "=== Running standard TPC-B workload for comparison ==="
$PGBENCH -c $PGBENCH_CLIENTS -j $PGBENCH_JOBS -T 30 -P 10 postgres

echo "=== Capturing post-test statistics ==="
$PSQL -c "SELECT 'POST_TEST' as phase, * FROM capture_prune_stats();" >>results.log
$PSQL -c "SELECT 'POST_TEST_EXIT_ANALYSIS' as phase, * FROM analyze_exit_reasons();" >>results.log

echo "=== Exit Reason Analysis and Recommendations ==="
$PSQL -c "
SELECT 
    'EXIT_REASON_SUMMARY' as report_type,
    context,
    calls_total,
    success_rate_pct,
    main_failure_reason,
    failure_count,
    failure_pct
FROM analyze_exit_reasons()
ORDER BY calls_total DESC;
" >>results.log

$PSQL -c "
SELECT 
    'RECOMMENDATIONS' as report_type,
    context,
    main_failure_reason,
    recommendation,
    failure_pct
FROM get_pruning_recommendations()
ORDER BY failure_pct DESC;
" >>results.log

echo "=== HOT Update Effectiveness ==="
$PSQL -c "
SELECT
    schemaname, relname,
    n_tup_ins, n_tup_upd, n_tup_hot_upd,
    CASE WHEN n_tup_upd > 0
	 THEN round(100.0 * n_tup_hot_upd / n_tup_upd, 2)
	 ELSE 0
    END as hot_update_pct,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as table_size
FROM pg_stat_user_tables
WHERE relname = 'pgbench_accounts';
" >>results.log

echo "=== Page-level analysis ==="
$PSQL -c "
SELECT
    relpages,
    reltuples,
    n_dead_tup,
    last_vacuum,
    last_autovacuum,
    pg_size_pretty(pg_relation_size('pgbench_accounts')) as heap_size
FROM pg_stat_user_tables
JOIN pg_class ON pg_class.relname = pg_stat_user_tables.relname
WHERE pg_stat_user_tables.relname = 'pgbench_accounts';
" >>results.log

echo "=== Detailed Statistics Breakdown ==="
$PSQL -c "
SELECT
    context,
    calls_total,
    pages_pruned,
    tuples_pruned,
    space_freed,
    exit_success,
    exit_invalid_xact_xid,
    exit_no_removable_xids,
    exit_page_not_prunable,
    exit_lock_failed,
    exit_other,
    prune_success_rate_pct,
    avg_time_per_call_us
FROM capture_prune_stats()
WHERE calls_total > 0
ORDER BY calls_total DESC;
" >>results.log

echo "=== Test complete. Results in results.log ==="
cat results.log
