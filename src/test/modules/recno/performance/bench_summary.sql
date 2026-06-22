--
-- bench_summary.sql
--
-- Collects final size comparisons and statistics across all
-- benchmark tables. Run after all other benchmarks.
--

\echo '============================================'
\echo 'RECNO vs HEAP - Final Size Comparison'
\echo '============================================'

-- Collect all table sizes
SELECT
    c.relname AS table_name,
    am.amname AS access_method,
    c.reltuples::bigint AS est_rows,
    c.relpages AS pages,
    pg_size_pretty(pg_relation_size(c.oid)) AS table_size,
    pg_relation_size(c.oid) AS size_bytes,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_class c
JOIN pg_am am ON c.relam = am.oid
WHERE c.relkind = 'r'
AND (c.relname LIKE 'heap_%' OR c.relname LIKE 'recno_%'
     OR c.relname LIKE 'pgbench_%')
ORDER BY c.relname;

-- Paired comparison (RECNO vs HEAP for each test)
\echo ''
\echo '============================================'
\echo 'Paired Size Comparison'
\echo '============================================'

WITH sizes AS (
    SELECT
        c.relname,
        am.amname,
        pg_relation_size(c.oid) AS size_bytes
    FROM pg_class c
    JOIN pg_am am ON c.relam = am.oid
    WHERE c.relkind = 'r'
    AND (c.relname LIKE 'heap_%' OR c.relname LIKE 'recno_%')
),
heap_sizes AS (
    SELECT
        replace(relname, 'heap_', '') AS test_name,
        size_bytes AS heap_bytes
    FROM sizes WHERE amname = 'heap'
),
recno_sizes AS (
    SELECT
        replace(relname, 'recno_', '') AS test_name,
        size_bytes AS recno_bytes
    FROM sizes WHERE amname = 'recno'
)
SELECT
    h.test_name,
    pg_size_pretty(h.heap_bytes) AS heap_size,
    pg_size_pretty(r.recno_bytes) AS recno_size,
    CASE WHEN h.heap_bytes > 0
         THEN round(100.0 * (1.0 - r.recno_bytes::numeric / h.heap_bytes), 1)
         ELSE 0
    END AS savings_pct,
    CASE WHEN r.recno_bytes > 0
         THEN round(h.heap_bytes::numeric / r.recno_bytes, 2)
         ELSE 0
    END AS ratio
FROM heap_sizes h
JOIN recno_sizes r ON h.test_name = r.test_name
ORDER BY h.test_name;

-- Statistics
\echo ''
\echo '============================================'
\echo 'Table Statistics'
\echo '============================================'

SELECT
    schemaname,
    relname,
    n_tup_ins AS inserts,
    n_tup_upd AS updates,
    n_tup_del AS deletes,
    n_tup_hot_upd AS hot_updates,
    n_live_tup AS live_tuples,
    n_dead_tup AS dead_tuples,
    vacuum_count,
    autovacuum_count
FROM pg_stat_user_tables
WHERE relname LIKE 'heap_%' OR relname LIKE 'recno_%' OR relname LIKE 'pgbench_%'
ORDER BY relname;
