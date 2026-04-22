--
-- RECNO vs HEAP Sequential Scan Performance Benchmark
--

CREATE TABLE bench_scan_heap (
    id INTEGER,
    value NUMERIC(10,2),
    data TEXT
) USING heap;

CREATE TABLE bench_scan_recno (
    id INTEGER,
    value NUMERIC(10,2),
    data TEXT
) USING recno;

-- Insert data
INSERT INTO bench_scan_heap
SELECT i, random() * 10000, repeat('test data ', 10)
FROM generate_series(1, 50000) i;

INSERT INTO bench_scan_recno
SELECT i, random() * 10000, repeat('test data ', 10)
FROM generate_series(1, 50000) i;

-- Show sizes
SELECT 'HEAP' AS am, pg_size_pretty(pg_total_relation_size('bench_scan_heap')) AS size;
SELECT 'RECNO' AS am, pg_size_pretty(pg_total_relation_size('bench_scan_recno')) AS size;

\timing on

-- Sequential scans with aggregation (run multiple times for consistency)
-- HEAP scans
SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM bench_scan_heap;
SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM bench_scan_heap;
SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM bench_scan_heap;

-- RECNO scans
SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM bench_scan_recno;
SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM bench_scan_recno;
SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM bench_scan_recno;

\timing off

DROP TABLE bench_scan_heap;
DROP TABLE bench_scan_recno;
