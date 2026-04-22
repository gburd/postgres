--
-- RECNO vs HEAP Insert Performance Benchmark
--

-- Test bulk inserts
CREATE TABLE bench_insert_heap (
    id SERIAL PRIMARY KEY,
    value INTEGER,
    data TEXT
) USING heap;

CREATE TABLE bench_insert_recno (
    id SERIAL PRIMARY KEY,
    value INTEGER,
    data TEXT
) USING recno;

\timing on

-- HEAP inserts
INSERT INTO bench_insert_heap (value, data)
SELECT i, md5(i::text)
FROM generate_series(1, 50000) i;

-- RECNO inserts (may be slightly slower due to timestamp overhead)
INSERT INTO bench_insert_recno (value, data)
SELECT i, md5(i::text)
FROM generate_series(1, 50000) i;

\timing off

-- Compare sizes
SELECT 'HEAP' AS am,
       pg_total_relation_size('bench_insert_heap') AS bytes,
       pg_size_pretty(pg_total_relation_size('bench_insert_heap')) AS size;

SELECT 'RECNO' AS am,
       pg_total_relation_size('bench_insert_recno') AS bytes,
       pg_size_pretty(pg_total_relation_size('bench_insert_recno')) AS size;

-- Verify row counts
SELECT 'HEAP' AS am, COUNT(*) AS rows FROM bench_insert_heap;
SELECT 'RECNO' AS am, COUNT(*) AS rows FROM bench_insert_recno;

DROP TABLE bench_insert_heap;
DROP TABLE bench_insert_recno;
