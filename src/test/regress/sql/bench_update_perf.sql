--
-- RECNO vs HEAP Update Performance Benchmark
-- Tests in-place update advantage and storage bloat
--

-- In-place updates (RECNO's strength)
CREATE TABLE bench_updates_heap (
    id INTEGER PRIMARY KEY,
    counter INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) USING heap;

CREATE TABLE bench_updates_recno (
    id INTEGER PRIMARY KEY,
    counter INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) USING recno;

-- Insert test data
INSERT INTO bench_updates_heap (id)
SELECT i FROM generate_series(1, 10000) i;

INSERT INTO bench_updates_recno (id)
SELECT i FROM generate_series(1, 10000) i;

-- Measure initial sizes
SELECT 'HEAP Initial' AS label,
       pg_size_pretty(pg_total_relation_size('bench_updates_heap')) AS size;

SELECT 'RECNO Initial' AS label,
       pg_size_pretty(pg_total_relation_size('bench_updates_recno')) AS size;

-- Perform multiple update rounds (creates bloat in HEAP, not in RECNO)
\timing on

-- HEAP updates
UPDATE bench_updates_heap SET counter = counter + 1;
UPDATE bench_updates_heap SET counter = counter + 1;
UPDATE bench_updates_heap SET counter = counter + 1;
UPDATE bench_updates_heap SET counter = counter + 1;
UPDATE bench_updates_heap SET counter = counter + 1;
UPDATE bench_updates_heap SET counter = counter + 1;
UPDATE bench_updates_heap SET counter = counter + 1;
UPDATE bench_updates_heap SET counter = counter + 1;
UPDATE bench_updates_heap SET counter = counter + 1;
UPDATE bench_updates_heap SET counter = counter + 1;

-- RECNO updates (should be faster and create less bloat)
UPDATE bench_updates_recno SET counter = counter + 1;
UPDATE bench_updates_recno SET counter = counter + 1;
UPDATE bench_updates_recno SET counter = counter + 1;
UPDATE bench_updates_recno SET counter = counter + 1;
UPDATE bench_updates_recno SET counter = counter + 1;
UPDATE bench_updates_recno SET counter = counter + 1;
UPDATE bench_updates_recno SET counter = counter + 1;
UPDATE bench_updates_recno SET counter = counter + 1;
UPDATE bench_updates_recno SET counter = counter + 1;
UPDATE bench_updates_recno SET counter = counter + 1;

\timing off

-- Measure final sizes (bloat comparison)
SELECT 'HEAP After Updates' AS label,
       pg_total_relation_size('bench_updates_heap') AS bytes,
       pg_size_pretty(pg_total_relation_size('bench_updates_heap')) AS size;

SELECT 'RECNO After Updates' AS label,
       pg_total_relation_size('bench_updates_recno') AS bytes,
       pg_size_pretty(pg_total_relation_size('bench_updates_recno')) AS size;

-- Calculate bloat difference
SELECT
    'Bloat Comparison' AS metric,
    heap_size.bytes AS heap_bytes,
    recno_size.bytes AS recno_bytes,
    round(100.0 * (heap_size.bytes - recno_size.bytes) / heap_size.bytes, 1) || '%' AS heap_bloat_reduction
FROM
    (SELECT pg_total_relation_size('bench_updates_heap') AS bytes) heap_size,
    (SELECT pg_total_relation_size('bench_updates_recno') AS bytes) recno_size;

-- Verify data integrity
SELECT 'HEAP counter check' AS label, COUNT(*) AS rows, SUM(counter) AS total_counter
FROM bench_updates_heap;

SELECT 'RECNO counter check' AS label, COUNT(*) AS rows, SUM(counter) AS total_counter
FROM bench_updates_recno;

-- Cleanup
DROP TABLE bench_updates_heap;
DROP TABLE bench_updates_recno;
