--
-- Benchmark: Storage Footprint (RECNO vs Heap)
--
-- Compares per-row overhead, total table sizes, and the effect of
-- RECNO's built-in compression on storage requirements.
--

-- ================================================================
-- Test 1: Minimal rows (measure per-row overhead)
-- ================================================================
\echo '=== Per-Row Overhead: Minimal Columns ==='

DROP TABLE IF EXISTS perf_footprint_heap_min;
DROP TABLE IF EXISTS perf_footprint_recno_min;

CREATE TABLE perf_footprint_heap_min (id integer) USING heap;
CREATE TABLE perf_footprint_recno_min (id integer) USING recno;

INSERT INTO perf_footprint_heap_min SELECT i FROM generate_series(1, 10000) i;
INSERT INTO perf_footprint_recno_min SELECT i FROM generate_series(1, 10000) i;

SELECT 'HEAP (minimal)' AS am,
       pg_relation_size('perf_footprint_heap_min') AS bytes,
       pg_size_pretty(pg_relation_size('perf_footprint_heap_min')) AS size,
       round(pg_relation_size('perf_footprint_heap_min')::numeric / 10000, 1) AS bytes_per_row
UNION ALL
SELECT 'RECNO (minimal)' AS am,
       pg_relation_size('perf_footprint_recno_min') AS bytes,
       pg_size_pretty(pg_relation_size('perf_footprint_recno_min')) AS size,
       round(pg_relation_size('perf_footprint_recno_min')::numeric / 10000, 1) AS bytes_per_row;

DROP TABLE perf_footprint_heap_min;
DROP TABLE perf_footprint_recno_min;

-- ================================================================
-- Test 2: Wide rows (many columns)
-- ================================================================
\echo ''
\echo '=== Storage: Wide Rows (10 columns) ==='

DROP TABLE IF EXISTS perf_footprint_heap_wide;
DROP TABLE IF EXISTS perf_footprint_recno_wide;

CREATE TABLE perf_footprint_heap_wide (
    id integer, c1 integer, c2 integer, c3 integer, c4 integer,
    c5 text, c6 text, c7 text, c8 text, c9 text
) USING heap;

CREATE TABLE perf_footprint_recno_wide (
    id integer, c1 integer, c2 integer, c3 integer, c4 integer,
    c5 text, c6 text, c7 text, c8 text, c9 text
) USING recno;

INSERT INTO perf_footprint_heap_wide
SELECT i, i, i*2, i*3, i*4,
       md5(i::text), md5((i+1)::text), md5((i+2)::text),
       md5((i+3)::text), md5((i+4)::text)
FROM generate_series(1, 10000) i;

INSERT INTO perf_footprint_recno_wide
SELECT i, i, i*2, i*3, i*4,
       md5(i::text), md5((i+1)::text), md5((i+2)::text),
       md5((i+3)::text), md5((i+4)::text)
FROM generate_series(1, 10000) i;

SELECT 'HEAP (wide)' AS am,
       pg_relation_size('perf_footprint_heap_wide') AS bytes,
       pg_size_pretty(pg_relation_size('perf_footprint_heap_wide')) AS size,
       round(pg_relation_size('perf_footprint_heap_wide')::numeric / 10000, 1) AS bytes_per_row
UNION ALL
SELECT 'RECNO (wide)' AS am,
       pg_relation_size('perf_footprint_recno_wide') AS bytes,
       pg_size_pretty(pg_relation_size('perf_footprint_recno_wide')) AS size,
       round(pg_relation_size('perf_footprint_recno_wide')::numeric / 10000, 1) AS bytes_per_row;

DROP TABLE perf_footprint_heap_wide;
DROP TABLE perf_footprint_recno_wide;

-- ================================================================
-- Test 3: Repetitive data (compression-friendly)
-- ================================================================
\echo ''
\echo '=== Storage: Repetitive Data (compression-friendly) ==='

DROP TABLE IF EXISTS perf_footprint_heap_rep;
DROP TABLE IF EXISTS perf_footprint_recno_rep;

CREATE TABLE perf_footprint_heap_rep (
    id integer,
    category text,
    description text
) USING heap;

CREATE TABLE perf_footprint_recno_rep (
    id integer,
    category text,
    description text
) USING recno;

-- Highly repetitive data benefits from RECNO compression
INSERT INTO perf_footprint_heap_rep
SELECT i,
       'category_' || (i % 10),
       'This is a repeated description string that appears many times in the dataset.'
FROM generate_series(1, 50000) i;

INSERT INTO perf_footprint_recno_rep
SELECT i,
       'category_' || (i % 10),
       'This is a repeated description string that appears many times in the dataset.'
FROM generate_series(1, 50000) i;

SELECT 'HEAP (repetitive)' AS am,
       pg_relation_size('perf_footprint_heap_rep') AS bytes,
       pg_size_pretty(pg_relation_size('perf_footprint_heap_rep')) AS size,
       round(pg_relation_size('perf_footprint_heap_rep')::numeric / 50000, 1) AS bytes_per_row
UNION ALL
SELECT 'RECNO (repetitive)' AS am,
       pg_relation_size('perf_footprint_recno_rep') AS bytes,
       pg_size_pretty(pg_relation_size('perf_footprint_recno_rep')) AS size,
       round(pg_relation_size('perf_footprint_recno_rep')::numeric / 50000, 1) AS bytes_per_row;

-- Compression ratio
SELECT
    'Compression Ratio' AS metric,
    round(heap.bytes::numeric / GREATEST(recno.bytes, 1), 2) AS heap_to_recno
FROM
    (SELECT pg_relation_size('perf_footprint_heap_rep') AS bytes) heap,
    (SELECT pg_relation_size('perf_footprint_recno_rep') AS bytes) recno;

DROP TABLE perf_footprint_heap_rep;
DROP TABLE perf_footprint_recno_rep;

-- ================================================================
-- Test 4: Post-update storage (bloat resistance)
-- ================================================================
\echo ''
\echo '=== Post-Update Storage (Bloat Resistance) ==='

DROP TABLE IF EXISTS perf_footprint_heap_bloat;
DROP TABLE IF EXISTS perf_footprint_recno_bloat;

CREATE TABLE perf_footprint_heap_bloat (id integer, counter integer DEFAULT 0) USING heap;
CREATE TABLE perf_footprint_recno_bloat (id integer, counter integer DEFAULT 0) USING recno;

INSERT INTO perf_footprint_heap_bloat SELECT i FROM generate_series(1, 20000) i;
INSERT INTO perf_footprint_recno_bloat SELECT i FROM generate_series(1, 20000) i;

\echo '--- Before updates ---'
SELECT 'HEAP' AS am,
       pg_size_pretty(pg_relation_size('perf_footprint_heap_bloat')) AS size
UNION ALL
SELECT 'RECNO' AS am,
       pg_size_pretty(pg_relation_size('perf_footprint_recno_bloat')) AS size;

-- 10 rounds of updates
UPDATE perf_footprint_heap_bloat SET counter = counter + 1;
UPDATE perf_footprint_heap_bloat SET counter = counter + 1;
UPDATE perf_footprint_heap_bloat SET counter = counter + 1;
UPDATE perf_footprint_heap_bloat SET counter = counter + 1;
UPDATE perf_footprint_heap_bloat SET counter = counter + 1;
UPDATE perf_footprint_heap_bloat SET counter = counter + 1;
UPDATE perf_footprint_heap_bloat SET counter = counter + 1;
UPDATE perf_footprint_heap_bloat SET counter = counter + 1;
UPDATE perf_footprint_heap_bloat SET counter = counter + 1;
UPDATE perf_footprint_heap_bloat SET counter = counter + 1;

UPDATE perf_footprint_recno_bloat SET counter = counter + 1;
UPDATE perf_footprint_recno_bloat SET counter = counter + 1;
UPDATE perf_footprint_recno_bloat SET counter = counter + 1;
UPDATE perf_footprint_recno_bloat SET counter = counter + 1;
UPDATE perf_footprint_recno_bloat SET counter = counter + 1;
UPDATE perf_footprint_recno_bloat SET counter = counter + 1;
UPDATE perf_footprint_recno_bloat SET counter = counter + 1;
UPDATE perf_footprint_recno_bloat SET counter = counter + 1;
UPDATE perf_footprint_recno_bloat SET counter = counter + 1;
UPDATE perf_footprint_recno_bloat SET counter = counter + 1;

\echo '--- After 10 update rounds ---'
SELECT 'HEAP' AS am,
       pg_relation_size('perf_footprint_heap_bloat') AS bytes,
       pg_size_pretty(pg_relation_size('perf_footprint_heap_bloat')) AS size
UNION ALL
SELECT 'RECNO' AS am,
       pg_relation_size('perf_footprint_recno_bloat') AS bytes,
       pg_size_pretty(pg_relation_size('perf_footprint_recno_bloat')) AS size;

SELECT
    'Bloat Factor' AS metric,
    round(heap.bytes::numeric / GREATEST(recno.bytes, 1), 2) AS heap_to_recno
FROM
    (SELECT pg_relation_size('perf_footprint_heap_bloat') AS bytes) heap,
    (SELECT pg_relation_size('perf_footprint_recno_bloat') AS bytes) recno;

DROP TABLE perf_footprint_heap_bloat;
DROP TABLE perf_footprint_recno_bloat;
