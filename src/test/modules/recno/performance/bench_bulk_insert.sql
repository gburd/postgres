--
-- bench_bulk_insert.sql
--
-- Measures bulk INSERT throughput and resulting storage size
-- for RECNO vs HEAP at various row counts.
--
-- Uses pg_stat_statements (if available) for timing, otherwise
-- relies on \timing output.
--

\timing on

-- ======================================================================
-- Scale 1: 100K rows - basic mixed-type table
-- ======================================================================
\echo '=== Bulk Insert: 100K rows ==='

DROP TABLE IF EXISTS heap_bulk_100k CASCADE;
DROP TABLE IF EXISTS recno_bulk_100k CASCADE;

CREATE TABLE heap_bulk_100k (
    id   INT4,
    val  INT8,
    name TEXT,
    data BYTEA
) USING heap;

CREATE TABLE recno_bulk_100k (
    id   INT4,
    val  INT8,
    name TEXT,
    data BYTEA
) USING recno;

\echo 'HEAP INSERT 100K:'
INSERT INTO heap_bulk_100k
SELECT i,
       i * 17,
       'User-' || i || '-record-' || (i % 1000),
       decode(md5(i::text), 'hex')
FROM generate_series(1, 100000) i;

\echo 'RECNO INSERT 100K:'
INSERT INTO recno_bulk_100k
SELECT i,
       i * 17,
       'User-' || i || '-record-' || (i % 1000),
       decode(md5(i::text), 'hex')
FROM generate_series(1, 100000) i;

SELECT
    '100K rows' AS scale,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_bulk_100k')) AS table_size,
    pg_relation_size('heap_bulk_100k') AS size_bytes,
    (SELECT count(*) FROM heap_bulk_100k) AS row_count
UNION ALL
SELECT
    '100K rows',
    'recno',
    pg_size_pretty(pg_relation_size('recno_bulk_100k')),
    pg_relation_size('recno_bulk_100k'),
    (SELECT count(*) FROM recno_bulk_100k);

-- ======================================================================
-- Scale 2: 1M rows
-- ======================================================================
\echo '=== Bulk Insert: 1M rows ==='

DROP TABLE IF EXISTS heap_bulk_1m CASCADE;
DROP TABLE IF EXISTS recno_bulk_1m CASCADE;

CREATE TABLE heap_bulk_1m (
    id   INT4,
    val  INT8,
    name TEXT,
    data BYTEA
) USING heap;

CREATE TABLE recno_bulk_1m (
    id   INT4,
    val  INT8,
    name TEXT,
    data BYTEA
) USING recno;

\echo 'HEAP INSERT 1M:'
INSERT INTO heap_bulk_1m
SELECT i,
       i * 17,
       'User-' || i || '-record-' || (i % 1000),
       decode(md5(i::text), 'hex')
FROM generate_series(1, 1000000) i;

\echo 'RECNO INSERT 1M:'
INSERT INTO recno_bulk_1m
SELECT i,
       i * 17,
       'User-' || i || '-record-' || (i % 1000),
       decode(md5(i::text), 'hex')
FROM generate_series(1, 1000000) i;

SELECT
    '1M rows' AS scale,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_bulk_1m')) AS table_size,
    pg_relation_size('heap_bulk_1m') AS size_bytes,
    (SELECT count(*) FROM heap_bulk_1m) AS row_count
UNION ALL
SELECT
    '1M rows',
    'recno',
    pg_size_pretty(pg_relation_size('recno_bulk_1m')),
    pg_relation_size('recno_bulk_1m'),
    (SELECT count(*) FROM recno_bulk_1m);

-- ======================================================================
-- Scale 3: Integer-only table (10M rows, lightweight)
-- Tests raw tuple throughput without variable-length overhead.
-- ======================================================================
\echo '=== Bulk Insert: 10M rows (integer-only) ==='

DROP TABLE IF EXISTS heap_bulk_10m CASCADE;
DROP TABLE IF EXISTS recno_bulk_10m CASCADE;

CREATE TABLE heap_bulk_10m (
    id   INT4,
    a    INT4,
    b    INT8
) USING heap;

CREATE TABLE recno_bulk_10m (
    id   INT4,
    a    INT4,
    b    INT8
) USING recno;

\echo 'HEAP INSERT 10M:'
INSERT INTO heap_bulk_10m
SELECT i, i % 1000, i::bigint * 31
FROM generate_series(1, 10000000) i;

\echo 'RECNO INSERT 10M:'
INSERT INTO recno_bulk_10m
SELECT i, i % 1000, i::bigint * 31
FROM generate_series(1, 10000000) i;

SELECT
    '10M rows (int-only)' AS scale,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_bulk_10m')) AS table_size,
    pg_relation_size('heap_bulk_10m') AS size_bytes,
    (SELECT count(*) FROM heap_bulk_10m) AS row_count
UNION ALL
SELECT
    '10M rows (int-only)',
    'recno',
    pg_size_pretty(pg_relation_size('recno_bulk_10m')),
    pg_relation_size('recno_bulk_10m'),
    (SELECT count(*) FROM recno_bulk_10m);

\timing off
