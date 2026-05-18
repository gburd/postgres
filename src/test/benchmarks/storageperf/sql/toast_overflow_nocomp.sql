--
-- TOAST (heap) vs Overflow (RECNO) Storage Model -- COMPRESSION OFF
--
-- Isolates the *storage model* (out-of-line TOAST vs inline overflow) with
-- compression disabled on both sides, so the numbers reflect placement and
-- in-place vs copy-on-write update behavior -- not the compressor.
--
-- Dimensions measured (after only, no before baseline):
--   1. Speed   -- bulk INSERT, full-scan retrieval, small-column UPDATE on a
--                 large row (the big column is left untouched).
--   2. Space   -- pg_total_relation_size for both AMs, plus an AM-specific
--                 detail line (heap: toast table + toast index; recno: main
--                 fork, which already contains inline overflow pages).
--   3. IO proxy -- EXPLAIN (ANALYZE, BUFFERS) on the small-column UPDATE.
--                 This is a buffers-touched proxy for IO cost, NOT a
--                 lock-contention measurement (a single session cannot show
--                 contention). RECNO updates in place -> fewer pages dirtied;
--                 HEAP writes a new tuple version.
--   4. Dead tuples -- n_dead_tup after the update round (RECNO in-place ~0;
--                 HEAP one dead tuple per updated row).
--
-- Value sizes: 1KB (below heap TOAST threshold), 8KB and 50KB (out-of-line).
--

\timing on

-- Disable compression on both storage models so we measure placement only.
-- RECNO: recno_enable_compression = off.
-- HEAP: per-column STORAGE EXTERNAL forces out-of-line *uncompressed* TOAST,
--   which is the honest "compression off" for heap (default_toast_compression
--   would still compress before deciding to push out of line).
SET recno_enable_compression = off;

\echo '=== TOAST vs Overflow (compression OFF): Setup ==='

-- ================================================================
-- Test 1: storage model at three value sizes
-- ================================================================
DROP TABLE IF EXISTS tonc_heap_1k;
DROP TABLE IF EXISTS tonc_recno_1k;
CREATE TABLE tonc_heap_1k (id serial, data text) USING heap;
CREATE TABLE tonc_recno_1k (id serial, data text) USING recno;
ALTER TABLE tonc_heap_1k ALTER COLUMN data SET STORAGE EXTERNAL;

\echo '--- 1KB INSERT (heap) ---'
INSERT INTO tonc_heap_1k (data) SELECT repeat('A', 1000) FROM generate_series(1, 10000);
\echo '--- 1KB INSERT (recno) ---'
INSERT INTO tonc_recno_1k (data) SELECT repeat('A', 1000) FROM generate_series(1, 10000);

DROP TABLE IF EXISTS tonc_heap_8k;
DROP TABLE IF EXISTS tonc_recno_8k;
CREATE TABLE tonc_heap_8k (id serial, data text) USING heap;
CREATE TABLE tonc_recno_8k (id serial, data text) USING recno;
ALTER TABLE tonc_heap_8k ALTER COLUMN data SET STORAGE EXTERNAL;

\echo '--- 8KB INSERT (heap) ---'
INSERT INTO tonc_heap_8k (data) SELECT repeat('B', 8000) FROM generate_series(1, 5000);
\echo '--- 8KB INSERT (recno) ---'
INSERT INTO tonc_recno_8k (data) SELECT repeat('B', 8000) FROM generate_series(1, 5000);

DROP TABLE IF EXISTS tonc_heap_50k;
DROP TABLE IF EXISTS tonc_recno_50k;
CREATE TABLE tonc_heap_50k (id serial, data text) USING heap;
CREATE TABLE tonc_recno_50k (id serial, data text) USING recno;
ALTER TABLE tonc_heap_50k ALTER COLUMN data SET STORAGE EXTERNAL;

\echo '--- 50KB INSERT (heap) ---'
INSERT INTO tonc_heap_50k (data) SELECT repeat('C', 50000) FROM generate_series(1, 1000);
\echo '--- 50KB INSERT (recno) ---'
INSERT INTO tonc_recno_50k (data) SELECT repeat('C', 50000) FROM generate_series(1, 1000);

-- ================================================================
-- Space: total relation size + AM-specific detail
-- ================================================================
\echo ''
\echo '=== Space: total relation size (all forks / toast included) ==='
SELECT 'heap_1k'  AS tbl, pg_size_pretty(pg_total_relation_size('tonc_heap_1k'))  AS total
UNION ALL SELECT 'recno_1k', pg_size_pretty(pg_total_relation_size('tonc_recno_1k'))
UNION ALL SELECT 'heap_8k',  pg_size_pretty(pg_total_relation_size('tonc_heap_8k'))
UNION ALL SELECT 'recno_8k', pg_size_pretty(pg_total_relation_size('tonc_recno_8k'))
UNION ALL SELECT 'heap_50k', pg_size_pretty(pg_total_relation_size('tonc_heap_50k'))
UNION ALL SELECT 'recno_50k', pg_size_pretty(pg_total_relation_size('tonc_recno_50k'));

\echo ''
\echo '=== Space detail (HEAP): main vs toast table vs toast index ==='
SELECT 'heap_8k' AS tbl,
       pg_size_pretty(pg_relation_size('tonc_heap_8k'))                       AS main,
       pg_size_pretty(pg_relation_size(reltoastrelid))                        AS toast_tbl,
       pg_size_pretty(pg_relation_size((SELECT indexrelid
                                          FROM pg_index
                                         WHERE indrelid = reltoastrelid)))    AS toast_idx
FROM pg_class WHERE relname = 'tonc_heap_8k'
UNION ALL
SELECT 'heap_50k',
       pg_size_pretty(pg_relation_size('tonc_heap_50k')),
       pg_size_pretty(pg_relation_size(reltoastrelid)),
       pg_size_pretty(pg_relation_size((SELECT indexrelid
                                          FROM pg_index
                                         WHERE indrelid = reltoastrelid)))
FROM pg_class WHERE relname = 'tonc_heap_50k';

\echo ''
\echo '=== Space detail (RECNO): main fork includes inline overflow ==='
\echo '(RECNO has no separate overflow relation; overflow pages live in MAIN)'
SELECT 'recno_8k'  AS tbl,
       pg_size_pretty(pg_relation_size('tonc_recno_8k', 'main'))  AS main_fork,
       pg_size_pretty(pg_total_relation_size('tonc_recno_8k'))    AS total_all_forks
UNION ALL
SELECT 'recno_50k',
       pg_size_pretty(pg_relation_size('tonc_recno_50k', 'main')),
       pg_size_pretty(pg_total_relation_size('tonc_recno_50k'));

-- ================================================================
-- Speed: full-scan retrieval of large values
-- ================================================================
\echo ''
\echo '=== Retrieval speed: 8KB values ==='
\echo 'heap:'
SELECT count(*), sum(length(data)) FROM tonc_heap_8k;
\echo 'recno:'
SELECT count(*), sum(length(data)) FROM tonc_recno_8k;

\echo '=== Retrieval speed: 50KB values ==='
\echo 'heap:'
SELECT count(*), sum(length(data)) FROM tonc_heap_50k;
\echo 'recno:'
SELECT count(*), sum(length(data)) FROM tonc_recno_50k;

-- ================================================================
-- Small-column UPDATE on a large row + IO proxy + dead tuples
-- ================================================================
\echo ''
\echo '=== Small-column UPDATE on large rows ==='
DROP TABLE IF EXISTS tonc_upd_heap;
DROP TABLE IF EXISTS tonc_upd_recno;
CREATE TABLE tonc_upd_heap (id serial PRIMARY KEY, status text, data text) USING heap;
CREATE TABLE tonc_upd_recno (id serial PRIMARY KEY, status text, data text) USING recno;
ALTER TABLE tonc_upd_heap ALTER COLUMN data SET STORAGE EXTERNAL;

INSERT INTO tonc_upd_heap (status, data)
  SELECT 'active', repeat('U', 10000) FROM generate_series(1, 5000);
INSERT INTO tonc_upd_recno (status, data)
  SELECT 'active', repeat('U', 10000) FROM generate_series(1, 5000);

\echo '--- IO proxy: EXPLAIN (ANALYZE, BUFFERS) small-column UPDATE (heap) ---'
\echo '(buffers-touched is an IO-cost proxy, NOT lock contention)'
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, COSTS OFF)
  UPDATE tonc_upd_heap SET status = 'updated';

\echo '--- IO proxy: EXPLAIN (ANALYZE, BUFFERS) small-column UPDATE (recno) ---'
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, COSTS OFF)
  UPDATE tonc_upd_recno SET status = 'updated';

\echo ''
\echo '=== Post-update storage + dead tuples ==='
-- n_dead_tup is flushed to the stats collector asynchronously; force a flush
-- so the reads below are deterministic rather than racing the backend.
SELECT pg_stat_force_next_flush();
SELECT 'heap_upd' AS tbl,
       pg_size_pretty(pg_total_relation_size('tonc_upd_heap')) AS total,
       n_dead_tup
FROM pg_stat_user_tables WHERE relname = 'tonc_upd_heap';
SELECT 'recno_upd' AS tbl,
       pg_size_pretty(pg_total_relation_size('tonc_upd_recno')) AS total,
       n_dead_tup
FROM pg_stat_user_tables WHERE relname = 'tonc_upd_recno';

-- Cleanup
DROP TABLE IF EXISTS tonc_heap_1k, tonc_recno_1k;
DROP TABLE IF EXISTS tonc_heap_8k, tonc_recno_8k;
DROP TABLE IF EXISTS tonc_heap_50k, tonc_recno_50k;
DROP TABLE IF EXISTS tonc_upd_heap, tonc_upd_recno;

RESET recno_enable_compression;

\timing off
