--
-- RECNO vs HEAP: scaled architectural comparison (single buffer regime)
--
-- Driven by run_scaled.sh, which starts a fresh cluster with a *fixed,
-- deliberately small* shared_buffers and invokes this file once per scale.
-- The shell varies :scale so that one invocation has a working set that fits
-- inside shared_buffers and another that greatly exceeds it -- letting us read
-- the buffer-manager behavior of each AM, not just cache-resident numbers.
--
-- psql vars (set by -v on the command line):
--   scale       integer  number of base rows (driver scales the byte volume)
--   label       text     human label for this regime (e.g. 'fits' / 'exceeds')
--   valbytes    integer  width of the wide text column
--
-- Everything is measured AFTER load (no before baseline). Each section prints
-- a labeled row so the shell can grep results out of the psql log.
--
-- Dimensions, mapped to the architectural differences they expose:
--   A. Footprint at rest        -- inline overflow (recno) vs out-of-line TOAST
--   B. Compression efficacy     -- entropy-driven auto (recno) vs TOAST lz4
--   C. Update-in-place vs CoW   -- dead tuples + size growth after wide-row upd
--   D. Scan throughput          -- seq scan cold (post-restart) and warm
--   E. Point lookup throughput  -- single-row fetch by key
--

\set ON_ERROR_STOP on
\timing off

\echo ''
\echo '################################################################'
\echo '# REGIME:' :label '  scale=' :scale '  valbytes=' :valbytes
\echo '################################################################'

SHOW shared_buffers;

-- Compression OFF on both AMs: this section isolates the *storage model* and
-- *buffer-manager behavior*, so on-disk size must track the raw byte volume
-- (otherwise a compressible payload would shrink the working set below
-- shared_buffers and the 'exceeds' regime would be a lie). The compression
-- dimension is measured separately in compression_matrix.sql.
SET recno_enable_compression = off;

-- ----------------------------------------------------------------
-- Build the two tables. Same logical content in both AMs.
-- A narrow hot column (status) + a wide payload column (data).
-- ----------------------------------------------------------------
DROP TABLE IF EXISTS rh_heap;
DROP TABLE IF EXISTS rh_recno;
CREATE TABLE rh_heap  (id bigint PRIMARY KEY, status text, data text) USING heap;
CREATE TABLE rh_recno (id bigint PRIMARY KEY, status text, data text) USING recno;
-- Force out-of-line, uncompressed TOAST for the heap wide column.
ALTER TABLE rh_heap ALTER COLUMN data SET STORAGE EXTERNAL;

\echo '--- LOAD heap ---'
\timing on
INSERT INTO rh_heap (id, status, data)
SELECT g, 'active',
       (SELECT string_agg(md5((g::bigint * 131071 + s)::text), '')
          FROM generate_series(1, GREATEST(:valbytes / 32, 1)) s)
FROM generate_series(1, :scale) g;
\timing off

\echo '--- LOAD recno ---'
\timing on
INSERT INTO rh_recno (id, status, data)
SELECT g, 'active',
       (SELECT string_agg(md5((g::bigint * 131071 + s)::text), '')
          FROM generate_series(1, GREATEST(:valbytes / 32, 1)) s)
FROM generate_series(1, :scale) g;
\timing off

-- ----------------------------------------------------------------
-- A. Footprint at rest (compression OFF on both AMs). Inline overflow
-- (recno, MAIN fork) vs out-of-line uncompressed TOAST (heap).
-- ----------------------------------------------------------------
\echo '=== A. FOOTPRINT (' :label ') ==='
SELECT 'FOOTPRINT' AS tag, :'label' AS regime, 'heap' AS am,
       pg_total_relation_size('rh_heap')  AS total_bytes,
       pg_size_pretty(pg_total_relation_size('rh_heap')) AS total_pretty
UNION ALL
SELECT 'FOOTPRINT', :'label', 'recno',
       pg_total_relation_size('rh_recno'),
       pg_size_pretty(pg_total_relation_size('rh_recno'));

-- ----------------------------------------------------------------
-- D. Sequential scan -- WARM (tables just loaded, buffers hot for the 'fits'
-- regime; for 'exceeds' the load already churned shared_buffers so this is
-- the buffer-eviction path). Measured twice and we keep the second (steadier).
-- ----------------------------------------------------------------
\echo '=== D. SEQ SCAN warm (' :label ') ==='
EXPLAIN (ANALYZE, BUFFERS, TIMING ON, COSTS OFF, FORMAT TEXT)
  SELECT count(*), sum(length(data)) FROM rh_heap;
EXPLAIN (ANALYZE, BUFFERS, TIMING ON, COSTS OFF, FORMAT TEXT)
  SELECT count(*), sum(length(data)) FROM rh_recno;

-- ----------------------------------------------------------------
-- E. Point lookup by primary key, 1000 random keys, aggregate timing.
-- ----------------------------------------------------------------
\echo '=== E. POINT LOOKUP x1000 (' :label ') ==='
\timing on
SELECT count(*) FROM (
  SELECT (SELECT length(data) FROM rh_heap WHERE id = k.k)
  FROM (SELECT (1 + (random() * (:scale - 1))::bigint) AS k
        FROM generate_series(1, 1000)) k
) s;
SELECT count(*) FROM (
  SELECT (SELECT length(data) FROM rh_recno WHERE id = k.k)
  FROM (SELECT (1 + (random() * (:scale - 1))::bigint) AS k
        FROM generate_series(1, 1000)) k
) s;
\timing off

-- ----------------------------------------------------------------
-- C. Wide-row update touching only the narrow column. This is where the AMs
-- diverge hardest: recno updates in place (the wide payload is never rewritten,
-- ~0 dead tuples); heap writes a whole new tuple version (one dead tuple per
-- row, size growth, future VACUUM debt). EXPLAIN BUFFERS is the IO proxy.
-- ----------------------------------------------------------------
\echo '=== C. WIDE-ROW small-column UPDATE (' :label ') ==='
\echo '--- heap update buffers ---'
EXPLAIN (ANALYZE, BUFFERS, TIMING ON, COSTS OFF)
  UPDATE rh_heap SET status = 'updated';
\echo '--- recno update buffers ---'
EXPLAIN (ANALYZE, BUFFERS, TIMING ON, COSTS OFF)
  UPDATE rh_recno SET status = 'updated';

SELECT pg_stat_force_next_flush();

\echo '=== C. POST-UPDATE size + dead tuples (' :label ') ==='
SELECT 'POSTUPD' AS tag, :'label' AS regime, 'heap' AS am,
       pg_total_relation_size('rh_heap')  AS total_bytes,
       pg_size_pretty(pg_total_relation_size('rh_heap')) AS total_pretty,
       n_dead_tup, n_live_tup
FROM pg_stat_user_tables WHERE relname = 'rh_heap'
UNION ALL
SELECT 'POSTUPD', :'label', 'recno',
       pg_total_relation_size('rh_recno'),
       pg_size_pretty(pg_total_relation_size('rh_recno')),
       n_dead_tup, n_live_tup
FROM pg_stat_user_tables WHERE relname = 'rh_recno';

-- ----------------------------------------------------------------
-- E2. VACUUM cost after the update round (heap has dead tuples to reclaim;
-- recno's in-place update leaves little for VACUUM to do).
-- ----------------------------------------------------------------
\echo '=== E2. VACUUM after update (' :label ') ==='
\timing on
VACUUM (ANALYZE) rh_heap;
VACUUM (ANALYZE) rh_recno;
\timing off

\echo '=== POST-VACUUM size (' :label ') ==='
SELECT 'POSTVAC' AS tag, :'label' AS regime, 'heap' AS am,
       pg_size_pretty(pg_total_relation_size('rh_heap')) AS total_pretty
UNION ALL
SELECT 'POSTVAC', :'label', 'recno',
       pg_size_pretty(pg_total_relation_size('rh_recno'));

DROP TABLE rh_heap;
DROP TABLE rh_recno;
