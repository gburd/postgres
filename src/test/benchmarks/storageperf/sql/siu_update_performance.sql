--
-- Benchmark: Selective Index Updates (SIU) Performance
--
-- SIU extends HOT to allow in-place updates even when indexed columns change.
-- Instead of inserting new entries into every index, SIU embeds a bitmap in the
-- tuple header recording which columns were modified and only updates indexes
-- that reference those columns.
--
-- This benchmark measures:
--   1. Overhead of SIU code when no indexed columns change (should be ~0)
--   2. Benefit when one indexed column changes (skip unaffected indexes)
--   3. Benefit scaling with number of indexes (1 to 5)
--   4. Impact on storage bloat
--   5. HOT update rates across scenarios
--
-- Run on both master (no SIU) and tepid (SIU) to compare.
--

-- ================================================================
-- Setup: 1-index table
-- ================================================================

DROP TABLE IF EXISTS siu_idx1;

CREATE TABLE siu_idx1 (
    id integer PRIMARY KEY,
    val integer,
    payload text DEFAULT repeat('x', 100)
);
CREATE INDEX idx1_val ON siu_idx1(val);

INSERT INTO siu_idx1 (id, val)
SELECT i, i FROM generate_series(1, 100000) i;

ANALYZE siu_idx1;

-- ================================================================
-- Setup: 2-index table
-- ================================================================

DROP TABLE IF EXISTS siu_idx2;

CREATE TABLE siu_idx2 (
    id integer PRIMARY KEY,
    col_a integer,
    col_b integer,
    payload text DEFAULT repeat('x', 100)
);
CREATE INDEX idx2_a ON siu_idx2(col_a);
CREATE INDEX idx2_b ON siu_idx2(col_b);

INSERT INTO siu_idx2 (id, col_a, col_b)
SELECT i, i, i * 2 FROM generate_series(1, 100000) i;

ANALYZE siu_idx2;

-- ================================================================
-- Setup: 3-index table
-- ================================================================

DROP TABLE IF EXISTS siu_idx3;

CREATE TABLE siu_idx3 (
    id integer PRIMARY KEY,
    col_a integer,
    col_b integer,
    col_c text,
    payload text DEFAULT repeat('x', 80)
);
CREATE INDEX idx3_a ON siu_idx3(col_a);
CREATE INDEX idx3_b ON siu_idx3(col_b);
CREATE INDEX idx3_c ON siu_idx3(col_c);

INSERT INTO siu_idx3 (id, col_a, col_b, col_c)
SELECT i, i, i * 2, 'val_' || i FROM generate_series(1, 100000) i;

ANALYZE siu_idx3;

-- ================================================================
-- Setup: 5-index table
-- ================================================================

DROP TABLE IF EXISTS siu_idx5;

CREATE TABLE siu_idx5 (
    id integer PRIMARY KEY,
    col_a integer,
    col_b integer,
    col_c text,
    col_d timestamp DEFAULT now(),
    col_e boolean DEFAULT true,
    payload text DEFAULT repeat('x', 60)
);
CREATE INDEX idx5_a ON siu_idx5(col_a);
CREATE INDEX idx5_b ON siu_idx5(col_b);
CREATE INDEX idx5_c ON siu_idx5(col_c);
CREATE INDEX idx5_d ON siu_idx5(col_d);
CREATE INDEX idx5_e ON siu_idx5(col_e);

INSERT INTO siu_idx5 (id, col_a, col_b, col_c)
SELECT i, i, i * 2, 'val_' || i FROM generate_series(1, 100000) i;

ANALYZE siu_idx5;

-- ================================================================
-- Record initial sizes
-- ================================================================

\echo '=== Initial Storage Sizes ==='
SELECT relname,
       pg_size_pretty(pg_relation_size(oid)) AS table_size,
       pg_size_pretty(pg_indexes_size(oid)) AS index_size,
       pg_size_pretty(pg_total_relation_size(oid)) AS total_size
FROM pg_class
WHERE relname IN ('siu_idx1', 'siu_idx2', 'siu_idx3', 'siu_idx5')
ORDER BY relname;

-- Record initial stats
SELECT pg_stat_reset();

-- ================================================================
-- Test 1: Update non-indexed column (should be HOT on both branches)
-- ================================================================

\echo ''
\echo '=== Test 1: Update NON-indexed column (payload) ==='
\echo 'Expected: Both master and tepid use HOT, similar performance.'
\echo ''

\timing on

\echo '--- siu_idx1: 5 rounds updating payload ---'
UPDATE siu_idx1 SET payload = repeat('y', 100);
UPDATE siu_idx1 SET payload = repeat('z', 100);
UPDATE siu_idx1 SET payload = repeat('a', 100);
UPDATE siu_idx1 SET payload = repeat('b', 100);
UPDATE siu_idx1 SET payload = repeat('c', 100);

\echo '--- siu_idx5: 5 rounds updating payload ---'
UPDATE siu_idx5 SET payload = repeat('y', 60);
UPDATE siu_idx5 SET payload = repeat('z', 60);
UPDATE siu_idx5 SET payload = repeat('a', 60);
UPDATE siu_idx5 SET payload = repeat('b', 60);
UPDATE siu_idx5 SET payload = repeat('c', 60);

\timing off

\echo ''
\echo '--- HOT update stats after non-indexed updates ---'
SELECT relname,
       n_tup_upd,
       n_tup_hot_upd,
       CASE WHEN n_tup_upd > 0
            THEN round(100.0 * n_tup_hot_upd / n_tup_upd, 1)
            ELSE 0 END AS hot_pct,
       n_tup_newpage_upd
FROM pg_stat_user_tables
WHERE relname IN ('siu_idx1', 'siu_idx5')
ORDER BY relname;

-- ================================================================
-- Test 2: Update ONE indexed column (SIU should help on tepid)
-- ================================================================

-- Reset stats for clean measurement
SELECT pg_stat_reset();

\echo ''
\echo '=== Test 2: Update ONE indexed column (col_a) ==='
\echo 'Expected on master: all indexes updated every time.'
\echo 'Expected on tepid:  SIU skips indexes not covering col_a.'
\echo ''

\timing on

\echo '--- siu_idx1: 5 rounds updating val (the only secondary index) ---'
UPDATE siu_idx1 SET val = val + 1;
UPDATE siu_idx1 SET val = val + 1;
UPDATE siu_idx1 SET val = val + 1;
UPDATE siu_idx1 SET val = val + 1;
UPDATE siu_idx1 SET val = val + 1;

\echo '--- siu_idx2: 5 rounds updating col_a (1 of 2 secondary indexes) ---'
UPDATE siu_idx2 SET col_a = col_a + 1;
UPDATE siu_idx2 SET col_a = col_a + 1;
UPDATE siu_idx2 SET col_a = col_a + 1;
UPDATE siu_idx2 SET col_a = col_a + 1;
UPDATE siu_idx2 SET col_a = col_a + 1;

\echo '--- siu_idx3: 5 rounds updating col_a (1 of 3 secondary indexes) ---'
UPDATE siu_idx3 SET col_a = col_a + 1;
UPDATE siu_idx3 SET col_a = col_a + 1;
UPDATE siu_idx3 SET col_a = col_a + 1;
UPDATE siu_idx3 SET col_a = col_a + 1;
UPDATE siu_idx3 SET col_a = col_a + 1;

\echo '--- siu_idx5: 5 rounds updating col_a (1 of 5 secondary indexes) ---'
UPDATE siu_idx5 SET col_a = col_a + 1;
UPDATE siu_idx5 SET col_a = col_a + 1;
UPDATE siu_idx5 SET col_a = col_a + 1;
UPDATE siu_idx5 SET col_a = col_a + 1;
UPDATE siu_idx5 SET col_a = col_a + 1;

\timing off

\echo ''
\echo '--- HOT update stats after single-column indexed updates ---'
SELECT relname,
       n_tup_upd,
       n_tup_hot_upd,
       CASE WHEN n_tup_upd > 0
            THEN round(100.0 * n_tup_hot_upd / n_tup_upd, 1)
            ELSE 0 END AS hot_pct,
       n_tup_newpage_upd
FROM pg_stat_user_tables
WHERE relname IN ('siu_idx1', 'siu_idx2', 'siu_idx3', 'siu_idx5')
ORDER BY relname;

\echo ''
\echo '--- Per-index insert counts (on tepid, unaffected indexes should show fewer) ---'
SELECT s.relname AS table,
       s.indexrelname AS index,
       s.idx_scan,
       s.idx_tup_read,
       s.idx_tup_fetch,
       pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size
FROM pg_stat_user_indexes s
WHERE s.relname IN ('siu_idx1', 'siu_idx2', 'siu_idx3', 'siu_idx5')
ORDER BY s.relname, s.indexrelname;

-- ================================================================
-- Test 3: Update ALL indexed columns
-- ================================================================

SELECT pg_stat_reset();

\echo ''
\echo '=== Test 3: Update ALL indexed columns ==='
\echo 'Expected: Similar performance on both branches (no indexes to skip).'
\echo ''

\timing on

\echo '--- siu_idx2: 5 rounds updating col_a AND col_b ---'
UPDATE siu_idx2 SET col_a = col_a + 1, col_b = col_b + 1;
UPDATE siu_idx2 SET col_a = col_a + 1, col_b = col_b + 1;
UPDATE siu_idx2 SET col_a = col_a + 1, col_b = col_b + 1;
UPDATE siu_idx2 SET col_a = col_a + 1, col_b = col_b + 1;
UPDATE siu_idx2 SET col_a = col_a + 1, col_b = col_b + 1;

\echo '--- siu_idx3: 5 rounds updating col_a, col_b, AND col_c ---'
UPDATE siu_idx3 SET col_a = col_a + 1, col_b = col_b + 1, col_c = 'upd_' || col_a;
UPDATE siu_idx3 SET col_a = col_a + 1, col_b = col_b + 1, col_c = 'upd_' || col_a;
UPDATE siu_idx3 SET col_a = col_a + 1, col_b = col_b + 1, col_c = 'upd_' || col_a;
UPDATE siu_idx3 SET col_a = col_a + 1, col_b = col_b + 1, col_c = 'upd_' || col_a;
UPDATE siu_idx3 SET col_a = col_a + 1, col_b = col_b + 1, col_c = 'upd_' || col_a;

\timing off

\echo ''
\echo '--- HOT update stats after all-column indexed updates ---'
SELECT relname,
       n_tup_upd,
       n_tup_hot_upd,
       CASE WHEN n_tup_upd > 0
            THEN round(100.0 * n_tup_hot_upd / n_tup_upd, 1)
            ELSE 0 END AS hot_pct,
       n_tup_newpage_upd
FROM pg_stat_user_tables
WHERE relname IN ('siu_idx2', 'siu_idx3')
ORDER BY relname;

-- ================================================================
-- Test 4: Storage bloat after sustained single-column updates
-- ================================================================

SELECT pg_stat_reset();

\echo ''
\echo '=== Test 4: Storage Bloat After 10 Rounds of Single-Column Updates ==='
\echo ''

\echo '--- Sizes before additional updates ---'
SELECT relname,
       pg_size_pretty(pg_relation_size(oid)) AS table_size,
       pg_size_pretty(pg_indexes_size(oid)) AS index_size,
       pg_size_pretty(pg_total_relation_size(oid)) AS total_size
FROM pg_class
WHERE relname IN ('siu_idx1', 'siu_idx2', 'siu_idx3', 'siu_idx5')
ORDER BY relname;

\timing on

\echo '--- siu_idx5: 10 more rounds updating col_a ---'
UPDATE siu_idx5 SET col_a = col_a + 1;
UPDATE siu_idx5 SET col_a = col_a + 1;
UPDATE siu_idx5 SET col_a = col_a + 1;
UPDATE siu_idx5 SET col_a = col_a + 1;
UPDATE siu_idx5 SET col_a = col_a + 1;
UPDATE siu_idx5 SET col_a = col_a + 1;
UPDATE siu_idx5 SET col_a = col_a + 1;
UPDATE siu_idx5 SET col_a = col_a + 1;
UPDATE siu_idx5 SET col_a = col_a + 1;
UPDATE siu_idx5 SET col_a = col_a + 1;

\timing off

\echo ''
\echo '--- Sizes after 10 additional single-column updates ---'
SELECT relname,
       pg_size_pretty(pg_relation_size(oid)) AS table_size,
       pg_size_pretty(pg_indexes_size(oid)) AS index_size,
       pg_size_pretty(pg_total_relation_size(oid)) AS total_size
FROM pg_class
WHERE relname IN ('siu_idx5')
ORDER BY relname;

\echo ''
\echo '--- Per-index sizes for siu_idx5 ---'
SELECT c.relname AS index_name,
       pg_size_pretty(pg_relation_size(c.oid)) AS size
FROM pg_class c
JOIN pg_index i ON c.oid = i.indexrelid
WHERE i.indrelid = 'siu_idx5'::regclass
ORDER BY c.relname;

\echo ''
\echo '--- Dead tuple counts ---'
SELECT relname,
       n_live_tup,
       n_dead_tup,
       n_tup_hot_upd,
       n_tup_newpage_upd,
       CASE WHEN n_tup_upd > 0
            THEN round(100.0 * n_tup_hot_upd / n_tup_upd, 1)
            ELSE 0 END AS hot_pct
FROM pg_stat_user_tables
WHERE relname IN ('siu_idx5')
ORDER BY relname;

-- ================================================================
-- Test 5: Vacuum recovery comparison
-- ================================================================

\echo ''
\echo '=== Test 5: Vacuum Recovery ==='
\echo ''

\echo '--- Before VACUUM ---'
SELECT relname,
       pg_size_pretty(pg_relation_size(oid)) AS table_size,
       pg_size_pretty(pg_total_relation_size(oid)) AS total_size
FROM pg_class
WHERE relname IN ('siu_idx5');

\timing on
VACUUM siu_idx5;
\timing off

\echo '--- After VACUUM ---'
SELECT relname,
       pg_size_pretty(pg_relation_size(oid)) AS table_size,
       pg_size_pretty(pg_total_relation_size(oid)) AS total_size
FROM pg_class
WHERE relname IN ('siu_idx5');

SELECT relname,
       n_live_tup,
       n_dead_tup
FROM pg_stat_user_tables
WHERE relname IN ('siu_idx5');

-- ================================================================
-- Cleanup
-- ================================================================
DROP TABLE IF EXISTS siu_idx1;
DROP TABLE IF EXISTS siu_idx2;
DROP TABLE IF EXISTS siu_idx3;
DROP TABLE IF EXISTS siu_idx5;
