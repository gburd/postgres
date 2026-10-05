--
-- TOAST (heap) vs Overflow (RECNO) Storage Comparison
--
-- Compares how heap's TOAST mechanism and RECNO's overflow records
-- handle large column values at different sizes.
--

\timing on

\echo '--- TOAST vs Overflow: Setup ---'

-- Small large values (1KB - below TOAST threshold for heap)
DROP TABLE IF EXISTS toast_heap_1k;
DROP TABLE IF EXISTS toast_recno_1k;
CREATE TABLE toast_heap_1k (id serial, data text) USING heap;
CREATE TABLE toast_recno_1k (id serial, data text) USING recno;

INSERT INTO toast_heap_1k (data) SELECT repeat('A', 1000) FROM generate_series(1, 10000);
INSERT INTO toast_recno_1k (data) SELECT repeat('A', 1000) FROM generate_series(1, 10000);

\echo '--- 1KB values: storage comparison ---'
SELECT 'heap_1k' AS table_name,
       pg_size_pretty(pg_relation_size('toast_heap_1k')) AS main,
       pg_size_pretty(pg_total_relation_size('toast_heap_1k')) AS total;
SELECT 'recno_1k' AS table_name,
       pg_size_pretty(pg_relation_size('toast_recno_1k')) AS main,
       pg_size_pretty(pg_total_relation_size('toast_recno_1k')) AS total;

-- Medium large values (8KB - triggers TOAST for heap)
DROP TABLE IF EXISTS toast_heap_8k;
DROP TABLE IF EXISTS toast_recno_8k;
CREATE TABLE toast_heap_8k (id serial, data text) USING heap;
CREATE TABLE toast_recno_8k (id serial, data text) USING recno;

INSERT INTO toast_heap_8k (data) SELECT repeat('B', 8000) FROM generate_series(1, 5000);
INSERT INTO toast_recno_8k (data) SELECT repeat('B', 8000) FROM generate_series(1, 5000);

\echo '--- 8KB values: storage comparison ---'
SELECT 'heap_8k' AS table_name,
       pg_size_pretty(pg_relation_size('toast_heap_8k')) AS main,
       pg_size_pretty(pg_total_relation_size('toast_heap_8k')) AS total;
SELECT 'recno_8k' AS table_name,
       pg_size_pretty(pg_relation_size('toast_recno_8k')) AS main,
       pg_size_pretty(pg_total_relation_size('toast_recno_8k')) AS total;

-- Large values (50KB - deep TOAST chain for heap)
DROP TABLE IF EXISTS toast_heap_50k;
DROP TABLE IF EXISTS toast_recno_50k;
CREATE TABLE toast_heap_50k (id serial, data text) USING heap;
CREATE TABLE toast_recno_50k (id serial, data text) USING recno;

INSERT INTO toast_heap_50k (data) SELECT repeat('C', 50000) FROM generate_series(1, 1000);
INSERT INTO toast_recno_50k (data) SELECT repeat('C', 50000) FROM generate_series(1, 1000);

\echo '--- 50KB values: storage comparison ---'
SELECT 'heap_50k' AS table_name,
       pg_size_pretty(pg_relation_size('toast_heap_50k')) AS main,
       pg_size_pretty(pg_total_relation_size('toast_heap_50k')) AS total;
SELECT 'recno_50k' AS table_name,
       pg_size_pretty(pg_relation_size('toast_recno_50k')) AS main,
       pg_size_pretty(pg_total_relation_size('toast_recno_50k')) AS total;

-- Retrieval speed: read back all large values
\echo '--- Retrieval speed: 8KB values ---'
\echo 'heap:'
SELECT count(*), sum(length(data)) FROM toast_heap_8k;
\echo 'recno:'
SELECT count(*), sum(length(data)) FROM toast_recno_8k;

\echo '--- Retrieval speed: 50KB values ---'
\echo 'heap:'
SELECT count(*), sum(length(data)) FROM toast_heap_50k;
\echo 'recno:'
SELECT count(*), sum(length(data)) FROM toast_recno_50k;

-- Update non-large column on rows with TOAST/overflow data
\echo '--- Update non-large column (small change, large row) ---'
DROP TABLE IF EXISTS toast_upd_heap;
DROP TABLE IF EXISTS toast_upd_recno;
CREATE TABLE toast_upd_heap (id serial PRIMARY KEY, status text, data text) USING heap;
CREATE TABLE toast_upd_recno (id serial PRIMARY KEY, status text, data text) USING recno;

INSERT INTO toast_upd_heap (status, data) SELECT 'active', repeat('U', 10000) FROM generate_series(1, 5000);
INSERT INTO toast_upd_recno (status, data) SELECT 'active', repeat('U', 10000) FROM generate_series(1, 5000);

\echo 'heap update (non-large column):'
UPDATE toast_upd_heap SET status = 'updated';
\echo 'recno update (non-large column):'
UPDATE toast_upd_recno SET status = 'updated';

\echo '--- Post-update storage ---'
SELECT 'heap_upd' AS tbl,
       pg_size_pretty(pg_total_relation_size('toast_upd_heap')) AS total,
       n_dead_tup FROM pg_stat_user_tables WHERE relname = 'toast_upd_heap';
SELECT 'recno_upd' AS tbl,
       pg_size_pretty(pg_total_relation_size('toast_upd_recno')) AS total,
       n_dead_tup FROM pg_stat_user_tables WHERE relname = 'toast_upd_recno';

-- Cleanup
DROP TABLE IF EXISTS toast_heap_1k, toast_recno_1k;
DROP TABLE IF EXISTS toast_heap_8k, toast_recno_8k;
DROP TABLE IF EXISTS toast_heap_50k, toast_recno_50k;
DROP TABLE IF EXISTS toast_upd_heap, toast_upd_recno;

\timing off
