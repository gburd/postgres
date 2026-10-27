--
-- BARK keys larger than an index item: overflow chains.
--
-- These cases check the index with bark_index_check() after the changes
-- they make, so they live here rather than in the core regression suite.
-- (The amcheck extension is created by the earlier "check" test.)
--

-- ===========================================================================
-- Oversized keys (P04): a key larger than BarkMaxItemSize is stored on an
-- overflow page chain, transparently to compare / scan / dedup / uniqueness /
-- vacuum.  nbtree errors on such a key; BARK indexes it.  Keys are built from a
-- deterministic md5 chain so they are incompressible (really exceed the item
-- ceiling, not merely large) and the test output is reproducible.
-- ===========================================================================

-- Deterministic incompressible string of ~n bytes, seeded by s (repeatable).
CREATE FUNCTION bark_bigstr(s int, n int) RETURNS text
  LANGUAGE sql IMMUTABLE AS
$$ SELECT substr(string_agg(md5(s::text || g::text), ''), 1, n)
   FROM generate_series(1, (n + 31) / 32) g $$;

-- A mix of small keys and oversized keys (prefix-distinct), built by CREATE
-- INDEX (the bulk loader's oversized second pass) over an already-filled heap.
CREATE TABLE bark_big (id int, k text);
INSERT INTO bark_big SELECT g, 'small-' || lpad(g::text, 6, '0')
  FROM generate_series(1, 40) g;
INSERT INTO bark_big SELECT 100 + g,
  'K' || lpad(g::text, 6, '0') || bark_bigstr(g, 5000)
  FROM generate_series(1, 30) g;
CREATE INDEX bark_big_idx ON bark_big USING bark (k);

-- A parallel build defers the oversized keys too: the participant that meets
-- one says so in the shared state, and the leader inserts them after the load,
-- so the index is the serial build's.  With no worker launched
-- (max_parallel_workers = 0) the build falls back to a serial one.  The
-- parallel_workers reloption grants the workers whatever the table's size.
ALTER TABLE bark_big SET (parallel_workers = 2);
SET max_parallel_maintenance_workers = 2;
CREATE INDEX bark_big_par ON bark_big USING bark (k);
SET max_parallel_workers = 0;
CREATE INDEX bark_big_noworkers ON bark_big USING bark (k);
RESET max_parallel_workers;
RESET max_parallel_maintenance_workers;
ALTER TABLE bark_big RESET (parallel_workers);
SELECT pg_relation_size('bark_big_par') = pg_relation_size('bark_big_idx')
         AS par_same_size,
       pg_relation_size('bark_big_noworkers') = pg_relation_size('bark_big_idx')
         AS noworkers_same_size;
SELECT bark_index_check('bark_big_par', true),
       bark_index_check('bark_big_noworkers', true);
DROP INDEX bark_big_par, bark_big_noworkers;

-- Insert more oversized keys into the built index, forcing leaf splits of
-- pages that hold oversized entries (and oversized pivots on internal pages).
INSERT INTO bark_big SELECT 200 + g,
  'M' || lpad(g::text, 6, '0') || bark_bigstr(1000 + g, 6000)
  FROM generate_series(1, 40) g;

-- Counts and the ordered-scan hash must match a sequential scan exactly: this
-- is the transparency proof (ordering of oversized keys == full-value order).
SET enable_seqscan = off;
SELECT count(*) AS idx_total FROM bark_big;
SELECT count(*) AS idx_oversized FROM bark_big WHERE k >= 'K';
SELECT md5(string_agg(id::text, ',' ORDER BY k)) AS idx_order_hash FROM bark_big;
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS seq_total FROM bark_big;
SELECT count(*) AS seq_oversized FROM bark_big WHERE k >= 'K';
SELECT md5(string_agg(id::text, ',' ORDER BY k)) AS seq_order_hash FROM bark_big;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

-- Keys sharing a long common prefix but differing only in the tail: a
-- prefix-only compare would wrongly tie them.  Equality must find exactly one,
-- and index order must equal full-value order.
CREATE TABLE bark_pfx (id int, k text);
INSERT INTO bark_pfx SELECT g, repeat('P', 5000) || lpad(g::text, 8, '0')
  FROM generate_series(1, 50) g;
CREATE INDEX bark_pfx_idx ON bark_pfx USING bark (k);
SET enable_seqscan = off;
SELECT id FROM bark_pfx WHERE k = repeat('P', 5000) || lpad('23', 8, '0');
SELECT md5(string_agg(id::text, ',' ORDER BY k)) AS pfx_idx_hash FROM bark_pfx;
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT md5(string_agg(id::text, ',' ORDER BY k)) AS pfx_seq_hash FROM bark_pfx;
RESET enable_seqscan;
RESET enable_indexscan;

-- Truly huge keys (20KB, 64KB): larger than a single page, multi-page overflow
-- chain.  Both index_form_tuple and nbtree reject these outright.
CREATE TABLE bark_huge (id int, k text);
INSERT INTO bark_huge VALUES (1, bark_bigstr(1, 20000));
INSERT INTO bark_huge VALUES (2, bark_bigstr(2, 65000));
INSERT INTO bark_huge VALUES (3, 'tiny');
CREATE INDEX bark_huge_idx ON bark_huge USING bark (k);
SET enable_seqscan = off;
SELECT id, length(k) FROM bark_huge ORDER BY k;
SELECT id FROM bark_huge WHERE k = bark_bigstr(2, 65000);
RESET enable_seqscan;

-- Uniqueness over oversized keys: a duplicate oversized key must be rejected.
CREATE TABLE bark_uniq (k text);
INSERT INTO bark_uniq VALUES (bark_bigstr(7, 5000));
CREATE UNIQUE INDEX bark_uniq_idx ON bark_uniq USING bark (k);
INSERT INTO bark_uniq VALUES (bark_bigstr(7, 5000));  -- duplicate: must error

-- Delete + VACUUM must reclaim overflow pages (mark them deleted) and leave the
-- index consistent; the remaining rows must still scan correctly.
DELETE FROM bark_big WHERE id > 200;
VACUUM bark_big;
SET enable_seqscan = off;
SELECT count(*) AS idx_after_vacuum FROM bark_big;
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT count(*) AS seq_after_vacuum FROM bark_big;
RESET enable_seqscan;
RESET enable_indexscan;

-- Oversized INCLUDE payload (P05): a small key with a non-key INCLUDE column too
-- large for the leaf is stored on the overflow chain with the key and returned
-- in full by an index-only scan.  Pivots never carry the INCLUDE column (they
-- are truncated to key attributes), so an oversized INCLUDE never bloats an
-- internal page.  32KB payloads force leaf splits of INCLUDE-oversized entries.
CREATE TABLE bark_inc_big (k int, payload text);
INSERT INTO bark_inc_big SELECT g, bark_bigstr(g, 32000)
  FROM generate_series(1, 15) g;
CREATE INDEX bark_inc_big_idx ON bark_inc_big USING bark (k) INCLUDE (payload);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
-- Index-only scan returns the full 32KB INCLUDE value, byte-for-byte.
EXPLAIN (COSTS OFF)
  SELECT k, length(payload) FROM bark_inc_big WHERE k BETWEEN 1 AND 15;
SELECT k, length(payload) FROM bark_inc_big WHERE k BETWEEN 1 AND 15 ORDER BY k;
SELECT bool_and(payload = bark_bigstr(k, 32000)) AS ios_payload_match
  FROM bark_inc_big WHERE k BETWEEN 1 AND 15;
RESET enable_seqscan;
RESET enable_bitmapscan;
-- Delete + vacuum reclaims the INCLUDE overflow chains; the rest still scans.
DELETE FROM bark_inc_big WHERE k <= 8;
VACUUM bark_inc_big;
SET enable_seqscan = off;
SELECT bool_and(payload = bark_bigstr(k, 32000)) AS after_vacuum_match
  FROM bark_inc_big WHERE k BETWEEN 1 AND 15;
SELECT count(*) AS idx_remaining FROM bark_inc_big WHERE k BETWEEN 1 AND 15;
RESET enable_seqscan;

-- The widest keys that stay inline coalesce as any other: rows of one key
-- make a LIST (forming one tries the coded form first), and a thousand
-- LIST or POSTING entries (which, depends on the block size), each with
-- the whole key as its key part.  The key
-- is a quarter of a page, up to 6000 bytes, so that with 32 kB blocks the
-- entry's body starts past 4095 bytes.
CREATE EXTENSION IF NOT EXISTS pageinspect;
CREATE TABLE bark_wide_dup (k text, n int);
CREATE INDEX bark_wide_dup_k ON bark_wide_dup USING bark (k);
CREATE FUNCTION bark_wide_key(s int) RETURNS text LANGUAGE sql AS
$$ SELECT bark_bigstr(s, least(current_setting('block_size')::int / 4, 6000)) $$;
CREATE FUNCTION bark_wide_shapes() RETURNS text[] LANGUAGE sql AS $$
  SELECT array_agg(DISTINCT i.shape ORDER BY i.shape)
    FROM bark_multi_page_stats('bark_wide_dup_k', 1, -1) p,
         LATERAL bark_page_items('bark_wide_dup_k', p.blkno) i
   WHERE p.type IN ('leaf', 'root') AND i.shape <> 'PIVOT' $$;
INSERT INTO bark_wide_dup SELECT bark_wide_key(1), g FROM generate_series(1, 2) g;
SELECT bark_wide_shapes() AS two_rows;
INSERT INTO bark_wide_dup SELECT bark_wide_key(1), g FROM generate_series(3, 20) g;
INSERT INTO bark_wide_dup SELECT bark_wide_key(2), g FROM generate_series(1, 1000) g;
SELECT bark_wide_shapes() <@ '{LIST,POSTING}' AS coalesced;
SELECT bark_index_check('bark_wide_dup_k', true);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT k = bark_wide_key(1) AS k1, count(*) FROM bark_wide_dup
 WHERE k IN (bark_wide_key(1), bark_wide_key(2)) GROUP BY 1 ORDER BY 1;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE bark_wide_dup;
DROP FUNCTION bark_wide_key(int), bark_wide_shapes();

DROP TABLE bark_big, bark_pfx, bark_huge, bark_uniq, bark_inc_big;
DROP FUNCTION bark_bigstr(int, int);
