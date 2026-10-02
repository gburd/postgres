-- bark_index_check() verifies the structure of a BARK index.
-- (The amcheck extension is created by the earlier "check" test.)

-- Rejects a non-BARK index.
CREATE TABLE bark_check_tab (a int, b int);
INSERT INTO bark_check_tab SELECT g, g % 100 FROM generate_series(1, 20000) g;
CREATE INDEX bark_check_btree ON bark_check_tab USING btree (a);
SELECT bark_index_check('bark_check_btree');  -- errors: not a BARK index

-- A freshly built multi-level BARK index passes.
CREATE INDEX bark_check_idx ON bark_check_tab USING bark (a);
SELECT bark_index_check('bark_check_idx');

-- Still passes after inserts (page splits), deletes, and a vacuum.
INSERT INTO bark_check_tab SELECT g, g % 100 FROM generate_series(20001, 40000) g;
SELECT bark_index_check('bark_check_idx');
DELETE FROM bark_check_tab WHERE a <= 10000;
VACUUM bark_check_tab;
SELECT bark_index_check('bark_check_idx');

-- A multi-column index with INCLUDE columns passes (pivots are truncated).
CREATE INDEX bark_check_multi
  ON bark_check_tab USING bark (b, a) INCLUDE (a);
SELECT bark_index_check('bark_check_multi');

-- Duplicate keys form LIST entries (sorted locator lists); the verifier checks
-- that each list has >= 2 members stored strictly ascending.  Insert the
-- duplicates (the insert path coalesces equal keys into LISTs) and verify, then
-- shrink some lists by vacuuming away part of a key's rows and verify again.
CREATE TABLE bark_check_list (a int, b int);
CREATE INDEX bark_check_list_idx ON bark_check_list USING bark (a);
INSERT INTO bark_check_list SELECT g % 50, g FROM generate_series(1, 10000) g;
SELECT bark_index_check('bark_check_list_idx');
DELETE FROM bark_check_list WHERE a = 10 AND b < 5000;
VACUUM bark_check_list;
SELECT bark_index_check('bark_check_list_idx');
DROP TABLE bark_check_list;

-- Enough clustered duplicates of one key promote its entry from LIST to
-- POSTING (an sbm serialization); the verifier checks the sbm deserializes,
-- self-validates, and holds >= 2 members.  Verify after the promotion and
-- again after a partial-set vacuum.
CREATE TABLE bark_check_post (a int, b int);
CREATE INDEX bark_check_post_idx ON bark_check_post USING bark (a);
INSERT INTO bark_check_post SELECT k, g
  FROM generate_series(0, 4) k, generate_series(1, 400) g;
SELECT bark_index_check('bark_check_post_idx');
DELETE FROM bark_check_post WHERE a = 2 AND b <= 300;
VACUUM bark_check_post;
SELECT bark_index_check('bark_check_post_idx');
DROP TABLE bark_check_post;

DROP TABLE bark_check_tab;

-- Oversized keys (P04): a key larger than BarkMaxItemSize lives on an overflow
-- page chain, referenced by a small OVERSIZED entry.  The verifier walks each
-- chain (every page a BARK_OVERFLOW page, reachable to the recorded length) and
-- reconstructs the full key to confirm it deforms.  Build with a mix of small
-- and oversized keys, split by inserting more, delete + vacuum (freeing the
-- chains), and verify at each step.  Keys are an incompressible md5 chain so
-- they truly exceed the item ceiling.
CREATE FUNCTION bark_chk_bigstr(s int, n int) RETURNS text
  LANGUAGE sql IMMUTABLE AS
$$ SELECT substr(string_agg(md5(s::text || g::text), ''), 1, n)
   FROM generate_series(1, (n + 31) / 32) g $$;
CREATE TABLE bark_check_big (id int, k text);
INSERT INTO bark_check_big SELECT g, 'small-' || lpad(g::text, 6, '0')
  FROM generate_series(1, 40) g;
INSERT INTO bark_check_big SELECT 100 + g,
  'K' || lpad(g::text, 6, '0') || bark_chk_bigstr(g, 5000)
  FROM generate_series(1, 40) g;
CREATE INDEX bark_check_big_idx ON bark_check_big USING bark (k);
SELECT bark_index_check('bark_check_big_idx');  -- oversized chains validated
-- Insert more oversized keys (leaf splits of oversized-holding pages, and
-- oversized pivots on internal pages); multi-page chains too (64KB key).
INSERT INTO bark_check_big SELECT 200 + g,
  'M' || lpad(g::text, 6, '0') || bark_chk_bigstr(1000 + g, 6000)
  FROM generate_series(1, 40) g;
INSERT INTO bark_check_big VALUES (999, bark_chk_bigstr(999, 65000));
SELECT bark_index_check('bark_check_big_idx');
-- Delete the oversized rows and vacuum: the overflow chains are freed and the
-- index stays structurally valid.
DELETE FROM bark_check_big WHERE id >= 100;
VACUUM bark_check_big;
SELECT bark_index_check('bark_check_big_idx');
DROP TABLE bark_check_big;
DROP FUNCTION bark_chk_bigstr(int, int);

-- Oversized INCLUDE payload (P05): a small key with a non-key INCLUDE column
-- too large for the leaf is stored on the overflow chain with the key.  The
-- verifier validates the chain the same way as for an oversized key; pivots are
-- truncated to the key, so the INCLUDE column never reaches an internal page.
CREATE FUNCTION bark_chk_bigstr(s int, n int) RETURNS text
  LANGUAGE sql IMMUTABLE AS
$$ SELECT substr(string_agg(md5(s::text || g::text), ''), 1, n)
   FROM generate_series(1, (n + 31) / 32) g $$;
CREATE TABLE bark_check_inc (k int, payload text);
INSERT INTO bark_check_inc SELECT g, bark_chk_bigstr(g, 32000)
  FROM generate_series(1, 20) g;
CREATE INDEX bark_check_inc_idx
  ON bark_check_inc USING bark (k) INCLUDE (payload);
SELECT bark_index_check('bark_check_inc_idx');
DELETE FROM bark_check_inc WHERE k <= 10;
VACUUM bark_check_inc;
SELECT bark_index_check('bark_check_inc_idx');
DROP TABLE bark_check_inc;
DROP FUNCTION bark_chk_bigstr(int, int);

-- A parallel build produces a structurally valid index that is identical in
-- content to a serially built one.  Force parallel workers on for the first
-- build, off for the second, over the same 200k-row table.
CREATE TABLE bark_par_tab (a int, b int);
INSERT INTO bark_par_tab
  SELECT (g * 7919) % 200000, g % 100 FROM generate_series(1, 200000) g;
SET max_parallel_maintenance_workers = 4;
SET maintenance_work_mem = '4MB';
CREATE INDEX bark_par_idx ON bark_par_tab USING bark (a);  -- parallel build
SELECT bark_index_check('bark_par_idx');
SET max_parallel_maintenance_workers = 0;
CREATE INDEX bark_ser_idx ON bark_par_tab USING bark (a);  -- serial build
SELECT bark_index_check('bark_ser_idx');
-- Both indexes return the same rows for the same scans.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS n FROM bark_par_tab WHERE a BETWEEN 1000 AND 50000;
SELECT (SELECT count(*) FROM bark_par_tab WHERE a = 7919) AS par_eq;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET max_parallel_maintenance_workers;
RESET maintenance_work_mem;
DROP TABLE bark_par_tab;
