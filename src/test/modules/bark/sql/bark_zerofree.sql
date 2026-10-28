--
-- Dividing an entry in place on a leaf with no free bytes at all.
--
-- An insert of a heap TID that falls inside an entry's TID range, in a
-- vector the entry does not cover, divides the entry in two
-- (bark_swap_tid_entry).  The two parts and the new part's line pointer must
-- fit in the old entry's room plus the page's free bytes.  On a page with
-- pd_lower == pd_upper PageGetFreeSpace clamps to zero instead of holding a
-- line pointer back, so two parts that fill exactly the old entry's room
-- must be refused (and the page split), or PageAddItem fails in a critical
-- section.
--
-- CREATE INDEX with fillfactor = 100 can leave a leaf with no free bytes:
-- the leaf holds X (key 'a', a POSTING entry), then 'b' and 'c' fillers
-- sized to the hole, then the high key, the pivot of 'd000000000', takes the
-- last 24 bytes.  X is m dense heap rows, then one row on each of s sparse
-- heap pages; the new TID goes on a sparse page, and the parts are a POSTING
-- (dense) and a coded LIST (the rest).  m and s were found by a sweep so that
-- the two parts take exactly X's room.
--
-- The sizes assume 8 kB pages and 8-byte MAXALIGN.
SELECT current_setting('block_size') <> '8192' OR
       (SELECT max_data_alignment FROM pg_control_init()) <> 8 AS skip_test \gset
\if :skip_test
\quit
\endif

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS pageinspect;
CREATE EXTENSION IF NOT EXISTS amcheck;
RESET client_min_messages;

\set m 750
\set s 40

-- Heap rows a page (all 'a' and 'z' rows are the same size).
CREATE TABLE zf_r (k text COLLATE "C") WITH (fillfactor = 90, autovacuum_enabled = off);
INSERT INTO zf_r SELECT 'z' FROM generate_series(1, 1000);
SELECT count(*) AS r FROM zf_r WHERE (ctid::text::point)[0] = 0 \gset
DROP TABLE zf_r;

-- The m dense 'a' rows first, padded with 'z' to a page boundary, then s
-- sparse pages with 'a' at offset 1 and 'z' in the rest.  Heap fillfactor
-- 90 leaves room for an updated row on its own page.
CREATE TABLE zf (k text COLLATE "C") WITH (fillfactor = 90, autovacuum_enabled = off);
SELECT ((:m + :r - 1) / :r) * :r AS d \gset
INSERT INTO zf
  SELECT CASE WHEN i <= :m THEN 'a'
              WHEN i <= :d THEN 'z'
              WHEN (i - :d - 1) % :r = 0 THEN 'a'
              ELSE 'z' END
  FROM generate_series(1, :d + :s * :r) i ORDER BY i;

-- A probe index on X alone gives X's size and the hole it leaves.
CREATE INDEX zf_p ON zf USING bark (k COLLATE "C")
  WITH (fillfactor = 100, prefix_compression = off) WHERE k = 'a';
SELECT shape, ntids FROM bark_page_items('zf_p', 1);
SELECT h.upper - h.lower AS hole, i.itemlen AS xlen
  FROM page_header(get_raw_page('zf_p', 1)) h, bark_page_items('zf_p', 1) i \gset
DROP INDEX zf_p;

-- Fillers for the hole less the high key (24 bytes and its line pointer):
-- 'b' items cost 28 bytes, 'c' items 20, 28b + 20c = hole - 28, and a 'c'
-- item comes last.
SELECT (:hole - 28) / 4 AS q \gset
SELECT CASE WHEN (3 * :q) % 7 = 0 THEN 7 ELSE (3 * :q) % 7 END AS c \gset
SELECT (:q - 5 * :c) / 7 AS b \gset
INSERT INTO zf SELECT 'b' || lpad(i::text, 9, '0') FROM generate_series(1, :b) i;
INSERT INTO zf SELECT 'c' || lpad(i::text, 6, '0') FROM generate_series(1, :c) i;
INSERT INTO zf VALUES ('d000000000');

CREATE INDEX zf_k ON zf USING bark (k COLLATE "C")
  WITH (fillfactor = 100, prefix_compression = off);
-- Leaf 1 has no free bytes, and X is as in the probe.
SELECT h.upper - h.lower AS free_bytes
  FROM page_header(get_raw_page('zf_k', 1)) h;
SELECT i.shape, i.itemlen = :xlen AS same_x
  FROM bark_page_items('zf_k', 1) i JOIN zf ON zf.ctid = i.htid
 WHERE zf.k = 'a';
SELECT pg_relation_size('zf_k') / 8192 AS pages_before \gset

-- A 'z' row on a middle sparse page becomes 'a': its new version's TID
-- falls inside X in a vector X does not cover.  The two parts take exactly
-- X's room, so with the line pointer they do not fit: the leaf splits.
UPDATE zf SET k = 'a' WHERE ctid = format('(%s,2)', :d / :r + :s / 2)::tid;
SELECT string_agg(i.shape, ' ' ORDER BY b, i.itemoffset) AS parts,
       sum((i.itemlen + 7) / 8 * 8) = :xlen AS parts_fill_x,
       sum(i.ntids) = :m + :s + 1 AS all_tids
  FROM generate_series(1, pg_relation_size('zf_k') / 8192 - 1) b,
       LATERAL bark_page_items('zf_k', b) i JOIN zf ON zf.ctid = i.htid
 WHERE zf.k = 'a' AND i.ntids > 0;
SELECT pg_relation_size('zf_k') / 8192 > :pages_before AS split;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) = :m + :s + 1 AS index_count_ok FROM zf WHERE k = 'a';
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT bark_index_check('zf_k', true);
SELECT bark_index_parent_check('zf_k', true, false);

DROP TABLE zf;
