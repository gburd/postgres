--
-- Coded LIST entries (V6): a LIST whose heap locators are clustered stores
-- them as the first TID raw, then delta+varbyte coded deltas (GIN's posting
-- list coding), when that is smaller than both the plain 6-bytes-a-locator
-- array and a POSTING entry.  bark_page_items reports both forms as shape
-- LIST, decoding through the same accessors the scan uses, so a coded entry
-- is told from a plain one by its length.
--
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS pageinspect;
CREATE EXTENSION IF NOT EXISTS amcheck;
RESET client_min_messages;

-- The plain LIST length of an entry: its key part (the length of a SINGLE
-- entry of the same key, less its locator-free header) plus six bytes a
-- locator.  The int4 key used below takes 16 bytes as a SINGLE.
CREATE FUNCTION v6_plain_len(ntids int) RETURNS int LANGUAGE sql
  AS $$ SELECT 16 + 6 * ntids $$;

-- Key k has n rows inserted together, so their heap TIDs are consecutive on
-- one page: the deltas are 1, a byte each, and the entry codes.
CREATE TABLE v6c (k int4) WITH (autovacuum_enabled = off);
INSERT INTO v6c SELECT k FROM generate_series(1, 10) n, generate_series(3, 11) k
  ORDER BY k, n;
CREATE INDEX v6c_k ON v6c USING bark (k);
-- Each clustered run is a LIST shorter than the plain form, and its decoded
-- TIDs are exactly the heap's.
SELECT shape, ntids, itemlen < v6_plain_len(ntids) AS coded,
       tids = (SELECT array_agg(ctid ORDER BY ctid) FROM v6c WHERE k = 3)
         AS tids_match
  FROM bark_page_items('v6c_k', 1)
 WHERE shape = 'LIST' AND ntids = 10
 LIMIT 1;
SELECT count(*) FILTER (WHERE itemlen < v6_plain_len(ntids)) AS coded_lists,
       count(*) AS lists
  FROM bark_page_items('v6c_k', 1) WHERE shape = 'LIST';
-- Every TID round-trips: the entries' decoded TIDs are exactly the heap's.
SELECT (SELECT array_agg(t ORDER BY t)
          FROM bark_page_items('v6c_k', 1) i, unnest(i.tids) t
         WHERE i.shape IN ('LIST', 'POSTING', 'SINGLE'))
       = (SELECT array_agg(ctid ORDER BY ctid) FROM v6c) AS all_tids_match;
SELECT bark_index_check('v6c_k', true);

-- Scattered TIDs: one row of the key per heap page (fillfactor 10 and a wide
-- row), so each delta spans a block and codes to several bytes; the chooser
-- keeps whichever form is shorter, never a coded LIST longer than plain.
CREATE TABLE v6s (k int4, pad text) WITH (autovacuum_enabled = off, fillfactor = 10);
INSERT INTO v6s SELECT 1, repeat('x', 700) FROM generate_series(1, 6);
CREATE INDEX v6s_k ON v6s USING bark (k);
SELECT shape, ntids, itemlen <= v6_plain_len(ntids) AS not_longer_than_plain,
       (SELECT count(DISTINCT (ctid::text::point)[0]) FROM v6s) AS heap_pages
  FROM bark_page_items('v6s_k', 1) WHERE shape = 'LIST';
SELECT bark_index_check('v6s_k', true);

-- VACUUM re-forms a coded LIST from its survivors in place.  A subset of a
-- coded LIST codes no longer than the whole (a removed member's two deltas
-- become their sum, which needs no more bytes), but a PLAIN subset can be
-- longer than the coded original: so the survivors stay coded rather than
-- falling back to plain, which would not fit the old slot.
DELETE FROM v6c WHERE k = 3 AND ctid IN
  (SELECT ctid FROM v6c WHERE k = 3 ORDER BY ctid LIMIT 4);
VACUUM v6c;
SELECT ntids, itemlen < v6_plain_len(ntids) AS still_coded,
       tids = (SELECT array_agg(ctid ORDER BY ctid) FROM v6c WHERE k = 3)
         AS tids_match
  FROM bark_page_items('v6c_k', 1)
 WHERE shape = 'LIST' AND ntids = 6;
SELECT bark_index_check('v6c_k', true);
-- Scans see exactly the surviving rows.
SET enable_seqscan = off;
SELECT k, count(*) FROM v6c WHERE k BETWEEN 3 AND 5 GROUP BY k ORDER BY k;
RESET enable_seqscan;

-- Inserting into a coded LIST (ADD_TID) re-codes it; the result is the same
-- set a seqscan sees.
INSERT INTO v6c VALUES (4), (4);
SET enable_seqscan = off;
SELECT count(*) AS k4_index FROM v6c WHERE k = 4;
RESET enable_seqscan;
SELECT count(*) AS k4_heap FROM v6c WHERE k = 4;
SELECT bark_index_check('v6c_k', true);

DROP TABLE v6c, v6s;

-- Heap TIDs inside a coded LIST's range: the heap leaves half of each page
-- free, and rows of other keys updated to the key go on their old version's
-- page, between the key's existing rows.  Each insert decodes the coded
-- LIST, checks the TID is new (bark_entry_has_tid), and re-codes it with the
-- TID in place (bark_entry_add_tid); an entry that reaches its size limit is
-- divided around the TID (INSERT_SWAP).  A row updated twice tests the
-- duplicate check's binary range test before the decode.
CREATE TABLE v6h (k int, v int) WITH (fillfactor = 50, autovacuum_enabled = off);
INSERT INTO v6h SELECT g % 40, g FROM generate_series(1, 40000) g;
CREATE INDEX v6h_k ON v6h USING bark (k);
SELECT count(*) FILTER (WHERE itemlen < v6_plain_len(ntids)) > 0 AS has_coded
  FROM generate_series(1, pg_relation_size('v6h_k') / 8192 - 1) b,
       bark_page_items('v6h_k', b) WHERE shape = 'LIST';
UPDATE v6h SET k = 1 WHERE k = 2 AND v % 4 = 2;
UPDATE v6h SET k = 1 WHERE k = 3 AND v % 8 = 3;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT k, count(*) FROM v6h WHERE k BETWEEN 1 AND 3 GROUP BY k ORDER BY k;
RESET enable_bitmapscan;
RESET enable_seqscan;
SELECT k, count(*) FROM v6h WHERE k BETWEEN 1 AND 3 GROUP BY k ORDER BY k;
SELECT bark_index_check('v6h_k', true);
-- VACUUM after the churn re-forms the coded entries from their survivors.
DELETE FROM v6h WHERE k = 1 AND v % 3 = 0;
VACUUM v6h;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS k1_index FROM v6h WHERE k = 1;
RESET enable_bitmapscan;
RESET enable_seqscan;
SELECT count(*) AS k1_heap FROM v6h WHERE k = 1;
SELECT bark_index_check('v6h_k', true);
DROP TABLE v6h;

-- The same TID-inside-the-range inserts on a prefix-compressed index (the
-- entries are stored prefix-coded, so the swap and its redo free a coded
-- copy) and on an unlogged one (no WAL: a fake LSN), large enough per key
-- that entries reach the item ceiling and are divided around the new TID
-- (INSERT_SWAP).
CREATE TABLE v6p (k text, v int) WITH (fillfactor = 50, autovacuum_enabled = off);
INSERT INTO v6p SELECT 'prefix-shared-by-every-key-' || (g % 4), g
  FROM generate_series(1, 60000) g;
CREATE INDEX v6p_k ON v6p USING bark (k) WITH (prefix_compression = on);
CREATE UNLOGGED TABLE v6u (k int, v int) WITH (fillfactor = 50, autovacuum_enabled = off);
INSERT INTO v6u SELECT g % 4, g FROM generate_series(1, 60000) g;
CREATE INDEX v6u_k ON v6u USING bark (k);
UPDATE v6p SET k = 'prefix-shared-by-every-key-1' WHERE k = 'prefix-shared-by-every-key-2' AND v % 4 = 2;
UPDATE v6u SET k = 1 WHERE k = 2 AND v % 4 = 2;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS p1_index FROM v6p WHERE k = 'prefix-shared-by-every-key-1';
SELECT count(*) AS u1_index FROM v6u WHERE k = 1;
RESET enable_bitmapscan;
RESET enable_seqscan;
SELECT count(*) AS p1_heap FROM v6p WHERE k = 'prefix-shared-by-every-key-1';
SELECT count(*) AS u1_heap FROM v6u WHERE k = 1;
SELECT bark_index_check('v6p_k', true), bark_index_check('v6u_k', true);
DROP TABLE v6p, v6u;

-- Paths whose existing tests now see smaller, coded entries and so reach
-- them less often, driven here with V6 in place.  Temp tables (no WAL):
-- after REINDEX packs the leaves, TIDs inside a run first merge the page's
-- equal-key entries (MERGE) and then divide entries around the TID; and
-- non-HOT updates of a dense key delete dead versions bottom-up from its
-- POSTING entries.
CREATE TEMP TABLE v6t (k text, v int) WITH (fillfactor = 50, autovacuum_enabled = off);
INSERT INTO v6t SELECT 'https://www.example.com/item/' || lpad((g % 6)::text, 8, '0'), g
  FROM generate_series(1, 60000) g;
CREATE INDEX v6t_k ON v6t USING bark (k) WITH (prefix_compression = on, fillfactor = 100);
UPDATE v6t SET k = 'https://www.example.com/item/00000001'
  WHERE k = 'https://www.example.com/item/00000002' AND v % 4 = 2;
REINDEX INDEX v6t_k;
UPDATE v6t SET k = 'https://www.example.com/item/00000001'
  WHERE k = 'https://www.example.com/item/00000003' AND v % 4 = 3;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS t1_index FROM v6t WHERE k = 'https://www.example.com/item/00000001';
RESET enable_bitmapscan;
RESET enable_seqscan;
SELECT count(*) AS t1_heap FROM v6t WHERE k = 'https://www.example.com/item/00000001';
SELECT bark_index_check('v6t_k', true);
CREATE TEMP TABLE v6b (id int PRIMARY KEY, k int, v int)
  WITH (fillfactor = 100, autovacuum_enabled = off);
INSERT INTO v6b SELECT g, g / 3000, 0 FROM generate_series(1, 30000) g;
CREATE INDEX v6b_k ON v6b USING bark (k) WITH (fillfactor = 100);
CREATE INDEX v6b_v ON v6b (v);
SELECT format('UPDATE v6b SET v = %s WHERE id %% 10 = 0', i)
  FROM generate_series(1, 12) i \gexec
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS b_index FROM v6b WHERE k BETWEEN 0 AND 10;
RESET enable_bitmapscan;
RESET enable_seqscan;
SELECT bark_index_check('v6b_k', true);
DROP TABLE v6t, v6b;

-- Prefix-coded leaves of a churned duplicate-key index (as bark.sql's
-- bark_cimu, larger): updates move rows between keys, so the leaves split
-- around entries the insert re-formed, with the halves keeping the page's
-- prefix.
CREATE TABLE v6m (id int, u text, pad text)
  WITH (fillfactor = 70, autovacuum_enabled = off);
INSERT INTO v6m SELECT g, 'https://www.example.com/item/' || lpad((g % 40)::text, 8, '0'),
       repeat('x', 200)
  FROM generate_series(1, 60000) g;
CREATE INDEX v6m_u ON v6m USING bark (u) WITH (prefix_compression = on);
UPDATE v6m SET u = 'https://www.example.com/item/' ||
                   lpad(((substring(u from 30)::int + 1) % 40)::text, 8, '0')
  WHERE hashint4(id) % 3 = 0;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), md5(string_agg(u, ',' ORDER BY id)) AS m_index FROM v6m WHERE u > 'h';
RESET enable_bitmapscan;
RESET enable_seqscan;
SELECT count(*), md5(string_agg(u, ',' ORDER BY id)) AS m_heap FROM v6m;
SELECT bark_index_check('v6m_u', true);
DROP TABLE v6m;
DROP FUNCTION v6_plain_len(int);
