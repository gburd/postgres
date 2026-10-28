-- BARK pages: the meta page, page statistics and items of every entry shape.

-- A one-row index: a root leaf at block 1.
CREATE TABLE bark1 (a int8, b text);
INSERT INTO bark1 VALUES (72057594037927937, 'text');
CREATE INDEX bark1_a_idx ON bark1 USING bark (a);

\x

SELECT * FROM bark_metap('bark1_a_idx');

SELECT * FROM bark_page_stats('bark1_a_idx', -1);
SELECT * FROM bark_page_stats('bark1_a_idx', 0);
SELECT * FROM bark_page_stats('bark1_a_idx', 1);
SELECT * FROM bark_page_stats('bark1_a_idx', 2);

SELECT * FROM bark_page_items('bark1_a_idx', -1);
SELECT * FROM bark_page_items('bark1_a_idx', 0);
SELECT * FROM bark_page_items('bark1_a_idx', 1);
SELECT * FROM bark_page_items('bark1_a_idx', 2);

-- The bytea form gives the same rows.
SELECT * FROM bark_page_items(get_raw_page('bark1_a_idx', 1));

\x

-- bark_multi_page_stats: ranges, a negative count, and a range past the end.
SELECT blkno, type FROM bark_multi_page_stats('bark1_a_idx', 0, 2);
SELECT blkno, type FROM bark_multi_page_stats('bark1_a_idx', 1, -1);
SELECT blkno, type FROM bark_multi_page_stats('bark1_a_idx', 1, 0);
SELECT blkno, type FROM bark_multi_page_stats('bark1_a_idx', 1, 2);

-- A two-level index of distinct keys.  Every leaf is reached by exactly one
-- of the root's downlinks, and the leaves hold every row once (each leaf but
-- the rightmost also holds a high key).
CREATE TABLE bark2 AS SELECT g::int8 AS a FROM generate_series(1, 10000) g;
CREATE INDEX bark2_a_idx ON bark2 USING bark (a);
SELECT level, root = (SELECT max(blkno) FROM bark_multi_page_stats('bark2_a_idx', 1, -1)
                      WHERE type = 'root') AS root_ok
  FROM bark_metap('bark2_a_idx');
SELECT type, flags, count(*) > 1 AS several
  FROM bark_multi_page_stats('bark2_a_idx', 0, -1)
  GROUP BY type, flags ORDER BY type, flags;
SELECT sum(live_items) - count(*) FILTER (WHERE bark_next <> 0) AS rows,
       count(*) FILTER (WHERE bark_prev = 0) AS leftmost,
       count(*) FILTER (WHERE bark_next = 0) AS rightmost
  FROM bark_multi_page_stats('bark2_a_idx', 1, -1) WHERE type = 'leaf';
SELECT (SELECT array_agg(downlink ORDER BY downlink)
          FROM bark_page_items('bark2_a_idx', (SELECT root FROM bark_metap('bark2_a_idx')))) =
       (SELECT array_agg(blkno ORDER BY blkno)
          FROM bark_multi_page_stats('bark2_a_idx', 1, -1) WHERE type = 'leaf')
  AS downlinks_ok;
-- The root's first downlink has no key attributes; the rest are pivots.
SELECT itemoffset, shape, ntids, htid, length(data) AS datalen
  FROM bark_page_items('bark2_a_idx', (SELECT root FROM bark_metap('bark2_a_idx')))
  WHERE itemoffset <= 2;
-- A leaf's heap TIDs are the table's.
SELECT (SELECT array_agg(htid ORDER BY htid) FROM bark_multi_page_stats('bark2_a_idx', 1, -1) s,
          bark_page_items('bark2_a_idx', s.blkno) i
          WHERE s.type = 'leaf' AND (s.bark_next = 0 OR i.itemoffset > 1)) =
       (SELECT array_agg(ctid ORDER BY ctid) FROM bark2) AS htids_ok;

-- LIST and POSTING entries: CREATE INDEX coalesces each key's rows.  Key 1
-- has a few rows spread over the heap, key 2 a long run in heap order.
CREATE TABLE bark3 (k int4);
INSERT INTO bark3 SELECT 2 FROM generate_series(1, 3000);
INSERT INTO bark3 SELECT CASE WHEN g % 100 = 0 THEN 1 ELSE 3 END
  FROM generate_series(1, 2000) g;
CREATE INDEX bark3_k_idx ON bark3 USING bark (k);
-- (data is in the machine's byte order, but sorts in key order either way.)
SELECT shape, count(*) AS entries, sum(ntids) AS ntids,
       bool_and(htid = tids[1]) AS htid_first,
       bool_and(cardinality(tids) = ntids) AS tids_ok
  FROM bark_multi_page_stats('bark3_k_idx', 1, -1) s,
       bark_page_items('bark3_k_idx', s.blkno) i
  WHERE s.type = 'leaf' AND (s.bark_next = 0 OR i.itemoffset > 1)
  GROUP BY data, shape ORDER BY data, shape;
-- Every row is in some entry's TIDs, once.
SELECT (SELECT array_agg(t ORDER BY t)
          FROM bark_multi_page_stats('bark3_k_idx', 1, -1) s,
               bark_page_items('bark3_k_idx', s.blkno) i, unnest(i.tids) t
          WHERE s.type = 'leaf') =
       (SELECT array_agg(ctid ORDER BY ctid) FROM bark3 WHERE k IN (1, 2, 3))
  AS tids_ok;

-- Pivots with a heap TID: with an INCLUDE column nothing coalesces, so a
-- run of one key spans several leaves and the separators need heap TIDs.
CREATE TABLE bark4 (k int4, v int4);
INSERT INTO bark4 SELECT 1, g FROM generate_series(1, 2000) g;
CREATE INDEX bark4_k_idx ON bark4 USING bark (k) INCLUDE (v);
SELECT allequalimage FROM bark_metap('bark4_k_idx');
SELECT shape, count(DISTINCT data) AS keys, count(*) > 1 AS several,
       bool_and(htid IS NOT NULL) AS has_htid
  FROM bark_page_items('bark4_k_idx', (SELECT root FROM bark_metap('bark4_k_idx')))
  WHERE itemoffset > 1
  GROUP BY shape;

-- OVERSIZED entries and their overflow pages.  The keys are incompressible
-- (a chain of md5s) and larger than the largest item a page takes.
CREATE FUNCTION bark_bigstr(s int, n int) RETURNS text
  LANGUAGE sql IMMUTABLE AS
$$ SELECT substr(string_agg(md5(s::text || g::text), ''), 1, n)
   FROM generate_series(1, (n + 31) / 32) g $$;
CREATE TABLE bark5 (k text);
INSERT INTO bark5 SELECT 'small' || g FROM generate_series(1, 3) g;
INSERT INTO bark5 SELECT bark_bigstr(g, 5000) FROM generate_series(1, 2) g;
CREATE INDEX bark5_k_idx ON bark5 USING bark (k);
SELECT shape, ntids, htid IS NOT NULL AS has_htid, overflow_blkno IS NOT NULL AS has_overflow, data IS NULL AS no_data
  FROM bark_page_items('bark5_k_idx', (SELECT root FROM bark_metap('bark5_k_idx')))
  ORDER BY shape, itemoffset;
SELECT s.type, s.live_items, s.flags, s.bark_next
  FROM bark_page_items('bark5_k_idx', (SELECT root FROM bark_metap('bark5_k_idx'))) i,
       bark_page_stats('bark5_k_idx', i.overflow_blkno) s
  WHERE i.shape = 'OVERSIZED';
SELECT * FROM bark_page_items(get_raw_page('bark5_k_idx',
  (SELECT min(overflow_blkno) FROM bark_page_items('bark5_k_idx',
     (SELECT root FROM bark_metap('bark5_k_idx'))))::int));

-- Prefix compression: a BARK_PREFIX leaf holds a PREFIX item, and its other
-- items decode to the same keys an uncompressed index stores.
CREATE TABLE bark6 (k text);
INSERT INTO bark6 SELECT 'a common prefix for all keys ' || lpad(g::text, 4, '0')
  FROM generate_series(1, 50) g;
CREATE INDEX bark6_plain ON bark6 USING bark (k);
CREATE INDEX bark6_pfx ON bark6 USING bark (k) WITH (prefix_compression = on);
SELECT flags FROM bark_page_stats('bark6_pfx', 1);
SELECT itemoffset, shape, ntids, htid, data
  FROM bark_page_items('bark6_pfx', 1) WHERE shape = 'PREFIX';
SELECT (SELECT array_agg(data || htid::text ORDER BY itemoffset)
          FROM bark_page_items('bark6_pfx', 1) WHERE shape <> 'PREFIX') =
       (SELECT array_agg(data || htid::text ORDER BY itemoffset)
          FROM bark_page_items('bark6_plain', 1)) AS same_keys,
       (SELECT sum(itemlen) FROM bark_page_items('bark6_pfx', 1)) <
       (SELECT sum(itemlen) FROM bark_page_items('bark6_plain', 1)) AS smaller;

-- Deleted pages: VACUUM deletes the leaves it empties.  A temp table keeps
-- the effects of VACUUM predictable.
CREATE TEMP TABLE bark7 AS SELECT g::int8 AS a FROM generate_series(1, 10000) g;
CREATE INDEX bark7_a_idx ON bark7 USING bark (a);
DELETE FROM bark7 WHERE a BETWEEN 1000 AND 5000;
VACUUM bark7;
SELECT type, flags, live_items, bark_level
  FROM bark_multi_page_stats('bark7_a_idx', 1, -1) WHERE type = 'deleted'
  LIMIT 1;
SELECT count(*) AS deleted
  FROM bark_multi_page_stats('bark7_a_idx', 1, -1) WHERE type = 'deleted';
SELECT * FROM bark_page_items('bark7_a_idx',
  (SELECT min(blkno) FROM bark_multi_page_stats('bark7_a_idx', 1, -1)
   WHERE type = 'deleted'));

-- Failures: not a BARK index, and pages that are not BARK pages.
CREATE INDEX bark1_a_btree ON bark1 USING btree (a);
SELECT bark_metap('bark1_a_btree');
SELECT bark_page_stats('bark1_a_btree', 1);
SELECT bark_page_items('bark1_a_btree', 1);
SELECT bark_metap('bark1');
\set VERBOSITY terse
SELECT bark_page_items(get_raw_page('bark1_a_btree', 1));
SELECT bark_page_items(get_raw_page('bark1', 0));
SELECT bark_page_items(get_raw_page('bark1_a_idx', 0));
SELECT bark_page_items('aaa'::bytea);
\set VERBOSITY default

-- An all-zero page has no items.
SHOW block_size \gset
SELECT count(*) FROM bark_page_items(decode(repeat('00', :block_size), 'hex'));

DROP TABLE bark1, bark2, bark3, bark4, bark5, bark6, bark7;
DROP FUNCTION bark_bigstr;
