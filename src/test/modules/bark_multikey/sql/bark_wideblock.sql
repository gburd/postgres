-- BARK on real locator offsets above MaxHeapTuplesPerPage.
-- bark_test_wideblock is heap with every TID translated: seven heap blocks
-- make one block, so a row's offset reaches 7 * MaxHeapTuplesPerPage (2037
-- at BLCKSZ 8192), and its offset > 291 rows are ones that a mapping laid
-- out for heap's stride would put on another block.  Every index result is
-- compared with a sequential scan; every index is checked with
-- bark_index_check(index, true).  The planner makes no bitmap scan of the
-- table (its offsets exceed a TID bitmap's).
CREATE EXTENSION IF NOT EXISTS bark_multikey;
CREATE EXTENSION IF NOT EXISTS pageinspect;
SET max_parallel_maintenance_workers = 0;

CREATE TABLE wb (id int, k int, u int, a int4[])
  USING bark_test_wideblock WITH (autovacuum_enabled = off);
-- k: 30 rows a key, scattered, so each key is a coded LIST; k = -1, every
-- row of a run of 3000, a POSTING; k >= 1000, a SINGLE each; k = -2, rows
-- 5000 TIDs apart, a plain LIST (its deltas code larger); u: unique; a: two
-- elements.
INSERT INTO wb SELECT g, CASE WHEN g BETWEEN 4001 AND 7000 THEN -1
                              WHEN g % 5000 = 1 THEN -2
                              WHEN g % 1000 = 999 THEN 1000 + g
                              ELSE g % 400 END, g,
  ARRAY[g % 13, 100 + g % 17] FROM generate_series(1, 15000) g;
SELECT max((ctid::text::point)[1]) > 291 AS wide_offsets,
       count(*) FILTER (WHERE (ctid::text::point)[1] > 291) > 10000 AS most_wide
  FROM wb;

-- Serial and parallel builds, a unique index, and a GIN-interface class.
CREATE INDEX wb_k ON wb USING bark (k);
CREATE UNIQUE INDEX wb_u ON wb USING bark (u);
CREATE INDEX wb_a ON wb USING bark (a bark_gin_int4_array_ops);
SET max_parallel_maintenance_workers = 2;
SET min_parallel_table_scan_size = 0;
ALTER TABLE wb SET (parallel_workers = 2);
CREATE INDEX wb_kp ON wb USING bark (k, id);
RESET max_parallel_maintenance_workers;
RESET min_parallel_table_scan_size;
SELECT bark_test_tid_stride('wb_k') AS stride, bark_test_meta_stride('wb_kp') AS stride_kp;
SELECT bark_index_check(i, true) FROM unnest(ARRAY['wb_k', 'wb_u', 'wb_a', 'wb_kp']::regclass[]) i;

-- The entry shapes the build made, and that their TIDs are the rows'.
-- A LIST is coded when its stored t_tid's offset has BARK_LIST_CODED (0x1000).
CREATE FUNCTION wb_shapes(idx text) RETURNS TABLE (shape text, n bigint, wide bool)
LANGUAGE sql AS $$
  SELECT i.shape || CASE WHEN i.shape = 'LIST' AND
                              ((i.ctid::text::point)[1]::int & 4096) <> 0
                         THEN ' coded' ELSE '' END,
         count(*), bool_or(EXISTS (SELECT 1 FROM unnest(coalesce(i.tids, ARRAY[i.htid])) t
                                   WHERE (t::text::point)[1] > 291))
  FROM bark_multi_page_stats(idx, 1, -1) s,
       LATERAL bark_page_items(idx, s.blkno) i
  WHERE s.type IN ('leaf', 'root') AND i.shape <> 'PIVOT'
  GROUP BY 1 ORDER BY 1 $$;
SELECT * FROM wb_shapes('wb_k');
-- the TIDs bark_page_items shows are exactly the rows'
SELECT (SELECT array_agg(ctid ORDER BY ctid) FROM wb) =
       (SELECT array_agg(t ORDER BY t) FROM bark_multi_page_stats('wb_k', 1, -1) s,
          LATERAL bark_page_items('wb_k', s.blkno) i,
          LATERAL unnest(coalesce(i.tids, ARRAY[i.htid])) t
        WHERE s.type IN ('leaf', 'root') AND i.shape <> 'PIVOT') AS built_exactly;

-- Index, index-only, backward, SAOP and GIN-interface scans against a
-- seqscan: 'ok' when they agree.
CREATE FUNCTION wb_same(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  m text;
  r text[] := '{}';
  h text;
BEGIN
  FOREACH m IN ARRAY ARRAY['index', 'indexonly', 'seq'] LOOP
    PERFORM set_config('enable_seqscan', (m = 'seq')::text, true);
    PERFORM set_config('enable_indexscan', (m = 'index')::text, true);
    PERFORM set_config('enable_indexonlyscan', (m = 'indexonly')::text, true);
    PERFORM set_config('enable_bitmapscan', 'off', true);
    EXECUTE 'SELECT md5(coalesce(string_agg(x::text, '','' ORDER BY x::text), '''')) FROM ('
      || q || ') x' INTO h;
    r := r || h;
  END LOOP;
  RETURN CASE WHEN r[1] = r[3] AND r[2] = r[3] THEN 'ok'
              ELSE 'MISMATCH ' || array_to_string(r, ' ') END;
END $$;
-- The rows of q, in their order, by index scan and by seqscan.
CREATE FUNCTION wb_order(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE a text; b text;
BEGIN
  PERFORM set_config('enable_seqscan', 'off', true);
  PERFORM set_config('enable_bitmapscan', 'off', true);
  EXECUTE 'SELECT string_agg(x::text, '','') FROM (' || q || ') x' INTO a;
  PERFORM set_config('enable_seqscan', 'on', true);
  PERFORM set_config('enable_indexscan', 'off', true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  EXECUTE 'SELECT string_agg(x::text, '','') FROM (' || q || ') x' INTO b;
  RETURN CASE WHEN a = b THEN 'ok' ELSE 'MISMATCH' END;
END $$;
CREATE TABLE wb_q (q text);
INSERT INTO wb_q VALUES
  ('SELECT id, ctid FROM wb WHERE k = 7'),
  ('SELECT id, ctid FROM wb WHERE k = -1'),
  ('SELECT k, ctid FROM wb WHERE k BETWEEN 10 AND 30'),
  ('SELECT id FROM wb WHERE k = ANY (ARRAY[-1, 3, 399, 1000])'),
  ('SELECT u FROM wb WHERE u BETWEEN 2000 AND 9000'),
  ('SELECT id, k FROM wb WHERE k = -1 AND id > 5000'),
  ('SELECT id FROM wb WHERE a && ''{3}''::int4[]'),
  ('SELECT id FROM wb WHERE a @> ''{5,105}''::int4[]');
SELECT q, wb_same(q) FROM wb_q ORDER BY q;
SELECT wb_order('SELECT u, ctid FROM wb WHERE u > 14000 ORDER BY u DESC') AS backward,
       wb_order('SELECT k, id, ctid FROM wb WHERE k IN (-1, 5) ORDER BY k DESC, id DESC') AS backward_kp,
       wb_order('SELECT ctid FROM wb WHERE k = -1 ORDER BY k DESC') AS backward_posting;
-- the GIN-interface scan is an index scan here, its bitmap folded
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM wb WHERE a && '{3}'::int4[];
-- and with a lossy bitmap
SET work_mem = '64kB';
SELECT wb_same('SELECT id FROM wb WHERE a && ''{3,4,5,6}''::int4[]') AS lossy;
RESET work_mem;
RESET enable_seqscan;

-- Inserts that extend each shape, out of order and at the end, and a
-- unique violation; then UPDATEs (new TIDs) and DELETEs.
INSERT INTO wb SELECT g, CASE WHEN g % 2 = 0 THEN -1 ELSE g % 400 END, g,
  ARRAY[g % 13] FROM generate_series(15001, 18000) g;
INSERT INTO wb VALUES (0, 7, 7, '{1}');
UPDATE wb SET k = k + 1 WHERE id % 9 = 0 AND k >= 0;
UPDATE wb SET a = a || 200 WHERE id % 11 = 0;
DELETE FROM wb WHERE id % 7 = 0;
SELECT q, wb_same(q) FROM wb_q ORDER BY q;
SELECT bark_index_check(i, true) FROM unnest(ARRAY['wb_k', 'wb_u', 'wb_a', 'wb_kp']::regclass[]) i;

-- VACUUM removes the deleted rows' TIDs, and only theirs.
VACUUM wb;
SELECT bark_index_check(i, true) FROM unnest(ARRAY['wb_k', 'wb_u', 'wb_a', 'wb_kp']::regclass[]) i;
SELECT q, wb_same(q) FROM wb_q ORDER BY q;
-- the index holds exactly the live rows' TIDs
SELECT (SELECT array_agg(ctid ORDER BY ctid) FROM wb) =
       (SELECT array_agg(t ORDER BY t) FROM bark_multi_page_stats('wb_k', 1, -1) s,
          LATERAL bark_page_items('wb_k', s.blkno) i,
          LATERAL unnest(coalesce(i.tids, ARRAY[i.htid])) t
        WHERE s.type IN ('leaf', 'root') AND i.shape <> 'PIVOT') AS vacuumed_exactly;
SELECT * FROM wb_shapes('wb_k');

-- Bottom-up deletion: non-HOT updates of a full leaf's rows.
CREATE TABLE wbb (id int, v int, k int) USING bark_test_wideblock
  WITH (autovacuum_enabled = off, fillfactor = 100);
INSERT INTO wbb SELECT g, 0, g / 4 FROM generate_series(1, 6000) g;
CREATE INDEX wbb_k ON wbb USING bark (k) WITH (fillfactor = 100);
CREATE INDEX wbb_v ON wbb USING bark (v);
UPDATE wbb SET v = v + 1;
UPDATE wbb SET v = v + 1;
UPDATE wbb SET v = v + 1;
SELECT bark_index_check('wbb_k', true), bark_index_check('wbb_v', true);
SELECT wb_same('SELECT id, ctid FROM wbb WHERE k BETWEEN 100 AND 900') AS bottomup;

-- CREATE INDEX CONCURRENTLY and REINDEX CONCURRENTLY validate with TIDs
-- above 291.
CREATE INDEX CONCURRENTLY wb_c ON wb USING bark (id);
REINDEX INDEX CONCURRENTLY wb_k;
SELECT bark_index_check('wb_c', true), bark_index_check('wb_k', true);
SELECT wb_same('SELECT id FROM wb WHERE id BETWEEN 3000 AND 12000') AS cic;

-- WHERE CURRENT OF, row locks and a TID range scan, through the index.
BEGIN;
SET LOCAL enable_seqscan = off;
DECLARE c CURSOR FOR SELECT id, ctid FROM wb WHERE k = -1 AND id > 6000 FOR UPDATE;
FETCH 1 FROM c;
UPDATE wb SET u = -u WHERE CURRENT OF c;
COMMIT;
SELECT count(*) FROM wb WHERE u < 0;
SELECT count(*) = (SELECT count(*) FROM wb WHERE ctid >= '(1,1)' AND ctid < '(2,0)')
  AS tidrange FROM wb WHERE (ctid::text::point)[0] = 1;

DROP TABLE wb, wbb, wb_q;
DROP FUNCTION wb_same, wb_order, wb_shapes;
DROP EXTENSION pageinspect;
DROP EXTENSION bark_multikey;
