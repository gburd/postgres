-- BARK indexes a table whose locator offsets exceed MaxHeapTuplesPerPage:
-- CREATE INDEX records the table's largest offset as the index's TID
-- stride, and every TID mapping (a POSTING entry's key, a coded LIST's) is
-- laid out for it.  bark_test_bigoffset is heap declaring offsets up to
-- MaxOffsetNumber, so its real offsets stay small; bark_test_wideblock
-- (bark_wideblock.sql) stores real ones up to about 2037.  At BLCKSZ 8192
-- heap's stride is 291 (MaxHeapTuplesPerPage) and the wide one 2048
-- (MaxOffsetNumber).
CREATE EXTENSION IF NOT EXISTS bark_multikey;
\set VERBOSITY terse
CREATE TABLE bl (a int, b int) USING bark_test_bigoffset;
INSERT INTO bl SELECT g, g % 10 FROM generate_series(1, 100) g;
CREATE INDEX bl_a ON bl USING bark (a);
CREATE INDEX CONCURRENTLY bl_c ON bl USING bark (b);
SELECT indexrelid::regclass, indisvalid FROM pg_index
 WHERE indrelid = 'bl'::regclass ORDER BY 1;
SELECT bark_test_tid_stride('bl_a') AS stride_a,
       bark_test_meta_stride('bl_a') AS meta_a,
       bark_test_tid_stride('bl_c') AS stride_c;
SET enable_seqscan = off;
SELECT a FROM bl WHERE b = 3 ORDER BY a LIMIT 3;
SELECT count(*) FROM bl WHERE a > 50;
RESET enable_seqscan;
SELECT bark_index_check('bl_c', true);
-- under heap a rebuild records heap's stride, and the switch back records
-- the wide one again: ALTER TABLE ... SET ACCESS METHOD rebuilds the indexes
ALTER TABLE bl SET ACCESS METHOD heap;
SELECT bark_test_tid_stride('bl_a') AS heap_stride,
       bark_test_meta_stride('bl_a') AS meta_heap_stride;
ALTER TABLE bl SET ACCESS METHOD bark_test_bigoffset;
SELECT amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam
 WHERE c.oid = 'bl'::regclass;
SELECT bark_test_tid_stride('bl_a') AS stride_again;
REINDEX INDEX bl_c;
SELECT bark_test_tid_stride('bl_c') AS stride_after_reindex;
SET enable_seqscan = off;
SELECT a FROM bl WHERE b = 3 ORDER BY a LIMIT 3;
RESET enable_seqscan;
DROP TABLE bl;
\set VERBOSITY default
DROP EXTENSION bark_multikey;
