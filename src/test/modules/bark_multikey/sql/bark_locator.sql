-- BARK refuses at build a table whose locator offsets exceed
-- MaxHeapTuplesPerPage, the most its POSTING key (block *
-- MaxHeapTuplesPerPage + offset - 1) maps back.  bark_test_bigoffset is heap
-- declaring offsets up to MaxOffsetNumber.  The check is in bark_build, so it
-- covers every way an index is built: CREATE INDEX, CREATE INDEX
-- CONCURRENTLY, and the rebuild of ALTER TABLE ... SET ACCESS METHOD (and of
-- REINDEX, which cannot be reached: no BARK index can exist on such a table).
-- The error details name block-size-dependent limits, so they are not shown.
CREATE EXTENSION IF NOT EXISTS bark_multikey;
\set VERBOSITY terse
CREATE TABLE bl (a int, b int) USING bark_test_bigoffset;
INSERT INTO bl SELECT g, g % 10 FROM generate_series(1, 100) g;
CREATE INDEX bl_a ON bl USING bark (a);
CREATE INDEX CONCURRENTLY bl_c ON bl USING bark (a);
-- the failed concurrent build leaves an invalid index behind
SELECT indexrelid::regclass, indisvalid FROM pg_index
 WHERE indrelid = 'bl'::regclass;
DROP INDEX bl_c;
-- under heap the table takes BARK indexes, which then block the switch back
ALTER TABLE bl SET ACCESS METHOD heap;
CREATE INDEX bl_a ON bl USING bark (a);
CREATE INDEX bl_b ON bl USING bark (b, a);
ALTER TABLE bl SET ACCESS METHOD bark_test_bigoffset;
SELECT amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam
 WHERE c.oid = 'bl'::regclass;
SET enable_seqscan = off;
SELECT a FROM bl WHERE b = 3 ORDER BY a LIMIT 3;
RESET enable_seqscan;
DROP TABLE bl;
\set VERBOSITY default
DROP EXTENSION bark_multikey;
