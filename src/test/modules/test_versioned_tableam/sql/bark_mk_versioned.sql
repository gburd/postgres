--
-- Versioned multikey BARK indexes: rule D.  An index with BARK_META_MKTAGS
-- keeps every entry a page read accepts, and returns a row's entries until
-- the table AM returns the row through one (its verdict VALID puts the row
-- in R, whose later entries are skipped); an entry it rejects (INVALID) is
-- remembered as (TID, tag) in J.  heap_versioned plays an AM that updated
-- rows in place: test_versioned_mark_history makes a row's entries valid
-- only when tagged with its xmin, and test_versioned_insert_entry adds the
-- untagged entries of an older value.
--
CREATE EXTENSION test_versioned_tableam;
CREATE EXTENSION bark_multikey;
CREATE EXTENSION pageinspect;
CREATE EXTENSION amcheck;
SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- Row 1 was {1,2}, updated in place to {2,3}: (1,L,0) and (2,L,0) are stale,
-- (2,L,x) and (3,L,x) valid.  Rows 2..40 have two keys each, all valid.
CREATE TABLE tmv (id int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off);
INSERT INTO tmv SELECT g, ARRAY[10 * g, 10 * g + 1] FROM generate_series(2, 40) g;
INSERT INTO tmv VALUES (1, '{1,2}');
CREATE INDEX tmv_doc ON tmv USING bark (doc bark_int4_array_ops);
SELECT flags & 6 AS versioned_mktags FROM bark_metap('tmv_doc');
UPDATE tmv SET doc = '{2,3}' WHERE id = 1;
SELECT test_versioned_mark_history('tmv', ctid, 'doc') FROM tmv WHERE id = 1;
SELECT test_versioned_insert_entry('tmv_doc', ctid, 0, '{1,2}') FROM tmv WHERE id = 1;

-- T2a: the stale entries come first; the row comes back once, through its
-- valid entry.  A TID seen set filled at page read returns 0.
EXPLAIN (COSTS OFF) SELECT count(*) FROM tmv WHERE doc && '{1,2}';
SELECT test_versioned_reset();
SELECT count(*) FROM tmv WHERE doc && '{1,2}';
SELECT counter, n FROM test_versioned_seen WHERE counter LIKE 'verdict%' OR counter LIKE 'judge%';
SELECT id FROM tmv WHERE doc && '{1}';
SELECT id FROM tmv WHERE doc && '{3}';
SELECT id FROM tmv WHERE doc && '{2}';
-- Rows whose two keys both match come back once (R).
SELECT count(*) FROM tmv WHERE doc && '{100,101,110,111,1,2,3}';

-- T2b: a row with valid entries of two tags (untagged and tagged, both
-- valid for a row without history) for each key is returned once.
SELECT test_versioned_insert_entry('tmv_doc', ctid, 0) FROM tmv WHERE id = 10;
SELECT count(*), count(*) FILTER (WHERE tag IS NULL) AS untagged
  FROM bark_page_items('tmv_doc', 1) WHERE htid = (SELECT ctid FROM tmv WHERE id = 10);
SELECT id FROM tmv WHERE doc && '{100,101}';

-- T2c: an ordered scan on the column returns a row at its first valid key:
-- row 1's smallest key is now 2, not the stale 1.
SELECT id, doc |<| 0 AS k FROM tmv WHERE id < 4 ORDER BY doc |<| 0;

-- Backward: the same rows in reverse.
CREATE TABLE tmw (id int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off);
INSERT INTO tmw SELECT g, ARRAY[g % 3, g % 3 + 1] FROM generate_series(1, 30) g;
CREATE INDEX tmw_id_doc ON tmw USING bark (id, doc bark_int4_array_ops);
UPDATE tmw SET doc = '{7,8}' WHERE id IN (4, 5);
SELECT test_versioned_mark_history('tmw', ctid, 'doc') FROM tmw WHERE id IN (4, 5);
SELECT test_versioned_insert_entry('tmw_id_doc', ctid, 0, '{0,1}') FROM tmw WHERE id IN (4, 5);
EXPLAIN (COSTS OFF) SELECT id FROM tmw WHERE id < 10 AND doc && '{1,7}' ORDER BY id DESC;
SELECT id FROM tmw WHERE id < 10 AND doc && '{1,7}' ORDER BY id DESC;
SELECT id FROM tmw WHERE id < 10 AND doc && '{1,7}' ORDER BY id;
-- each group (id) holds one row: R and J empty at each group change
SELECT id FROM tmw WHERE id < 10 AND doc && '{0,1,2,7,8}' ORDER BY id;

-- T2f: a cursor read partly and closed, LIMIT 1, and a nested-loop inner
-- rescan each leave a verdict unread; the next scan starts clean.
BEGIN;
DECLARE c CURSOR FOR SELECT id FROM tmv WHERE doc && '{1,2,3}';
FETCH 1 FROM c;
CLOSE c;
COMMIT;
SELECT id FROM tmv WHERE doc && '{1,2,3}' LIMIT 1;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
EXPLAIN (COSTS OFF)
SELECT o.v, count(i.id) FROM (VALUES (1), (2), (3)) o(v)
  JOIN tmw i ON i.id < 10 AND i.doc && ARRAY[o.v, 7] GROUP BY o.v ORDER BY o.v;
SELECT o.v, count(i.id) FROM (VALUES (1), (2), (3)) o(v)
  JOIN tmw i ON i.id < 10 AND i.doc && ARRAY[o.v, 7] GROUP BY o.v ORDER BY o.v;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;

-- A SCROLL cursor over the index is materialized (no backward scan of a
-- multikey index with a seen set), and reads the same rows both ways.
BEGIN;
DECLARE s SCROLL CURSOR FOR SELECT id FROM tmw WHERE id < 10 AND doc && '{1,7}' ORDER BY id;
FETCH 3 FROM s;
FETCH BACKWARD 2 FROM s;
FETCH ALL FROM s;
COMMIT;

-- Mark and restore: a merge join whose inner side is an index scan of the
-- (id, doc) index restores to marks taken inside a group; each outer row
-- meets the inner rows of its id once.
SET enable_hashjoin = off;
SET enable_nestloop = off;
SET enable_material = off;
SET enable_sort = off;
CREATE TABLE tmo (id int) WITH (autovacuum_enabled = off);
INSERT INTO tmo SELECT g FROM generate_series(1, 9) g, generate_series(1, 3);
CREATE INDEX tmo_id ON tmo (id);
ANALYZE tmo;
EXPLAIN (COSTS OFF)
SELECT o.id, count(*) FROM tmo o JOIN tmw i ON i.id = o.id
  WHERE i.doc && '{1,7}' GROUP BY o.id ORDER BY o.id;
SELECT o.id, count(*) FROM tmo o JOIN tmw i ON i.id = o.id
  WHERE i.doc && '{1,7}' GROUP BY o.id ORDER BY o.id;
RESET enable_hashjoin;
RESET enable_nestloop;
RESET enable_material;
RESET enable_sort;

-- KNN on the leading column: the same rows, nearest first.
EXPLAIN (COSTS OFF) SELECT id FROM tmw WHERE doc && '{1,7}' ORDER BY id <~> 5 LIMIT 5;
SELECT id FROM tmw WHERE doc && '{1,7}' ORDER BY id <~> 5 LIMIT 5;

-- T2d: a table AM that leaves the verdict unset gets an error from the
-- next call, never a wrong answer.
SET test_versioned_tableam.forget_verdict = 'all';
SELECT count(*) FROM tmv WHERE doc && '{1,2}';
SELECT id FROM tmw WHERE doc && '{1,7}' ORDER BY id <~> 5 LIMIT 5;
RESET test_versioned_tableam.forget_verdict;

-- T3a: a row with valid entries of two tags whose keys lie on different
-- leaves (the build's tagged ones, and untagged twins, as VACUUM's partial
-- untagging leaves a row) is returned once, by an index scan, a bitmap
-- scan and an ordered scan.
CREATE TABLE tml (id int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off);
INSERT INTO tml SELECT g, ARRAY[g + 1] FROM generate_series(1, 3000) g;
INSERT INTO tml VALUES (0, '{1,5000}');
CREATE INDEX tml_doc ON tml USING bark (doc bark_int4_array_ops);
SELECT test_versioned_insert_entry('tml_doc', ctid, 0) FROM tml WHERE id = 0;
SELECT count(DISTINCT blk) AS leaves, count(*) AS entries,
       count(*) FILTER (WHERE tag IS NULL) AS untagged
  FROM (SELECT b AS blk, i.tag FROM generate_series(1, pg_relation_size('tml_doc') / current_setting('block_size')::int - 1) b,
          LATERAL bark_page_items('tml_doc', b) i
         WHERE i.htid = (SELECT ctid FROM tml WHERE id = 0)) e;
SELECT count(*) FROM tml WHERE doc && '{1,5000}';
SELECT id, doc |<| 0 AS k FROM tml WHERE doc && '{1,5000}' ORDER BY doc |<| 0;
SET enable_indexscan = off;
SET enable_bitmapscan = on;
EXPLAIN (COSTS OFF) SELECT count(*) FROM tml WHERE doc && '{1,5000}';
SELECT count(*) FROM tml WHERE doc && '{1,5000}';
RESET enable_indexscan;
SET enable_bitmapscan = off;

-- T2e: bitmap scans need no rule D.  The bitmap holds row 1 through its
-- stale (1,L,0); the table AM rechecks a row with history on its visible
-- version ({2,3}), so the row is not returned for '{1}', and returned once
-- for '{1,2}'.  A BitmapOr with a scalar index, too.
SET enable_indexscan = off;
SET enable_bitmapscan = on;
EXPLAIN (COSTS OFF) SELECT id FROM tmv WHERE doc && '{1}';
SELECT id FROM tmv WHERE doc && '{1}';
SELECT count(*) FROM tmv WHERE doc && '{1,2}';
CREATE INDEX tmv_id ON tmv USING bark (id);
EXPLAIN (COSTS OFF) SELECT id FROM tmv WHERE doc && '{1}' OR id = 2;
SELECT id FROM tmv WHERE doc && '{1}' OR id = 2;
SELECT id FROM tmv WHERE doc && '{2}' OR id = 1;
EXPLAIN (COSTS OFF) SELECT id FROM tmv WHERE doc && '{1,2,3}' AND id < 3;
SELECT id FROM tmv WHERE doc && '{1,2,3}' AND id < 3;
DROP INDEX tmv_id;
RESET enable_indexscan;
SET enable_bitmapscan = off;

-- T4a: index-only scans.  Row 1 was (a 1, doc {5,6}), now (2, {6,7}) in
-- place: (1,6,L,0) and (1,7,L,0) are stale, (2,6,L,x) and (2,7,L,x)
-- valid.  VACUUM keeps the stale entries (stale_tags = live) and sets the
-- page all-visible (no heap fetches), so only the table AM's judgment of
-- each entry keeps the IOS right: a = 1 has no row.
CREATE TABLE tmi (id int, a int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off);
INSERT INTO tmi SELECT g, g + 10, ARRAY[g, g + 100] FROM generate_series(2, 20) g;
INSERT INTO tmi VALUES (1, 1, '{5,6}');
CREATE INDEX tmi_a_doc ON tmi USING bark (a, doc bark_int4_array_ops);
UPDATE tmi SET a = 2, doc = '{6,7}' WHERE id = 1;
SELECT test_versioned_mark_history('tmi', ctid, 'a') FROM tmi WHERE id = 1;
SELECT test_versioned_insert_entry('tmi_a_doc', ctid, 0, '1') FROM tmi WHERE id = 1;
SET test_versioned_tableam.stale_tags = live;
VACUUM tmi;
RESET test_versioned_tableam.stale_tags;
SELECT count(*) FILTER (WHERE tag IS NULL) AS untagged, count(*) AS entries
  FROM bark_page_items('tmi_a_doc', 1) WHERE htid = (SELECT ctid FROM tmi WHERE id = 1);
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT count(*) FROM tmi WHERE a = 1;
SELECT count(*) FROM tmi WHERE a = 1;
SELECT count(*) FROM tmi WHERE a = 2;
SELECT count(*) FROM tmi WHERE a <= 2;

-- T4b: a full index-only scan with no keys after an in-place update of
-- every row counts each row once.  Every row's stale entries sort before
-- its valid ones; a page-read seen set would lose every row.
CREATE TABLE tmb (id int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off);
INSERT INTO tmb SELECT g, ARRAY[g, g + 1000] FROM generate_series(1, 20) g;
CREATE INDEX tmb_doc ON tmb USING bark (doc bark_int4_array_ops);
UPDATE tmb SET doc = ARRAY[id + 1000, id + 2000];
SELECT count(test_versioned_mark_history('tmb', ctid, 'doc')) FROM tmb;
SELECT count(test_versioned_insert_entry('tmb_doc', ctid, 0, format('{%s,%s}', id, id + 1000)))
  FROM tmb;
SET test_versioned_tableam.stale_tags = live;
VACUUM tmb;
RESET test_versioned_tableam.stale_tags;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT count(*) FROM tmb;
SELECT count(*) FROM tmb;
SELECT count(*) FROM tmb WHERE doc && '{1,2,3,1001,1002,2003}';

-- T4c: a table AM that skips its verdict in index-only scans only: the IOS
-- errors, the plain index scan does not.
SET test_versioned_tableam.forget_verdict = 'ios';
SELECT count(*) FROM tmb;
SELECT count(*) FROM tmi WHERE a = 2;
SELECT id FROM tmb WHERE doc && '{1001}';
RESET test_versioned_tableam.forget_verdict;

-- T6a: an update that keeps the locator inserts the new value's full key
-- set, tagged, unchanged keys included.  ({1,2} -> {2,3}: with only the
-- added key 3 inserted, doc && '{2}' would lose the row.)  heap_versioned
-- moves the row and, with keep_locator = pretend, says it kept it, so the
-- executor inserts only into the indexes the update affected.
CREATE TABLE tmu (id int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off);
INSERT INTO tmu SELECT g, ARRAY[10 * g, 10 * g + 1] FROM generate_series(2, 40) g;
INSERT INTO tmu VALUES (1, '{1,2}');
CREATE INDEX tmu_doc ON tmu USING bark (doc bark_int4_array_ops);
SET test_versioned_tableam.keep_locator = pretend;
SELECT test_versioned_reset();
UPDATE tmu SET doc = '{2,3}' WHERE id = 1;
SELECT n FROM test_versioned_counts() WHERE counter = 'update_kept';
RESET test_versioned_tableam.keep_locator;
SELECT count(*) AS entries,
       count(*) FILTER (WHERE tag = (SELECT xmin::text::int8 FROM tmu WHERE id = 1)) AS tagged_new
  FROM bark_page_items('tmu_doc', 1) WHERE htid = (SELECT ctid FROM tmu WHERE id = 1);
SELECT id FROM tmu WHERE doc && '{2}';
SELECT id FROM tmu WHERE doc && '{3}';
SELECT id FROM tmu WHERE doc && '{1}';
SELECT id, doc |<| 0 AS k FROM tmu WHERE id < 3 ORDER BY doc |<| 0;
SET enable_indexscan = off;
SET enable_bitmapscan = on;
SELECT id FROM tmu WHERE doc && '{2}';
SELECT id FROM tmu WHERE doc && '{1}';
RESET enable_indexscan;
SET enable_bitmapscan = off;

-- T6b: {20,21} -> {21,20} gives the same entries (the class sorts keys):
-- the index is not affected, keeps its entries, and gets none.
SELECT count(*) AS before FROM bark_page_items('tmu_doc', 1);
SET test_versioned_tableam.keep_locator = pretend;
SELECT test_versioned_reset();
UPDATE tmu SET doc = '{21,20}' WHERE id = 2;
SELECT n FROM test_versioned_counts() WHERE counter = 'update_kept';
RESET test_versioned_tableam.keep_locator;
SELECT count(*) AS after FROM bark_page_items('tmu_doc', 1);

-- T6c: an index on an expression of the column and the multikey index are
-- both affected, both get tagged entries, and both find the row.
CREATE INDEX tmu_len ON tmu USING bark (cardinality(doc));
SET test_versioned_tableam.keep_locator = pretend;
SELECT test_versioned_reset();
UPDATE tmu SET doc = '{30,31,32}' WHERE id = 3;
SELECT n FROM test_versioned_counts() WHERE counter = 'update_kept';
RESET test_versioned_tableam.keep_locator;
SELECT (SELECT count(*) FROM bark_page_items('tmu_len', 1)
         WHERE htid = t.ctid AND tag = t.xmin::text::int8) AS len_tagged,
       (SELECT count(*) FROM bark_page_items('tmu_doc', 1)
         WHERE htid = t.ctid AND tag = t.xmin::text::int8) AS doc_tagged
  FROM tmu t WHERE id = 3;
SELECT id FROM tmu WHERE cardinality(doc) = 3;
SELECT id FROM tmu WHERE doc && '{32}';
DROP INDEX tmu_len;

-- T6d: an aborted update's tagged entries are valid for no snapshot; the
-- row is found by its old keys only, and VACUUM removes them.
SELECT count(*) AS before FROM bark_page_items('tmu_doc', 1);
BEGIN;
SET LOCAL test_versioned_tableam.keep_locator = pretend;
UPDATE tmu SET doc = '{40,41,42}' WHERE id = 4;
ROLLBACK;
SELECT count(*) AS with_aborted FROM bark_page_items('tmu_doc', 1);
SELECT id FROM tmu WHERE doc && '{40,41,42}';
SELECT id FROM tmu WHERE doc && '{42}';
VACUUM tmu;
SELECT count(*) AS after_vacuum FROM bark_page_items('tmu_doc', 1);
SELECT bark_index_check('tmu_doc');
DROP TABLE tmu;

-- A tagged multikey index made an exclusion constraint's after its build
-- (as DocumentDB's collMod prepareUnique does: index_constraint_create on
-- the existing index, then pg_index.indisexclusion by hand) keeps
-- BARK_META_MKTAGS and its tags, but no longer lets an update keep the
-- locator (amindexmaykeeplocator).  Its MVCC scans still skip a stale
-- entry (rule D), and the constraint is checked through it.
CREATE TABLE tmc (id int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off);
INSERT INTO tmc SELECT g, ARRAY[10 * g, 10 * g + 1] FROM generate_series(2, 40) g;
INSERT INTO tmc VALUES (1, '{1,2}');
CREATE INDEX tmc_doc ON tmc USING bark (doc bark_int4_array_ops);
UPDATE tmc SET doc = '{2,3}' WHERE id = 1;
SELECT test_versioned_mark_history('tmc', ctid, 'doc') FROM tmc WHERE id = 1;
SELECT test_versioned_insert_entry('tmc_doc', ctid, 0, '{1,2}') FROM tmc WHERE id = 1;
SET test_versioned_tableam.keep_locator = pretend;
SELECT test_versioned_reset();
UPDATE tmc SET doc = '{100,101,102}' WHERE id = 10;
SELECT n AS kept_before FROM test_versioned_counts() WHERE counter = 'update_kept';
RESET test_versioned_tableam.keep_locator;
SELECT test_versioned_add_exclusion('tmc_doc', '&&(anyarray,anyarray)');
UPDATE pg_catalog.pg_index SET indisexclusion = true
  WHERE indexrelid = 'tmc_doc'::regclass;
SELECT contype, conname FROM pg_constraint WHERE conindid = 'tmc_doc'::regclass;
SELECT flags & 6 AS versioned_mktags FROM bark_metap('tmc_doc');
-- an update of the column now moves the row; its new entries are tagged
SET test_versioned_tableam.keep_locator = pretend;
SELECT test_versioned_reset();
UPDATE tmc SET doc = '{110,111,112}' WHERE id = 11;
SELECT n AS kept_after FROM test_versioned_counts() WHERE counter = 'update_kept';
RESET test_versioned_tableam.keep_locator;
SELECT count(*) FILTER (WHERE tag = t.xmin::text::int8) AS tagged
  FROM tmc t, bark_page_items('tmc_doc', 1) i WHERE t.id = 11 AND i.htid = t.ctid;
-- MVCC scans: the stale (1,L,0) is skipped, the valid (2,L,x) returned
SELECT id FROM tmc WHERE doc && '{1,2}';
SELECT id FROM tmc WHERE doc && '{1}';
-- the exclusion check: {1} overlaps only row 1's stale entry, no conflict;
-- {3} overlaps row 1's valid one, and {112} row 11's
INSERT INTO tmc VALUES (100, '{1}');
INSERT INTO tmc VALUES (101, '{3}');
INSERT INTO tmc VALUES (102, '{112,500}');
INSERT INTO tmc VALUES (103, '{500,501}');
SELECT id FROM tmc WHERE doc && '{1,500}' ORDER BY id;
SELECT bark_index_check('tmc_doc');
DROP TABLE tmc;

-- The same for a unique index (pg_index.indisunique set by hand): the
-- unique check judges each entry with its tag, so the stale (1,L,0) is no
-- conflict, and a live key is.
CREATE TABLE tmq (id int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off);
INSERT INTO tmq SELECT g, ARRAY[10 * g, 10 * g + 1] FROM generate_series(2, 40) g;
INSERT INTO tmq VALUES (1, '{1,2}');
CREATE INDEX tmq_doc ON tmq USING bark (doc bark_int4_array_ops);
UPDATE tmq SET doc = '{2,3}' WHERE id = 1;
SELECT test_versioned_mark_history('tmq', ctid, 'doc') FROM tmq WHERE id = 1;
SELECT test_versioned_insert_entry('tmq_doc', ctid, 0, '{1,2}') FROM tmq WHERE id = 1;
UPDATE pg_catalog.pg_index SET indisunique = true
  WHERE indexrelid = 'tmq_doc'::regclass;
SET test_versioned_tableam.keep_locator = pretend;
SELECT test_versioned_reset();
UPDATE tmq SET doc = '{110,111,112}' WHERE id = 11;
SELECT n AS kept FROM test_versioned_counts() WHERE counter = 'update_kept';
RESET test_versioned_tableam.keep_locator;
INSERT INTO tmq VALUES (100, '{1}');
INSERT INTO tmq VALUES (101, '{3}');
INSERT INTO tmq VALUES (102, '{112}');
SELECT id FROM tmq WHERE doc && '{1,2,3}' ORDER BY id;
SELECT bark_index_check('tmq_doc');
DROP TABLE tmq;

-- An index without MKTAGS keeps the page-read seen set: the GIN-interface
-- class, and a multikey index on heap.
CREATE INDEX tmv_gin ON tmv USING bark (doc bark_gin_int4_array_ops);
SELECT flags & 6 AS versioned_mktags FROM bark_metap('tmv_gin');
DROP INDEX tmv_gin;

RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE tmv, tmw, tmo, tml, tmi, tmb;
DROP EXTENSION amcheck;
DROP EXTENSION pageinspect;
DROP EXTENSION bark_multikey;
DROP EXTENSION test_versioned_tableam;
