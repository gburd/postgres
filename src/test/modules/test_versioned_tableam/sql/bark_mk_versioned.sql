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
-- BARK_META_CONVERTING (8) waits for the first BARK writer to see it
SELECT flags & 30 AS mktags_converting FROM bark_metap('tmc_doc');
-- an update of the column now moves the row; its new entries are tagged
SET test_versioned_tableam.keep_locator = pretend;
SELECT test_versioned_reset();
UPDATE tmc SET doc = '{110,111,112}' WHERE id = 11;
SELECT n AS kept_after FROM test_versioned_counts() WHERE counter = 'update_kept';
RESET test_versioned_tableam.keep_locator;
SELECT flags & 30 AS mktags_converting FROM bark_metap('tmc_doc');
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
-- The new row's own entries 500 and 501 both match.  The check's
-- dirty scan of an index made a constraint's after its build keeps no row
-- per locator, except the row this backend
-- inserted last, so it meets its own row once, not twice ("found self tuple
-- multiple times").
INSERT INTO tmc VALUES (103, '{500,501}');
INSERT INTO tmc VALUES (104, '{600,601,602}');
SELECT id FROM tmc WHERE doc && '{1,500}' ORDER BY id;
SELECT bark_index_check('tmc_doc');
-- CONVERTING outlives the constraint: with indisexclusion cleared, an
-- update still moves the row, since VACUUM's barrier must see every writer
-- that could keep a locator
UPDATE pg_catalog.pg_index SET indisexclusion = false
  WHERE indexrelid = 'tmc_doc'::regclass;
SET test_versioned_tableam.keep_locator = pretend;
SELECT test_versioned_reset();
BEGIN;
UPDATE tmc SET doc = '{120,121}' WHERE id = 12;
UPDATE tmc SET doc = '{130,131}' WHERE id = 13;
COMMIT;
SELECT n AS kept_dropped FROM test_versioned_counts() WHERE counter = 'update_kept';
RESET test_versioned_tableam.keep_locator;
SELECT flags & 30 AS mktags_converting FROM bark_metap('tmc_doc');
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
-- ON CONFLICT's pre-check scans the converted index under a dirty
-- snapshot: {3,700} meets row 1, {700} nothing
INSERT INTO tmq VALUES (103, '{3,700}') ON CONFLICT DO NOTHING;
INSERT INTO tmq VALUES (104, '{700}') ON CONFLICT DO NOTHING;
SELECT id FROM tmq WHERE doc && '{1,2,3,700}' ORDER BY id;
SELECT flags & 30 AS mktags_converting FROM bark_metap('tmq_doc');
SELECT bark_index_check('tmq_doc');
DROP TABLE tmq;

-- Deletion before a split asks the table AM about each tagged entry
-- (bark_tagged_delete).  A multikey row has an entry per key on a leaf, and
-- each TID goes to the table AM once, but once per tag: row 1's stale
-- entries (tag xmin - 5) are dead, its current ones are live, and a split
-- of their leaf must delete only the stale two.
CREATE TABLE tmd (id int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off);
INSERT INTO tmd SELECT g, ARRAY[10 * g, 10 * g + 1] FROM generate_series(2, 300) g;
INSERT INTO tmd VALUES (1, '{100000,100001}');
SET test_versioned_tableam.build_tag_percent = 100;
CREATE INDEX tmd_doc ON tmd USING bark (doc bark_int4_array_ops);
SET test_versioned_tableam.stale_tags = live;
SELECT test_versioned_insert_entry('tmd_doc', ctid, -5) FROM tmd WHERE id = 1;
SELECT count(*) AS entries_of_row_1
  FROM tmd t, generate_series(1, pg_relation_size('tmd_doc') / 8192 - 1) b,
       LATERAL bark_page_items('tmd_doc', b) i
 WHERE t.id = 1 AND i.htid = t.ctid;
RESET test_versioned_tableam.stale_tags;
INSERT INTO tmd SELECT 1000 + g, ARRAY[200000 + 2 * g, 200001 + 2 * g]
  FROM generate_series(1, 600) g;
SELECT count(*) AS entries_of_row_1
  FROM tmd t, generate_series(1, pg_relation_size('tmd_doc') / 8192 - 1) b,
       LATERAL bark_page_items('tmd_doc', b) i
 WHERE t.id = 1 AND i.htid = t.ctid;
SELECT id FROM tmd WHERE doc && '{100000}';
SELECT id FROM tmd WHERE doc && '{100001}';
SELECT bark_index_check('tmd_doc');
DROP TABLE tmd;

-- VACUUM notices a conversion too, and nothing sets CONVERTING on an
-- index without MKTAGS (a multikey index on heap made unique by hand).
-- tmz's index has no tagged entry, so that VACUUM also completes it (T12a).
CREATE TABLE tmz (id int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off);
INSERT INTO tmz VALUES (1, '{1,2}'), (2, '{3,4}');
CREATE INDEX tmz_doc ON tmz USING bark (doc bark_int4_array_ops);
CREATE TABLE tmzh (id int, doc int4[]) WITH (autovacuum_enabled = off);
INSERT INTO tmzh VALUES (1, '{1,2}'), (2, '{3,4}');
CREATE INDEX tmzh_doc ON tmzh USING bark (doc bark_int4_array_ops);
UPDATE pg_catalog.pg_index SET indisunique = true
  WHERE indexrelid IN ('tmz_doc'::regclass, 'tmzh_doc'::regclass);
SELECT flags & 30 AS mktags_converting FROM bark_metap('tmz_doc');
VACUUM tmz;
VACUUM tmzh;
INSERT INTO tmzh VALUES (3, '{5,6}');
SELECT flags & 30 AS mktags_converting FROM bark_metap('tmz_doc');
SELECT flags & 30 AS mktags_converting FROM bark_metap('tmzh_doc');
SELECT bark_index_check('tmz_doc'), bark_index_check('tmzh_doc');
DROP TABLE tmz, tmzh;

-- An index CREATE INDEX CONCURRENTLY left before its build (a lock
-- timeout in its first wait, for a prepared writer) has no meta page yet;
-- an update asks whether it may keep the locator in it, which reads the
-- flags the build would write (MKTAGS, not converting), and then moves the
-- row anyway, since the index is not ready.  A conversion of an unlogged
-- table's index writes its meta page without WAL.
CREATE TABLE tmn (id int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off);
INSERT INTO tmn VALUES (1, '{1,2}');
BEGIN;
INSERT INTO tmn VALUES (2, '{3,4}');
PREPARE TRANSACTION 'tmn_writer';
SET lock_timeout = '10ms';
\set VERBOSITY terse
CREATE INDEX CONCURRENTLY tmn_doc ON tmn USING bark (doc bark_int4_array_ops);
\set VERBOSITY default
RESET lock_timeout;
SELECT pg_relation_size('tmn_doc') AS size, indisvalid, indisready
  FROM pg_index WHERE indexrelid = 'tmn_doc'::regclass;
SET test_versioned_tableam.keep_locator = pretend;
SELECT test_versioned_reset();
UPDATE tmn SET doc = '{5,6}' WHERE id = 1;
SELECT n AS kept FROM test_versioned_counts() WHERE counter = 'update_kept';
RESET test_versioned_tableam.keep_locator;
COMMIT PREPARED 'tmn_writer';
DROP TABLE tmn;
CREATE UNLOGGED TABLE tmn (id int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off);
INSERT INTO tmn VALUES (1, '{10}');
CREATE INDEX tmn_doc ON tmn USING bark (doc bark_int4_array_ops);
-- the first row with two keys sets MULTIKEY, unlogged too
INSERT INTO tmn SELECT g, ARRAY[10 * g, 10 * g + 1] FROM generate_series(2, 20) g;
SELECT flags & 1 AS multikey FROM bark_metap('tmn_doc');
UPDATE pg_catalog.pg_index SET indisunique = true
  WHERE indexrelid = 'tmn_doc'::regclass;
INSERT INTO tmn VALUES (21, '{210}');
SELECT flags & 30 AS mktags_converting FROM bark_metap('tmn_doc');
VACUUM tmn;
SELECT flags & 30 AS mktags_converting FROM bark_metap('tmn_doc');
SELECT id FROM tmn WHERE doc && '{10,210}' ORDER BY id;
SELECT bark_index_check('tmn_doc');
DROP TABLE tmn;

-- T12a: the whole conversion.  A tagged multikey index (the build tags
-- every row) is made an exclusion constraint's; an insert begins the
-- conversion (CONVERTING, 8); MVCC results do not change; a VACUUM whose
-- table AM untags nothing (untag_tag names no entry's tag) passes the
-- barrier (SETTLED, 16) and keeps the tagged entries; an update after
-- SETTLED stores its entries untagged though the table AM tags them; the
-- next VACUUM untags the rest and completes the conversion, clearing
-- MKTAGS (4) with both bits.  amcheck is clean throughout.
CREATE TABLE tmk (id int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off);
INSERT INTO tmk SELECT g, ARRAY[10 * g, 10 * g + 1] FROM generate_series(1, 40) g;
CREATE INDEX tmk_doc ON tmk USING bark (doc bark_int4_array_ops);
CREATE FUNCTION tmk_state(OUT flags int8, OUT tagged int8, OUT entries int8)
  LANGUAGE sql AS $$
  SELECT (SELECT m.flags & 30 FROM bark_metap('tmk_doc') m),
         count(i.tag), count(*)
    FROM generate_series(1, pg_relation_size('tmk_doc') / 8192 - 1) b,
         LATERAL bark_page_items('tmk_doc', b) i
   WHERE i.shape IN ('SINGLE', 'LIST', 'POSTING') $$;
SELECT * FROM tmk_state();
SELECT test_versioned_add_exclusion('tmk_doc', '&&(anyarray,anyarray)');
UPDATE pg_catalog.pg_index SET indisexclusion = true
  WHERE indexrelid = 'tmk_doc'::regclass;
SELECT * FROM tmk_state();
INSERT INTO tmk VALUES (41, '{410,411}');
SELECT * FROM tmk_state();
SELECT id FROM tmk WHERE doc && '{10,200,411}' ORDER BY id;
SELECT bark_index_check('tmk_doc');
SET test_versioned_tableam.untag_tag = 1;
VACUUM tmk;
RESET test_versioned_tableam.untag_tag;
SELECT * FROM tmk_state();
SELECT id FROM tmk WHERE doc && '{10,200,411}' ORDER BY id;
SELECT bark_index_check('tmk_doc');
SET test_versioned_tableam.keep_locator = pretend;
SELECT test_versioned_reset();
UPDATE tmk SET doc = '{20,21,22}' WHERE id = 2;
SELECT n AS kept FROM test_versioned_counts() WHERE counter = 'update_kept';
RESET test_versioned_tableam.keep_locator;
SELECT count(i.tag) AS tagged, count(*) AS entries
  FROM tmk t, bark_page_items('tmk_doc', 1) i WHERE t.id = 2 AND i.htid = t.ctid;
SELECT * FROM tmk_state();
VACUUM tmk;
SELECT * FROM tmk_state();
SELECT id FROM tmk WHERE doc && '{10,22,200,411}' ORDER BY id;
SET enable_indexscan = off;
SET enable_bitmapscan = on;
SELECT id FROM tmk WHERE doc && '{10,22,200,411}' ORDER BY id;
RESET enable_indexscan;
SET enable_bitmapscan = off;
-- the index now stores no tags (the executor tags no entry of it), and
-- the constraint still holds
UPDATE tmk SET doc = '{30,31,33}' WHERE id = 3;
SELECT count(i.tag) AS tagged, count(*) AS entries
  FROM tmk t, bark_page_items('tmk_doc', 1) i WHERE t.id = 3 AND i.htid = t.ctid;
INSERT INTO tmk VALUES (42, '{22}');
INSERT INTO tmk VALUES (42, '{420}');
SELECT bark_index_check('tmk_doc');
DROP TABLE tmk;
DROP FUNCTION tmk_state();

-- keep_locator = inplace: an update of the column keeps the row's locator L
-- for real (heap's HOT chain at L holds the row's versions) and inserts the
-- new key set tagged.  While the update is in progress (a prepared
-- transaction here), a dirty snapshot, as a constraint check uses, sees two
-- versions at L.  A scalar index's scan (on doc[1]) returns both, each
-- through the entry valid for it.  Rule D's R, keyed by L, returns only the
-- first version a multikey scan reaches: v0, through (2,L,0); v1 = {2,3}
-- is never compared, which is the hole a constraint check must not have.
CREATE TABLE tmp (id int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off, fillfactor = 50);
INSERT INTO tmp VALUES (1, '{1,2}'), (2, '{20,21}');
CREATE INDEX tmp_doc ON tmp USING bark (doc bark_int4_array_ops);
CREATE INDEX tmp_d1 ON tmp USING bark ((doc[1]));
SET test_versioned_tableam.keep_locator = inplace;
SELECT test_versioned_reset();
BEGIN;
UPDATE tmp SET doc = '{2,3}' WHERE id = 1;
PREPARE TRANSACTION 'tmp_inplace';
RESET test_versioned_tableam.keep_locator;
SELECT n AS kept FROM test_versioned_counts() WHERE counter = 'update_kept';
SELECT count(*) AS entries, count(tag) AS tagged
  FROM bark_page_items('tmp_doc', 1) WHERE htid = '(0,1)';
SELECT locator, version, version = locator AS v0, recheck
  FROM test_versioned_dirty_scan('tmp_d1', '>=(int4,int4)', 1) ORDER BY version;
SELECT locator, version = locator AS v0, recheck
  FROM test_versioned_dirty_scan('tmp_doc', '&&(anyarray,anyarray)', '{2,3}'::int4[]);
-- Once the index is made an exclusion constraint's, its dirty scans keep
-- no row per locator: each entry is judged by
-- itself, so v1 is returned through (2,L,t1) and (3,L,t1), and every row
-- with xs_recheck, since a version need not hold the key that reached it.
SELECT test_versioned_add_exclusion('tmp_doc', '=(anyarray,anyarray)');
UPDATE pg_catalog.pg_index SET indisexclusion = true
  WHERE indexrelid = 'tmp_doc'::regclass;
SELECT locator, version = locator AS v0, recheck
  FROM test_versioned_dirty_scan('tmp_doc', '&&(anyarray,anyarray)', '{2,3}'::int4[]);
-- that scan, the first BARK call to see the constraint, set CONVERTING
SELECT flags & 30 AS mktags_converting FROM bark_metap('tmp_doc');
-- MVCC scans see v0 only, through its entries, once
SELECT id, doc FROM tmp WHERE doc && '{1,2,3}';
SELECT id, doc FROM tmp WHERE doc[1] >= 1;
COMMIT PREPARED 'tmp_inplace';
SELECT id, doc FROM tmp WHERE doc && '{1}';
SELECT id, doc FROM tmp WHERE doc && '{1,2,3}';
SELECT id, doc FROM tmp WHERE doc[1] = 1;
SELECT id, doc FROM tmp WHERE doc[1] >= 1;
SELECT locator, version = locator AS v0, recheck
  FROM test_versioned_dirty_scan('tmp_doc', '&&(anyarray,anyarray)', '{1,2,3}'::int4[]);
VACUUM tmp;
SELECT count(*) AS entries, count(tag) AS tagged
  FROM bark_page_items('tmp_doc', 1) WHERE htid = '(0,1)';
SELECT id, doc FROM tmp WHERE doc && '{1,2,3}';
SELECT bark_index_check('tmp_doc'), bark_index_check('tmp_d1');
DROP TABLE tmp;

-- A (key, TID, tag) is stored once.  A table AM may give several versions
-- of a row one tag (heap_versioned tags each with its transaction ID), so
-- updates that keep the locator can insert an entry the row already has:
-- k 1 -> 2 -> 1 -> 2 in one transaction inserts (2,L,t) twice, and doc
-- {20,21} -> {21,22} -> {21,23} inserts (21,L,t) twice.  A second copy
-- would break the order of the index, and a scan of k = 2 would return the
-- row through both.  No query asks for k = 1 or doc && '{22}': (1,L,t) and
-- (22,L,t) carry the last version's tag too, so heap_versioned would take
-- them for valid, a limit of tagging by transaction ID, not of the index.
CREATE TABLE tmt (id int, k int, doc int4[]) USING heap_versioned
  WITH (autovacuum_enabled = off, fillfactor = 50);
INSERT INTO tmt VALUES (1, 1, '{1,2}'), (2, 20, '{20,21}');
CREATE INDEX tmt_k ON tmt USING bark (k);
CREATE INDEX tmt_doc ON tmt USING bark (doc bark_int4_array_ops);
SET test_versioned_tableam.keep_locator = inplace;
SELECT test_versioned_reset();
BEGIN;
UPDATE tmt SET k = 2 WHERE id = 1;
UPDATE tmt SET k = 1 WHERE id = 1;
UPDATE tmt SET k = 2 WHERE id = 1;
UPDATE tmt SET doc = '{21,22}' WHERE id = 2;
UPDATE tmt SET doc = '{21,23}' WHERE id = 2;
COMMIT;
RESET test_versioned_tableam.keep_locator;
SELECT n AS kept FROM test_versioned_counts() WHERE counter = 'update_kept';
SELECT 'tmt_k' AS idx, htid, count(*) AS entries, count(tag) AS tagged,
       count(DISTINCT (data, tag)) AS distinct_entries
  FROM bark_page_items('tmt_k', 1) GROUP BY htid
UNION ALL
SELECT 'tmt_doc', htid, count(*), count(tag), count(DISTINCT (data, tag))
  FROM bark_page_items('tmt_doc', 1) WHERE shape = 'SINGLE' GROUP BY htid
ORDER BY 1, 2;
SELECT id, k FROM tmt WHERE k = 2;
SELECT id, k FROM tmt WHERE k >= 2 ORDER BY k DESC;
SELECT id, doc FROM tmt WHERE doc && '{21,23}';
SELECT bark_index_check('tmt_k'), bark_index_check('tmt_doc');
DROP TABLE tmt;

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
