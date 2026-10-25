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
  FROM (SELECT b AS blk, i.tag FROM generate_series(1, pg_relation_size('tml_doc') / 8192 - 1) b,
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

-- An index without MKTAGS keeps the page-read seen set: the GIN-interface
-- class, and a multikey index on heap.
CREATE INDEX tmv_gin ON tmv USING bark (doc bark_gin_int4_array_ops);
SELECT flags & 6 AS versioned_mktags FROM bark_metap('tmv_gin');
DROP INDEX tmv_gin;

RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE tmv, tmw, tmo, tml;
DROP EXTENSION pageinspect;
DROP EXTENSION bark_multikey;
DROP EXTENSION test_versioned_tableam;
