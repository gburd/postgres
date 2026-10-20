--
-- Version-tag plumbing between a table AM whose locator is versioned
-- (heap_versioned) and an index AM that stores tags (test_versioned_btree).
-- heap_versioned's tag for a row version is its xmin, and it raises an
-- error wherever it is handed a (TID, tag) whose tag is neither 0 nor the
-- xmin of the tuple at that TID; test_versioned_seen shows which checks
-- saw tags.
--
CREATE EXTENSION test_versioned_tableam;
SELECT amname, pg_indexam_has_property(oid, 'can_version')
  FROM pg_am WHERE amname IN ('btree', 'test_versioned_btree') ORDER BY 1;

-- btree indexes on heap_versioned are implemented by test_versioned_btree
CREATE TABLE tv (id int PRIMARY KEY, a int, b int, c text) USING heap_versioned;
CREATE INDEX tv_a ON tv (a);
CREATE INDEX tv_b ON tv USING hash (b);
SELECT c.relname, am.amname AS logical, i.amname AS impl
  FROM pg_index x JOIN pg_class c ON c.oid = x.indexrelid
  JOIN pg_am am ON am.oid = c.relam LEFT JOIN pg_am i ON i.oid = x.indimplam
 WHERE x.indrelid = 'tv'::regclass ORDER BY 1;

-- INSERT: new rows are untagged
SELECT test_versioned_reset();
INSERT INTO tv SELECT g, g, g, 'x' || g FROM generate_series(1, 20) g;
SELECT * FROM test_versioned_seen;
SELECT count(*) AS untagged FROM tv WHERE test_versioned_tag('tv_a', ctid) = 0;

-- UPDATE of an indexed column: the row moves, and the moved version's
-- entries carry its tag, its xmin
SELECT test_versioned_reset();
UPDATE tv SET a = a + 100 WHERE id <= 5;
SELECT counter, n FROM test_versioned_seen WHERE counter = 'insert_tagged';
SELECT count(*) AS tagged_with_xmin FROM tv
 WHERE id <= 5 AND test_versioned_tag('tv_a', ctid) = xmin::text::bigint
   AND test_versioned_tag('tv_pkey', ctid) = xmin::text::bigint;
-- hash stores no tags, so nothing is recorded for it
SELECT count(test_versioned_tag('tv_b', ctid)) FROM tv;

-- plain index scan, index-only scan, bitmap scan: tags reach the table AM
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT test_versioned_reset();
SET enable_indexonlyscan = off;
EXPLAIN (COSTS OFF) SELECT id, a FROM tv WHERE a > 100;
SELECT id, a FROM tv WHERE a > 100 ORDER BY a;
SELECT * FROM test_versioned_seen;
SELECT test_versioned_reset();
SET enable_indexonlyscan = on;
EXPLAIN (COSTS OFF) SELECT a FROM tv WHERE a > 100;
SELECT a FROM tv WHERE a > 100 ORDER BY a;
SELECT * FROM test_versioned_seen;
SELECT test_versioned_reset();
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = on;
EXPLAIN (COSTS OFF) SELECT id FROM tv WHERE a > 100;
SELECT id FROM tv WHERE a > 100 ORDER BY id;
SELECT * FROM test_versioned_seen;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;

-- fetch_tid_version gets an entry's tag and index, as from an index AM's
-- unique check
SELECT test_versioned_reset();
SELECT bool_and(test_versioned_fetch('tv_a', ctid)) FROM tv;
SELECT * FROM test_versioned_seen;

-- unique violations
INSERT INTO tv VALUES (1, 0, 0, 'dup');
UPDATE tv SET id = 2 WHERE id = 1;

-- ON CONFLICT: the arbiter probe is an index scan; id 3's entry is tagged
SELECT test_versioned_reset();
INSERT INTO tv VALUES (3, 0, 0, 'conflict') ON CONFLICT (id) DO UPDATE SET c = 'upd';
INSERT INTO tv VALUES (6, 0, 0, 'conflict') ON CONFLICT DO NOTHING;
SELECT id, c FROM tv WHERE id IN (3, 6) ORDER BY id;
SELECT * FROM test_versioned_seen;

-- DELETE, then VACUUM: heap's own index pass removes the deleted row's
-- entries; the version callback then untags the live tagged ones.  id 3's
-- HOT update (above) leaves a redirect at its root, which is live.
SELECT test_versioned_reset();
DELETE FROM tv WHERE id = 20;
VACUUM tv;
SELECT * FROM test_versioned_seen;
SELECT count(*) AS still_tagged FROM tv WHERE test_versioned_tag('tv_a', ctid) <> 0;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS found_by_index FROM tv WHERE a > 0;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- bottom-up index deletion reaches index_delete_tuples, whose
-- TM_IndexStatus carries each entry's tag (0 from nbtree).  Every update
-- changes b, so it is not HOT, and gives tbu_a an unchanged duplicate.
SELECT test_versioned_reset();
CREATE TABLE tbu (a int, b int) USING heap_versioned;
CREATE INDEX tbu_a ON tbu (a);
CREATE INDEX tbu_b ON tbu USING hash (b);
INSERT INTO tbu SELECT g, 0 FROM generate_series(1, 400) g;
DO $$
BEGIN
  FOR i IN 1..10 LOOP
    UPDATE tbu SET b = b + 1;
  END LOOP;
END $$;
SELECT n > 0 AS deletion_saw_entries FROM test_versioned_counts()
 WHERE counter = 'delete_status';
DROP TABLE tbu;

-- CREATE INDEX: the build tags each row it hands over with the xmin of
-- the tuple at the entry's TID.  A HOT-updated row's entry is at its
-- chain's root, not at its ctid, so looking it up by ctid finds nothing.
SELECT test_versioned_reset();
UPDATE tv SET c = c || '!' WHERE id <= 3;  -- HOT
CREATE INDEX tv_c ON tv (c);
SELECT * FROM test_versioned_seen;
SELECT id <= 3 AS hot, test_versioned_tag('tv_c', ctid) = xmin::text::bigint AS tag_is_xmin,
       count(*)
  FROM tv GROUP BY 1, 2 ORDER BY 1, 2;

-- CREATE INDEX CONCURRENTLY: validation gets (TID, tag) pairs
SELECT test_versioned_reset();
CREATE INDEX CONCURRENTLY tv_ab ON tv (a, b);
SELECT counter FROM test_versioned_seen WHERE counter LIKE 'validate%' OR counter = 'build_tagged';

-- REINDEX, REPACK and VACUUM FULL rebuild with tags
SELECT test_versioned_reset();
REINDEX TABLE tv;
SELECT counter FROM test_versioned_seen;
REPACK tv;
VACUUM FULL tv;
-- REPACK (CONCURRENTLY) accepts only heap's own routine, since it cannot
-- tell whether another AM's changes are decoded
REPACK (CONCURRENTLY) tv;
SET enable_seqscan = off;
SELECT count(*) FROM tv WHERE a > 0;
RESET enable_seqscan;

DROP TABLE tv;

-- TU_Affected.  With keep_locator = pretend, heap_versioned reports that it
-- kept the row's locator whenever the executor let it (see the module's
-- header: the table is left without entries for the moved rows in the
-- unaffected indexes, so use a scratch table).  The executor then gives
-- tagged entries only to the indexes the update affected and to summarizing
-- ones.
CREATE TABLE tk (id int, a int, b int) USING heap_versioned;
CREATE INDEX tk_a ON tk (a);
CREATE INDEX tk_b ON tk (b);
CREATE INDEX tk_brin ON tk USING brin (id);
INSERT INTO tk VALUES (1, 1, 1);
SELECT test_versioned_reset();
SET test_versioned_tableam.keep_locator = pretend;
UPDATE tk SET a = 2;
SELECT test_versioned_tag('tk_a', ctid) = xmin::text::bigint AS a_tagged,
       test_versioned_tag('tk_b', ctid) AS b_tag
  FROM tk;
SELECT * FROM test_versioned_seen;
-- a hash index on b stores no tags, so an update of b must move the row
CREATE INDEX tk_bh ON tk USING hash (b);
SELECT test_versioned_reset();
UPDATE tk SET b = 2;
SELECT * FROM test_versioned_seen;
-- TU_Affected with tag 0 is refused
SET test_versioned_tableam.keep_locator = untagged;
UPDATE tk SET a = 3;
RESET test_versioned_tableam.keep_locator;
DROP TABLE tk;

-- An exclusion constraint's index that keeps its entries under TU_Affected
-- gets no new entry, so its constraint is checked after the update
-- (ExecCheckHotExclusion), as after a HOT update.  hashint8(1) =
-- hashint8(2^32), so changing k from 1 to 2^32 gives the hash index the same
-- entry; the change to a needs an entry in tx_a, so the update is not HOT,
-- and with keep_locator = pretend the row keeps its locator.  A conflict
-- with another row is found by that check alone.
CREATE TABLE tx (id int, k int8, a int, EXCLUDE USING hash (k WITH =))
  USING heap_versioned;
CREATE INDEX tx_a ON tx (a);
INSERT INTO tx VALUES (1, 1, 1), (2, 4294967296, 2);
SET test_versioned_tableam.keep_locator = pretend;
UPDATE tx SET k = 4294967296, a = 10 WHERE id = 1;
DELETE FROM tx WHERE id = 2;
SELECT test_versioned_reset();
UPDATE tx SET k = 4294967296, a = 10 WHERE id = 1;
SELECT * FROM test_versioned_seen;
-- A noted index that the update affects after all gets an entry, and its
-- constraint is checked with that entry, not again afterwards.  Writing a
-- compressed t back plain gives tz's exclusion index the same entry, so it
-- is noted; but the row enters tz_p's predicate, so t stays modified and
-- both indexes get tagged entries.  The check scans tz_t_excl once and
-- finds the row through its new entry: one scan_tagged, where a second
-- check after the update would make two.
CREATE TABLE tz (id int, t text, a int, EXCLUDE USING btree (t WITH =))
  USING heap_versioned;
CREATE INDEX tz_p ON tz (t) WHERE a > 1;
INSERT INTO tz VALUES (1, repeat('z', 3000), 1);
SELECT pg_column_compression(t) IS NOT NULL AS compressed FROM tz;
SELECT test_versioned_reset();
UPDATE tz SET t = t || '', a = 2;
SELECT * FROM test_versioned_seen;
RESET test_versioned_tableam.keep_locator;
DROP TABLE tx, tz;

DROP EXTENSION test_versioned_tableam;
