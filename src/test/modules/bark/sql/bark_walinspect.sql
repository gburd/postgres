--
-- Test the BARK rmgr's record descriptions (barkdesc.c).
--
-- A workload logs every BARK record type that SQL can produce, and
-- pg_walinspect reads its WAL back.  Each record type must appear, and every
-- record of the type must have the description barkdesc.c writes for it, with
-- each of its fields named.  The variant checks then cover the optional parts
-- of a description: the deleted and updated offsets of VACUUM, DELETE and
-- MERGE records, and a VACUUM record that has a full-page image instead of
-- them, and the split flags.  Only booleans are printed, so no LSN,
-- block number or relfilenode reaches the expected output.
--
-- CREATE_ROOT is listed as not logged: only an index that has no root gets
-- one, CREATE INDEX always writes a root, and the only rootless BARK index is
-- the init fork of an unlogged index, whose inserts are not WAL-logged.
--
set client_min_messages TO 'warning';
create extension if not exists pg_walinspect;
create extension if not exists amcheck;
reset client_min_messages;

-- Keep the WAL from the start of the workload until it has been read, even
-- if a checkpoint happens in between.
SELECT 1 FROM pg_create_physical_replication_slot('bark_walinspect', true);
SELECT pg_current_wal_lsn() AS start_lsn \gset

-- Unique keys in random order: leaf inserts, leaf and internal splits with
-- the new entry on either side, downlinks into parents with room, and new
-- roots up to a third level (300-byte keys fit about 25 to a page).
CREATE TABLE wi_split (k int, s text) WITH (autovacuum_enabled = off);
CREATE INDEX wi_split_idx ON wi_split USING bark (s);
SELECT setseed(0.5);
INSERT INTO wi_split SELECT g, lpad(g::text, 300, '0')
  FROM generate_series(1, 20000) g ORDER BY random();

-- Prefix-coded leaves: the first run of keys shares a long prefix that both
-- halves of a split keep; the second shares none, so a split among its keys
-- takes the prefix away from the left half, which is then logged whole.
CREATE TABLE wi_prefix (k int, s text) WITH (autovacuum_enabled = off);
CREATE INDEX wi_prefix_idx ON wi_prefix USING bark (s)
  WITH (prefix_compression = on);
SELECT setseed(0.7);
INSERT INTO wi_prefix SELECT g, 'https://www.example.com/item/' || lpad(g::text, 8, '0')
  FROM generate_series(1, 5000) g ORDER BY random();
INSERT INTO wi_prefix SELECT g, md5(g::text) || 'https://x/' || g
  FROM generate_series(1, 2000) g ORDER BY random();

-- One key: each split cuts the last POSTING entry of the left page, whose
-- left part stays there (the replaced entry).  Keys inserted round-robin and
-- in runs form LIST and POSTING entries (OVERWRITE) and grow them a TID at a
-- time (ADD_TID).
CREATE TABLE wi_dup (k int) WITH (autovacuum_enabled = off);
CREATE INDEX wi_dup_idx ON wi_dup USING bark (k);
INSERT INTO wi_dup SELECT 1 FROM generate_series(1, 150000);
INSERT INTO wi_dup SELECT 100000 + g % 100 FROM generate_series(1, 20000) g;
INSERT INTO wi_dup SELECT 200000 + (g - 1) / 1000 FROM generate_series(1, 20000) g;

-- Heap TIDs inside the TID range of LIST (wi_hkl) and POSTING (wi_hkp)
-- entries: the heap leaves half of each page free, and rows of another key
-- updated to the key go on their old version's page, inside the key's run.
-- Entries that cannot take the TID are divided around it (INSERT_SWAP).
-- After REINDEX the pages are full, and an insert first merges the page's
-- equal-key entries (MERGE).
CREATE TABLE wi_hkl (k int, v int) WITH (fillfactor = 50, autovacuum_enabled = off);
CREATE INDEX wi_hkl_idx ON wi_hkl USING bark (k);
INSERT INTO wi_hkl SELECT g % 50, g FROM generate_series(1, 100000) g;
UPDATE wi_hkl SET k = 1 WHERE k = 2 AND v % 4 = 2;
REINDEX INDEX wi_hkl_idx;
UPDATE wi_hkl SET k = 1 WHERE k = 3 AND v % 4 = 3;
CREATE TABLE wi_hkp (k int, v int) WITH (fillfactor = 50, autovacuum_enabled = off);
CREATE INDEX wi_hkp_idx ON wi_hkp USING bark (k);
INSERT INTO wi_hkp SELECT g % 2, g FROM generate_series(1, 100000) g;
UPDATE wi_hkp SET k = 1 WHERE k = 0 AND v % 4 = 0;
REINDEX INDEX wi_hkp_idx;
UPDATE wi_hkp SET k = 1 WHERE k = 0 AND v % 4 = 2;

-- Oversized keys go to overflow pages (OVERFLOW, one to several pages a
-- record); deleting them and vacuuming frees the pages (MARK_DELETED).
CREATE TABLE wi_big (id int, k text) WITH (autovacuum_enabled = off);
CREATE INDEX wi_big_idx ON wi_big USING bark (k);
INSERT INTO wi_big SELECT g, (SELECT string_agg(md5(g::text || i::text), '')
                              FROM generate_series(1, 160 + (g % 3) * 300) i)
  FROM generate_series(1, 30) g;
DELETE FROM wi_big WHERE id % 2 = 0;
VACUUM wi_big;

-- Bottom-up deletion: non-HOT updates that leave k unchanged insert new
-- versions into full k leaves, which first delete the entries of dead
-- versions (DELETE), whole entries and members of LIST entries (u).
CREATE TABLE wi_churn (k int, v int, u text)
  WITH (fillfactor = 100, autovacuum_enabled = off);
CREATE INDEX wi_churn_k ON wi_churn USING bark (k);
CREATE INDEX wi_churn_v ON wi_churn USING bark (v);
CREATE INDEX wi_churn_u ON wi_churn USING bark (u)
  WITH (prefix_compression = on);
INSERT INTO wi_churn
  SELECT g, 0, 'https://www.example.com/item/' || lpad((g % 1250)::text, 8, '0')
  FROM generate_series(1, 5000) g;
DELETE FROM wi_churn WHERE k % 10 = 0;
UPDATE wi_churn SET v = v + 1;
CHECKPOINT;
UPDATE wi_churn SET v = v + 1;

-- VACUUM: delete a range of interior keys, so leaves are emptied and unlinked
-- (UNLINK_PAGE), and every 7th duplicate, so LIST and POSTING entries lose
-- members (nupdated).  After the checkpoint the first VACUUM record of each
-- leaf carries a full-page image; a second round on the same leaves logs
-- the offsets.  A later VACUUM records the deleted pages as free, and the
-- refill reuses them (REUSE_PAGE).
CREATE TABLE wi_vac (a int, pad text) WITH (autovacuum_enabled = off);
INSERT INTO wi_vac SELECT g, repeat('x', 100) FROM generate_series(1, 20000) g;
INSERT INTO wi_vac SELECT 100000 + g % 20, 'y' FROM generate_series(1, 20000) g;
CREATE INDEX wi_vac_idx ON wi_vac USING bark (a);
CHECKPOINT;
DELETE FROM wi_vac WHERE a BETWEEN 2000 AND 12000 OR (a > 100000 AND ctid::text LIKE '%7)');
VACUUM wi_vac;
DELETE FROM wi_vac WHERE a % 3 = 0 OR (a > 100000 AND ctid::text LIKE '%3)');
VACUUM wi_vac;
SELECT 1 FROM txid_current();
VACUUM wi_vac;
INSERT INTO wi_vac SELECT 2000 + g % 10000, repeat('z', 100)
  FROM generate_series(1, 20000) g;

SELECT bark_index_check(i, true)
  FROM unnest(ARRAY['wi_split_idx', 'wi_prefix_idx', 'wi_dup_idx', 'wi_hkl_idx',
                    'wi_hkp_idx', 'wi_big_idx', 'wi_churn_k', 'wi_churn_v',
                    'wi_churn_u', 'wi_vac_idx']::regclass[]) i;

CREATE TEMP TABLE wi_rec AS
  SELECT record_type, description
  FROM pg_get_wal_records_info(:'start_lsn', pg_current_wal_lsn())
  WHERE resource_manager = 'Bark';

-- Every record type, and the full shape of every one of its descriptions.
WITH f(rtype, pat) AS (VALUES
  ('VACUUM', '^ndeleted: \d+, nupdated: \d+(, deleted: \[[0-9, ]*\], updated: \[[0-9, ]*\])?$'),
  ('DELETE', '^snapshotConflictHorizon: \d+, ndeleted: \d+, nupdated: \d+, isCatalogRel: [TF](, deleted: \[[0-9, ]*\], updated: \[[0-9, ]*\])?$'),
  ('MERGE', '^ndeleted: \d+, nupdated: \d+(, deleted: \[[0-9, ]*\], updated: \[[0-9, ]*\])?$'),
  ('UNLINK_PAGE', '^left: \d+, right: \d+, safexid: \d+:\d+, poffset: \d+$'),
  ('REUSE_PAGE', '^rel: \d+/\d+/\d+, blk: \d+, snapshotConflictHorizon: \d+:\d+, isCatalogRel: [TF]$'),
  ('MARK_DELETED', '^next: \d+, safexid: \d+:\d+$'),
  ('INSERT_LEAF', '^off: \d+$'),
  ('INSERT_UPPER', '^off: \d+$'),
  ('OVERWRITE', '^off: \d+$'),
  ('ADD_TID', '^off: \d+, tid: \(\d+,\d+\)$'),
  ('SPLIT', '^level: \d+, leaf: [TF], left: \d+, right: \d+, cycleid: \d+, prefix: [L-][R-](, left logged whole|, firstrightoff: \d+(, newitemoff: \d+)?(, replaceoff: \d+)?)$'),
  ('OVERFLOW', '^npages: [1-9]\d*$'),
  ('NEWROOT', '^root: \d+, level: [1-9]\d*$'),
  ('CREATE_ROOT', '^root: \d+$'),
  ('INSERT_SWAP', '^off: \d+, tid: \(\d+,\d+\)$'))
SELECT f.rtype, count(r.record_type) > 0 AS logged,
       coalesce(bool_and(r.description ~ f.pat), false) AS described
  FROM f LEFT JOIN wi_rec r ON r.record_type = f.rtype
  GROUP BY f.rtype ORDER BY f.rtype;

-- The optional parts of the descriptions.
\x on
SELECT
  bool_or(record_type = 'VACUUM' AND description ~ 'deleted: \[\d') AS vacuum_offsets,
  bool_or(record_type = 'VACUUM' AND description ~ 'updated: \[\d') AS vacuum_updated,
  bool_or(record_type = 'VACUUM' AND description !~ ', deleted:') AS vacuum_image,
  bool_or(record_type = 'DELETE' AND description ~ 'deleted: \[\d') AS delete_offsets,
  bool_or(record_type = 'DELETE' AND description ~ 'updated: \[\d') AS delete_updated,
  bool_or(record_type = 'MERGE' AND description ~ 'deleted: \[\d') AS merge_offsets,
  bool_or(record_type = 'OVERFLOW' AND description ~ 'npages: ([2-9]|\d\d)') AS overflow_multi,
  bool_or(record_type = 'NEWROOT' AND description ~ 'level: 2$') AS newroot_level2,
  bool_and((description ~ 'level: 0,') = (description ~ 'leaf: T'))
    FILTER (WHERE record_type = 'SPLIT') AS split_leaf_is_level0,
  bool_or(record_type = 'SPLIT' AND description ~ 'leaf: F') AS split_internal,
  bool_or(record_type = 'SPLIT' AND description ~ 'prefix: --') AS split_noprefix,
  bool_or(record_type = 'SPLIT' AND description ~ 'prefix: LR') AS split_bothprefix,
  bool_or(record_type = 'SPLIT' AND description ~ 'prefix: -R') AS split_rightprefix,
  bool_or(record_type = 'SPLIT' AND description ~ 'left logged whole') AS split_leftwhole,
  bool_or(record_type = 'SPLIT' AND description ~ 'firstrightoff: \d+$') AS split_newright,
  bool_or(record_type = 'SPLIT' AND description ~ 'newitemoff:') AS split_newleft,
  bool_or(record_type = 'SPLIT' AND description ~ 'replaceoff:') AS split_replace
  FROM wi_rec;
\x off

SELECT pg_drop_replication_slot('bark_walinspect');
DROP TABLE wi_rec, wi_split, wi_prefix, wi_dup, wi_hkl, wi_hkp, wi_big,
  wi_churn, wi_vac;
