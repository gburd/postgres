-- checkunique for BARK indexes (amcheck 1.8): two entries with equal key
-- columns whose heap rows are both visible to the check's snapshot are a
-- uniqueness violation.  Healthy indexes pass, with dead duplicates left
-- by aborted inserts and an UPDATE that keeps keys; real violations, made
-- by inserting while the index is marked not ready, are reported, for a
-- scalar index, an OVERSIZED key and a multikey index.
CREATE EXTENSION IF NOT EXISTS amcheck;
ALTER EXTENSION amcheck UPDATE TO '1.8';
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS bark_multikey;
CREATE EXTENSION IF NOT EXISTS pageinspect;
RESET client_min_messages;

-- Report the check's outcome, not its exact text (which has TIDs).
CREATE FUNCTION bcu(idx regclass, parent bool DEFAULT false) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  IF parent THEN
    PERFORM bark_index_parent_check(idx, false, true);
  ELSE
    PERFORM bark_index_check(idx, false, true);
  END IF;
  RETURN 'ok';
EXCEPTION WHEN index_corrupted THEN
  RETURN regexp_replace(SQLERRM, '\(\d+,\d+\)', '(t)', 'g');
END $$;

-- Healthy scalar unique index: dead duplicates (an aborted insert, a
-- deleted row, the old versions of updates that kept the key) pass.
CREATE TABLE bcu_s (id int, k int, v int);
CREATE UNIQUE INDEX bcu_s_k ON bcu_s USING bark (k);
CREATE INDEX bcu_s_v ON bcu_s (v);	-- the UPDATEs are not HOT
INSERT INTO bcu_s SELECT g, g, 0 FROM generate_series(1, 5000) g;
BEGIN;
DELETE FROM bcu_s WHERE k = 10;
INSERT INTO bcu_s VALUES (-10, 10, 0);
ROLLBACK;
DELETE FROM bcu_s WHERE k = 20;
INSERT INTO bcu_s VALUES (-20, 20, 0);
UPDATE bcu_s SET v = v + 1 WHERE k BETWEEN 100 AND 400;
UPDATE bcu_s SET v = v + 1 WHERE k BETWEEN 100 AND 400;
INSERT INTO bcu_s VALUES (-1, NULL, 0), (-2, NULL, 0);
SELECT bcu('bcu_s_k'), bcu('bcu_s_k', true);
-- The one- and two-argument forms do not check uniqueness.
SELECT bark_index_check('bcu_s_k'), bark_index_check('bcu_s_k', true),
       bark_index_parent_check('bcu_s_k', true);
-- A btree index is refused; a non-unique BARK index has nothing to check.
SELECT bark_index_check('bcu_s_v'::regclass, false, true);
CREATE INDEX bcu_s_nu ON bcu_s USING bark (v);
SELECT bark_index_check('bcu_s_nu', false, true);
DROP INDEX bcu_s_nu;

-- A real violation: two live rows with key 7.  With indisunique cleared
-- the insert is not checked; restoring it makes the index claim a
-- uniqueness it does not have, as a bug would.  deduplicate_items = off
-- keeps the duplicate in an entry of its own, as a unique index's writers
-- do: an entry of several heap TIDs is reported before the uniqueness
-- check runs (bcu_d below).
ALTER INDEX bcu_s_k SET (deduplicate_items = off);
UPDATE pg_index SET indisunique = false WHERE indexrelid = 'bcu_s_k'::regclass;
INSERT INTO bcu_s VALUES (-7, 7, 0);
UPDATE pg_index SET indisunique = true WHERE indexrelid = 'bcu_s_k'::regclass;
SELECT bcu('bcu_s_k'), bcu('bcu_s_k', true);
-- The structural check still passes: the index is well formed.
SELECT bark_index_check('bcu_s_k', true);
DELETE FROM bcu_s WHERE id = -7;
SELECT bcu('bcu_s_k');
-- A run of SINGLE entries across a leaf boundary: an INCLUDE column keeps
-- entries from coalescing, and 400 rows of one key with a 100-byte INCLUDE
-- value do not fit on one leaf.
ALTER TABLE bcu_s ADD COLUMN pad text DEFAULT repeat('p', 100);
CREATE UNIQUE INDEX bcu_s_ki ON bcu_s USING bark (k) INCLUDE (pad);
SELECT bcu('bcu_s_ki');
UPDATE pg_index SET indisunique = false WHERE indexrelid = 'bcu_s_ki'::regclass;
DROP INDEX bcu_s_k;
\c -
INSERT INTO bcu_s SELECT -g, 4999, 0 FROM generate_series(1, 400) g;
UPDATE pg_index SET indisunique = true WHERE indexrelid = 'bcu_s_ki'::regclass;
\c -
SELECT count(DISTINCT s.blkno) > 1 AS run_spans_leaves,
       count(*) FILTER (WHERE i.shape <> 'SINGLE') AS coalesced
  FROM bark_multi_page_stats('bcu_s_ki', 1, -1) s,
       LATERAL bark_page_items('bcu_s_ki', s.blkno::int) i
 WHERE s.type = 'leaf' AND i.data LIKE '87 13 00 00 %'
   AND (s.bark_next = 0 OR i.itemoffset > 1);
-- (the high key of a leaf that is not rightmost is item 1)
SELECT bcu('bcu_s_ki'), bcu('bcu_s_ki', true);
-- Only one of them alive: no violation.
DELETE FROM bcu_s WHERE k = 4999 AND id <> -400;
SELECT bcu('bcu_s_ki');
DROP TABLE bcu_s;

-- OVERSIZED keys: the run compares full keys from the overflow chains.
CREATE TABLE bcu_o (id int, k text);
CREATE UNIQUE INDEX bcu_o_k ON bcu_o USING bark (k);
INSERT INTO bcu_o SELECT g, repeat(md5(g::text), 400) FROM generate_series(1, 20) g;
SELECT bcu('bcu_o_k');
ALTER INDEX bcu_o_k SET (deduplicate_items = off);
UPDATE pg_index SET indisunique = false WHERE indexrelid = 'bcu_o_k'::regclass;
INSERT INTO bcu_o VALUES (-5, repeat(md5('5'), 400));
-- Same prefix, different key: not a violation.
INSERT INTO bcu_o VALUES (-6, repeat(md5('6'), 399) || 'x');
UPDATE pg_index SET indisunique = true WHERE indexrelid = 'bcu_o_k'::regclass;
SELECT bcu('bcu_o_k');
DELETE FROM bcu_o WHERE id = -5;
SELECT bcu('bcu_o_k');
DROP TABLE bcu_o;

-- Multikey: unique over the keys.  A row repeating a key, and NULL keys,
-- are not violations; two live rows sharing one key are.
CREATE TABLE bcu_m (id int, tenant int, a int4[]);
CREATE UNIQUE INDEX bcu_m_a ON bcu_m USING bark (a bark_int4_array_ops);
CREATE UNIQUE INDEX bcu_m_ta ON bcu_m USING bark (tenant, a bark_int4_array_ops);
INSERT INTO bcu_m SELECT g, g % 3, ARRAY[g * 10, g * 10 + 1] FROM generate_series(1, 2000) g;
INSERT INTO bcu_m VALUES (-1, 0, '{5,5,5}'), (-2, 0, '{}'), (-3, 0, '{}'),
  (-4, 0, NULL), (-5, 0, '{NULL,6}'), (-6, 1, '{NULL}');
UPDATE bcu_m SET a = a || 7 WHERE id = -1;
SELECT bcu('bcu_m_a'), bcu('bcu_m_ta'), bcu('bcu_m_a', true);
ALTER INDEX bcu_m_a SET (deduplicate_items = off);
UPDATE pg_index SET indisunique = false WHERE indexrelid = 'bcu_m_a'::regclass;
INSERT INTO bcu_m VALUES (-10, 5, '{1000,999999}');
UPDATE pg_index SET indisunique = true WHERE indexrelid = 'bcu_m_a'::regclass;
SELECT bcu('bcu_m_a');
-- The compound index sees tenant 5 apart from tenant 1, which holds 1000.
SELECT bcu('bcu_m_ta');
DELETE FROM bcu_m WHERE id = -10;
SELECT bcu('bcu_m_a');
ALTER INDEX bcu_m_ta SET (deduplicate_items = off);
UPDATE pg_index SET indisunique = false WHERE indexrelid = 'bcu_m_ta'::regclass;
-- bcu_m_a is still unique, so a row sharing 1000 there would be refused:
-- drop it first.  (A coalescing insert into a unique index that cached its
-- state as non-unique would make a LIST entry; the insert check reports
-- that as corruption rather than read past the entry.)
DROP INDEX bcu_m_a;
\c -
INSERT INTO bcu_m VALUES (-11, 1, '{1000}');
UPDATE pg_index SET indisunique = true WHERE indexrelid = 'bcu_m_ta'::regclass;
\c -
SELECT bcu('bcu_m_ta');
DROP TABLE bcu_m;

-- The 1.7 two-argument forms keep working after an update to 1.8.
CREATE TABLE bcu_e (k int);
CREATE UNIQUE INDEX bcu_e_k ON bcu_e USING bark (k);
DROP FUNCTION bcu(regclass, bool);
DROP EXTENSION amcheck;
CREATE EXTENSION amcheck VERSION '1.7';
SELECT bark_index_check('bcu_e_k', true);
ALTER EXTENSION amcheck UPDATE TO '1.8';
SELECT bark_index_check('bcu_e_k', true), bark_index_check('bcu_e_k', true, true);
DROP TABLE bcu_e;

-- An insert into a unique index that meets an entry of several heap TIDs
-- (which a unique index never makes; here a non-unique session coalesced
-- one) reports corruption instead of reading past the entry.
CREATE TABLE bcu_c (k int);
CREATE UNIQUE INDEX bcu_c_k ON bcu_c USING bark (k);
INSERT INTO bcu_c VALUES (1);
UPDATE pg_index SET indisunique = false WHERE indexrelid = 'bcu_c_k'::regclass;
\c -
INSERT INTO bcu_c SELECT 1 FROM generate_series(1, 3);
UPDATE pg_index SET indisunique = true WHERE indexrelid = 'bcu_c_k'::regclass;
\c -
INSERT INTO bcu_c VALUES (1);
DROP TABLE bcu_c;

-- A unique index has no LIST or POSTING entry: its writers never coalesce,
-- since bark_index_coalesces asks indisunique, not whether this write
-- enforces the constraint (CLUSTER, VACUUM FULL and REPACK rebuild without
-- enforcing it, and index the dead duplicates an older snapshot sees).  An
-- index holding one anyway, as such a rebuild made before that fix, is
-- reported by the structural check, with or without checkunique.  Here
-- non-unique partial indexes with a LIST and a POSTING entry are marked
-- unique.
CREATE TABLE bcu_d (k int);
INSERT INTO bcu_d SELECT 1 FROM generate_series(1, 3);
INSERT INTO bcu_d SELECT 2 FROM generate_series(1, 2000);
CREATE INDEX bcu_d_l ON bcu_d USING bark (k) WHERE k = 1;
CREATE INDEX bcu_d_p ON bcu_d USING bark (k) WHERE k = 2;
SELECT 'bcu_d_l' AS idx, shape, ntids FROM bark_page_items('bcu_d_l', 1)
UNION ALL
SELECT 'bcu_d_p', shape, ntids FROM bark_page_items('bcu_d_p', 1);
SELECT bark_index_check('bcu_d_l', true), bark_index_check('bcu_d_p', true);
UPDATE pg_index SET indisunique = true
  WHERE indexrelid IN ('bcu_d_l'::regclass, 'bcu_d_p'::regclass);
\c -
SELECT bark_index_check('bcu_d_l');
SELECT bark_index_check('bcu_d_p', true, true);
SELECT bark_index_parent_check('bcu_d_l', true);
DROP TABLE bcu_d;

-- heapallindexed on a multikey index forms each row's keys with procedure 7
-- and probes for each: it passes on a sound index, with LIST and POSTING
-- entries and NULL and empty arrays, unique or not, and reports the row
-- whose keys an insert that bypassed the index left out, here one array of
-- several keys.
CREATE TABLE bcu_mk (id int, a int4[]);
INSERT INTO bcu_mk SELECT g, ARRAY[g % 50, g % 7 + 100, g] FROM generate_series(1, 3000) g;
INSERT INTO bcu_mk VALUES (3001, NULL), (3002, '{}'), (3003, '{NULL,5}');
CREATE INDEX bcu_mk_a ON bcu_mk USING bark (a bark_int4_array_ops);
CREATE INDEX bcu_mk_ia ON bcu_mk USING bark (id, a bark_int4_array_ops);
CREATE UNIQUE INDEX bcu_mk_u ON bcu_mk USING bark ((ARRAY[id]) bark_int4_array_ops);
SELECT bark_index_check(i, true), bark_index_parent_check(i, true)
  FROM unnest('{bcu_mk_a,bcu_mk_ia,bcu_mk_u}'::regclass[]) i;
UPDATE pg_index SET indisready = false WHERE indexrelid = 'bcu_mk_a'::regclass;
\c -
INSERT INTO bcu_mk VALUES (3004, '{7,8,9}');
UPDATE pg_index SET indisready = true WHERE indexrelid = 'bcu_mk_a'::regclass;
\c -
SELECT bark_index_check('bcu_mk_a');
SELECT bark_index_check('bcu_mk_a', true);
SELECT bark_index_parent_check('bcu_mk_a', true);
REINDEX INDEX bcu_mk_a;
SELECT bark_index_check('bcu_mk_a', true);
DROP TABLE bcu_mk;
DROP EXTENSION bark_multikey;
DROP EXTENSION pageinspect;
