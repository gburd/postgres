-- Unique BARK indexes with an extracted column ("M4" in
-- BARK-Design.mediawiki): unique over the entries' keys.  Two rows conflict
-- when they share a key; one row may repeat one; NULL keys never conflict.
CREATE EXTENSION IF NOT EXISTS bark_multikey;

-- Try one insert, reporting a unique violation instead of raising it.
CREATE FUNCTION u_try(stmt text) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE stmt;
  RETURN 'ok';
EXCEPTION WHEN unique_violation THEN
  RETURN 'unique_violation';
END $$;

-- One key per row: bark(a) over one-element arrays decides as a unique
-- btree on (a[1]) does, row by row, and the tables end up equal.
CREATE TABLE ub (a int4[]);
CREATE UNIQUE INDEX ub_a ON ub USING bark (a bark_int4_array_ops);
CREATE TABLE ut (a int4[]);
CREATE UNIQUE INDEX ut_a ON ut ((a[1]));
SELECT v, u_try(format('INSERT INTO ub VALUES (%L)', v)) AS bark,
       u_try(format('INSERT INTO ut VALUES (%L)', v)) AS btree
FROM unnest('{"{1}","{2}","{1}","{NULL}","{NULL}","{3}","{2}","{4}"}'::text[])
     WITH ORDINALITY AS t(v, n)
ORDER BY n;
SELECT (SELECT count(*) FROM (SELECT a FROM ub EXCEPT ALL SELECT a FROM ut) x)
       AS bark_only,
       (SELECT count(*) FROM (SELECT a FROM ut EXCEPT ALL SELECT a FROM ub) y)
       AS btree_only;
SELECT bark_index_check('ub_a');

-- Several keys per row.  A row may repeat a key; two rows may not share
-- one, and the detail names the shared key.  A NULL value, an empty array
-- and a NULL element are the NULL key, which never conflicts, while the
-- row's other keys are still checked.
CREATE TABLE um (id int, a int4[]);
CREATE UNIQUE INDEX um_a ON um USING bark (a bark_int4_array_ops);
INSERT INTO um VALUES (1, '{1,1,2}');
INSERT INTO um VALUES (2, '{0,2}');
INSERT INTO um VALUES (3, '{}'), (4, NULL), (5, '{NULL}'), (6, '{}'),
  (7, '{NULL,9}'), (8, '{NULL}');
INSERT INTO um VALUES (9, '{9,NULL}');
INSERT INTO um VALUES (10, '{10,NULL}');
-- The heap, read by a sequential scan, has no key in two rows.
SELECT e, count(DISTINCT id) FROM um, unnest(a) e WHERE e IS NOT NULL
GROUP BY e HAVING count(DISTINCT id) > 1;
SELECT id, a FROM um ORDER BY id;
SELECT bark_index_check('um_a');

-- Row 2 failed on its second key, 2: its first key, 0, is in the index as
-- an entry of a dead tuple, which VACUUM removes.
SELECT count(*) FROM bark_multikey_entries('um_a') WHERE NOT marker AND key = 0;
VACUUM um;
SELECT count(*) FROM bark_multikey_entries('um_a') WHERE NOT marker AND key = 0;
SELECT bark_index_check('um_a');

-- An UPDATE that keeps some keys conflicts with nothing: the old version's
-- deleter is the updater.  Another row then cannot take a kept key, and
-- may take the dropped one.
INSERT INTO um VALUES (20, '{21,22,23}');
UPDATE um SET a = '{22,23,24}' WHERE id = 20;
INSERT INTO um VALUES (21, '{22}');
INSERT INTO um VALUES (22, '{21}');
SELECT e, count(DISTINCT id) FROM um, unnest(a) e WHERE e IS NOT NULL
GROUP BY e HAVING count(DISTINCT id) > 1;
SELECT bark_index_check('um_a');

-- Until ON CONFLICT is supported (M4b), an INSERT ... ON CONFLICT into a
-- table with such an index fails in core, where the executor looks for each
-- column's equality operator, rather than give a wrong result.
DO $$
BEGIN
  INSERT INTO um VALUES (30, '{30}') ON CONFLICT DO NOTHING;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE '%', regexp_replace(SQLERRM, '[0-9]+', 'N', 'g');
END $$;

-- Compound: unique per tenant.  A NULL fixed column puts a NULL in every
-- key, so such a row is never checked.
CREATE TABLE uc (tenant int, a int4[]);
CREATE UNIQUE INDEX uc_ta ON uc USING bark (tenant, a bark_int4_array_ops);
INSERT INTO uc VALUES (1, '{1,2}'), (2, '{1,2}');
INSERT INTO uc VALUES (1, '{5,2}');
INSERT INTO uc VALUES (NULL, '{1,2}'), (NULL, '{1,2}');
SELECT tenant, e, count(*) FROM uc, unnest(a) e
WHERE tenant IS NOT NULL AND e IS NOT NULL
GROUP BY tenant, e HAVING count(*) > 1;
SELECT bark_index_check('uc_ta');

-- The planner does not read such an index as a unique key: a row may hold
-- many keys.  get_relation_info sets info->unique false, so a join is not
-- proved to return one row per outer row.
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM um WHERE a = '{22,23,24}';
RESET enable_seqscan;

-- CREATE INDEX: two live rows sharing a key fail with no key named (the
-- sorted key is of the storage type); the old version of an updated row,
-- which the build indexes but does not check, shares keys with its new
-- version and does not.
CREATE TABLE ubuild (id int, a int4[]);
CREATE INDEX ubuild_bt ON ubuild (a);	-- so the UPDATE below is not HOT
INSERT INTO ubuild VALUES (1, '{1,2,3}'), (2, '{3,7}');
CREATE UNIQUE INDEX ubuild_a ON ubuild USING bark (a bark_int4_array_ops);
DELETE FROM ubuild WHERE id = 2;
BEGIN;
UPDATE ubuild SET a = '{2,3,4}' WHERE id = 1;
CREATE UNIQUE INDEX ubuild_a ON ubuild USING bark (a bark_int4_array_ops);
SELECT bark_index_check('ubuild_a');
SELECT count(*) FROM bark_multikey_entries('ubuild_a') WHERE NOT marker;
COMMIT;
INSERT INTO ubuild VALUES (3, '{4}');
INSERT INTO ubuild VALUES (4, '{1}');
SELECT id, a FROM ubuild ORDER BY id;
SELECT bark_index_check('ubuild_a');

DROP TABLE ub, ut, um, uc, ubuild;
DROP FUNCTION u_try(text);
