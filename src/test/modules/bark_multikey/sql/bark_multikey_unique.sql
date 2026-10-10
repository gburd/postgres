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

-- ON CONFLICT.  One key per row: on bark(a) over one-element arrays, DO
-- NOTHING and DO UPDATE decide as on the unique btree on (a[1]).
INSERT INTO ub VALUES ('{2}'), ('{5}'), ('{NULL}') ON CONFLICT DO NOTHING
  RETURNING a;
INSERT INTO ut VALUES ('{2}'), ('{5}'), ('{NULL}') ON CONFLICT DO NOTHING
  RETURNING a;
INSERT INTO ub VALUES ('{3}'), ('{6}') ON CONFLICT (a)
  DO UPDATE SET a = ARRAY[ub.a[1] + 100] RETURNING a;
INSERT INTO ut VALUES ('{3}'), ('{6}') ON CONFLICT ((a[1]))
  DO UPDATE SET a = ARRAY[ut.a[1] + 100] RETURNING a;
SELECT (SELECT count(*) FROM (SELECT a FROM ub EXCEPT ALL SELECT a FROM ut) x)
       AS bark_only,
       (SELECT count(*) FROM (SELECT a FROM ut EXCEPT ALL SELECT a FROM ub) y)
       AS btree_only;
SELECT bark_index_check('ub_a');

-- Several keys per row: the pre-check scans with the class's overlap
-- operator.  A row conflicting with one row, then with two, inserts
-- nothing; so does the second of two rows of one statement that share a
-- key; a row sharing no key is inserted.
CREATE TABLE uo (id int, a int4[], note text);
CREATE UNIQUE INDEX uo_a ON uo USING bark (a bark_int4_array_ops);
INSERT INTO uo VALUES (1, '{1,4}'), (2, '{2,5}'), (3, '{3}');
INSERT INTO uo VALUES (10, '{4,7}') ON CONFLICT DO NOTHING;
INSERT INTO uo VALUES (11, '{5,1}') ON CONFLICT DO NOTHING;
INSERT INTO uo VALUES (12, '{7,8}') ON CONFLICT DO NOTHING;
INSERT INTO uo VALUES (13, '{9,9,NULL}'), (14, '{9}') ON CONFLICT DO NOTHING;
INSERT INTO uo VALUES (15, '{NULL}'), (16, '{}') ON CONFLICT DO NOTHING;
SELECT id, a FROM uo ORDER BY id;
-- One row may conflict with several: DO UPDATE updates the one holding the
-- new value's smallest shared key, which a sequential scan names first.
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT id FROM uo WHERE a && '{5,3,1}'
ORDER BY (SELECT min(e) FROM unnest(a) e WHERE e = ANY ('{5,3,1}')) LIMIT 1;
RESET enable_indexscan;
RESET enable_bitmapscan;
INSERT INTO uo VALUES (20, '{5,3,1}') ON CONFLICT (a)
  DO UPDATE SET note = 'hit by ' || EXCLUDED.id RETURNING id, note;
-- A row sharing every key with one row updates it once.
INSERT INTO uo VALUES (21, '{5,2}') ON CONFLICT (a)
  DO UPDATE SET note = 'hit by ' || EXCLUDED.id RETURNING id, note;
-- The pre-check scan returns a row with several keys once: given the row's
-- own TID, as logical replication gives it, it finds no conflict.
SELECT id, bark_multikey_precheck('uo_a', ctid) FROM uo WHERE id IN (2, 13)
ORDER BY id;
SELECT e, count(DISTINCT id) FROM uo, unnest(a) e WHERE e IS NOT NULL
GROUP BY e HAVING count(DISTINCT id) > 1;
SELECT bark_index_check('uo_a');

-- A btree arbiter beside a unique multikey index: ON CONFLICT (id) decides
-- on id alone, and the BARK index still raises its violation; with no
-- conflict target both indexes are arbiters.
CREATE TABLE ua (id int UNIQUE, a int4[]);
CREATE UNIQUE INDEX ua_a ON ua USING bark (a bark_int4_array_ops);
INSERT INTO ua VALUES (1, '{1,2}'), (2, '{3}');
INSERT INTO ua VALUES (1, '{4}') ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a;
INSERT INTO ua VALUES (3, '{2}') ON CONFLICT (id) DO NOTHING;
SELECT u_try('INSERT INTO ua VALUES (4, ''{3,9}'') ON CONFLICT (id) DO NOTHING');
SELECT u_try('INSERT INTO ua VALUES (4, ''{3,9}'') ON CONFLICT DO NOTHING');
SELECT u_try('INSERT INTO ua VALUES (2, ''{9}'') ON CONFLICT DO NOTHING');
SELECT id, a FROM ua ORDER BY id;
SELECT bark_index_check('ua_a');

-- A unique index needs the class's overlap operator (procedure 15); the
-- range class has none.  It may still build a non-unique index.
CREATE UNIQUE INDEX ON uo USING bark (a bark_int4_array_range_ops);
CREATE INDEX uo_range ON uo USING bark (a bark_int4_array_range_ops);
DROP INDEX uo_range;

-- A unique multikey index has no equality.  A foreign key passes it by and
-- finds a btree index on the same column; with none it fails as for a
-- table with no unique index.  The foreign key compares by equality.
CREATE TABLE fkp (a int4[]);
CREATE UNIQUE INDEX fkp_a ON fkp USING bark (a bark_int4_array_ops);
CREATE TABLE fkc (a int4[] REFERENCES fkp (a));
CREATE UNIQUE INDEX fkp_bt ON fkp (a);
CREATE TABLE fkc (a int4[] REFERENCES fkp (a));
SELECT conindid::regclass FROM pg_constraint WHERE conrelid = 'fkc'::regclass;
INSERT INTO fkp VALUES ('{1,2}');
INSERT INTO fkc VALUES ('{1,2}');
INSERT INTO fkc VALUES ('{1}');
DROP TABLE fkc, fkp;

-- REFRESH MATERIALIZED VIEW CONCURRENTLY joins on a unique index's
-- equality: a multikey index alone does not qualify, and beside a btree
-- one it is passed by.
CREATE MATERIALIZED VIEW umv AS SELECT id, a FROM uo;
CREATE UNIQUE INDEX umv_a ON umv USING bark (a bark_int4_array_ops);
REFRESH MATERIALIZED VIEW CONCURRENTLY umv;
CREATE UNIQUE INDEX umv_id ON umv (id);
UPDATE uo SET a = '{30,31}' WHERE id = 3;
DELETE FROM uo WHERE id = 12;
REFRESH MATERIALIZED VIEW CONCURRENTLY umv;
SELECT (SELECT count(*) FROM (TABLE umv EXCEPT ALL SELECT id, a FROM uo) x)
       AS mv_only,
       (SELECT count(*) FROM (SELECT id, a FROM uo EXCEPT ALL TABLE umv) y)
       AS table_only;
SELECT bark_index_check('umv_a');
DROP MATERIALIZED VIEW umv;

-- Replication looks a row up by equality on its identity's columns.
CREATE TABLE uri (a int4[] NOT NULL);
CREATE UNIQUE INDEX uri_a ON uri USING bark (a bark_int4_array_ops);
ALTER TABLE uri REPLICA IDENTITY USING INDEX uri_a;
DROP TABLE uri;

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

DROP TABLE ub, ut, um, uo, ua, uc, ubuild;

-- Procedure 15 that names a strategy with no operator under the column's
-- type, one with the wrong signature, and dropping it again: a unique index
-- is refused, amvalidate reports the signature, and the family loses the
-- member without losing its classes.
CREATE FUNCTION u_cmp_bad(int4) RETURNS int2 LANGUAGE sql IMMUTABLE
  AS 'SELECT 99::int2';
CREATE FUNCTION u_cmp_badsig(int8) RETURNS int2 LANGUAGE sql IMMUTABLE
  AS 'SELECT 1::int2';
ALTER OPERATOR FAMILY bark_int4_array_range_ops USING bark ADD
  FUNCTION 15 ("any", "any") u_cmp_bad(int4);
CREATE TABLE ur (a int4[]);
CREATE UNIQUE INDEX ur_a ON ur USING bark (a bark_int4_array_range_ops);
ALTER OPERATOR FAMILY bark_int4_array_range_ops USING bark
  DROP FUNCTION 15 ("any", "any");
ALTER OPERATOR FAMILY bark_int4_array_range_ops USING bark ADD
  FUNCTION 15 ("any", "any") u_cmp_badsig(int8);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bark_int4_array_range_ops';
ALTER OPERATOR FAMILY bark_int4_array_range_ops USING bark
  DROP FUNCTION 15 ("any", "any");
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bark_int4_array_range_ops';
-- Procedure 15 is looked up under ("any", "any") only; under other types
-- amvalidate reports it.
CREATE FUNCTION u_cmp_ok(int4) RETURNS int2 LANGUAGE sql IMMUTABLE
  AS 'SELECT 1::int2';
ALTER OPERATOR FAMILY bark_int4_array_range_ops USING bark ADD
  FUNCTION 15 (int4[], int4[]) u_cmp_ok(int4);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bark_int4_array_range_ops';
ALTER OPERATOR FAMILY bark_int4_array_range_ops USING bark
  DROP FUNCTION 15 (int4[], int4[]);
ALTER OPERATOR FAMILY bark_int4_array_range_ops USING bark ADD
  FUNCTION 15 ("any", int4[]) u_cmp_ok(int4);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bark_int4_array_range_ops';
ALTER OPERATOR FAMILY bark_int4_array_range_ops USING bark
  DROP FUNCTION 15 ("any", int4[]);
DROP FUNCTION u_cmp_ok(int4);
CREATE INDEX ur_a ON ur USING bark (a bark_int4_array_range_ops);
DROP TABLE ur;
DROP FUNCTION u_cmp_bad(int4), u_cmp_badsig(int8);
DROP FUNCTION u_try(text);
