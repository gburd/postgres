-- Test the executor's use of amcomparedatums on UPDATE: a column that an
-- index stores directly does not block HOT if the index's access method
-- says the new value gives the same entries.
CREATE EXTENSION dummy_index_am;
CREATE TABLE dummy_cmp (id int, a int, b int) WITH (fillfactor = 50);
INSERT INTO dummy_cmp SELECT g, g, g FROM generate_series(1, 10) g;
-- With option_real = 1, dummy_index_am says int4 values equal modulo
-- option_int give the same entries.  A btree index on another column, and
-- no index on b.
CREATE INDEX dummy_cmp_a ON dummy_cmp USING dummy_index_am (a)
  WITH (option_real = 1, option_int = 10);
CREATE INDEX dummy_cmp_id ON dummy_cmp (id);
CREATE FUNCTION dummy_cmp_hot(OUT updated bigint, OUT hot bigint)
LANGUAGE sql AS $$
  SELECT pg_stat_get_xact_tuples_updated('dummy_cmp'::regclass),
         pg_stat_get_xact_tuples_hot_updated('dummy_cmp'::regclass) $$;
-- count from zero: flush what earlier transactions counted
SELECT pg_stat_force_next_flush();
BEGIN;
-- a + 10 is equal modulo 10: HOT
UPDATE dummy_cmp SET a = a + 10;
SELECT * FROM dummy_cmp_hot();
-- a + 1 is not: not HOT
UPDATE dummy_cmp SET a = a + 1;
SELECT * FROM dummy_cmp_hot();
-- b is not indexed: HOT
UPDATE dummy_cmp SET b = b + 1;
SELECT * FROM dummy_cmp_hot();
-- id is in a btree index, which cannot compare: not HOT, even though a
-- changes in a way dummy_index_am accepts
UPDATE dummy_cmp SET a = a + 10, id = id + 100;
SELECT * FROM dummy_cmp_hot();
-- to and from null always needs new entries
UPDATE dummy_cmp SET a = NULL WHERE id = 101;
UPDATE dummy_cmp SET a = 2 WHERE id = 101;
SELECT * FROM dummy_cmp_hot();
COMMIT;
-- A column also used by an index with no amcomparedatums is decided as
-- before.
CREATE INDEX dummy_cmp_a2 ON dummy_cmp (a);
-- count from zero: flush what earlier transactions counted
SELECT pg_stat_force_next_flush();
BEGIN;
UPDATE dummy_cmp SET a = a + 10;
SELECT * FROM dummy_cmp_hot();
COMMIT;
DROP INDEX dummy_cmp_a2;
-- The columns of a unique index that a foreign key could reference are
-- never compared this way, nor are those of the replica identity.
CREATE UNIQUE INDEX dummy_cmp_id_u ON dummy_cmp (id);
CREATE INDEX dummy_cmp_id_d ON dummy_cmp USING dummy_index_am (id)
  WITH (option_real = 1, option_int = 100);
-- count from zero: flush what earlier transactions counted
SELECT pg_stat_force_next_flush();
BEGIN;
UPDATE dummy_cmp SET id = id + 100;
SELECT * FROM dummy_cmp_hot();
COMMIT;
DROP TABLE dummy_cmp;
DROP FUNCTION dummy_cmp_hot;
DROP EXTENSION dummy_index_am;
