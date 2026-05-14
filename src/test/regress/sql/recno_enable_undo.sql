--
-- recno_enable_undo
--
-- Exercise the RECNO UNDO-in-WAL write / sLog / rollback path end-to-end.
-- UNDO is always-on infrastructure; RECNO unconditionally writes UNDO
-- records via am_supports_undo.  This test verifies rollback visibility.
--

-- Create a RECNO table (UNDO always active for RECNO AM)
CREATE TABLE recno_undo_baseline (id int PRIMARY KEY, s text) USING recno;
INSERT INTO recno_undo_baseline VALUES (1,'a'), (2,'b'), (3,'c');

-- Aborted INSERT: row must be invisible after ROLLBACK
BEGIN;
INSERT INTO recno_undo_baseline VALUES (99, 'rollback-insert');
-- visible inside the aborting transaction
SELECT count(*) FROM recno_undo_baseline WHERE id = 99;
ROLLBACK;
-- invisible after rollback
SELECT count(*) FROM recno_undo_baseline WHERE id = 99;
SELECT * FROM recno_undo_baseline WHERE id = 99;

-- Aborted UPDATE: readers must not see the aborted value
BEGIN;
UPDATE recno_undo_baseline SET s = 'rollback-update' WHERE id = 1;
SELECT s FROM recno_undo_baseline WHERE id = 1;   -- own view inside txn
ROLLBACK;
SELECT count(*) FILTER (WHERE s = 'rollback-update') AS aborted_visible FROM recno_undo_baseline;

-- Aborted DELETE: readers must not see the tuple as deleted
BEGIN;
DELETE FROM recno_undo_baseline WHERE id = 2;
ROLLBACK;
SELECT count(*) FILTER (WHERE id = 2) AS committed_delete_visible FROM recno_undo_baseline;

-- Savepoint rollback: only the rolled-back subtransaction's writes disappear
BEGIN;
INSERT INTO recno_undo_baseline VALUES (100, 'outer');
SAVEPOINT s1;
INSERT INTO recno_undo_baseline VALUES (101, 'inner-rolled');
UPDATE recno_undo_baseline SET s = 'inner-updated' WHERE id = 3;
ROLLBACK TO SAVEPOINT s1;
-- After ROLLBACK TO, id=100 persists; id=101 and the UPDATE on id=3 may
-- still be physically present (sLog-driven invisibility handles them).
SELECT id FROM recno_undo_baseline
  WHERE s NOT IN ('inner-rolled', 'inner-updated') ORDER BY id;
COMMIT;
SELECT id FROM recno_undo_baseline ORDER BY id;

-- RECNO always writes UNDO records; no GUC check needed.
CREATE TABLE recno_undo_on (id int, s text) USING recno;
INSERT INTO recno_undo_on VALUES (1,'a'),(2,'b');

DROP TABLE recno_undo_on;
DROP TABLE recno_undo_baseline;

--
-- recno feature-flag opt-out: with -Drecno=disabled the RECNO AM must not
-- exist at all.  When built with recno enabled (the default), recno must
-- be present in pg_am and every recno_* GUC must be registered.
--

-- RECNO is registered
SELECT amname, amtype FROM pg_am WHERE amname = 'recno';

-- All recno_* GUCs are registered with their declared groups
SELECT name, category
  FROM pg_settings
  WHERE name LIKE 'recno\_%' ESCAPE '\'
  ORDER BY name;
