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

-- Eager subtransaction rollback with a COMMITTING parent.
--
-- This is the case that pure deferral cannot handle: when a subtransaction
-- aborts but the parent COMMITS, no top-level ATM entry is ever created, so
-- deferred UNDO would never run and the aborted subtransaction's writes would
-- leak as committed.  AtSubAbort_XactUndo applies the subtransaction's UNDO
-- eagerly (bounded to the subtransaction's own batches), so the rows below
-- must be absent after the parent commits while the parent's own writes
-- survive.
BEGIN;
INSERT INTO recno_undo_baseline VALUES (200, 'parent-keep');
SAVEPOINT sub;
INSERT INTO recno_undo_baseline VALUES (201, 'sub-insert-gone');
UPDATE recno_undo_baseline SET s = 'sub-update-gone' WHERE id = 1;
DELETE FROM recno_undo_baseline WHERE id = 2;
ROLLBACK TO SAVEPOINT sub;
-- parent continues and commits its own write
INSERT INTO recno_undo_baseline VALUES (202, 'parent-keep-2');
COMMIT;
-- 201 must be gone; 1 restored to 'a'; 2 restored; 200 and 202 present
SELECT id, s FROM recno_undo_baseline WHERE id IN (1, 2, 200, 201, 202)
  ORDER BY id;
SELECT count(*) FILTER (WHERE s LIKE 'sub-%-gone') AS leaked_subxact_writes
  FROM recno_undo_baseline;
-- restore baseline for the feature-flag checks below
DELETE FROM recno_undo_baseline WHERE id IN (200, 202);

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

--
-- 2PC for RECNO: a transaction that touched a RECNO table generates
-- per-relation UNDO, which is now serialized into the 2PC state file by
-- AtPrepare_Recno() (RECNO_2PC_RELUNDO records) and replayed by
-- recno_twophase_postabort() via RelUndoApplyChain() on ROLLBACK PREPARED.
-- COMMIT PREPARED keeps the change.  Skips cleanly when the build's
-- max_prepared_transactions is 0 (the default), matching
-- prepared_xacts.sql's pattern -- but the regress test config sets it to 2,
-- so this runs for real under meson test.
--
SELECT current_setting('max_prepared_transactions')::integer < 1 AS skip_2pc_test \gset
\if :skip_2pc_test
\quit
\endif

CREATE TABLE recno_2pc (id int, s text) USING recno;
INSERT INTO recno_2pc VALUES (1, 'a');

-- ROLLBACK PREPARED reverts the in-place UPDATE.
BEGIN;
UPDATE recno_2pc SET s = 'b' WHERE id = 1;
PREPARE TRANSACTION 'recno_2pc_rb';
ROLLBACK PREPARED 'recno_2pc_rb';
SELECT count(*) FROM pg_prepared_xacts WHERE gid = 'recno_2pc_rb';
SELECT s FROM recno_2pc WHERE id = 1;

-- COMMIT PREPARED keeps the in-place UPDATE.
BEGIN;
UPDATE recno_2pc SET s = 'c' WHERE id = 1;
PREPARE TRANSACTION 'recno_2pc_cp';
COMMIT PREPARED 'recno_2pc_cp';
SELECT s FROM recno_2pc WHERE id = 1;

-- A plain (non-UNDO-generating) transaction must still be able to PREPARE.
CREATE TABLE heap_2pc_ok (id int) USING heap;
BEGIN;
INSERT INTO heap_2pc_ok VALUES (1);
PREPARE TRANSACTION 'heap_2pc_ok_txn';
COMMIT PREPARED 'heap_2pc_ok_txn';
SELECT * FROM heap_2pc_ok;

DROP TABLE recno_2pc;
DROP TABLE heap_2pc_ok;
