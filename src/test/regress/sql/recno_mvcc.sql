--
-- Test RECNO MVCC: snapshot isolation, repeatable read, serializable
-- (Single-session tests; multi-session tests belong in isolation tests)
--

-- =============================================
-- Basic transaction visibility
-- =============================================

CREATE TABLE recno_mvcc_basic (
    id serial PRIMARY KEY,
    value integer
) USING recno;

-- Committed data is visible
INSERT INTO recno_mvcc_basic (value) VALUES (1);
SELECT value FROM recno_mvcc_basic;

-- Rolled-back data is not visible
BEGIN;
INSERT INTO recno_mvcc_basic (value) VALUES (2);
ROLLBACK;
SELECT COUNT(*) FROM recno_mvcc_basic;

-- Multiple operations in a transaction
BEGIN;
INSERT INTO recno_mvcc_basic (value) VALUES (10);
INSERT INTO recno_mvcc_basic (value) VALUES (20);
UPDATE recno_mvcc_basic SET value = value + 100 WHERE value = 1;
DELETE FROM recno_mvcc_basic WHERE value = 20;
COMMIT;

SELECT value FROM recno_mvcc_basic ORDER BY value;

DROP TABLE recno_mvcc_basic;

-- =============================================
-- Read Committed behavior
-- =============================================

CREATE TABLE recno_mvcc_rc (
    id serial PRIMARY KEY,
    status text DEFAULT 'active',
    counter integer DEFAULT 0
) USING recno;

INSERT INTO recno_mvcc_rc (status) VALUES ('active'), ('active'), ('active');

-- In READ COMMITTED, each statement sees the latest committed data
BEGIN ISOLATION LEVEL READ COMMITTED;

-- First read
SELECT COUNT(*) AS initial FROM recno_mvcc_rc WHERE status = 'active';

-- Self-visibility: changes within the same transaction are visible
UPDATE recno_mvcc_rc SET status = 'inactive' WHERE id = 1;
SELECT COUNT(*) AS after_update FROM recno_mvcc_rc WHERE status = 'active';

-- Multiple updates in same transaction
UPDATE recno_mvcc_rc SET counter = counter + 1;
UPDATE recno_mvcc_rc SET counter = counter + 1;
SELECT id, status, counter FROM recno_mvcc_rc ORDER BY id;

COMMIT;

-- Verify final state
SELECT id, status, counter FROM recno_mvcc_rc ORDER BY id;

DROP TABLE recno_mvcc_rc;

-- =============================================
-- Repeatable Read behavior
-- =============================================

CREATE TABLE recno_mvcc_rr (
    id serial PRIMARY KEY,
    value integer
) USING recno;

INSERT INTO recno_mvcc_rr (value) VALUES (100), (200), (300);

-- In REPEATABLE READ, the snapshot is taken at the first query
BEGIN ISOLATION LEVEL REPEATABLE READ;

-- Take snapshot
SELECT SUM(value) AS initial_sum FROM recno_mvcc_rr;

-- Self-modifications are visible
UPDATE recno_mvcc_rr SET value = value + 10;
SELECT SUM(value) AS after_self_update FROM recno_mvcc_rr;

-- Insert is visible within transaction
INSERT INTO recno_mvcc_rr (value) VALUES (400);
SELECT COUNT(*) AS count_with_insert FROM recno_mvcc_rr;

COMMIT;

-- Final state
SELECT id, value FROM recno_mvcc_rr ORDER BY id;

DROP TABLE recno_mvcc_rr;

-- =============================================
-- Serializable behavior
-- =============================================

CREATE TABLE recno_mvcc_ser (
    id serial PRIMARY KEY,
    category text,
    amount integer
) USING recno;

INSERT INTO recno_mvcc_ser (category, amount) VALUES
    ('A', 100), ('A', 200), ('B', 300), ('B', 400);

BEGIN ISOLATION LEVEL SERIALIZABLE;

-- Read aggregate
SELECT category, SUM(amount) AS total
FROM recno_mvcc_ser GROUP BY category ORDER BY category;

-- Modify based on read
UPDATE recno_mvcc_ser SET amount = amount + 10 WHERE category = 'A';

-- Re-read shows our changes
SELECT category, SUM(amount) AS total
FROM recno_mvcc_ser GROUP BY category ORDER BY category;

COMMIT;

DROP TABLE recno_mvcc_ser;

-- =============================================
-- Savepoints
-- =============================================

CREATE TABLE recno_mvcc_sp (
    id serial PRIMARY KEY,
    label text
) USING recno;

BEGIN;

INSERT INTO recno_mvcc_sp (label) VALUES ('before_sp1');

SAVEPOINT sp1;
INSERT INTO recno_mvcc_sp (label) VALUES ('in_sp1');

SAVEPOINT sp2;
INSERT INTO recno_mvcc_sp (label) VALUES ('in_sp2');

-- Rollback to sp2 (undoes 'in_sp2')
ROLLBACK TO sp2;
SELECT label FROM recno_mvcc_sp ORDER BY id;

-- Rollback to sp1 (undoes 'in_sp1')
ROLLBACK TO sp1;
SELECT label FROM recno_mvcc_sp ORDER BY id;

-- Continue after rollback to savepoint
INSERT INTO recno_mvcc_sp (label) VALUES ('after_rollback');

COMMIT;

SELECT label FROM recno_mvcc_sp ORDER BY id;

DROP TABLE recno_mvcc_sp;

-- =============================================
-- Nested savepoints
-- =============================================

CREATE TABLE recno_mvcc_nested (
    id serial PRIMARY KEY,
    step integer
) USING recno;

BEGIN;

INSERT INTO recno_mvcc_nested (step) VALUES (1);
SAVEPOINT a;

INSERT INTO recno_mvcc_nested (step) VALUES (2);
SAVEPOINT b;

INSERT INTO recno_mvcc_nested (step) VALUES (3);
SAVEPOINT c;

INSERT INTO recno_mvcc_nested (step) VALUES (4);

-- Rollback to middle savepoint
ROLLBACK TO b;

-- Only steps 1 and 2 should be visible
SELECT step FROM recno_mvcc_nested ORDER BY step;

-- Continue and commit
INSERT INTO recno_mvcc_nested (step) VALUES (5);
COMMIT;

SELECT step FROM recno_mvcc_nested ORDER BY step;

DROP TABLE recno_mvcc_nested;

-- =============================================
-- FOR UPDATE / FOR SHARE locking
-- =============================================

CREATE TABLE recno_mvcc_lock (
    id serial PRIMARY KEY,
    value integer
) USING recno;

INSERT INTO recno_mvcc_lock (value) VALUES (1), (2), (3);

-- SELECT FOR UPDATE
BEGIN;
SELECT * FROM recno_mvcc_lock WHERE id = 1 FOR UPDATE;
UPDATE recno_mvcc_lock SET value = 99 WHERE id = 1;
COMMIT;

SELECT value FROM recno_mvcc_lock WHERE id = 1;

-- SELECT FOR SHARE
BEGIN;
SELECT * FROM recno_mvcc_lock WHERE id = 2 FOR SHARE;
-- Can still read
SELECT value FROM recno_mvcc_lock WHERE id = 2;
COMMIT;

-- SELECT FOR UPDATE with subquery
BEGIN;
SELECT * FROM recno_mvcc_lock WHERE id IN (
    SELECT id FROM recno_mvcc_lock WHERE value > 1 ORDER BY id LIMIT 1
) FOR UPDATE;
COMMIT;

-- FOR UPDATE SKIP LOCKED
BEGIN;
SELECT * FROM recno_mvcc_lock ORDER BY id FOR UPDATE SKIP LOCKED;
COMMIT;

-- FOR UPDATE NOWAIT (should succeed since no other lockers)
BEGIN;
SELECT * FROM recno_mvcc_lock WHERE id = 3 FOR UPDATE NOWAIT;
COMMIT;

DROP TABLE recno_mvcc_lock;

-- =============================================
-- Cursor-based reads and MVCC
-- =============================================

CREATE TABLE recno_mvcc_cursor (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_mvcc_cursor (data)
SELECT 'row_' || i FROM generate_series(1, 100) i;

-- Cursor within transaction
BEGIN;
DECLARE cur CURSOR FOR SELECT * FROM recno_mvcc_cursor ORDER BY id;
FETCH 5 FROM cur;
FETCH 5 FROM cur;

-- Move to last
FETCH LAST FROM cur;
CLOSE cur;
COMMIT;

DROP TABLE recno_mvcc_cursor;

-- =============================================
-- Visibility after DELETE+INSERT (same PK)
-- =============================================

CREATE TABLE recno_mvcc_reuse (
    id integer PRIMARY KEY,
    version integer
) USING recno;

INSERT INTO recno_mvcc_reuse VALUES (1, 1);

-- Delete and re-insert same PK in one transaction
BEGIN;
DELETE FROM recno_mvcc_reuse WHERE id = 1;
INSERT INTO recno_mvcc_reuse VALUES (1, 2);
COMMIT;

SELECT * FROM recno_mvcc_reuse;

-- Verify only one row with version 2
SELECT COUNT(*) AS row_count, MAX(version) AS latest_version
FROM recno_mvcc_reuse WHERE id = 1;

DROP TABLE recno_mvcc_reuse;

-- =============================================
-- Command ID visibility within transactions
-- =============================================

CREATE TABLE recno_mvcc_cid (
    id serial PRIMARY KEY,
    label text,
    counter integer DEFAULT 0
) USING recno;

BEGIN;

-- CID 0: insert
INSERT INTO recno_mvcc_cid (label) VALUES ('first');

-- CID 1: insert
INSERT INTO recno_mvcc_cid (label) VALUES ('second');

-- CID 2: update first row
UPDATE recno_mvcc_cid SET counter = 1 WHERE label = 'first';

-- CID 3: delete second row
DELETE FROM recno_mvcc_cid WHERE label = 'second';

-- Current state within transaction
SELECT label, counter FROM recno_mvcc_cid ORDER BY id;

COMMIT;

-- Final committed state
SELECT label, counter FROM recno_mvcc_cid ORDER BY id;

DROP TABLE recno_mvcc_cid;

-- =============================================
-- MVCC with large (overflow) tuples
-- =============================================
-- Known issue: in-place UPDATE of overflow tuples does not
-- preserve the old overflow chain for ROLLBACK.  The old overflow
-- records are overwritten during the update, so rollback cannot
-- restore the original data.  This needs a design-level fix to
-- either defer overflow chain cleanup until commit, or copy-on-write
-- the old overflow chain before modifying.

CREATE TABLE recno_mvcc_overflow (
    id serial PRIMARY KEY,
    data text
) USING recno;
BEGIN;
INSERT INTO recno_mvcc_overflow (data) VALUES (repeat('T', 10000));
SELECT length(data) AS len FROM recno_mvcc_overflow;
COMMIT;
BEGIN;
UPDATE recno_mvcc_overflow SET data = repeat('U', 20000);
ROLLBACK;
DROP TABLE recno_mvcc_overflow;

-- =============================================
-- Transaction isolation with aggregates
-- =============================================

CREATE TABLE recno_mvcc_agg (
    id serial PRIMARY KEY,
    amount numeric(10,2)
) USING recno;

INSERT INTO recno_mvcc_agg (amount)
SELECT (i * 10.50)::numeric(10,2) FROM generate_series(1, 100) i;

-- Consistent read within a transaction
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT SUM(amount) AS sum1 FROM recno_mvcc_agg;

-- Self-modification
UPDATE recno_mvcc_agg SET amount = amount + 1 WHERE id <= 10;

-- Sum should reflect our change
SELECT SUM(amount) AS sum2 FROM recno_mvcc_agg;

COMMIT;

DROP TABLE recno_mvcc_agg;

-- =============================================
-- ON CONFLICT (UPSERT) MVCC behavior
-- =============================================
-- Speculative insertion (INSERT ... ON CONFLICT)
-- Previously crashed with Assert("TransactionIdIsValid(xid)") in
-- SpeculativeInsertionWait.  Fixed by recording the inserting xid
-- in recno_tuple_insert_speculative().
-- =============================================
CREATE TABLE recno_mvcc_upsert (
    id integer PRIMARY KEY,
    value text,
    update_count integer DEFAULT 0
) USING recno;
INSERT INTO recno_mvcc_upsert VALUES (1, 'initial', 0);
INSERT INTO recno_mvcc_upsert VALUES (1, 'conflict', 0)
ON CONFLICT (id) DO UPDATE SET value = 'upserted',
  update_count = recno_mvcc_upsert.update_count + 1;
SELECT * FROM recno_mvcc_upsert;
DROP TABLE recno_mvcc_upsert;

-- =============================================
-- RETURNING clause visibility
-- =============================================

CREATE TABLE recno_mvcc_returning (
    id serial PRIMARY KEY,
    value integer
) USING recno;

-- INSERT ... RETURNING
INSERT INTO recno_mvcc_returning (value) VALUES (42) RETURNING id, value;

-- UPDATE ... RETURNING
UPDATE recno_mvcc_returning SET value = 99 WHERE id = 1 RETURNING id, value;

-- DELETE ... RETURNING
DELETE FROM recno_mvcc_returning WHERE id = 1 RETURNING id, value;

-- Should be empty now
SELECT COUNT(*) FROM recno_mvcc_returning;

DROP TABLE recno_mvcc_returning;

-- =============================================
-- Transaction rollback with index updates
-- =============================================

CREATE TABLE recno_mvcc_idx (
    id serial PRIMARY KEY,
    val integer
) USING recno;

CREATE INDEX idx_mvcc_val ON recno_mvcc_idx (val);

INSERT INTO recno_mvcc_idx (val) VALUES (10), (20), (30);

-- Rollback should undo index updates too
BEGIN;
INSERT INTO recno_mvcc_idx (val) VALUES (40);
UPDATE recno_mvcc_idx SET val = 99 WHERE val = 10;
DELETE FROM recno_mvcc_idx WHERE val = 20;
ROLLBACK;

-- Original state should be preserved
SET enable_seqscan = off;
SELECT val FROM recno_mvcc_idx ORDER BY val;
RESET enable_seqscan;

-- Commit should persist index updates
BEGIN;
INSERT INTO recno_mvcc_idx (val) VALUES (40);
UPDATE recno_mvcc_idx SET val = 99 WHERE val = 10;
COMMIT;

SET enable_seqscan = off;
SELECT val FROM recno_mvcc_idx ORDER BY val;
RESET enable_seqscan;

DROP TABLE recno_mvcc_idx;

-- =============================================
-- Aborted transaction cleanup
-- =============================================

CREATE TABLE recno_mvcc_abort (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- Multiple aborted transactions should not leave visible garbage
BEGIN; INSERT INTO recno_mvcc_abort (data) VALUES ('abort1'); ROLLBACK;
BEGIN; INSERT INTO recno_mvcc_abort (data) VALUES ('abort2'); ROLLBACK;
BEGIN; INSERT INTO recno_mvcc_abort (data) VALUES ('abort3'); ROLLBACK;

SELECT COUNT(*) FROM recno_mvcc_abort;

-- Now commit one
INSERT INTO recno_mvcc_abort (data) VALUES ('committed');
SELECT data FROM recno_mvcc_abort;

-- VACUUM should handle aborted transaction tuples
VACUUM recno_mvcc_abort;
SELECT data FROM recno_mvcc_abort;

DROP TABLE recno_mvcc_abort;

-- =============================================
-- Mixed heap/recno transaction
-- =============================================

CREATE TABLE recno_mvcc_mixed_r (
    id serial PRIMARY KEY,
    val integer
) USING recno;

CREATE TABLE recno_mvcc_mixed_h (
    id serial PRIMARY KEY,
    val integer
) USING heap;

-- Transaction spanning both access methods
BEGIN;
INSERT INTO recno_mvcc_mixed_r (val) VALUES (1);
INSERT INTO recno_mvcc_mixed_h (val) VALUES (1);
UPDATE recno_mvcc_mixed_r SET val = 2;
UPDATE recno_mvcc_mixed_h SET val = 2;
COMMIT;

SELECT val FROM recno_mvcc_mixed_r;
SELECT val FROM recno_mvcc_mixed_h;

-- Rollback across both
BEGIN;
INSERT INTO recno_mvcc_mixed_r (val) VALUES (99);
INSERT INTO recno_mvcc_mixed_h (val) VALUES (99);
ROLLBACK;

SELECT COUNT(*) FROM recno_mvcc_mixed_r;
SELECT COUNT(*) FROM recno_mvcc_mixed_h;

DROP TABLE recno_mvcc_mixed_r;
DROP TABLE recno_mvcc_mixed_h;
