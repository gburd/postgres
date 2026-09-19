--
-- FLUX table access method: core functional + index-integrity tests.
--
-- The critical property is that
-- index scans, sequential scans, and bitmap scans agree after key-changing
-- UPDATEs, including A -> B -> A recurrences.  FLUX achieves this by doing a
-- non-in-place (new-TID) UPDATE whenever an indexed column changes, so
-- secondary indexes are maintained by the standard heap-TID path.
--

CREATE TABLE flux_basic (id int, k int, v text) USING flux;
INSERT INTO flux_basic SELECT g, g, 'v' || g FROM generate_series(1, 20) g;
CREATE INDEX flux_basic_k_idx ON flux_basic (k);

-- non-key UPDATE (in place): TID and index unchanged
UPDATE flux_basic SET v = 'updated' WHERE id = 5;
SELECT id, k, v FROM flux_basic WHERE id = 5;

-- key-changing UPDATEs (out of place): old index entry dies, new one inserted
UPDATE flux_basic SET k = 105 WHERE id = 5;
UPDATE flux_basic SET k = 106 WHERE id = 6;
-- A -> B -> A recurrence on an indexed key
UPDATE flux_basic SET k = 999 WHERE id = 7;
UPDATE flux_basic SET k = 7   WHERE id = 7;

DELETE FROM flux_basic WHERE id = 20;

-- The gate: idxscan == seqscan == bitmapscan.
SET enable_seqscan = on;  SET enable_indexscan = off; SET enable_bitmapscan = off;
SELECT count(*) AS seq_count, sum(k) AS seq_sumk FROM flux_basic;
SET enable_seqscan = off; SET enable_indexscan = on;  SET enable_bitmapscan = off;
SELECT count(*) AS idx_count, sum(k) AS idx_sumk FROM flux_basic WHERE k > -2147483648;
SELECT count(*) AS idx_k_eq7   FROM flux_basic WHERE k = 7;    -- 1, not 2
SELECT count(*) AS idx_k_eq5   FROM flux_basic WHERE k = 5;    -- 0 (moved to 105)
SELECT count(*) AS idx_k_eq105 FROM flux_basic WHERE k = 105;  -- 1
SET enable_seqscan = off; SET enable_indexscan = off; SET enable_bitmapscan = on;
SELECT count(*) AS bmp_count, sum(k) AS bmp_sumk FROM flux_basic WHERE k > -2147483648;
RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan;

-- VACUUM then amcheck (heapallindexed): no error means the index is consistent
-- with the heap after key churn.
VACUUM flux_basic;
CREATE EXTENSION IF NOT EXISTS amcheck;
SELECT bt_index_check('flux_basic_k_idx'::regclass, true);

-- ROLLBACK restores the old value (non-key) and old key + index entry (key).
BEGIN;
UPDATE flux_basic SET v = 'should_not_persist' WHERE id = 10;
ROLLBACK;
SELECT v FROM flux_basic WHERE id = 10;

BEGIN;
UPDATE flux_basic SET k = 5000 WHERE id = 11;
ROLLBACK;
SELECT k FROM flux_basic WHERE id = 11;
SET enable_seqscan = off; SET enable_indexscan = on; SET enable_bitmapscan = off;
SELECT count(*) AS idx_k_eq11   FROM flux_basic WHERE k = 11;    -- 1
SELECT count(*) AS idx_k_eq5000 FROM flux_basic WHERE k = 5000;  -- 0
RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan;

-- TOAST: a >8KB value round-trips exactly and the relation gets a TOAST table.
CREATE TABLE flux_toast (id int, big text) USING flux;
INSERT INTO flux_toast VALUES (1, repeat('X', 100000));
SELECT id, length(big) AS len, (big = repeat('X', 100000)) AS exact FROM flux_toast;
SELECT reltoastrelid <> 0 AS has_toast_table FROM pg_class WHERE relname = 'flux_toast';

DROP TABLE flux_basic;
DROP TABLE flux_toast;

--
-- BUG 2 regression: an in-place UPDATE of a NON-indexed column that GROWS the
-- row on a full page must NOT error with "does not fit on page".  FLUX falls
-- back to an out-of-place (new-TID) update and rebuilds all index entries, so
-- the row survives and every index stays consistent with the heap.
--
CREATE TABLE flux_grow (id int, k int, pad text, v text) USING flux
  WITH (fillfactor = 100);
CREATE INDEX flux_grow_k_idx ON flux_grow (k);
-- Pack pages tight: many rows with a NULL v (narrowest) so the page is full
-- of committed tuples with no free space and no slack for in-place growth.
INSERT INTO flux_grow SELECT g, g, repeat('p', 200), NULL
  FROM generate_series(1, 2000) g;
VACUUM flux_grow;
-- Grow a NON-indexed column (v: NULL -> long string) on rows across full
-- pages.  Pre-fix this raised: ERROR updated flux tuple does not fit on page.
UPDATE flux_grow SET v = repeat('w', 400) WHERE id <= 2000;
-- All rows survived with the grown value.
SELECT count(*) AS grown, count(*) FILTER (WHERE v = repeat('w', 400)) AS correct
  FROM flux_grow;
-- The non-indexed grow may have moved TIDs; every index must still agree with
-- the heap (idxscan == seqscan) and amcheck must pass.
VACUUM flux_grow;
SELECT bt_index_check('flux_grow_k_idx'::regclass, true);
SET enable_seqscan = on;  SET enable_indexscan = off; SET enable_bitmapscan = off;
SELECT count(*) AS seq_count, sum(k) AS seq_sumk FROM flux_grow;
SET enable_seqscan = off; SET enable_indexscan = on; SET enable_bitmapscan = off;
SELECT count(*) AS idx_count, sum(k) AS idx_sumk FROM flux_grow;
RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan;
-- A specific indexed lookup after the TID-moving grow returns exactly one row.
SELECT id, k, length(v) AS vlen FROM flux_grow WHERE k = 1000;
DROP TABLE flux_grow;

--
-- BUG 7/8 regression: SAVEPOINT interaction with in-place UPDATE.
-- (7) Re-UPDATE inside a savepoint a row already updated by an earlier command
--     must NOT self-wait/assert.  (8) ROLLBACK TO SAVEPOINT must RESTORE the
--     pre-savepoint value of in-place updates (and undo in-savepoint deletes),
--     not vanish the row or over-revert to the original committed value.
--
CREATE TABLE flux_sp (id int PRIMARY KEY, v int) USING flux;
INSERT INTO flux_sp VALUES (1,100),(2,200),(3,300);
BEGIN;
UPDATE flux_sp SET v = 101 WHERE id = 1;   -- before savepoint
SAVEPOINT s1;
UPDATE flux_sp SET v = 999 WHERE id = 1;   -- re-update same row (bug 7)
DELETE FROM flux_sp WHERE id = 2;          -- in-savepoint delete
UPDATE flux_sp SET v = 301 WHERE id = 3;   -- in-savepoint update
ROLLBACK TO s1;                             -- must restore pre-savepoint state
-- Expect 1|101 (pre-savepoint update kept), 2|200 (delete undone), 3|300 (update undone)
SELECT id, v FROM flux_sp ORDER BY id;
COMMIT;
SELECT id, v FROM flux_sp ORDER BY id;
DROP TABLE flux_sp;

--
-- Phase 8c: ALWAYS-IN-PLACE indexed-column UPDATE via delete-marking.
-- An UPDATE of an indexed column keeps the STABLE TID (ctid unchanged), the
-- index sees the new key (old key delete-marked), unique is respected, and
-- ROLLBACK of an in-place key update restores the old key + removes the new
-- index entry.  amcheck stays clean.
--
CREATE TABLE flux_ip (id int PRIMARY KEY, k int, v text) USING flux;
INSERT INTO flux_ip SELECT g, g, 'v'||g FROM generate_series(1,10) g;
CREATE INDEX flux_ip_k ON flux_ip (k);
CREATE UNIQUE INDEX flux_ip_uk ON flux_ip (k);
-- (a) STABLE TID: ctid identical before and after an indexed-column UPDATE.
SELECT ctid AS ip_before FROM flux_ip WHERE id = 3;
UPDATE flux_ip SET k = 333 WHERE id = 3;
SELECT ctid AS ip_after, k FROM flux_ip WHERE id = 3;   -- same ctid, k=333
-- index scan sees new key, not old; idxscan == seqscan (no duplicate rows).
SET enable_seqscan = off; SET enable_indexscan = on; SET enable_bitmapscan = off;
SELECT count(*) AS ip_k3   FROM flux_ip WHERE k = 3;    -- 0 (moved)
SELECT count(*) AS ip_k333 FROM flux_ip WHERE k = 333;  -- 1
SELECT count(*) AS ip_idx_total, sum(k) AS ip_idx_sumk FROM flux_ip WHERE k > -2147483648;
RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan;
SELECT count(*) AS ip_seq_total, sum(k) AS ip_seq_sumk FROM flux_ip;  -- must match idx
-- (d) UNIQUE respected: another row cannot take the new key.
INSERT INTO flux_ip VALUES (11, 11, 'x');
UPDATE flux_ip SET k = 333 WHERE id = 11;   -- ERROR: duplicate key
-- (c) ROLLBACK of an in-place key UPDATE restores old key and removes new entry.
SELECT ctid AS ip_roll_before FROM flux_ip WHERE id = 4;
BEGIN;
UPDATE flux_ip SET k = 4444 WHERE id = 4;
SELECT ctid AS ip_roll_intxn, k FROM flux_ip WHERE id = 4;   -- same ctid, k=4444
ROLLBACK;
SELECT ctid AS ip_roll_after, k FROM flux_ip WHERE id = 4;   -- same ctid, k=4 restored
SET enable_seqscan = off; SET enable_indexscan = on; SET enable_bitmapscan = off;
SELECT count(*) AS ip_roll_k4    FROM flux_ip WHERE k = 4;     -- 1 (restored)
SELECT count(*) AS ip_roll_k4444 FROM flux_ip WHERE k = 4444;  -- 0 (new entry gone)
RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan;
-- Unique still works after rollback: k=4444 free to insert, k=4 taken.
INSERT INTO flux_ip VALUES (12, 4444, 'y');   -- ok
INSERT INTO flux_ip VALUES (13, 4, 'z');       -- ERROR: duplicate key (4 live)
-- (e) amcheck clean on both indexes carrying delete-marks.
SELECT bt_index_check('flux_ip_k'::regclass, true);
SELECT bt_index_check('flux_ip_uk'::regclass, true);
DROP TABLE flux_ip;
