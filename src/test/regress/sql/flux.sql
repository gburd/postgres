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
