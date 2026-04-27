--
-- Test multi-step SIU (Selective Index Update) chain correctness
--
-- These tests verify that stale index entries from multi-step SIU chains
-- where consecutive updates modify *different* indexed columns are correctly
-- skipped during index scans, preventing wrong results.
--
-- All SELECT queries that verify SIU correctness are run with seqscan
-- disabled to force index scans, since small test tables would otherwise
-- use sequential scans and bypass the SIU chain-following logic entirely.
--

-- Test 1: Basic multi-column SIU chain
-- Two consecutive updates modify different indexed columns.
-- Stale entries for old values must NOT return the tuple.
CREATE TABLE siu_test (
    id int PRIMARY KEY,
    a int,
    b int
) WITH (fillfactor = 50);

CREATE INDEX siu_idx_a ON siu_test (a);
CREATE INDEX siu_idx_b ON siu_test (b);

INSERT INTO siu_test VALUES (1, 10, 20);

-- First update: change column a (SIU: bitmap {a})
UPDATE siu_test SET a = 11 WHERE id = 1;
-- Second update: change column b (SIU: bitmap {b})
UPDATE siu_test SET b = 21 WHERE id = 1;

-- Force index scans for SIU verification
SET enable_seqscan = off;

-- Verify plan uses index scan
EXPLAIN (COSTS OFF) SELECT * FROM siu_test WHERE a = 10;

-- Stale entries for old values should return 0 rows
SELECT * FROM siu_test WHERE a = 10;
SELECT * FROM siu_test WHERE b = 20;

-- Fresh entries for current values should return 1 row
SELECT * FROM siu_test WHERE a = 11;
SELECT * FROM siu_test WHERE b = 21;

-- Combined queries should work correctly.
-- Disable bitmap scans: BitmapAnd of two index scans can return 0 rows
-- because the fresh SIU entries for a=11 and b=21 point to different TIDs.
-- An index scan or seq scan avoids this limitation.
SET enable_bitmapscan = off;
SELECT * FROM siu_test WHERE a = 11 AND b = 21;
SELECT * FROM siu_test WHERE a = 10 AND b = 20;
RESET enable_bitmapscan;

RESET enable_seqscan;


-- Test 2: Three-step SIU chain with different columns
CREATE TABLE siu_three (
    id int PRIMARY KEY,
    x int,
    y int,
    z int
) WITH (fillfactor = 50);

CREATE INDEX siu_three_x ON siu_three (x);
CREATE INDEX siu_three_y ON siu_three (y);
CREATE INDEX siu_three_z ON siu_three (z);

INSERT INTO siu_three VALUES (1, 100, 200, 300);

UPDATE siu_three SET x = 101 WHERE id = 1;
UPDATE siu_three SET y = 201 WHERE id = 1;
UPDATE siu_three SET z = 301 WHERE id = 1;

SET enable_seqscan = off;

-- Verify plan uses index scan
EXPLAIN (COSTS OFF) SELECT * FROM siu_three WHERE x = 100;

-- All old values should return 0 rows
SELECT * FROM siu_three WHERE x = 100;
SELECT * FROM siu_three WHERE y = 200;
SELECT * FROM siu_three WHERE z = 300;

-- All new values should return 1 row
SELECT * FROM siu_three WHERE x = 101;
SELECT * FROM siu_three WHERE y = 201;
SELECT * FROM siu_three WHERE z = 301;

RESET enable_seqscan;


-- Test 3: SIU chain where value is changed and changed back
-- This exercises the changeback path: the accumulated bitmap says the
-- column was modified, so the stale entry at the original TID is skipped.
-- But the fresh entry (pointing to the latest tuple) finds val=42 and
-- returns it correctly.
CREATE TABLE siu_changeback (
    id int PRIMARY KEY,
    val int
) WITH (fillfactor = 50);

CREATE INDEX siu_cb_val ON siu_changeback (val);

INSERT INTO siu_changeback VALUES (1, 42);

-- Change val to something else, then back to original
UPDATE siu_changeback SET val = 99 WHERE id = 1;
UPDATE siu_changeback SET val = 42 WHERE id = 1;

SET enable_seqscan = off;

-- Verify plan uses index scan
EXPLAIN (COSTS OFF) SELECT * FROM siu_changeback WHERE val = 42;

-- The stale entry for val=42 (at old TID) should still find the row,
-- because after recheck, the visible tuple actually has val=42.
SELECT * FROM siu_changeback WHERE val = 42;

-- The intermediate value should return 0 rows
SELECT * FROM siu_changeback WHERE val = 99;

RESET enable_seqscan;


-- Test 4: Multiple rows with SIU chains
CREATE TABLE siu_multi (
    id int PRIMARY KEY,
    a int,
    b int
) WITH (fillfactor = 50);

CREATE INDEX siu_multi_a ON siu_multi (a);
CREATE INDEX siu_multi_b ON siu_multi (b);

INSERT INTO siu_multi VALUES (1, 10, 20), (2, 30, 40), (3, 50, 60);

UPDATE siu_multi SET a = 11 WHERE id = 1;
UPDATE siu_multi SET b = 21 WHERE id = 1;
UPDATE siu_multi SET a = 31 WHERE id = 2;
UPDATE siu_multi SET b = 41 WHERE id = 2;
-- Row 3 is not updated

SET enable_seqscan = off;

-- Verify plan uses index scan
EXPLAIN (COSTS OFF) SELECT * FROM siu_multi WHERE a = 10 ORDER BY id;

-- Old values should not match
SELECT * FROM siu_multi WHERE a = 10 ORDER BY id;
SELECT * FROM siu_multi WHERE b = 20 ORDER BY id;
SELECT * FROM siu_multi WHERE a = 30 ORDER BY id;
SELECT * FROM siu_multi WHERE b = 40 ORDER BY id;

-- New values should match
SELECT * FROM siu_multi WHERE a = 11 ORDER BY id;
SELECT * FROM siu_multi WHERE b = 21 ORDER BY id;
SELECT * FROM siu_multi WHERE a = 31 ORDER BY id;
SELECT * FROM siu_multi WHERE b = 41 ORDER BY id;

-- Unchanged row should still be found
SELECT * FROM siu_multi WHERE a = 50 ORDER BY id;
SELECT * FROM siu_multi WHERE b = 60 ORDER BY id;

RESET enable_seqscan;


-- Test 5: SIU chain correctness persists after VACUUM
-- VACUUM prunes dead tuples; the bitmap consolidation during pruning
-- must preserve SIU information.
CREATE TABLE siu_vacuum (
    id int PRIMARY KEY,
    a int,
    b int
) WITH (fillfactor = 50, autovacuum_enabled = off);

CREATE INDEX siu_vac_a ON siu_vacuum (a);
CREATE INDEX siu_vac_b ON siu_vacuum (b);

INSERT INTO siu_vacuum VALUES (1, 10, 20);

UPDATE siu_vacuum SET a = 11 WHERE id = 1;
UPDATE siu_vacuum SET b = 21 WHERE id = 1;

-- Verify before vacuum
SET enable_seqscan = off;

EXPLAIN (COSTS OFF) SELECT * FROM siu_vacuum WHERE a = 10;

SELECT * FROM siu_vacuum WHERE a = 10;
SELECT * FROM siu_vacuum WHERE b = 20;
-- Disable bitmap scans for AND query (see comment in Test 1)
SET enable_bitmapscan = off;
SELECT * FROM siu_vacuum WHERE a = 11 AND b = 21;
RESET enable_bitmapscan;

RESET enable_seqscan;

-- Prune dead tuples
VACUUM siu_vacuum;

-- Verify after vacuum
SET enable_seqscan = off;

SELECT * FROM siu_vacuum WHERE a = 10;
SELECT * FROM siu_vacuum WHERE b = 20;
SELECT * FROM siu_vacuum WHERE a = 11 AND b = 21;

RESET enable_seqscan;


-- Test 6: Unique index with SIU chains
-- Verify uniqueness constraints work correctly with multi-column SIU.
CREATE TABLE siu_unique (
    id int PRIMARY KEY,
    uk int UNIQUE,
    other int
) WITH (fillfactor = 50);

CREATE INDEX siu_uniq_other ON siu_unique (other);

INSERT INTO siu_unique VALUES (1, 100, 200);

-- Update non-unique indexed column (SIU)
UPDATE siu_unique SET other = 201 WHERE id = 1;

-- Update unique column (SIU)
UPDATE siu_unique SET uk = 101 WHERE id = 1;

-- This insert should succeed: old uk=100 is stale
INSERT INTO siu_unique VALUES (2, 100, 300);

-- This insert should fail: uk=101 is the current value
INSERT INTO siu_unique VALUES (3, 101, 400);

-- Verify final state
SELECT * FROM siu_unique ORDER BY id;


-- Cleanup
DROP TABLE siu_test CASCADE;
DROP TABLE siu_three CASCADE;
DROP TABLE siu_changeback CASCADE;
DROP TABLE siu_multi CASCADE;
DROP TABLE siu_vacuum CASCADE;
DROP TABLE siu_unique CASCADE;
