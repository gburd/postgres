# RECNO row locking test
#
# Tests tuple-level locking behavior in RECNO tables with
# SELECT FOR UPDATE, FOR SHARE, FOR NO KEY UPDATE, FOR KEY SHARE,
# and related locking operations.
#
# This exercises RECNO's tuple locking implementation which must
# correctly manage lock modes, blocking, NOWAIT, SKIP LOCKED,
# and lock release on COMMIT/ROLLBACK.

setup
{
  CREATE TABLE recno_lock_test (id int PRIMARY KEY, val text, amount int) USING recno;
  INSERT INTO recno_lock_test VALUES (1, 'row1', 100);
  INSERT INTO recno_lock_test VALUES (2, 'row2', 200);
  INSERT INTO recno_lock_test VALUES (3, 'row3', 300);
  INSERT INTO recno_lock_test VALUES (4, 'row4', 400);
  INSERT INTO recno_lock_test VALUES (5, 'row5', 500);
}

teardown
{
  DROP TABLE recno_lock_test;
}

session s1
setup { BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1_lock_upd     { SELECT * FROM recno_lock_test WHERE id = 1 FOR UPDATE; }
step s1_lock_share   { SELECT * FROM recno_lock_test WHERE id = 1 FOR SHARE; }
step s1_lock_nokeyup { SELECT * FROM recno_lock_test WHERE id = 1 FOR NO KEY UPDATE; }
step s1_lock_keysh   { SELECT * FROM recno_lock_test WHERE id = 1 FOR KEY SHARE; }
step s1_lock_r2      { SELECT * FROM recno_lock_test WHERE id = 2 FOR UPDATE; }
step s1_update       { UPDATE recno_lock_test SET amount = amount + 50 WHERE id = 1; }
step s1_update_key   { UPDATE recno_lock_test SET id = 11 WHERE id = 1; }
step s1_delete       { DELETE FROM recno_lock_test WHERE id = 1; }
step s1_select       { SELECT * FROM recno_lock_test ORDER BY id; }
step s1_commit       { COMMIT; }
step s1_rollback     { ROLLBACK; }

session s2
setup { BEGIN ISOLATION LEVEL READ COMMITTED; }
step s2_lock_upd     { SELECT * FROM recno_lock_test WHERE id = 1 FOR UPDATE; }
step s2_lock_share   { SELECT * FROM recno_lock_test WHERE id = 1 FOR SHARE; }
step s2_lock_nokeyup { SELECT * FROM recno_lock_test WHERE id = 1 FOR NO KEY UPDATE; }
step s2_lock_keysh   { SELECT * FROM recno_lock_test WHERE id = 1 FOR KEY SHARE; }
step s2_lock_r2      { SELECT * FROM recno_lock_test WHERE id = 2 FOR UPDATE; }
step s2_update       { UPDATE recno_lock_test SET amount = amount + 100 WHERE id = 1; }
step s2_select       { SELECT * FROM recno_lock_test ORDER BY id; }
step s2_commit       { COMMIT; }
step s2_rollback     { ROLLBACK; }

session s3
setup { BEGIN ISOLATION LEVEL READ COMMITTED; }
step s3_lock_nowait  { SELECT * FROM recno_lock_test WHERE id = 1 FOR UPDATE NOWAIT; }
step s3_lock_skip    { SELECT * FROM recno_lock_test ORDER BY id FOR UPDATE SKIP LOCKED; }
step s3_update_other { UPDATE recno_lock_test SET amount = amount + 25 WHERE id = 3; }
step s3_select       { SELECT * FROM recno_lock_test ORDER BY id; }
step s3_commit       { COMMIT; }

# ============================================================
# FOR UPDATE tests
# ============================================================

# Test 1: Basic FOR UPDATE blocking - s2 should block on s1's lock
permutation s1_lock_upd s2_lock_upd s1_commit s2_commit s3_select s3_commit

# Test 2: FOR UPDATE with actual update
permutation s1_lock_upd s1_update s2_update s1_commit s2_commit s3_select s3_commit

# Test 3: FOR UPDATE on different rows - no blocking
permutation s1_lock_upd s2_lock_r2 s1_commit s2_commit

# ============================================================
# FOR SHARE tests
# ============================================================

# Test 4: FOR SHARE compatibility - multiple sessions can hold FOR SHARE
permutation s1_lock_share s2_lock_share s1_commit s2_commit

# Test 5: FOR SHARE blocks FOR UPDATE
permutation s1_lock_share s2_lock_upd s1_commit s2_commit

# Test 6: FOR UPDATE blocks FOR SHARE
permutation s1_lock_upd s2_lock_share s1_commit s2_commit

# ============================================================
# FOR NO KEY UPDATE tests
# ============================================================

# Test 7: FOR NO KEY UPDATE blocks FOR UPDATE
permutation s1_lock_nokeyup s2_lock_upd s1_commit s2_commit

# Test 8: FOR NO KEY UPDATE blocks another FOR NO KEY UPDATE
permutation s1_lock_nokeyup s2_lock_nokeyup s1_commit s2_commit

# Test 9: FOR NO KEY UPDATE allows FOR KEY SHARE
permutation s1_lock_nokeyup s2_lock_keysh s1_commit s2_commit

# ============================================================
# FOR KEY SHARE tests
# ============================================================

# Test 10: FOR KEY SHARE allows FOR SHARE
permutation s1_lock_keysh s2_lock_share s1_commit s2_commit

# Test 11: FOR KEY SHARE allows FOR KEY SHARE
permutation s1_lock_keysh s2_lock_keysh s1_commit s2_commit

# Test 12: FOR KEY SHARE blocks FOR UPDATE
permutation s1_lock_keysh s2_lock_upd s1_commit s2_commit

# ============================================================
# NOWAIT and SKIP LOCKED tests
# ============================================================

# Test 13: NOWAIT fails immediately if lock not available
permutation s1_lock_upd s3_lock_nowait s1_commit s3_commit

# Test 14: SKIP LOCKED skips locked rows and returns unlocked ones
permutation s1_lock_upd s3_lock_skip s1_commit s3_commit

# Test 15: NOWAIT succeeds when no lock contention
permutation s3_lock_nowait s3_commit

# ============================================================
# Lock release tests
# ============================================================

# Test 16: Rollback releases locks
permutation s1_lock_upd s2_lock_upd s1_rollback s2_commit s3_select s3_commit

# Test 17: Commit releases locks for next waiter
permutation s1_lock_upd s2_lock_upd s1_commit s2_update s2_commit s3_select s3_commit

# ============================================================
# Lock with DML interaction tests
# ============================================================

# Test 18: Non-conflicting locks on different rows
permutation s1_lock_upd s3_update_other s1_commit s3_commit s2_select s2_commit

# Test 19: Lock then delete - s2 should see the delete after s1 commits
permutation s1_lock_upd s1_delete s2_lock_upd s1_commit s2_commit

# Test 20: Lock then key update - should block KEY SHARE holders
permutation s1_lock_upd s1_update_key s2_lock_keysh s1_commit s2_commit

# ============================================================
# Deadlock detection
# ============================================================

# Test 21: Potential deadlock - each session locks different row then
# tries to lock the other's row. PostgreSQL should detect and abort one.
permutation s1_lock_upd s2_lock_r2 s1_lock_r2 s2_lock_upd s1_commit s2_commit
