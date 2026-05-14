# Test concurrent UPDATE behavior for RECNO table access method.
#
# Verifies that:
# 1. Two transactions updating the same row serialize correctly
# 2. The second updater either waits or gets TM_Updated and retries
# 3. Lost updates are prevented under READ COMMITTED
#
# RECNO's sLog tracks in-progress UPDATE operations.  When a second
# transaction attempts to UPDATE a row that has an in-progress sLog
# DELETE/UPDATE entry, it must wait for the first to commit/abort.

setup
{
  CREATE TABLE recno_cu (id int, counter int) USING recno;
  INSERT INTO recno_cu VALUES (1, 0);
  INSERT INTO recno_cu VALUES (2, 100);
}

teardown
{
  DROP TABLE recno_cu;
}

session s1
setup			{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1_update		{ UPDATE recno_cu SET counter = counter + 1 WHERE id = 1; }
step s1_delete		{ DELETE FROM recno_cu WHERE id = 2; }
step s1_read		{ SELECT id, counter FROM recno_cu ORDER BY id; }
step s1_commit		{ COMMIT; }
step s1_abort		{ ROLLBACK; }

session s2
setup			{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s2_update		{ UPDATE recno_cu SET counter = counter + 10 WHERE id = 1; }
step s2_delete		{ DELETE FROM recno_cu WHERE id = 2; }
step s2_read		{ SELECT id, counter FROM recno_cu ORDER BY id; }
step s2_commit		{ COMMIT; }

# Permutation 1: s1 updates, s2 tries to update same row — should block
# until s1 commits, then s2 re-evaluates and applies on top of s1's result.
permutation s1_update s2_update s1_commit s2_read s2_commit

# Permutation 2: s1 updates then aborts — s2 should proceed with
# the original value (the abort is resolved via sLog ABORTED entry).
permutation s1_update s2_update s1_abort s2_read s2_commit

# Permutation 3: s1 deletes, s2 tries to delete same row — should block,
# then after s1 commits the row is gone and s2 finds nothing to delete.
permutation s1_delete s2_delete s1_commit s2_read s2_commit

# Permutation 4: Both succeed on different rows (no contention).
permutation s1_update s2_delete s1_commit s2_commit s1_read
