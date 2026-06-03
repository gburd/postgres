# Test subtransaction abort visibility for RECNO table access method.
#
# Verifies that:
# 1. A DELETE inside a savepoint that is rolled back does not hide the row
# 2. An UPDATE inside a savepoint that is rolled back preserves original value
# 3. Concurrent readers see the correct state after subtransaction rollback
#
# This exercises the sLog subtransaction handling:
# - RecnoRestoreBeforeImages physically restores tuples from before-images
# - SLogTupleRemoveBySubXid marks entries as SLOG_OP_ABORTED (not removed)

setup
{
  CREATE TABLE recno_svp (id int, val text) USING recno;
  INSERT INTO recno_svp VALUES (1, 'original_1');
  INSERT INTO recno_svp VALUES (2, 'original_2');
}

teardown
{
  DROP TABLE recno_svp;
}

session s1
setup			{ BEGIN; }
step s1_svp		{ SAVEPOINT sp1; }
step s1_delete		{ DELETE FROM recno_svp WHERE id = 1; }
step s1_update		{ UPDATE recno_svp SET val = 'changed_2' WHERE id = 2; }
step s1_rollback_svp	{ ROLLBACK TO sp1; }
step s1_read		{ SELECT id, val FROM recno_svp ORDER BY id; }
step s1_commit		{ COMMIT; }

session s2
setup			{ BEGIN; }
step s2_read		{ SELECT id, val FROM recno_svp ORDER BY id; }
step s2_commit		{ COMMIT; }

# Permutation 1: DELETE in savepoint, rollback, verify row is still visible
# to both the same transaction and a concurrent reader.
permutation s1_svp s1_delete s1_rollback_svp s1_read s2_read s1_commit s2_commit

# Permutation 2: UPDATE in savepoint, rollback, verify original value persists.
permutation s1_svp s1_update s1_rollback_svp s1_read s2_read s1_commit s2_commit

# Permutation 3: Concurrent reader during the in-progress DELETE (before rollback)
# should not see the delete (sLog entry makes it invisible only to others if
# committed, but for in-progress it depends on isolation level).
permutation s1_svp s1_delete s2_read s1_rollback_svp s2_read s1_commit s2_commit

# Permutation 4: Nested — DELETE then rollback, then do a real operation after.
permutation s1_svp s1_delete s1_rollback_svp s1_svp s1_update s1_commit s2_read s2_commit
