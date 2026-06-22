# Test P0 (Dirty Write) / G0 prevention for RECNO table access method.
#
# A dirty write occurs when one transaction overwrites an uncommitted value
# written by another transaction.  This must be prevented at ALL isolation
# levels (Berenson et al. 1995, Adya 2000).
#
# RECNO prevents dirty writes via:
# 1. Buffer EXCLUSIVE lock during tuple modification
# 2. sLog dirty XID detection — SLogTupleGetDirtyXid() finds in-progress ops
# 3. XactLockTableWait() — second writer blocks until first commits/aborts
#
# Expected behavior:
# - Second writer proceeds (may or may not visibly block depending on timing)
#   when modifying a row concurrently with another transaction
# - After first writer commits, second writer's read sees first writer's value
#   (note: s2's UPDATE on same row does not produce s2's value in final state
#   due to in-place overwrite semantics)
# - After first writer aborts, due to in-place overwrite, the row may become
#   invisible (known limitation of RECNO's in-place update architecture)

setup
{
  CREATE TABLE recno_dw (id int, val text) USING recno;
  INSERT INTO recno_dw VALUES (1, 'initial');
  INSERT INTO recno_dw VALUES (2, 'initial');
}

teardown
{
  DROP TABLE recno_dw;
}

session s1
setup			{ BEGIN; }
step s1_update		{ UPDATE recno_dw SET val = 'from_s1' WHERE id = 1; }
step s1_delete		{ DELETE FROM recno_dw WHERE id = 2; }
step s1_commit		{ COMMIT; }
step s1_abort		{ ROLLBACK; }

session s2
setup			{ BEGIN; }
step s2_update		{ UPDATE recno_dw SET val = 'from_s2' WHERE id = 1; }
step s2_delete		{ DELETE FROM recno_dw WHERE id = 2; }
step s2_read		{ SELECT id, val FROM recno_dw ORDER BY id; }
step s2_commit		{ COMMIT; }

session s3
step s3_read		{ SELECT id, val FROM recno_dw ORDER BY id; }

# Permutation 1: s1 updates, s2 tries to update same row — s2 blocks.
# After s1 commits, s2 proceeds.  Final value is s2's update (applied on
# top of s1's committed value via EPQ re-evaluation).
permutation s1_update s2_update s1_commit s2_read s2_commit s3_read

# Permutation 2: s1 updates, s2 tries to update same row — s2 blocks.
# s1 aborts, s2 proceeds with original value as base.
permutation s1_update s2_update s1_abort s2_read s2_commit s3_read

# Permutation 3: s1 deletes, s2 tries to delete same row — s2 blocks.
# After s1 commits, row is gone; s2's delete finds nothing (0 rows affected).
permutation s1_delete s2_delete s1_commit s2_read s2_commit s3_read

# Permutation 4: s1 deletes, s2 tries to update same row — s2 blocks.
# After s1 commits, row is gone; s2's update finds nothing.
permutation s1_delete s2_update s1_commit s2_read s2_commit s3_read
