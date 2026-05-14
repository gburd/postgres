# Test row-level locking for RECNO table access method.
#
# Verifies that:
# 1. FOR UPDATE blocks concurrent UPDATE/DELETE on the same row
# 2. FOR SHARE allows concurrent FOR SHARE but blocks FOR UPDATE
# 3. Lock release at COMMIT unblocks waiters
#
# RECNO uses sLog LOCK_EXCL/LOCK_SHARE entries for row locks.
# SLogTupleHasLockConflict() checks for conflicts.

setup
{
  CREATE TABLE recno_lock (id int, val text) USING recno;
  INSERT INTO recno_lock VALUES (1, 'original');
  INSERT INTO recno_lock VALUES (2, 'another');
}

teardown
{
  DROP TABLE recno_lock;
}

session s1
setup			{ BEGIN; }
step s1_for_update	{ SELECT * FROM recno_lock WHERE id = 1 FOR UPDATE; }
step s1_for_share	{ SELECT * FROM recno_lock WHERE id = 1 FOR SHARE; }
step s1_update		{ UPDATE recno_lock SET val = 's1_updated' WHERE id = 1; }
step s1_commit		{ COMMIT; }

session s2
setup			{ BEGIN; }
step s2_for_update	{ SELECT * FROM recno_lock WHERE id = 1 FOR UPDATE; }
step s2_for_share	{ SELECT * FROM recno_lock WHERE id = 1 FOR SHARE; }
step s2_update		{ UPDATE recno_lock SET val = 's2_updated' WHERE id = 1; }
step s2_delete		{ DELETE FROM recno_lock WHERE id = 1; }
step s2_read		{ SELECT id, val FROM recno_lock ORDER BY id; }
step s2_commit		{ COMMIT; }

# Permutation 1: FOR UPDATE blocks concurrent FOR UPDATE until commit.
permutation s1_for_update s2_for_update s1_commit s2_commit

# Permutation 2: FOR UPDATE blocks concurrent UPDATE until commit.
permutation s1_for_update s2_update s1_commit s2_read s2_commit

# Permutation 3: FOR UPDATE blocks concurrent DELETE until commit.
permutation s1_for_update s2_delete s1_commit s2_read s2_commit

# Permutation 4: FOR SHARE allows concurrent FOR SHARE (no conflict).
permutation s1_for_share s2_for_share s1_commit s2_commit

# Permutation 5: FOR SHARE blocks concurrent FOR UPDATE.
permutation s1_for_share s2_for_update s1_commit s2_commit

# Permutation 6: Holder updates after locking, blocker unblocked on commit.
permutation s1_for_update s1_update s2_update s1_commit s2_read s2_commit
