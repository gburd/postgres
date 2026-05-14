# Test READ COMMITTED isolation for RECNO table access method.
#
# Verifies that:
# 1. Uncommitted DELETEs are invisible to concurrent readers
# 2. Once committed, DELETEs become visible to new statements
# 3. Aborted DELETEs remain invisible (row stays visible)
#
# RECNO uses HLC timestamps + sLog for MVCC (not xmin/xmax), so these
# tests verify that the sLog-based visibility correctly implements RC.
#
# NOTE: UPDATE permutations are excluded because RECNO uses in-place
# updates (no old version chain).  The old tuple data is overwritten,
# so the original row cannot be reconstructed during an in-progress or
# aborted UPDATE.  This is a known architectural limitation.

setup
{
  CREATE TABLE recno_rc (id int, val text) USING recno;
  INSERT INTO recno_rc VALUES (1, 'original_1');
  INSERT INTO recno_rc VALUES (2, 'original_2');
  INSERT INTO recno_rc VALUES (3, 'original_3');
}

teardown
{
  DROP TABLE recno_rc;
}

session s1
setup			{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1_delete		{ DELETE FROM recno_rc WHERE id = 1; }
step s1_commit		{ COMMIT; }
step s1_abort		{ ROLLBACK; }

session s2
setup			{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s2_read_all	{ SELECT id, val FROM recno_rc ORDER BY id; }
step s2_read_count	{ SELECT count(*) FROM recno_rc; }
step s2_commit		{ COMMIT; }

# Permutation 1: Reader sees all rows while DELETE is uncommitted,
# then sees the delete after commit.
permutation s1_delete s2_read_all s1_commit s2_read_all s2_commit

# Permutation 2: Aborted DELETE remains invisible (row still visible).
permutation s1_delete s2_read_all s1_abort s2_read_all s2_commit

# Permutation 3: Reader starts first, sees uncommitted delete as invisible,
# then sees committed delete in next statement (READ COMMITTED semantics).
permutation s2_read_count s1_delete s2_read_count s1_commit s2_read_count s2_commit
