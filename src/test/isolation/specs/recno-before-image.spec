# Test shared before-image serving for RECNO in-place updates.
#
# RECNO overwrites tuples in place during UPDATE.  For MVCC correctness,
# the pre-update tuple data is stored in a shared DSA area (via
# SLogTupleStoreBeforeImage) and served to concurrent readers whose
# snapshot predates the update commit (via SLogTupleGetSharedBeforeImage
# in recno_handler.c).
#
# This test exercises:
# 1. A REPEATABLE READ reader with an older snapshot receives the
#    DSA-stored before-image when reading a row updated and committed
#    by another transaction.
# 2. Multiple rows updated — before-images served for all modified rows.
# 3. New transaction starting after commit sees on-page data directly.
# 4. READ COMMITTED sees new data after commit (no before-image served).
# 5. Chained updates within one transaction — the shared before-image
#    preserves the original pre-transaction state (first update's
#    before-image is retained, not overwritten by subsequent updates).

setup
{
  CREATE TABLE recno_bi (id int, val text, num int) USING recno;
  INSERT INTO recno_bi VALUES (1, 'alpha', 100);
  INSERT INTO recno_bi VALUES (2, 'beta', 200);
  INSERT INTO recno_bi VALUES (3, 'gamma', 300);
}

teardown
{
  DROP TABLE recno_bi;
}

# s1: long-running REPEATABLE READ reader (snapshot before updates)
session s1
setup			{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1_read		{ SELECT id, val, num FROM recno_bi ORDER BY id; }
step s1_commit		{ COMMIT; }

# s2: writer that modifies rows
session s2
setup			{ BEGIN; }
step s2_update_one	{ UPDATE recno_bi SET val = 'ALPHA', num = 101 WHERE id = 1; }
step s2_update_two	{ UPDATE recno_bi SET val = 'BETA', num = 202 WHERE id = 2; }
step s2_update_chain	{ UPDATE recno_bi SET val = 'ALPHA_V2', num = 111 WHERE id = 1; }
step s2_commit		{ COMMIT; }

# s3: fresh reader that starts after s2 commits (tests no before-image needed)
session s3
step s3_begin_rr	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s3_begin_rc	{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s3_read		{ SELECT id, val, num FROM recno_bi ORDER BY id; }
step s3_commit		{ COMMIT; }

# Permutation 1: Basic shared before-image serving.
# s1 takes snapshot (sees original), s2 updates row 1 and commits,
# s1 re-reads and must still see the original via DSA before-image.
permutation s1_read s2_update_one s2_commit s1_read s1_commit

# Permutation 2: Multiple rows updated — before-images for all.
# s1 snapshot precedes both updates; both rows must show originals.
permutation s1_read s2_update_one s2_update_two s2_commit s1_read s1_commit

# Permutation 3: New REPEATABLE READ snapshot after commit.
# s3 starts after s2 commits, so its snapshot_hlc > commit_hlc;
# it reads on-page data directly (no before-image lookup).
# Meanwhile s1 (earlier snapshot) still sees original via before-image.
permutation s1_read s2_update_one s2_commit s3_begin_rr s3_read s1_read s3_commit s1_commit

# Permutation 4: READ COMMITTED sees new value after commit.
# s3 uses RC, starts new statement after s2 commits — sees updated data.
permutation s1_read s2_update_one s2_commit s3_begin_rc s3_read s1_read s3_commit s1_commit

# Permutation 5: Chained update (same row updated twice in one txn).
# The shared before-image preserves the original pre-transaction state:
# s1 correctly sees 'alpha' (original) despite two in-place overwrites.
permutation s1_read s2_update_one s2_update_chain s2_commit s1_read s1_commit
