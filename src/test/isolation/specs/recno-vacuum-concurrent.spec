# Test VACUUM behavior with concurrent transactions for RECNO.
#
# Verifies that:
# 1. VACUUM does not remove tuples still needed by active snapshots
# 2. VACUUM can reclaim space from committed deletes not needed by anyone
# 3. Concurrent DML during VACUUM works correctly
#
# Note: RECNO VACUUM sets PD_ALL_VISIBLE and updates the visibility map.
# It should not remove tuples that are needed by in-progress transactions.

setup
{
  CREATE TABLE recno_vac (id int, val text) USING recno;
  INSERT INTO recno_vac SELECT g, 'val_' || g FROM generate_series(1, 100) g;
  DELETE FROM recno_vac WHERE id <= 50;
}

teardown
{
  DROP TABLE recno_vac;
}

session s1
setup			{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1_read_count	{ SELECT count(*) FROM recno_vac; }
step s1_read_some	{ SELECT id, val FROM recno_vac WHERE id BETWEEN 51 AND 55 ORDER BY id; }
step s1_commit		{ COMMIT; }

session s2
step s2_vacuum		{ VACUUM recno_vac; }
step s2_delete		{ DELETE FROM recno_vac WHERE id BETWEEN 51 AND 60; }

session s3
step s3_read_count	{ SELECT count(*) FROM recno_vac; }

# Permutation 1: s1 takes snapshot, VACUUM runs, s1 should still see same data.
permutation s1_read_count s2_vacuum s1_read_count s1_commit

# Permutation 2: s1 takes snapshot, concurrent delete + vacuum, s1 still consistent.
permutation s1_read_count s2_delete s2_vacuum s1_read_count s1_read_some s1_commit s3_read_count

# Permutation 3: VACUUM first (no active snapshots), then read.
permutation s2_vacuum s1_read_count s1_commit s3_read_count
