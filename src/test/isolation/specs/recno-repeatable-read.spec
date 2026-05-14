# Test REPEATABLE READ (snapshot isolation) for RECNO table access method.
#
# Verifies that:
# 1. A snapshot taken at transaction start does not see later commits
# 2. Concurrent INSERTs after snapshot are invisible (no phantoms at SI level)
# 3. Concurrent DELETEs after snapshot don't remove rows from snapshot view
# 4. Concurrent UPDATEs after snapshot don't change values in snapshot view
#
# RECNO's HLC-based MVCC must ensure that once a snapshot HLC is established,
# only tuples with birth_hlc <= snapshot_hlc are visible, and tuples
# deleted/updated by transactions that committed after the snapshot are
# still visible in their pre-modification form.

setup
{
  CREATE TABLE recno_rr (id int, val text) USING recno;
  INSERT INTO recno_rr VALUES (1, 'original_1');
  INSERT INTO recno_rr VALUES (2, 'original_2');
  INSERT INTO recno_rr VALUES (3, 'original_3');
}

teardown
{
  DROP TABLE recno_rr;
}

session s1
setup			{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1_read_snap	{ SELECT id, val FROM recno_rr ORDER BY id; }
step s1_commit		{ COMMIT; }

session s2
setup			{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s2_insert		{ INSERT INTO recno_rr VALUES (4, 'new_4'); }
step s2_delete		{ DELETE FROM recno_rr WHERE id = 1; }
step s2_update		{ UPDATE recno_rr SET val = 'modified_2' WHERE id = 2; }
step s2_commit		{ COMMIT; }

# Permutation 1: s1 takes snapshot, s2 inserts and commits,
# s1 should NOT see the new row (no phantom).
permutation s1_read_snap s2_insert s2_commit s1_read_snap s1_commit

# Permutation 2: s1 takes snapshot, s2 deletes and commits,
# s1 should still see the deleted row.
permutation s1_read_snap s2_delete s2_commit s1_read_snap s1_commit

# Permutation 3: s1 takes snapshot, s2 updates and commits,
# s1 should still see the original value.
permutation s1_read_snap s2_update s2_commit s1_read_snap s1_commit

# Permutation 4: Multiple modifications committed by s2, none visible to s1's snapshot.
permutation s1_read_snap s2_delete s2_update s2_insert s2_commit s1_read_snap s1_commit
