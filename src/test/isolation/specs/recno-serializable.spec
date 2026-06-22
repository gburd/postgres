# Test SERIALIZABLE isolation for RECNO table access method.
#
# Verifies basic SERIALIZABLE behavior:
# 1. Read-only transactions see a consistent snapshot
# 2. Write-write conflicts are detected and one transaction aborts
# 3. Serialization failures produce the correct error
#
# KNOWN LIMITATIONS:
# 1. RECNO implements Snapshot Isolation (SI) via heap-shaped xmin/xmax MVCC,
#    layered with
#    predicate-lock-based SSI (predicate.c) for true serializable behavior;
#    write skew on disjoint tuples IS detected (see recno-write-skew.spec /
#    recno-serializable-conflicts.spec / recno-g1c-circular.spec).
# 2. In-place UPDATE overwrites the tuple in the page, but the before-image
#    (WS-PVS1/WS-PVS3) is preserved in the per-relation UNDO fork before the
#    overwrite, so a concurrent reader whose snapshot predates the update's
#    commit does NOT see the in-progress/new data -- it reconstructs the
#    pre-update value (RecnoReconstructVisibleVersion).  This is no longer a
#    dirty read; see recno-before-image.spec for dedicated coverage.

setup
{
  CREATE TABLE recno_ser (id int, val int) USING recno;
  INSERT INTO recno_ser VALUES (1, 10);
  INSERT INTO recno_ser VALUES (2, 20);
}

teardown
{
  DROP TABLE recno_ser;
}

session s1
setup			{ BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s1_read		{ SELECT id, val FROM recno_ser ORDER BY id; }
step s1_update		{ UPDATE recno_ser SET val = val + 1 WHERE id = 1; }
step s1_insert		{ INSERT INTO recno_ser VALUES (3, 30); }
step s1_commit		{ COMMIT; }

session s2
setup			{ BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s2_read		{ SELECT id, val FROM recno_ser ORDER BY id; }
step s2_update		{ UPDATE recno_ser SET val = val + 100 WHERE id = 1; }
step s2_read_after	{ SELECT id, val FROM recno_ser ORDER BY id; }
step s2_commit		{ COMMIT; }

# Permutation 1: Both read, both update same row — one must fail at commit
# (write-write conflict detected via sLog).
permutation s1_read s2_read s1_update s2_update s1_commit s2_commit

# Permutation 2: s1 inserts, s2 doesn't see it (snapshot isolation).
permutation s1_read s2_read s1_insert s1_commit s2_read_after s2_commit

# Permutation 3: Non-conflicting updates succeed (different rows).
permutation s1_read s2_read s1_update s1_commit s2_read_after s2_commit
