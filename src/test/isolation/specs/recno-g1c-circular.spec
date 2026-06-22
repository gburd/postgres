# Test G1c (Circular Information Flow) prevention for RECNO.
#
# G1c occurs when there is a cycle in the dependency graph between
# committed transactions (Adya 2000).  Specifically, if T1 reads a
# version written by T2, and T2 reads a version written by T1, there
# is a cycle (ww or wr dependencies form a cycle).
#
# In the simpler formulation: two concurrent transactions each write
# to different items, then read the other's item.  Under Snapshot
# Isolation, neither sees the other's write (since both started before
# the other committed).  This is NOT a G1c violation — it's correct
# SI behavior where both transactions see a consistent pre-concurrent
# snapshot.
#
# Concurrent committed UPDATEs are reconstructed per reader snapshot from the
# per-relation UNDO fork (WS-PVS2), so a SERIALIZABLE reader sees the correct
# pre-concurrent version, not uncommitted data.
#
# For same-tuple write-write conflicts (true G1c prerequisite), RECNO
# detects cycles via:
# 1. sLog write-write conflict → second writer blocks (P0 prevention)
# 2. SIREAD predicate locks + rw-antidependency reporting through
#    CheckForSerializableConflictIn/Out, with dangerous-structure detection
#    performed by the core predicate.c (identical to heap)
#
# This test verifies:
# 1. Under SERIALIZABLE with same-row RW conflicts: cycle detected, one aborts
# 2. Under SERIALIZABLE with different rows: no cycle (SI allows this)

setup
{
  CREATE TABLE recno_g1c (id int, val int) USING recno;
  INSERT INTO recno_g1c VALUES (1, 10);
  INSERT INTO recno_g1c VALUES (2, 20);
}

teardown
{
  DROP TABLE recno_g1c;
}

session s1
setup			{ BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s1_read_1		{ SELECT val FROM recno_g1c WHERE id = 1; }
step s1_read_2		{ SELECT val FROM recno_g1c WHERE id = 2; }
step s1_write_1		{ UPDATE recno_g1c SET val = 11 WHERE id = 1; }
step s1_commit		{ COMMIT; }

session s2
setup			{ BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s2_read_1		{ SELECT val FROM recno_g1c WHERE id = 1; }
step s2_read_2		{ SELECT val FROM recno_g1c WHERE id = 2; }
step s2_write_2		{ UPDATE recno_g1c SET val = 22 WHERE id = 2; }
step s2_commit		{ COMMIT; }

session s3
step s3_verify		{ SELECT id, val FROM recno_g1c ORDER BY id; }

# Permutation 1: Classic G1c test with disjoint writes.
# s1 writes x, s2 writes y, then each reads the other's row.
# Under SI: neither sees the other's uncommitted write.  With heap-shaped
# xmin/xmax MVCC, the other writer's in-progress in-place update carries its
# uncommitted xmin, so the reader reconstructs the committed before-image from
# the UNDO fork (s1 sees id=2 = 20, s2 sees id=1 = 10) -- correct SI, no
# uncommitted-read leak.  Both commit successfully.
permutation s1_write_1 s2_write_2 s1_read_2 s2_read_1 s1_commit s2_commit s3_verify

# Permutation 2: Cross-read before writes.
# Both read both rows, then write to different rows.
# This is the write-skew pattern on disjoint tuples.
# SSI detects the rw-antidependency cycle: s2's commit fails.
permutation s1_read_1 s1_read_2 s2_read_1 s2_read_2 s1_write_1 s2_write_2 s1_commit s2_commit s3_verify

# Permutation 3: Writes interleaved with cross-reads.
# s1 writes row 1, s2 reads row 1 (sees old value due to snapshot),
# s2 writes row 2, s1 reads row 2 (sees old value due to snapshot).
# Under SI: both see pre-concurrent values, both commit.
permutation s1_write_1 s2_read_1 s2_write_2 s1_read_2 s1_commit s2_commit s3_verify
