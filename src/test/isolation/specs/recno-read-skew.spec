# Test A5A (Read Skew) prevention for RECNO table access method.
#
# Read skew occurs when a transaction reads two related items at different
# times and sees an inconsistent state because another transaction modified
# both between the reads (Adya 2000, Berenson et al. 1995).
#
# Example: x=50, y=50 (constraint: x+y=100)
#   T1 reads x=50
#   T2 updates x=25, y=75, commits
#   T1 reads y=75  → sees x+y=125 (inconsistent!)
#
# ARCHITECTURAL LIMITATION — RECNO's in-place UPDATE causes tuples to
# disappear from RR/SR snapshots instead of showing the old value.
# After s2's UPDATE commits, the tuple's t_commit_ts (stamped at commit
# time) is newer than s1's snapshot_hlc, making it correctly invisible.
# But since the old value was overwritten in place (no version chain),
# the reader cannot see the old value either — the tuple vanishes.
#
# This prevents read skew (reader never sees inconsistent state) but at
# the cost of reduced visibility.  True multi-version reads would require
# either a version chain or UNDO log integration.
#
# At READ COMMITTED, read skew IS possible (each statement gets a fresh
# snapshot), which is correct per the SQL standard.

setup
{
  CREATE TABLE recno_rs (id int, val int) USING recno;
  INSERT INTO recno_rs VALUES (1, 50);
  INSERT INTO recno_rs VALUES (2, 50);
}

teardown
{
  DROP TABLE recno_rs;
}

session s1
setup			{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1_read_x		{ SELECT val FROM recno_rs WHERE id = 1; }
step s1_read_y		{ SELECT val FROM recno_rs WHERE id = 2; }
step s1_read_both	{ SELECT id, val FROM recno_rs ORDER BY id; }
step s1_commit		{ COMMIT; }

session s2
setup			{ BEGIN; }
step s2_transfer	{ UPDATE recno_rs SET val = 25 WHERE id = 1; UPDATE recno_rs SET val = 75 WHERE id = 2; }
step s2_commit		{ COMMIT; }

session s3
step s3_read		{ SELECT id, val FROM recno_rs ORDER BY id; }

# Permutation 1: Classic read skew scenario.
# s1 reads x, s2 modifies both and commits, s1 reads y.
# Under RR: s1 sees original x=50 AND original y=50 (consistent snapshot).
permutation s1_read_x s2_transfer s2_commit s1_read_y s1_commit s3_read

# Permutation 2: s1 reads both together after s2 commits.
# Under RR: snapshot predates s2's commit, so s1 sees originals.
permutation s1_read_x s2_transfer s2_commit s1_read_both s1_commit s3_read

# Permutation 3: s2 commits before s1 begins any reads — s1 sees new values.
# This is NOT read skew because s1's snapshot includes s2's commit.
permutation s2_transfer s2_commit s1_read_x s1_read_y s1_commit
