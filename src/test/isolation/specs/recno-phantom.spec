# Test P3 (Phantom) prevention for RECNO table access method.
#
# A phantom occurs when a transaction re-executes a range query and
# finds new rows inserted by a concurrent committed transaction
# (Berenson et al. 1995).
#
# RECNO prevents phantoms at REPEATABLE READ via its fixed HLC snapshot:
# - Tuples inserted after xact_start_hlc are invisible (birth_hlc > snapshot)
# - This gives full phantom protection at the SI level
#
# At SERIALIZABLE, true phantom prevention requires predicate/range locking
# to detect that an INSERT conflicts with a prior range scan.  RECNO does
# NOT implement predicate locking, so it cannot detect write skew involving
# phantoms (e.g., INSERT + aggregate check).  However, simple phantom READS
# are prevented because the snapshot is fixed.
#
# Expected behavior:
# - Under RR: range query returns same result before and after concurrent INSERT+COMMIT
# - Under SR: same as RR (SI semantics), phantoms invisible to fixed snapshot
# - LIMITATION: A serialization anomaly involving phantom insert + disjoint
#   read would NOT be detected (would require predicate locking)

setup
{
  CREATE TABLE recno_phantom (id int, category text, val int) USING recno;
  INSERT INTO recno_phantom VALUES (1, 'A', 10);
  INSERT INTO recno_phantom VALUES (2, 'A', 20);
  INSERT INTO recno_phantom VALUES (3, 'B', 30);
  INSERT INTO recno_phantom VALUES (10, 'A', 100);
}

teardown
{
  DROP TABLE recno_phantom;
}

session s1
setup			{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1_range		{ SELECT count(*) FROM recno_phantom WHERE id BETWEEN 1 AND 10; }
step s1_range_cat	{ SELECT count(*), sum(val) FROM recno_phantom WHERE category = 'A'; }
step s1_read_all	{ SELECT id, category, val FROM recno_phantom ORDER BY id; }
step s1_commit		{ COMMIT; }

session s2
setup			{ BEGIN; }
step s2_insert_mid	{ INSERT INTO recno_phantom VALUES (5, 'A', 50); }
step s2_insert_end	{ INSERT INTO recno_phantom VALUES (11, 'A', 110); }
step s2_delete		{ DELETE FROM recno_phantom WHERE id = 3; }
step s2_commit		{ COMMIT; }

session s3
step s3_read		{ SELECT id, category, val FROM recno_phantom ORDER BY id; }

# Permutation 1: s1 scans range, s2 inserts into the range and commits,
# s1 re-scans — must see same count (no phantom).
permutation s1_range s2_insert_mid s2_commit s1_range s1_commit s3_read

# Permutation 2: s1 scans by category, s2 inserts matching row + commits,
# s1 re-scans — must see same count and sum (no phantom).
permutation s1_range_cat s2_insert_mid s2_commit s1_range_cat s1_commit s3_read

# Permutation 3: s2 inserts AND deletes, then commits.  s1's snapshot
# must see neither the insert nor the delete effect.
permutation s1_read_all s2_insert_mid s2_delete s2_commit s1_read_all s1_commit s3_read

# Permutation 4: Insert outside range — s1's range query unaffected, and
# s1's full table read also doesn't see it (snapshot predates insert).
permutation s1_range s2_insert_end s2_commit s1_range s1_read_all s1_commit s3_read
