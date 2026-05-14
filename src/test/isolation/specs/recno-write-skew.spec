# Test A5B (Write Skew) for RECNO table access method.
#
# Write skew occurs when two transactions each read overlapping data sets,
# make disjoint updates based on the read values, and the combined result
# violates an integrity constraint (Adya 2000, Fekete et al. 2005).
#
# Classic "doctors on call" scenario:
#   Constraint: at least 1 doctor must be on call
#   T1 reads count(on_call)=2, sets doctor_1 off
#   T2 reads count(on_call)=2, sets doctor_2 off
#   Both commit → 0 doctors on call (violation!)
#
# RECNO now integrates with PostgreSQL's predicate locking (SSI) via
# predicate.c.  The scan path acquires SIREAD predicate locks on tuples,
# and the DML paths call CheckForSerializableConflictIn() to detect
# rw-antidependencies.  Write skew IS prevented: in permutations 1 and 2,
# the second transaction to commit receives serialization_failure.

setup
{
  CREATE TABLE recno_oncall (id int, name text, on_call boolean) USING recno;
  INSERT INTO recno_oncall VALUES (1, 'Alice', true);
  INSERT INTO recno_oncall VALUES (2, 'Bob', true);
}

teardown
{
  DROP TABLE recno_oncall;
}

session s1
setup			{ BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s1_check		{ SELECT count(*) FROM recno_oncall WHERE on_call = true; }
step s1_update		{ UPDATE recno_oncall SET on_call = false WHERE id = 1; }
step s1_commit		{ COMMIT; }

session s2
setup			{ BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s2_check		{ SELECT count(*) FROM recno_oncall WHERE on_call = true; }
step s2_update		{ UPDATE recno_oncall SET on_call = false WHERE id = 2; }
step s2_commit		{ COMMIT; }

session s3
step s3_verify		{ SELECT id, name, on_call FROM recno_oncall ORDER BY id; }

# Permutation 1: Classic write skew — disjoint tuple updates.
# Both read count=2, both decide it's safe to go off-call.
# SSI detects the rw-antidependency cycle and aborts s2 at commit.
permutation s1_check s2_check s1_update s2_update s1_commit s2_commit s3_verify

# Permutation 2: Same as above but s2 commits first.
# SSI aborts s1 at commit (the last to commit in the cycle).
permutation s1_check s2_check s2_update s1_update s2_commit s1_commit s3_verify

# Permutation 3: s1 commits before s2 begins its read.  s2 sees count=1
# (s1's update is visible in s2's snapshot).  No anomaly — both succeed.
permutation s1_check s1_update s1_commit s2_check s2_update s2_commit s3_verify
