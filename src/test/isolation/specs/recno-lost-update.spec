# Test P4 (Lost Update) prevention for RECNO table access method.
#
# A lost update occurs when two transactions read a row, then both update
# it based on the read value, and one update overwrites the other without
# incorporating it (Berenson et al. 1995).
#
# RECNO correctly prevents lost updates (P4) under READ COMMITTED:
#
# 1. The visibility function returns "visible" for tuples with an in-progress
#    UPDATE by another transaction (the tuple existed before the update).
# 2. recno_tuple_update detects the concurrent modification via
#    RECNO_TUPLE_UNCOMMITTED + sLog, and blocks (XactLockTableWait).
# 3. After the first updater commits, TM_Updated is returned to the executor.
# 4. The EPQ mechanism (EvalPlanQual) re-fetches the tuple with SnapshotAny,
#    re-evaluates the WHERE clause, and re-projects the target expressions
#    using the latest committed data.
#
# Result: counter reaches 2 (first update: 0→1, second update re-evaluates
# counter+1 with counter=1 → produces 2).

setup
{
  CREATE TABLE recno_lu (id int, counter int) USING recno;
  INSERT INTO recno_lu VALUES (1, 0);
  INSERT INTO recno_lu VALUES (2, 100);
}

teardown
{
  DROP TABLE recno_lu;
}

# --- READ COMMITTED test ---

session s1
setup			{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1_read		{ SELECT id, counter FROM recno_lu WHERE id = 1; }
step s1_update		{ UPDATE recno_lu SET counter = counter + 1 WHERE id = 1; }
step s1_commit		{ COMMIT; }

session s2
setup			{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s2_read		{ SELECT id, counter FROM recno_lu WHERE id = 1; }
step s2_update		{ UPDATE recno_lu SET counter = counter + 1 WHERE id = 1; }
step s2_read_after	{ SELECT id, counter FROM recno_lu WHERE id = 1; }
step s2_commit		{ COMMIT; }

session s3
step s3_read		{ SELECT id, counter FROM recno_lu ORDER BY id; }

# Permutation 1: Classic lost update scenario under READ COMMITTED.
# Both read counter=0.  s1 updates to 1, commits.  s2 blocked during s1's
# update, then EPQ re-reads counter=1 and applies +1 → counter=2.
permutation s1_read s2_read s1_update s2_update s1_commit s2_read_after s2_commit s3_read

# Permutation 2: s2 updates first, s1 blocked.  Same logic, final=2.
permutation s1_read s2_read s2_update s1_update s2_commit s1_commit s3_read

# Permutation 3: Multiple increments — verifies no updates lost.
# s1 increments, commits, then s2 increments (no contention).
permutation s1_read s1_update s1_commit s2_read s2_update s2_commit s3_read
