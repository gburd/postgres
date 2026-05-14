# Test concurrent INSERT behavior for RECNO table access method.
#
# Verifies that:
# 1. Concurrent INSERTs to the same table do not block each other
# 2. Each transaction sees only its own uncommitted inserts
# 3. After commit, all inserts are visible to new transactions
#
# RECNO INSERTs use lightweight local-only sLog tracking (no shared hash
# entry created), so there should be no lock contention between inserters.

setup
{
  CREATE TABLE recno_ci (id int, val text) USING recno;
}

teardown
{
  DROP TABLE recno_ci;
}

session s1
setup			{ BEGIN; }
step s1_insert_1	{ INSERT INTO recno_ci VALUES (1, 'from_s1'); }
step s1_insert_2	{ INSERT INTO recno_ci VALUES (2, 'from_s1'); }
step s1_read		{ SELECT id, val FROM recno_ci ORDER BY id; }
step s1_commit		{ COMMIT; }

session s2
setup			{ BEGIN; }
step s2_insert_3	{ INSERT INTO recno_ci VALUES (3, 'from_s2'); }
step s2_insert_4	{ INSERT INTO recno_ci VALUES (4, 'from_s2'); }
step s2_read		{ SELECT id, val FROM recno_ci ORDER BY id; }
step s2_commit		{ COMMIT; }

session s3
step s3_read		{ SELECT id, val FROM recno_ci ORDER BY id; }

# Permutation 1: Interleaved inserts — each session only sees its own rows.
permutation s1_insert_1 s2_insert_3 s1_insert_2 s2_insert_4 s1_read s2_read s1_commit s2_commit s3_read

# Permutation 2: s1 commits first, s2 still can't see s1's rows (READ COMMITTED
# snapshot taken per-statement, but s2's BEGIN precedes s1's commit... depends
# on isolation level, default is READ COMMITTED where each statement gets new snapshot).
permutation s1_insert_1 s1_commit s2_insert_3 s2_read s2_commit s3_read

# Permutation 3: Both commit, then verify final state.
permutation s1_insert_1 s1_insert_2 s2_insert_3 s2_insert_4 s1_commit s2_commit s3_read
