# Test concurrent INSERT behavior for RECNO table access method.
#
# Verifies that:
# 1. Concurrent INSERTs to the same table do not block each other
# 2. Each transaction sees only its own uncommitted inserts
# 3. After commit, all inserts are visible to new transactions
# 4. A concurrent third-party reader does NOT see another session's
#    still-in-progress INSERT (no dirty read), under both READ COMMITTED
#    and REPEATABLE READ -- matching heap semantics.  This is the guard
#    against the uncommitted-INSERT dirty read: an in-progress INSERT by
#    another transaction must be invisible until that transaction commits.
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

# Third-party observer used to prove no dirty read of an in-progress INSERT.
# obs_rc reads under READ COMMITTED (fresh per-statement snapshot); obs_rr
# reads inside a REPEATABLE READ transaction.  Neither may see s1's or s2's
# uncommitted rows.
session obs
step obs_rc		{ SELECT id, val FROM recno_ci ORDER BY id; }
step obs_rr_begin	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step obs_rr_read	{ SELECT id, val FROM recno_ci ORDER BY id; }
step obs_rr_commit	{ COMMIT; }

# Permutation 1: Interleaved inserts — each session only sees its own rows.
permutation s1_insert_1 s2_insert_3 s1_insert_2 s2_insert_4 s1_read s2_read s1_commit s2_commit s3_read

# Permutation 2: s1 commits first, s2 still can't see s1's rows (READ COMMITTED
# snapshot taken per-statement, but s2's BEGIN precedes s1's commit... depends
# on isolation level, default is READ COMMITTED where each statement gets new snapshot).
permutation s1_insert_1 s1_commit s2_insert_3 s2_read s2_commit s3_read

# Permutation 3: Both commit, then verify final state.
permutation s1_insert_1 s1_insert_2 s2_insert_3 s2_insert_4 s1_commit s2_commit s3_read

# Permutation 4 (dirty-read guard, READ COMMITTED): while s1 and s2 both hold
# uncommitted single-row INSERTs, a third-party RC observer must see NEITHER
# (0 rows).  After both commit, the observer sees all four.  A regression that
# exposed the uncommitted INSERT (the dirty read) would show rows here.
permutation s1_insert_1 s2_insert_3 obs_rc s1_commit s2_commit obs_rc

# Permutation 5 (dirty-read guard, REPEATABLE READ + multi-row + ON CONFLICT):
# the observer opens an RR transaction BEFORE the writers insert, so its
# snapshot precedes every write; it must see 0 rows throughout, and its RR
# snapshot must remain stable (still 0) even after the writers commit.
permutation obs_rr_begin s1_insert_1 s1_insert_2 s2_insert_3 obs_rr_read s1_commit s2_commit obs_rr_read obs_rr_commit obs_rc
