# GATE 4: RECNO escrow (commutative delta-accumulation) concurrency.
#
# The INV-4 relaxation (the throughput win): two concurrent UNCOMMITTED escrow
# `+=` updates on the SAME hot row must NOT block each other.  A normal RECNO
# UPDATE blocks the second writer on XactLockTableWait until the first commits
# (see recno-lost-update.spec); for an escrow column the second writer applies
# its own += on top under the content lock instead, converting a commit-length
# wait into a latch-length one.
#
# This is the lost-update linchpin (H1): both deltas are applied to the on-page
# running sum under the content lock, so after both commit the sum includes
# BOTH deltas -- no lost update.  If one writer aborts, its delta is subtracted
# (absolute-per-record reverse) and the other's survives.
#
# The escrow column is int8 (the prototype fast-path type).

setup
{
  CREATE TABLE esc_cc (id int primary key, ytd bigint not null default 0) USING recno;
  INSERT INTO esc_cc VALUES (1, 1000);
  ALTER TABLE esc_cc ALTER COLUMN ytd SET (escrow=true);
}

teardown
{
  DROP TABLE esc_cc;
}

session s1
setup			{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1_add10	{ UPDATE esc_cc SET ytd = ytd + 10 WHERE id = 1; }
step s1_commit	{ COMMIT; }
step s1_abort	{ ROLLBACK; }

session s2
setup			{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s2_add20	{ UPDATE esc_cc SET ytd = ytd + 20 WHERE id = 1; }
step s2_commit	{ COMMIT; }
step s2_abort	{ ROLLBACK; }

session s3
step s3_read	{ SELECT id, ytd FROM esc_cc WHERE id = 1; }

# P1: two concurrent uncommitted += do NOT block; both commit -> 1000+10+20=1030.
# If s2_add20 blocked on s1, the isolation tester would print "<waiting ...>";
# it must NOT, and the final sum proves no lost update (H1).
permutation s1_add10 s2_add20 s1_commit s2_commit s3_read

# P2: interleaved commit order -- s2 commits first, then s1.  Still 1030.
permutation s1_add10 s2_add20 s2_commit s1_commit s3_read

# P3: s1 aborts after both applied uncommitted; s2 commits.
# s1's +10 is subtracted, s2's +20 survives -> 1000+20 = 1020.
permutation s1_add10 s2_add20 s1_abort s2_commit s3_read

# P4: s2 aborts after both applied; s1 commits -> 1000+10 = 1010.
permutation s1_add10 s2_add20 s2_abort s1_commit s3_read

# P5: both abort -> back to base 1000.
permutation s1_add10 s2_add20 s1_abort s2_abort s3_read
