# GATE 5: RECNO escrow visibility / read-your-writes / MVCC matrix.
#
# Escrow accumulates a running sum on the page; visibility must still obey
# snapshot rules:
#   * A writer sees its OWN uncommitted += (read-your-writes) -- the delta is
#     on the page and its xid stops the reconstruct walk.
#   * A concurrent txn does NOT see another's uncommitted += -- the reader
#     reconstructs the pre-delta version from the UNDO-fork version chain.
#   * After commit the delta becomes visible per the reader's snapshot.
#   * An OLD REPEATABLE READ snapshot taken BEFORE any += still sees the
#     pre-delta base (the pvs walk subtracts the committed delta).
#
# Escrow column is int8 (the prototype fast-path type).

setup
{
  CREATE TABLE esc_vis (id int primary key, ytd bigint not null default 0) USING recno;
  INSERT INTO esc_vis VALUES (1, 500);
  ALTER TABLE esc_vis ALTER COLUMN ytd SET (escrow=true);
}

teardown
{
  DROP TABLE esc_vis;
}

# writer
session w
step w_begin_rc	{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step w_begin_rr	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step w_add100	{ UPDATE esc_vis SET ytd = ytd + 100 WHERE id = 1; }
step w_self	{ SELECT id, ytd FROM esc_vis WHERE id = 1; }
step w_commit	{ COMMIT; }

# concurrent reader (READ COMMITTED)
session rc
step rc_begin	{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step rc_read	{ SELECT id, ytd FROM esc_vis WHERE id = 1; }
step rc_commit	{ COMMIT; }

# old-snapshot reader (REPEATABLE READ), snapshot taken before the +=
session rr
step rr_begin	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step rr_snap	{ SELECT id, ytd FROM esc_vis WHERE id = 1; }
step rr_reread	{ SELECT id, ytd FROM esc_vis WHERE id = 1; }
step rr_commit	{ COMMIT; }

# M1 read-your-writes (RC): writer sees its own uncommitted += (600); a
# concurrent RC reader still sees the pre-delta base (500) until commit; after
# commit a fresh RC read sees 600.
permutation w_begin_rc w_add100 w_self rc_begin rc_read w_commit rc_read rc_commit

# M2 old RR snapshot: rr takes its snapshot (500) BEFORE the += commits; after
# the writer commits +100, rr STILL sees 500 (reconstructed pre-delta base via
# the pvs walk), while a fresh RC read sees 600.
permutation rr_begin rr_snap w_begin_rc w_add100 w_commit rr_reread rc_begin rc_read rc_commit rr_commit

# M3 read-your-writes (RR): a REPEATABLE READ writer sees its own uncommitted
# += within the txn, and continues to see it after commit.
permutation w_begin_rr w_add100 w_self w_commit w_begin_rr w_self w_commit
