# Pre-commit visibility window for RECNO in-place UPDATE and DELETE.
#
# This spec pins the observable read-visibility of a still-in-progress
# UPDATE/DELETE as seen by a concurrent third-party reader, under both READ
# COMMITTED and REPEATABLE READ.  It is the regression guard for the
# "visibility follows commit" invariant on the modify path.
#
# Correct (heap-equivalent) behaviour:
#   - A REPEATABLE READ observer whose snapshot was taken BEFORE the writer's
#     UPDATE/DELETE must continue to see the pre-modification value for the
#     entire life of its snapshot.  After the writer COMMITS, RECNO's
#     before-image reconstruction (RecnoReconstructVisibleVersion / WS-PVS3)
#     correctly serves the old value to the old snapshot.
#   - A READ COMMITTED observer takes a fresh snapshot per statement, so once
#     the writer commits it sees the new value; while the writer is still
#     in progress it must NOT treat the change as committed.
#
# Known RECNO read behaviour (heap-shaped xmin/xmax MVCC):
#   RECNO now uses ordinary heap-compatible xmin/xmax visibility.  During an
#   *in-progress* in-place UPDATE the on-page image carries the updater's
#   uncommitted xmin; a concurrent reader whose snapshot cannot see that xmin
#   reads the pre-update version back from the per-relation UNDO fork
#   (RecnoReconstructVisibleVersion).  So permutation 1's mid-flight RR read
#   correctly shows the pre-write value (10), NOT the uncommitted 999 -- the
#   old HLC-model in-place read-imprecision (documented here previously) is
#   closed by the xmin/xmax switch.  The row stays visible (as the old
#   before-image) so DML scans can still detect the write-write conflict and
#   EPQ-retry; the write-write conflict machinery (recno-lost-update.spec) is
#   unaffected.  These permutations PIN the heap-shaped behaviour.

setup
{
  CREATE TABLE recno_pcv (id int, val int) USING recno;
  INSERT INTO recno_pcv VALUES (1, 10);
  INSERT INTO recno_pcv VALUES (2, 20);
}

teardown
{
  DROP TABLE recno_pcv;
}

# Writer: modifies rows but holds the transaction open across the observer's
# reads, so the observer reads while the write is in progress.
session w
setup			{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step w_update		{ UPDATE recno_pcv SET val = 999 WHERE id = 1; }
step w_delete		{ DELETE FROM recno_pcv WHERE id = 2; }
step w_commit		{ COMMIT; }

# READ COMMITTED observer: fresh snapshot per SELECT.
session orc
step orc_read		{ SELECT id, val FROM recno_pcv ORDER BY id; }

# REPEATABLE READ observer: single snapshot, taken before the writer modifies.
session orr
step orr_begin		{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step orr_read		{ SELECT id, val FROM recno_pcv ORDER BY id; }
step orr_commit		{ COMMIT; }

# Permutation 1 (REPEATABLE READ, snapshot stability + before-image):
# the RR observer's snapshot precedes the writer's UPDATE and DELETE.  Its
# mid-flight read correctly shows the pre-write value for id=1 (10, served
# from the UNDO-fork before-image because the updater's xmin is invisible to
# the observer) and id=2 still visible (the DELETE is not yet committed).
# After the writer commits, before-image reconstruction keeps the pre-write
# view (1,10) and (2,20) to the old snapshot -- snapshot stability holds.
permutation orr_begin orr_read w_update w_delete orr_read w_commit orr_read orr_commit

# Permutation 2 (READ COMMITTED, commit-visibility): the RC observer must not
# treat the DELETE as committed while it is in progress (row id=2 stays
# visible), and must see the committed result after w_commit (id=2 gone,
# id=1 updated).  While the UPDATE is in progress the observer reads the
# committed before-image (10), not the uncommitted 999 (heap-shaped xmin/xmax).
permutation w_update w_delete orc_read w_commit orc_read
