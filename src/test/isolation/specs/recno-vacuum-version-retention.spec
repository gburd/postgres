# Test: VACUUM must not discard a version diff a live snapshot still needs.
#
# WS-PVS4 retention gate.  Committed in-place UPDATEs keep their before-image
# only as a byte-diff in the per-relation UNDO fork; a REPEATABLE READ snapshot
# reconstructs the prior version by walking that fork (RecnoReconstructVisibleVersion).
# RelUndoVacuum discards diffs whose updater xid precedes
# GetOldestNonRemovableTransactionId.  A live RR snapshot holds that horizon
# back, so VACUUM must leave the diff intact and s1 must still reconstruct the
# original value after VACUUM runs.
#
# Scenario:
#   1. s1 starts REPEATABLE READ and reads val=0 (fixes its snapshot)
#   2. s2 auto-commits UPDATE val=1 (before-image diff written to the fork)
#   3. s2 runs VACUUM -- the discard horizon is pinned by s1's snapshot, so the
#      diff for the (now committed) update must NOT be reclaimed
#   4. s1 reads again -- must still see val=0, reconstructed from the retained
#      fork diff.  If VACUUM wrongly discarded it, s1 would see val=1 or error.
#   5. After s1 commits, a fresh VACUUM may reclaim; s3 sees the current value.

setup
{
    CREATE TABLE recno_ver_retain (id int PRIMARY KEY, val int) USING recno;
    INSERT INTO recno_ver_retain VALUES (1, 0);
}

teardown
{
    DROP TABLE recno_ver_retain;
}

session s1
setup           { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1_read1   { SELECT val FROM recno_ver_retain WHERE id = 1; }
step s1_read2   { SELECT val FROM recno_ver_retain WHERE id = 1; }
step s1_commit  { COMMIT; }

session s2
step s2_update  { UPDATE recno_ver_retain SET val = 1 WHERE id = 1; }
step s2_vacuum  { VACUUM recno_ver_retain; }

session s3
step s3_read    { SELECT val FROM recno_ver_retain WHERE id = 1; }
step s3_vacuum  { VACUUM recno_ver_retain; }

# s1 pins a snapshot at val=0; s2 updates+commits then VACUUMs.  The diff must
# survive because s1's snapshot needs it, so s1_read2 still sees 0.  After s1
# commits, s3 sees the current committed value (1).
permutation
    s1_read1
    s2_update
    s2_vacuum
    s1_read2
    s1_commit
    s3_vacuum
    s3_read
