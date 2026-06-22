# Test: the VACUUM/index-delete reclamation gate must use the XID horizon, so a
# committed-deleted row is NOT physically reclaimed while a live snapshot (whose
# horizon predates the deleter) can still need it.
#
# This guards the post-pivot reclamation-gate fix: recno_handler.c's all_dead /
# index_delete / copy_for_cluster / index_build sites now call
# RecnoTupleDeadToAll(tuple, RecnoGetOldestXminHorizon(rel)) -- a heap-shaped XID
# horizon (t_xmax committed AND < oldest_xmin) -- instead of the removed HLC
# RecnoCanVacuumTimestamp, which compared the on-page word as a wall-clock
# timestamp.  Post-pivot that word packs the XID-based t_xmax, so the old gate
# compared incomparable units and could reclaim a tuple a live snapshot needs.
#
# Uses INDEX scans (enable_seqscan/bitmapscan off): this exercises BOTH the
# reclamation gate (row not physically reclaimed while s1's horizon pins it) AND
# the index-fetch visibility path (a committed-DELETED tuple still visible to an
# RR snapshot must be served through the index, not dropped).
#
# Scenario:
#   1. s1 RR snapshot reads id=2 live (pins oldest_xmin below the deleter xid).
#   2. s2 auto-commits DELETE of id=2 (t_xmax stamped, committed).
#   3. s2 VACUUMs: the delete is committed but its xmax does NOT precede s1's
#      pinned horizon -> RecnoTupleDeadToAll returns false -> row RETAINED.
#   4. s1 re-reads id=2 (seqscan) -- must STILL see (2,20): its snapshot predates
#      the delete.  If the reclamation gate wrongly reclaimed (old timestamp bug),
#      the row is gone.
#   5. After s1 commits, a fresh VACUUM reclaims; s3 no longer sees id=2.

setup
{
    CREATE TABLE recno_reclaim_gate (id int PRIMARY KEY, val int) USING recno;
    INSERT INTO recno_reclaim_gate SELECT g, g * 10 FROM generate_series(1, 5) g;
}

teardown
{
    DROP TABLE recno_reclaim_gate;
}

session s1
setup           { BEGIN ISOLATION LEVEL REPEATABLE READ; SET enable_seqscan = off; SET enable_bitmapscan = off; }
step s1_read1   { SELECT id, val FROM recno_reclaim_gate WHERE id = 2; }
step s1_read2   { SELECT id, val FROM recno_reclaim_gate WHERE id = 2; }
step s1_commit  { COMMIT; }

session s2
step s2_delete  { DELETE FROM recno_reclaim_gate WHERE id = 2; }
step s2_vacuum  { VACUUM recno_reclaim_gate; }

session s3
setup           { SET enable_seqscan = off; SET enable_bitmapscan = off; }
step s3_read    { SELECT id, val FROM recno_reclaim_gate WHERE id = 2; }
step s3_vacuum  { VACUUM recno_reclaim_gate; }

permutation
    s1_read1
    s2_delete
    s2_vacuum
    s1_read2
    s1_commit
    s3_vacuum
    s3_read
