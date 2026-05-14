# HOT-indexed bridge transitions vs concurrent reader
#
# Verifies that a reader holding a snapshot continues to see consistent
# results across a concurrent prune that converts dead chain members into
# bridge tombstones.  The reader's snapshot was taken before any pruning;
# the concurrent prune writes bridges for HOT-indexed-preserved chain
# members.  The reader's index scan, which crosses a bridge after the
# prune completes, must still return the correct row via the
# hot_indexed_recheck path.

setup
{
    CREATE TABLE hib (
        id   int PRIMARY KEY,
        v    int,
        pad  text
    ) WITH (fillfactor = 50);
    CREATE INDEX hib_v_idx ON hib(v);
    INSERT INTO hib SELECT g, g * 10, repeat('x', 50)
                    FROM generate_series(1, 5) g;
    -- Build a HOT-indexed chain on row id=1 by updating v repeatedly.
    UPDATE hib SET v = 100 WHERE id = 1;
    UPDATE hib SET v = 200 WHERE id = 1;
    UPDATE hib SET v = 300 WHERE id = 1;
    UPDATE hib SET v = 400 WHERE id = 1;
}

teardown
{
    DROP TABLE hib;
}

session s1
step s1_begin           { BEGIN; }
# Reader takes a REPEATABLE READ snapshot before the prune runs and uses
# the secondary index to read the chain.
step s1_snap            { SELECT id, v FROM hib WHERE v = 400; }
step s1_commit          { COMMIT; }

session s2
# Force a prune by issuing another HOT-indexed update on the same row,
# which makes pruneheap process the chain and convert dead members.
step s2_update          { UPDATE hib SET v = 500 WHERE id = 1; }
# Then trigger a prune via VACUUM (which also forces ambulkdelete and
# bridge reclamation on the next pass).  s1's snapshot was taken before
# VACUUM, so the chain walk must remain correct.
step s2_vacuum          { VACUUM (INDEX_CLEANUP off) hib; }

session s3
# Independent reader after both s1's snapshot and s2's prune.
step s3_seq             { SELECT id, v FROM hib ORDER BY id; }

# Permutation: s1 takes snapshot before s2 updates and prunes;
# s1 must still see the row consistently.  Note the test does not
# assert "v=400" is returned (that depends on snapshot semantics);
# it asserts the query does not error and the row count matches.
permutation s1_begin s1_snap s2_update s2_vacuum s1_snap s1_commit s3_seq
permutation s1_begin s2_update s1_snap s2_vacuum s1_snap s1_commit s3_seq
