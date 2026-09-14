# RECNO old-snapshot read (BUG 1): a REPEATABLE READ reader whose snapshot
# predates a committed in-place (non-key) UPDATE must still see the OLD value
# reconstructed from the per-backend UNDO version chain (t_verptr ->
# RecnoReconstructVisibleVersion), not the new value and not an absent row.
# A reader whose snapshot follows the UPDATE sees the new value.  The TID is
# stable (in-place update), so no row moves.  This is the RECNO analog of
# flux_inplace.spec and the direct regression guard for the MVCC-correctness
# bug where an old snapshot saw the row as ABSENT after an in-place UPDATE.

setup
{
  CREATE TABLE recno_os (id int PRIMARY KEY, v bigint) USING recno;
  INSERT INTO recno_os VALUES (5, 100);
}

teardown
{
  DROP TABLE recno_os;
}

session s_old
# Establish the old snapshot BEFORE the writer commits by reading in setup.
setup           { BEGIN ISOLATION LEVEL REPEATABLE READ;
                  SELECT v FROM recno_os WHERE id = 5; }
step o_read_seq { SET enable_seqscan = on; SET enable_indexscan = off; SET enable_bitmapscan = off;
                  SELECT v FROM recno_os WHERE id = 5; }   # old snapshot: still 100 (seqscan)
step o_read_idx { SET enable_seqscan = off; SET enable_indexscan = on; SET enable_bitmapscan = off;
                  SELECT v FROM recno_os WHERE id = 5; }   # old snapshot: still 100 (index)
step o_commit   { COMMIT; }

session s_writer
step w_update   { UPDATE recno_os SET v = 999 WHERE id = 5; }   # in-place, commits

session s_new
step n_read     { SELECT v FROM recno_os WHERE id = 5; }   # fresh snapshot: sees 999

# Writer updates + commits, then the old snapshot re-reads (must see 100 via
# both seq and index scans), a fresh reader sees 999, then the old snapshot
# commits.
permutation w_update o_read_seq o_read_idx n_read o_commit
