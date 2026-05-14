# Test: Per-TID retained entry reclamation respects snapshot horizon.
#
# Validates that when the sLog ops array for a hot row fills (32 slots)
# and triggers oldest-retained-entry reclamation, REPEATABLE READ
# transactions that hold older snapshots still see correct before-images.
#
# Scenario:
#   1. s1 starts REPEATABLE READ and reads a row (sees val=0)
#   2. s2 performs 35 separate auto-committed UPDATEs to the same row
#      (val=1 through val=35), filling all 32 retained entry slots and
#      triggering reclamation of the oldest entries
#   3. s1 reads the row again — must still see val=0 (the original
#      value at snapshot time), served from the sLog before-image
#
# If reclamation improperly evicts an entry needed by s1's snapshot,
# s1 would see the latest committed value (val=35) instead of val=0.

setup
{
    CREATE TABLE recno_reclaim_test (id int PRIMARY KEY, val int) USING recno;
    INSERT INTO recno_reclaim_test VALUES (1, 0);
}

teardown
{
    DROP TABLE recno_reclaim_test;
}

session s1
setup           { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1_read1   { SELECT val FROM recno_reclaim_test WHERE id = 1; }
step s1_read2   { SELECT val FROM recno_reclaim_test WHERE id = 1; }
step s1_commit  { COMMIT; }

session s2
step s2_u01 { UPDATE recno_reclaim_test SET val =  1 WHERE id = 1; }
step s2_u02 { UPDATE recno_reclaim_test SET val =  2 WHERE id = 1; }
step s2_u03 { UPDATE recno_reclaim_test SET val =  3 WHERE id = 1; }
step s2_u04 { UPDATE recno_reclaim_test SET val =  4 WHERE id = 1; }
step s2_u05 { UPDATE recno_reclaim_test SET val =  5 WHERE id = 1; }
step s2_u06 { UPDATE recno_reclaim_test SET val =  6 WHERE id = 1; }
step s2_u07 { UPDATE recno_reclaim_test SET val =  7 WHERE id = 1; }
step s2_u08 { UPDATE recno_reclaim_test SET val =  8 WHERE id = 1; }
step s2_u09 { UPDATE recno_reclaim_test SET val =  9 WHERE id = 1; }
step s2_u10 { UPDATE recno_reclaim_test SET val = 10 WHERE id = 1; }
step s2_u11 { UPDATE recno_reclaim_test SET val = 11 WHERE id = 1; }
step s2_u12 { UPDATE recno_reclaim_test SET val = 12 WHERE id = 1; }
step s2_u13 { UPDATE recno_reclaim_test SET val = 13 WHERE id = 1; }
step s2_u14 { UPDATE recno_reclaim_test SET val = 14 WHERE id = 1; }
step s2_u15 { UPDATE recno_reclaim_test SET val = 15 WHERE id = 1; }
step s2_u16 { UPDATE recno_reclaim_test SET val = 16 WHERE id = 1; }
step s2_u17 { UPDATE recno_reclaim_test SET val = 17 WHERE id = 1; }
step s2_u18 { UPDATE recno_reclaim_test SET val = 18 WHERE id = 1; }
step s2_u19 { UPDATE recno_reclaim_test SET val = 19 WHERE id = 1; }
step s2_u20 { UPDATE recno_reclaim_test SET val = 20 WHERE id = 1; }
step s2_u21 { UPDATE recno_reclaim_test SET val = 21 WHERE id = 1; }
step s2_u22 { UPDATE recno_reclaim_test SET val = 22 WHERE id = 1; }
step s2_u23 { UPDATE recno_reclaim_test SET val = 23 WHERE id = 1; }
step s2_u24 { UPDATE recno_reclaim_test SET val = 24 WHERE id = 1; }
step s2_u25 { UPDATE recno_reclaim_test SET val = 25 WHERE id = 1; }
step s2_u26 { UPDATE recno_reclaim_test SET val = 26 WHERE id = 1; }
step s2_u27 { UPDATE recno_reclaim_test SET val = 27 WHERE id = 1; }
step s2_u28 { UPDATE recno_reclaim_test SET val = 28 WHERE id = 1; }
step s2_u29 { UPDATE recno_reclaim_test SET val = 29 WHERE id = 1; }
step s2_u30 { UPDATE recno_reclaim_test SET val = 30 WHERE id = 1; }
step s2_u31 { UPDATE recno_reclaim_test SET val = 31 WHERE id = 1; }
step s2_u32 { UPDATE recno_reclaim_test SET val = 32 WHERE id = 1; }
step s2_u33 { UPDATE recno_reclaim_test SET val = 33 WHERE id = 1; }
step s2_u34 { UPDATE recno_reclaim_test SET val = 34 WHERE id = 1; }
step s2_u35 { UPDATE recno_reclaim_test SET val = 35 WHERE id = 1; }

# s1 takes snapshot, then s2 performs 35 updates (exceeding 32-slot limit),
# then s1 reads again and must still see the original value.
permutation
    s1_read1
    s2_u01 s2_u02 s2_u03 s2_u04 s2_u05 s2_u06 s2_u07 s2_u08 s2_u09 s2_u10
    s2_u11 s2_u12 s2_u13 s2_u14 s2_u15 s2_u16 s2_u17 s2_u18 s2_u19 s2_u20
    s2_u21 s2_u22 s2_u23 s2_u24 s2_u25 s2_u26 s2_u27 s2_u28 s2_u29 s2_u30
    s2_u31 s2_u32 s2_u33 s2_u34 s2_u35
    s1_read2
    s1_commit
