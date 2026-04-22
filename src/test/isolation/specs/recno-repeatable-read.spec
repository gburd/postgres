# Test REPEATABLE READ isolation level with RECNO tables
#
# This test verifies that REPEATABLE READ isolation works correctly
# with RECNO's timestamp-based MVCC. In REPEATABLE READ:
# - A transaction sees a consistent snapshot from its first query
# - External committed changes are NOT visible after snapshot
# - Own changes ARE visible
# - UPDATE on a row modified by another committed transaction
#   causes a serialization error (cannot serialize)

setup
{
    CREATE TABLE recno_rr_test (
        id int PRIMARY KEY,
        value int
    ) USING recno;

    INSERT INTO recno_rr_test VALUES (1, 100), (2, 200), (3, 300);
}

teardown
{
    DROP TABLE recno_rr_test;
}

session s1
setup { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1r1 { SELECT SUM(value) FROM recno_rr_test; }
step s1r2 { SELECT SUM(value) FROM recno_rr_test; }
step s1r3 { SELECT value FROM recno_rr_test WHERE id = 1; }
step s1ra { SELECT * FROM recno_rr_test ORDER BY id; }
step s1u  { UPDATE recno_rr_test SET value = value + 1 WHERE id = 1; }
step s1u2 { UPDATE recno_rr_test SET value = value + 1 WHERE id = 2; }
step s1c  { COMMIT; }

session s2
setup { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s2r  { SELECT SUM(value) FROM recno_rr_test; }
step s2ra { SELECT * FROM recno_rr_test ORDER BY id; }
step s2u1 { UPDATE recno_rr_test SET value = value + 10 WHERE id = 1; }
step s2u2 { UPDATE recno_rr_test SET value = value + 10 WHERE id = 2; }
step s2i  { INSERT INTO recno_rr_test VALUES (4, 400); }
step s2d  { DELETE FROM recno_rr_test WHERE id = 3; }
step s2c  { COMMIT; }

session s3
setup { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s3ra { SELECT * FROM recno_rr_test ORDER BY id; }
step s3c  { COMMIT; }

# Test 1: REPEATABLE READ maintains consistent snapshot
# s1 should see same SUM even after s2 commits an update
permutation s1r1 s2u1 s2c s1r2 s1c

# Test 2: New inserts not visible in REPEATABLE READ
# s2 inserts row 4, s1 should not see it
permutation s1r1 s2i s2c s1r2 s1c

# Test 3: Deletes not visible in REPEATABLE READ
# s2 deletes row 3, s1 should still see it
permutation s1r1 s2d s2c s1r2 s1c

# Test 4: Own changes are visible
# s1 updates row 1, s1 should see its own change
permutation s1r1 s1u s1r2 s1c

# Test 5: Multiple concurrent updates by other session
permutation s1r1 s2u1 s2u2 s2c s1r2 s1c

# Test 6: Cannot update row modified by concurrent committed transaction
# s2 updates row 1 and commits, s1 tries to update same row
# s1 should get "could not serialize" error
permutation s1r1 s2u1 s2c s1u s1c

# Test 7: Can update different row than concurrent transaction
# s2 updates row 1, s1 updates row 2 - should succeed
permutation s1r1 s2u1 s2c s1u2 s1c

# Test 8: Snapshot consistency across multiple reads and DML
# s1 reads, s2 does insert+update+delete+commit, s1 reads again
permutation s1r1 s2u1 s2i s2d s2c s1r2 s1ra s1c

# Test 9: Two REPEATABLE READ transactions with non-overlapping updates
permutation s1r1 s2r s1u s2u2 s1c s2c

# Test 10: Verify final state visible to new transaction after concurrent RR
permutation s1r1 s2u1 s2c s1u2 s1c s3ra s3c
