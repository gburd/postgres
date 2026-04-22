# Test READ COMMITTED isolation level with RECNO tables
#
# This test verifies that READ COMMITTED isolation works correctly
# with RECNO's timestamp-based MVCC. Under READ COMMITTED:
# - Each statement sees its own snapshot
# - Committed changes from other transactions become visible
# - Uncommitted changes are never visible
# - UPDATE/DELETE re-evaluates quals after waiting (EvalPlanQual)

setup
{
    CREATE TABLE recno_rc_test (
        id int PRIMARY KEY,
        value int
    ) USING recno;

    INSERT INTO recno_rc_test VALUES (1, 10), (2, 20), (3, 30);
}

teardown
{
    DROP TABLE recno_rc_test;
}

session s1
setup { BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1r1 { SELECT value FROM recno_rc_test WHERE id = 1; }
step s1r2 { SELECT value FROM recno_rc_test WHERE id = 1; }
step s1ra { SELECT * FROM recno_rc_test ORDER BY id; }
step s1u  { UPDATE recno_rc_test SET value = value + 1 WHERE id = 1; }
step s1d  { DELETE FROM recno_rc_test WHERE id = 3; }
step s1i  { INSERT INTO recno_rc_test VALUES (4, 40); }
step s1c  { COMMIT; }
step s1rb { ROLLBACK; }

session s2
setup { BEGIN ISOLATION LEVEL READ COMMITTED; }
step s2r  { SELECT value FROM recno_rc_test WHERE id = 1; }
step s2ra { SELECT * FROM recno_rc_test ORDER BY id; }
step s2u  { UPDATE recno_rc_test SET value = value + 10 WHERE id = 1; }
step s2u_cond { UPDATE recno_rc_test SET value = value + 10 WHERE id = 1 AND value < 15; }
step s2d  { DELETE FROM recno_rc_test WHERE id = 3; }
step s2c  { COMMIT; }

session s3
setup { BEGIN ISOLATION LEVEL READ COMMITTED; }
step s3ra { SELECT * FROM recno_rc_test ORDER BY id; }
step s3c  { COMMIT; }

# Test 1: READ COMMITTED sees other committed changes
# s1 reads, s2 updates and commits, s1 reads again - should see new value
permutation s1r1 s2u s2c s1r2 s1c

# Test 2: UPDATE waits for concurrent UPDATE, then re-evaluates
permutation s1u s2u s1c s2c

# Test 3: Multiple reads within transaction see latest committed state
# s1r1 sees 10, s2 updates but hasn't committed, s1r1 still sees 10,
# s2 commits, s1r2 now sees 20
permutation s1r1 s2u s1r1 s2c s1r2 s1c

# Test 4: INSERT visibility - new rows appear after commit
permutation s1i s2ra s1c s2ra s2c

# Test 5: DELETE visibility - deleted rows disappear after commit
permutation s1d s2ra s1c s2ra s2c

# Test 6: EvalPlanQual with conditional update
# s1 updates value to 11, s2 has WHERE value < 15
# After s1 commits, s2 re-evaluates and value=11 still matches
permutation s1u s2u_cond s1c s2c s3ra s3c

# Test 7: EvalPlanQual with conditional update - condition fails
# s1 updates value by 10 (to 20), s2 has WHERE value < 15
# After s1 commits, s2 re-evaluates but value=20 fails the condition
permutation s2u s2u_cond s2c s1r1 s1c

# Test 8: Uncommitted changes never visible
permutation s1u s2r s1c s2r s2c

# Test 9: Rollback means changes were never visible
permutation s1u s1i s2ra s1rb s2ra s2c

# Test 10: Concurrent delete of same row
permutation s1d s2d s1c s2c s3ra s3c

# Test 11: Mixed operations - insert, update, delete across sessions
permutation s1u s1i s2u s1c s2c s3ra s3c
