# Test SERIALIZABLE isolation level with RECNO tables
#
# This test verifies that SERIALIZABLE isolation detects read-write
# conflicts correctly with RECNO's timestamp-based MVCC and anti-dependency
# tracking. Serializable Snapshot Isolation (SSI) detects dangerous
# structures (pivot transactions) and aborts to prevent anomalies.

setup
{
    CREATE TABLE recno_ser_test (
        id int PRIMARY KEY,
        category text,
        balance int
    ) USING recno;

    INSERT INTO recno_ser_test VALUES
        (1, 'A', 100),
        (2, 'A', 200),
        (3, 'B', 300),
        (4, 'B', 400);
}

teardown
{
    DROP TABLE recno_ser_test;
}

session s1
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s1ra { SELECT SUM(balance) FROM recno_ser_test WHERE category = 'A'; }
step s1rb { SELECT SUM(balance) FROM recno_ser_test WHERE category = 'B'; }
step s1r1 { SELECT balance FROM recno_ser_test WHERE id = 1; }
step s1ua { UPDATE recno_ser_test SET balance = balance + 50 WHERE category = 'A'; }
step s1ub { UPDATE recno_ser_test SET balance = balance - 50 WHERE category = 'B'; }
step s1ia { INSERT INTO recno_ser_test VALUES (5, 'A', 500); }
step s1c  { COMMIT; }
step s1r  { ROLLBACK; }

session s2
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s2ra { SELECT SUM(balance) FROM recno_ser_test WHERE category = 'A'; }
step s2rb { SELECT SUM(balance) FROM recno_ser_test WHERE category = 'B'; }
step s2r1 { SELECT balance FROM recno_ser_test WHERE id = 1; }
step s2ua { UPDATE recno_ser_test SET balance = balance + 50 WHERE category = 'A'; }
step s2ub { UPDATE recno_ser_test SET balance = balance - 50 WHERE category = 'B'; }
step s2ib { INSERT INTO recno_ser_test VALUES (6, 'B', 600); }
step s2c  { COMMIT; }
step s2r  { ROLLBACK; }

session s3
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s3ra { SELECT * FROM recno_ser_test ORDER BY id; }
step s3r1 { SELECT balance FROM recno_ser_test WHERE id = 1; }
step s3r2 { SELECT balance FROM recno_ser_test WHERE id = 2; }
step s3u2 { UPDATE recno_ser_test SET balance = balance + 100 WHERE id = 2; }
step s3c  { COMMIT; }

# Test 1: Classic write skew (should detect conflict)
# s1 reads A, updates B; s2 reads B, updates A
# Creates a dangerous structure - one should be aborted
permutation s1ra s2rb s1ub s2ua s1c s2c

# Test 2: Read-write dependency (may cause serialization failure)
# s3 reads a row that s2 updates
permutation s3r1 s2ua s3u2 s2c s3c

# Test 3: Successful serializable execution (no conflicts)
# Sequential execution: s1 finishes before s2 starts
permutation s1ra s1ub s1c s2ra s2ua s2c

# Test 4: Write-write on non-overlapping sets
# s1 updates B, s2 updates A - no read-write dependencies
permutation s1ub s2ua s1c s2c

# Test 5: Phantom reads prevented
# s1 reads category A, s2 updates A and commits, s1 reads A again
# s1 should still see original snapshot
permutation s1ra s2ua s2c s1ra s1c

# Test 6: Insert phantom prevention
# s1 reads SUM of A, s2 inserts into B - no conflict
permutation s1ra s2ib s1c s2c

# Test 7: Insert causing conflict
# s1 reads A and inserts into A, s2 reads A and inserts into B
# The read-write on A creates a dependency
permutation s1ra s2ra s1ia s2ib s1c s2c

# Test 8: Read-only transaction safe with concurrent writes
# s3 only reads - should never cause serialization failure
permutation s3ra s1ub s2ua s1c s2c s3c

# Test 9: Write skew with rollback recovery
# s1 reads A, updates B, s2 reads B, updates A
# s2 gets serialization error, rolls back - s1 succeeds
permutation s1ra s2rb s1ub s2ua s1c s2c

# Test 10: Three-session interaction
# s1 reads A, s2 reads B, s3 reads and updates a single row
permutation s1ra s2rb s3r1 s3u2 s1ub s2ua s3c s1c s2c

# Test 11: Verify correct final state after serializable operations
permutation s1ra s1ub s1c s3ra s3c
