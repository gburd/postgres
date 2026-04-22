# Test concurrent updates with RECNO tables
#
# This test specifically verifies RECNO's in-place update mechanism
# under concurrent access, ensuring proper locking and visibility.
# RECNO performs in-place updates when the new tuple fits in the
# same slot, which differs from heap's copy-on-write approach.

setup
{
    CREATE TABLE recno_concurrent (
        id int PRIMARY KEY,
        counter int DEFAULT 0,
        status text DEFAULT 'active'
    ) USING recno;

    INSERT INTO recno_concurrent (id) VALUES (1), (2), (3), (4), (5);
}

teardown
{
    DROP TABLE recno_concurrent;
}

session s1
step s1b  { BEGIN; }
step s1u1 { UPDATE recno_concurrent SET counter = counter + 1 WHERE id = 1; }
step s1u2 { UPDATE recno_concurrent SET counter = counter + 1 WHERE id = 2; }
step s1u3 { UPDATE recno_concurrent SET counter = counter + 1 WHERE id = 3; }
step s1r1 { SELECT counter FROM recno_concurrent WHERE id = 1; }
step s1r2 { SELECT counter, status FROM recno_concurrent WHERE id = 2; }
step s1ra { SELECT id, counter, status FROM recno_concurrent ORDER BY id; }
step s1d1 { DELETE FROM recno_concurrent WHERE id = 5; }
step s1us { UPDATE recno_concurrent SET status = 'updated_s1' WHERE id = 1; }
step s1c  { COMMIT; }
step s1r  { ROLLBACK; }

session s2
step s2b  { BEGIN; }
step s2u1 { UPDATE recno_concurrent SET counter = counter + 10 WHERE id = 1; }
step s2u2 { UPDATE recno_concurrent SET counter = counter + 10 WHERE id = 2; }
step s2u3 { UPDATE recno_concurrent SET counter = counter + 10 WHERE id = 3; }
step s2r1 { SELECT counter FROM recno_concurrent WHERE id = 1; }
step s2ra { SELECT id, counter, status FROM recno_concurrent ORDER BY id; }
step s2us { UPDATE recno_concurrent SET status = 'updated_s2' WHERE id = 1; }
step s2d1 { DELETE FROM recno_concurrent WHERE id = 1; }
step s2c  { COMMIT; }
step s2r  { ROLLBACK; }

session s3
step s3b  { BEGIN; }
step s3u1 { UPDATE recno_concurrent SET counter = counter + 100 WHERE id = 1; }
step s3r1 { SELECT counter FROM recno_concurrent WHERE id = 1; }
step s3ra { SELECT id, counter, status FROM recno_concurrent ORDER BY id; }
step s3ua { UPDATE recno_concurrent SET counter = counter + 1; }
step s3c  { COMMIT; }

# Test 1: Sequential in-place updates (no blocking)
permutation s1b s1u1 s1c s2b s2u1 s2c

# Test 2: Concurrent update on same row (s2 blocks on s1)
permutation s1b s1u1 s2b s2u1 s1c s2c

# Test 3: Concurrent updates on different rows (no blocking)
permutation s1b s1u1 s2b s2u2 s1c s2c

# Test 4: Three-way concurrent update (serialization)
permutation s1b s1u1 s2b s2u1 s3b s3u1 s1c s2c s3c

# Test 5: Read after concurrent updates - verify correct values
permutation s1b s1u1 s2b s2u1 s1r1 s1c s2r1 s2c

# Test 6: Multiple updates in single transaction (in-place)
permutation s1b s1u1 s1u1 s1u1 s1r1 s1c

# Test 7: Verify final state after concurrent updates
permutation s1b s1u1 s2b s2u1 s1c s2c s3b s3r1 s3c

# Test 8: Concurrent update and delete on same row
# s1 updates, s2 tries to delete - s2 should block until s1 commits
permutation s1b s1u1 s2b s2d1 s1c s2c s3b s3ra s3c

# Test 9: Rollback releases update lock
# s1 updates then rolls back, s2 should see original value
permutation s1b s1u1 s2b s2u1 s1r s2c s3b s3r1 s3c

# Test 10: Concurrent non-key column updates on same row
# Both sessions update the same row but different columns
permutation s1b s1u1 s2b s2us s1c s2c s3b s3ra s3c

# Test 11: Delete then update remaining rows in same transaction
permutation s1b s1d1 s1u1 s1u2 s1u3 s1c s3b s3ra s3c

# Test 12: Verify all rows after concurrent updates on multiple rows
permutation s1b s1u1 s1u2 s2b s2u2 s2u3 s1c s2c s3b s3ra s3c

# Test 13: Update-all-rows concurrent with single-row update
permutation s1b s1u1 s3b s3ua s1c s3c s2b s2ra s2c

# Test 14: Rollback with concurrent waiting updater
# s1 updates row, s2 waits, s1 rollback - s2 should proceed with original
permutation s1b s1u1 s2b s2u1 s1r s2r1 s2c

# Test 15: Multiple sessions updating different rows - no blocking
permutation s1b s2b s3b s1u1 s2u2 s3u1 s1u2 s2u3 s1c s2c s3c

# Test 16: EvalPlanQual - UPDATE re-evaluation after wait
# s1 updates counter, s2 also updates counter - after s1 commits,
# s2's counter+10 should use the updated value from s1
permutation s1b s1u1 s2b s2u1 s1c s2c s3b s3r1 s3c
