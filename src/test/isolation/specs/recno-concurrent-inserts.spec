# RECNO concurrent inserts test
#
# Tests concurrent INSERT operations on RECNO tables to verify
# timestamp-based MVCC and page-level concurrency control.
#
# RECNO tables use a different storage layout than heap tables,
# so concurrent inserts exercise different code paths for page
# allocation, tuple placement, and visibility checking.

setup
{
  CREATE TABLE recno_insert_test (id int, val text) USING recno;
}

teardown
{
  DROP TABLE recno_insert_test;
}

# Session 1: basic inserts
session s1
setup { BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1_insert1 { INSERT INTO recno_insert_test VALUES (1, 'session1_row1'); }
step s1_insert2 { INSERT INTO recno_insert_test VALUES (2, 'session1_row2'); }
step s1_insert3 { INSERT INTO recno_insert_test VALUES (10, 'session1_row3'); }
step s1_select  { SELECT * FROM recno_insert_test ORDER BY id; }
step s1_count   { SELECT COUNT(*) FROM recno_insert_test; }
step s1_commit  { COMMIT; }
step s1_rollback { ROLLBACK; }

# Session 2: concurrent inserts
session s2
setup { BEGIN ISOLATION LEVEL READ COMMITTED; }
step s2_insert1 { INSERT INTO recno_insert_test VALUES (3, 'session2_row1'); }
step s2_insert2 { INSERT INTO recno_insert_test VALUES (4, 'session2_row2'); }
step s2_insert3 { INSERT INTO recno_insert_test VALUES (11, 'session2_row3'); }
step s2_select  { SELECT * FROM recno_insert_test ORDER BY id; }
step s2_count   { SELECT COUNT(*) FROM recno_insert_test; }
step s2_commit  { COMMIT; }
step s2_rollback { ROLLBACK; }

# Session 3: observer / third concurrent inserter
session s3
setup { BEGIN ISOLATION LEVEL READ COMMITTED; }
step s3_insert  { INSERT INTO recno_insert_test VALUES (5, 'session3_row1'); }
step s3_select  { SELECT * FROM recno_insert_test ORDER BY id; }
step s3_count   { SELECT COUNT(*) FROM recno_insert_test; }
step s3_commit  { COMMIT; }

# Session 4: bulk insert
session s4
setup { BEGIN ISOLATION LEVEL READ COMMITTED; }
step s4_bulk_insert { INSERT INTO recno_insert_test SELECT i, 'bulk_' || i FROM generate_series(100, 109) i; }
step s4_select { SELECT * FROM recno_insert_test WHERE id >= 100 ORDER BY id; }
step s4_commit { COMMIT; }
step s4_rollback { ROLLBACK; }

# Test 1: Basic concurrent inserts - should not block each other
# since RECNO uses timestamp-based MVCC
permutation s1_insert1 s2_insert1 s1_insert2 s2_insert2 s1_commit s2_commit s3_select s3_commit

# Test 2: Interleaved inserts and reads
# Under READ COMMITTED, s2_select should not see s1's uncommitted rows
permutation s1_insert1 s2_select s2_insert1 s1_select s1_commit s2_commit s3_select s3_commit

# Test 3: Three-way concurrent inserts
permutation s1_insert1 s2_insert1 s3_insert s1_commit s2_commit s3_commit

# Test 4: Rollback visibility - uncommitted rows should not be visible
# s1 inserts and rolls back; s2 should never see s1's rows
permutation s1_insert1 s1_insert2 s2_select s1_rollback s2_select s2_commit

# Test 5: Insert after rollback - space should be reusable
permutation s1_insert1 s1_rollback s2_insert1 s2_commit s3_select s3_commit

# Test 6: Concurrent bulk insert with single-row inserts
permutation s1_insert1 s4_bulk_insert s2_insert1 s1_commit s4_commit s2_commit s3_select s3_commit

# Test 7: Bulk insert rollback should leave no visible rows
permutation s4_bulk_insert s4_rollback s3_select s3_commit

# Test 8: Count verification after concurrent operations
permutation s1_insert1 s1_insert2 s2_insert1 s2_insert2 s1_commit s2_commit s3_count s3_commit

# Test 9: Mixed commit/rollback with three sessions
# s1 commits, s2 rolls back - only s1's rows visible
permutation s1_insert1 s2_insert1 s1_insert2 s2_insert2 s1_commit s2_rollback s3_select s3_commit

# Test 10: Rapid interleaved inserts with observer
permutation s1_insert1 s2_insert1 s1_insert2 s2_insert2 s1_insert3 s2_insert3 s1_commit s2_commit s3_count s3_commit

# Test 11: Observer reads between commits
# s3 reads after s1 commits but before s2 commits
permutation s1_insert1 s2_insert1 s1_commit s3_select s2_commit s3_commit

# Test 12: Bulk insert concurrent with multiple single inserts
permutation s4_bulk_insert s1_insert1 s1_insert2 s1_insert3 s4_commit s1_commit s3_count s3_commit
