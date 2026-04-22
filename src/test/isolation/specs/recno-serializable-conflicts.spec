# RECNO serializable isolation conflicts test
#
# Tests serializable isolation level on RECNO tables. RECNO uses
# timestamp-based MVCC, which interacts with SSI (Serializable
# Snapshot Isolation) differently than XID-based heap.
#
# This test focuses on specific conflict patterns:
# - Read-write conflicts (anti-dependencies)
# - Write-write conflicts
# - Write skew anomalies
# - Phantom prevention
# - Multi-session dangerous structures

setup
{
  CREATE TABLE recno_ssi_test (id int PRIMARY KEY, class int, value int) USING recno;
  INSERT INTO recno_ssi_test VALUES (1, 1, 10);
  INSERT INTO recno_ssi_test VALUES (2, 1, 20);
  INSERT INTO recno_ssi_test VALUES (3, 2, 30);
  INSERT INTO recno_ssi_test VALUES (4, 2, 40);
}

teardown
{
  DROP TABLE recno_ssi_test;
}

session s1
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s1_read_class1  { SELECT SUM(value) FROM recno_ssi_test WHERE class = 1; }
step s1_read_class2  { SELECT SUM(value) FROM recno_ssi_test WHERE class = 2; }
step s1_read_all     { SELECT * FROM recno_ssi_test ORDER BY id; }
step s1_insert_class2 { INSERT INTO recno_ssi_test VALUES (5, 2, 50); }
step s1_update       { UPDATE recno_ssi_test SET value = value + 5 WHERE class = 1; }
step s1_delete       { DELETE FROM recno_ssi_test WHERE id = 4; }
step s1_commit       { COMMIT; }
step s1_rollback     { ROLLBACK; }

session s2
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s2_read_class1  { SELECT SUM(value) FROM recno_ssi_test WHERE class = 1; }
step s2_read_class2  { SELECT SUM(value) FROM recno_ssi_test WHERE class = 2; }
step s2_read_all     { SELECT * FROM recno_ssi_test ORDER BY id; }
step s2_insert_class1 { INSERT INTO recno_ssi_test VALUES (6, 1, 60); }
step s2_update       { UPDATE recno_ssi_test SET value = value + 10 WHERE class = 2; }
step s2_delete       { DELETE FROM recno_ssi_test WHERE id = 2; }
step s2_commit       { COMMIT; }
step s2_rollback     { ROLLBACK; }

session s3
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s3_read_all     { SELECT * FROM recno_ssi_test ORDER BY id; }
step s3_read_class1  { SELECT SUM(value) FROM recno_ssi_test WHERE class = 1; }
step s3_update_both  { UPDATE recno_ssi_test SET value = value + 100; }
step s3_insert       { INSERT INTO recno_ssi_test VALUES (7, 1, 70); }
step s3_commit       { COMMIT; }

# ============================================================
# Read-write conflicts (anti-dependencies)
# ============================================================

# Test 1: Basic read-write conflict
# s1 reads class 1, s2 modifies class 1
# This should cause serialization failure in one session
permutation s1_read_class1 s2_insert_class1 s1_commit s2_commit

# Test 2: Read-write on different classes - no conflict
permutation s1_read_class1 s2_update s1_commit s2_commit

# Test 3: Both read then write to same class
permutation s1_read_class1 s2_read_class1 s1_update s2_insert_class1 s1_commit s2_commit

# ============================================================
# Write-write conflicts
# ============================================================

# Test 4: Write-write conflict on overlapping rows
permutation s1_update s2_update s1_commit s2_commit

# Test 5: Write-write on non-overlapping sets (should succeed)
permutation s1_update s2_update s1_commit s2_commit

# ============================================================
# Write skew anomalies
# ============================================================

# Test 6: Classic write skew
# s1 reads class 1, inserts into class 2
# s2 reads class 2, inserts into class 1
# Creates dangerous structure - should cause serialization failure
permutation s1_read_class1 s2_read_class2 s1_insert_class2 s2_insert_class1 s1_commit s2_commit

# Test 7: Write skew with updates instead of inserts
permutation s1_read_class1 s2_read_class2 s1_insert_class2 s2_update s1_commit s2_commit

# ============================================================
# Read-only transaction safety
# ============================================================

# Test 8: Read-only transaction with concurrent updates
# s3 should see consistent snapshot and not cause failure
permutation s3_read_all s1_update s2_update s1_commit s2_commit s3_commit

# Test 9: Read-only in the middle of two writers
permutation s1_read_class1 s3_read_all s2_update s1_update s2_commit s1_commit s3_commit

# ============================================================
# Delete interactions
# ============================================================

# Test 10: Read then delete creates anti-dependency
permutation s1_read_class1 s2_delete s1_commit s2_commit

# Test 11: Concurrent deletes on different rows
permutation s1_delete s2_delete s1_commit s2_commit

# ============================================================
# Three-way conflicts
# ============================================================

# Test 12: Three sessions, all read and modify
# At least one should fail
permutation s1_read_class1 s2_read_class2 s3_update_both s1_insert_class2 s2_insert_class1 s3_commit s1_commit s2_commit

# Test 13: Three sessions with inserts creating phantom conflicts
permutation s1_read_class1 s2_read_class2 s3_read_class1 s1_insert_class2 s2_insert_class1 s3_insert s1_commit s2_commit s3_commit

# ============================================================
# Rollback recovery
# ============================================================

# Test 14: Rollback avoids conflict - second transaction can commit
permutation s1_read_class1 s2_read_class2 s1_insert_class2 s2_insert_class1 s1_rollback s2_commit

# Test 15: Update conflict resolved by rollback
permutation s1_read_class1 s2_update s1_update s2_commit s1_rollback
