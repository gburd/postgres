# RECNO VACUUM with concurrent DML test
#
# Tests VACUUM operations on RECNO tables with concurrent
# INSERT/UPDATE/DELETE operations. RECNO uses timestamp-based
# visibility, so VACUUM behavior differs from heap.
#
# Key areas tested:
# - Space reclamation after DELETE
# - VACUUM with concurrent INSERT/UPDATE/DELETE
# - Long-running transaction preventing cleanup
# - VACUUM ANALYZE concurrent behavior
# - VACUUM FULL with concurrent access

setup
{
  CREATE TABLE recno_vacuum_test (id int PRIMARY KEY, val text, data text) USING recno;
  INSERT INTO recno_vacuum_test SELECT i, 'value' || i, repeat('x', 100) FROM generate_series(1, 100) i;
}

teardown
{
  DROP TABLE recno_vacuum_test;
}

session s1
step s1_begin  { BEGIN; }
step s1_delete { DELETE FROM recno_vacuum_test WHERE id <= 50; }
step s1_delete_all { DELETE FROM recno_vacuum_test; }
step s1_update { UPDATE recno_vacuum_test SET data = repeat('y', 100) WHERE id > 50 AND id <= 75; }
step s1_update_wide { UPDATE recno_vacuum_test SET data = repeat('w', 500) WHERE id > 90; }
step s1_select_count { SELECT COUNT(*) FROM recno_vacuum_test; }
step s1_select_ids { SELECT id FROM recno_vacuum_test WHERE id <= 10 ORDER BY id; }
step s1_commit { COMMIT; }
step s1_rollback { ROLLBACK; }

session s2
step s2_vacuum { VACUUM recno_vacuum_test; }
step s2_vacuum_verbose { VACUUM (VERBOSE) recno_vacuum_test; }
step s2_vacuum_analyze { VACUUM ANALYZE recno_vacuum_test; }
step s2_vacuum_full { VACUUM FULL recno_vacuum_test; }
step s2_analyze { ANALYZE recno_vacuum_test; }
step s2_select_count { SELECT COUNT(*) FROM recno_vacuum_test; }
step s2_select_dead { SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'recno_vacuum_test'; }

session s3
step s3_begin  { BEGIN; }
step s3_insert { INSERT INTO recno_vacuum_test VALUES (101, 'new_row', repeat('z', 100)); }
step s3_insert_bulk { INSERT INTO recno_vacuum_test SELECT i, 'new_' || i, repeat('n', 50) FROM generate_series(102, 111) i; }
step s3_update { UPDATE recno_vacuum_test SET val = 'updated' WHERE id = 60; }
step s3_delete { DELETE FROM recno_vacuum_test WHERE id = 80; }
step s3_select_count { SELECT COUNT(*) FROM recno_vacuum_test; }
step s3_select { SELECT COUNT(*) FROM recno_vacuum_test WHERE id > 50; }
step s3_commit { COMMIT; }

session s4
step s4_begin { BEGIN; }
step s4_hold_snapshot { SELECT COUNT(*) FROM recno_vacuum_test; }
step s4_select { SELECT COUNT(*) FROM recno_vacuum_test; }
step s4_commit { COMMIT; }

# ============================================================
# Basic VACUUM after DML
# ============================================================

# Test 1: VACUUM after deletes - should reclaim space from deleted tuples
permutation s1_delete s1_commit s2_vacuum s2_select_count

# Test 2: VACUUM after updates - should reclaim old versions
permutation s1_update s1_commit s2_vacuum s2_select_count

# Test 3: VACUUM after delete all rows
permutation s1_delete_all s1_commit s2_vacuum s2_select_count

# ============================================================
# VACUUM with concurrent INSERT
# ============================================================

# Test 4: Concurrent insert during VACUUM - insert should not block VACUUM
permutation s1_delete s1_commit s3_begin s2_vacuum s3_insert s3_commit s2_select_count

# Test 5: Bulk insert concurrent with VACUUM
permutation s1_delete s1_commit s3_begin s3_insert_bulk s2_vacuum s3_commit s2_select_count

# ============================================================
# VACUUM with concurrent UPDATE
# ============================================================

# Test 6: Concurrent update during VACUUM
permutation s1_delete s1_commit s3_begin s2_vacuum s3_update s3_commit s2_select_count

# Test 7: Wide update concurrent with VACUUM (tuple grows beyond original size)
permutation s1_begin s1_update_wide s1_commit s2_vacuum s2_select_count

# ============================================================
# VACUUM with concurrent DELETE
# ============================================================

# Test 8: Concurrent delete during VACUUM
permutation s3_begin s3_delete s2_vacuum s3_commit s2_select_count

# ============================================================
# VACUUM ANALYZE tests
# ============================================================

# Test 9: VACUUM ANALYZE with concurrent operations
permutation s1_update s1_commit s3_begin s3_insert s2_vacuum_analyze s3_commit s2_select_count

# Test 10: ANALYZE alone with concurrent DML
permutation s3_begin s3_insert s2_analyze s3_commit s2_select_count

# ============================================================
# Long-running transaction horizon tests
# ============================================================

# Test 11: VACUUM blocked by long-running transaction holding snapshot
# s4 holds a snapshot, s1 deletes, s2 VACUUMs - VACUUM may not reclaim
# tuples visible to s4's snapshot
permutation s4_begin s4_hold_snapshot s1_delete s1_commit s2_vacuum s4_select s4_commit s2_select_count

# Test 12: VACUUM after long-running transaction ends
# Same as above but s4 commits before VACUUM
permutation s4_begin s4_hold_snapshot s1_delete s1_commit s4_commit s2_vacuum s2_select_count

# Test 13: Open transaction with uncommitted delete
# s1 has open transaction, s2 VACUUM should complete but may not reclaim
permutation s1_begin s1_delete s2_vacuum s1_commit s2_select_count

# Test 14: Open transaction rolled back, then VACUUM
permutation s1_begin s1_delete s1_rollback s2_vacuum s2_select_count

# ============================================================
# VACUUM FULL tests
# ============================================================

# Test 15: VACUUM FULL after deletes - should compact table
permutation s1_delete s1_commit s2_vacuum_full s2_select_count

# Test 16: VACUUM FULL blocks concurrent DML
# VACUUM FULL takes AccessExclusiveLock, so s3 should block
permutation s1_delete s1_commit s2_vacuum_full s3_begin s3_insert s3_commit s2_select_count

# ============================================================
# Multiple concurrent DML during VACUUM
# ============================================================

# Test 17: Insert + update + delete all concurrent with VACUUM
permutation s1_delete s1_commit s3_begin s3_insert s3_update s2_vacuum s3_delete s3_commit s2_select_count

# Test 18: Sequential delete-vacuum-insert cycle
permutation s1_delete s1_commit s2_vacuum s3_insert s3_commit s2_select_count

# Test 19: Multiple VACUUMs - second should be fast (no dead tuples)
permutation s1_delete s1_commit s2_vacuum s2_vacuum s2_select_count
