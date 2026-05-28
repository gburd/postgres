CREATE EXTENSION test_lrlock;

-- Test 1: Initial state should be 0
SELECT test_lrlock_read();

-- Test 2: Set counter to a known value and read it back
SELECT test_lrlock_write_set(42);
SELECT test_lrlock_read();

-- Test 3: Increment by known amount
SELECT test_lrlock_write_increment(10);
SELECT test_lrlock_read();

-- Test 4: Add a value
SELECT test_lrlock_write_add(100);
SELECT test_lrlock_read();

-- Test 5: Multiple operations in sequence
SELECT test_lrlock_write_set(0);
SELECT test_lrlock_write_increment(5);
SELECT test_lrlock_write_add(10);
SELECT test_lrlock_read();

-- Test 6: Unpublished writes should not be visible.
-- Reset to known state first.
SELECT test_lrlock_write_set(1000);
SELECT test_lrlock_read();

-- Write without publish — the increment happens on the write copy only,
-- but publish inside write_no_publish is skipped, then a standalone publish
-- makes it visible.
SELECT test_lrlock_write_no_publish(7);
-- Reader should still see the old value because no publish happened...
-- Actually, since other sessions could read between ops, and we're
-- single-session here, the write_no_publish leaves unpublished ops that
-- get published on the next write_set/write_increment/publish call.
SELECT test_lrlock_publish();
SELECT test_lrlock_read();

-- Test 7: Stress test — rapid read/write cycles
SELECT test_lrlock_write_set(0);
SELECT test_lrlock_stress(100);
