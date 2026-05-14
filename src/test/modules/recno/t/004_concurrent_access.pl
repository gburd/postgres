#!/usr/bin/perl

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Test concurrent access to RECNO tables.
# Verifies that concurrent operations (INSERT, UPDATE, DELETE) work correctly,
# that committed data is visible after commit, and that basic locking works.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;
use Time::HiRes qw(usleep);

# Initialize cluster
my $node = PostgreSQL::Test::Cluster->new('recno_concurrent');
$node->init;
$node->start;

# ============================================================
# Test 1: Concurrent INSERT operations from separate sessions
# ============================================================

note "Testing concurrent INSERT operations";

# Create test table
$node->safe_psql('postgres',
    'CREATE TABLE recno_concurrent (id int PRIMARY KEY, val int, data text) USING recno');

# Start multiple backend connections using background_psql
my $conn1 = $node->background_psql('postgres');
my $conn2 = $node->background_psql('postgres');
my $conn3 = $node->background_psql('postgres');

# Each connection inserts in its own transaction and commits
$conn1->query_safe('BEGIN');
$conn1->query_safe("INSERT INTO recno_concurrent VALUES (1, 100, 'conn1')");
$conn1->query_safe('COMMIT');

$conn2->query_safe('BEGIN');
$conn2->query_safe("INSERT INTO recno_concurrent VALUES (2, 200, 'conn2')");
$conn2->query_safe('COMMIT');

$conn3->query_safe('BEGIN');
$conn3->query_safe("INSERT INTO recno_concurrent VALUES (3, 300, 'conn3')");
$conn3->query_safe('COMMIT');

# Verify all data is visible after commits
my $count = $node->safe_psql('postgres', 'SELECT COUNT(*) FROM recno_concurrent');
is($count, '3', 'All inserts visible after commit');

my $sum = $node->safe_psql('postgres', 'SELECT SUM(val) FROM recno_concurrent');
is($sum, '600', 'All inserted values correct');

# ============================================================
# Test 2: Concurrent UPDATE operations on different rows
# ============================================================

note "Testing concurrent UPDATE operations on different rows";

$conn1->query_safe('BEGIN');
$conn2->query_safe('BEGIN');

# Each connection updates a different row
$conn1->query_safe("UPDATE recno_concurrent SET val = 101 WHERE id = 1");
$conn2->query_safe("UPDATE recno_concurrent SET val = 201 WHERE id = 2");

$conn1->query_safe('COMMIT');
$conn2->query_safe('COMMIT');

# Verify updates
my $val1 = $node->safe_psql('postgres', 'SELECT val FROM recno_concurrent WHERE id = 1');
is($val1, '101', 'Concurrent update on row 1 succeeded');

my $val2 = $node->safe_psql('postgres', 'SELECT val FROM recno_concurrent WHERE id = 2');
is($val2, '201', 'Concurrent update on row 2 succeeded');

# ============================================================
# Test 3: Committed data visibility
# ============================================================

note "Testing committed data visibility across sessions";

# conn1 commits an update
$conn1->query_safe('BEGIN');
$conn1->query_safe("UPDATE recno_concurrent SET val = 102 WHERE id = 1");
$conn1->query_safe('COMMIT');

# conn2 should see the committed update
my $val = $conn2->query_safe('SELECT val FROM recno_concurrent WHERE id = 1');
is($val, '102', 'Committed update visible to other session');

# ============================================================
# Test 4: Row-level locking (FOR UPDATE)
# ============================================================

note "Testing row-level locking";

# Verify that FOR UPDATE at least works without error on a single session
$conn1->query_safe('BEGIN');
$conn1->query_safe('SELECT * FROM recno_concurrent WHERE id = 1 FOR UPDATE');
$conn1->query_safe('COMMIT');
pass('FOR UPDATE succeeds on RECNO table');

# ============================================================
# Test 5: Concurrent DELETE operations on different rows
# ============================================================

note "Testing concurrent DELETE operations";

# Insert more test data
$node->safe_psql('postgres',
    "INSERT INTO recno_concurrent VALUES (10, 1000, 'delete1'), (11, 1100, 'delete2')");

$conn1->query_safe('BEGIN');
$conn2->query_safe('BEGIN');

# Concurrent deletes of different rows
$conn1->query_safe('DELETE FROM recno_concurrent WHERE id = 10');
$conn2->query_safe('DELETE FROM recno_concurrent WHERE id = 11');

# Each should succeed without blocking
$conn1->query_safe('COMMIT');
$conn2->query_safe('COMMIT');

$count = $node->safe_psql('postgres',
    'SELECT COUNT(*) FROM recno_concurrent WHERE id IN (10, 11)');
is($count, '0', 'Concurrent deletes of different rows succeeded');

# ============================================================
# Test 6: MVCC with timestamps
# ============================================================

note "Testing MVCC with timestamps";

# Create table with timestamp column
$node->safe_psql('postgres',
    'CREATE TABLE recno_mvcc_ts (
        id int PRIMARY KEY,
        val int,
        ts timestamp DEFAULT clock_timestamp()
    ) USING recno');

# Insert rows in separate transactions to get different timestamps
$node->safe_psql('postgres',
    "INSERT INTO recno_mvcc_ts (id, val) VALUES (1, 100)");

usleep(10000);  # Small delay

$node->safe_psql('postgres',
    "INSERT INTO recno_mvcc_ts (id, val) VALUES (2, 200)");

# Verify timestamps are different and ordered
my $ts_check = $node->safe_psql('postgres',
    "SELECT COUNT(DISTINCT ts) > 1 AND
            MIN(ts) < MAX(ts)
     FROM recno_mvcc_ts");
is($ts_check, 't', 'MVCC timestamps are distinct and ordered');

# ============================================================
# Test 7: Concurrent updates on non-indexed columns
# ============================================================

note "Testing concurrent updates on non-indexed columns";

$node->safe_psql('postgres',
    'CREATE TABLE recno_inplace_concurrent (
        id int PRIMARY KEY,
        indexed int,
        non_indexed text
    ) USING recno');

$node->safe_psql('postgres',
    'CREATE INDEX ON recno_inplace_concurrent(indexed)');

$node->safe_psql('postgres',
    "INSERT INTO recno_inplace_concurrent VALUES (1, 10, 'data1'), (2, 20, 'data2')");

# Concurrent updates (non-indexed column, different rows)
$conn1->query_safe('BEGIN');
$conn2->query_safe('BEGIN');

$conn1->query_safe("UPDATE recno_inplace_concurrent SET non_indexed = 'updated1' WHERE id = 1");
$conn2->query_safe("UPDATE recno_inplace_concurrent SET non_indexed = 'updated2' WHERE id = 2");

# Both should succeed without blocking (different rows)
$conn1->query_safe('COMMIT');
$conn2->query_safe('COMMIT');

# Verify updates
my $result = $node->safe_psql('postgres',
    "SELECT COUNT(*) FROM recno_inplace_concurrent WHERE non_indexed LIKE 'updated%'");
is($result, '2', 'Concurrent non-indexed column updates succeeded');

# ============================================================
# Test 8: Concurrent operations with VACUUM
# ============================================================

note "Testing concurrent operations with VACUUM";

$node->safe_psql('postgres',
    'CREATE TABLE recno_vacuum_concurrent (
        id int PRIMARY KEY,
        val int
    ) USING recno');

# Insert and delete some rows to create dead tuples
$node->safe_psql('postgres',
    'INSERT INTO recno_vacuum_concurrent SELECT i, i FROM generate_series(1, 100) i');
$node->safe_psql('postgres',
    'DELETE FROM recno_vacuum_concurrent WHERE id <= 50');

# Run VACUUM while another session reads
$conn1->query_safe('BEGIN');
my $pre_vacuum = $conn1->query_safe('SELECT COUNT(*) FROM recno_vacuum_concurrent');

# VACUUM in another session
$node->safe_psql('postgres', 'VACUUM recno_vacuum_concurrent');

my $post_vacuum = $conn1->query_safe('SELECT COUNT(*) FROM recno_vacuum_concurrent');
$conn1->query_safe('COMMIT');

is($pre_vacuum, '50', 'Correct count before VACUUM');
is($post_vacuum, '50', 'Correct count after concurrent VACUUM');

# ============================================================
# Test 9: TRUNCATE on RECNO table
# ============================================================

note "Testing TRUNCATE on RECNO table";

# Verify TRUNCATE works correctly
my $before_truncate = $node->safe_psql('postgres',
    'SELECT COUNT(*) FROM recno_vacuum_concurrent');
cmp_ok($before_truncate, '>', '0', 'Table has rows before TRUNCATE');

$node->safe_psql('postgres', 'TRUNCATE recno_vacuum_concurrent');

my $after_truncate = $node->safe_psql('postgres',
    'SELECT COUNT(*) FROM recno_vacuum_concurrent');
is($after_truncate, '0', 'TRUNCATE removes all rows from RECNO table');

# ============================================================
# Test 10: Rapid concurrent inserts from multiple sessions
# ============================================================

note "Testing rapid concurrent inserts";

$node->safe_psql('postgres',
    'CREATE TABLE recno_rapid (id serial PRIMARY KEY, val int, session_id int) USING recno');

# Each session inserts a batch of rows
$conn1->query_safe('BEGIN');
$conn2->query_safe('BEGIN');
$conn3->query_safe('BEGIN');

$conn1->query_safe("INSERT INTO recno_rapid (val, session_id) SELECT i, 1 FROM generate_series(1, 50) i");
$conn2->query_safe("INSERT INTO recno_rapid (val, session_id) SELECT i, 2 FROM generate_series(1, 50) i");
$conn3->query_safe("INSERT INTO recno_rapid (val, session_id) SELECT i, 3 FROM generate_series(1, 50) i");

$conn1->query_safe('COMMIT');
$conn2->query_safe('COMMIT');
$conn3->query_safe('COMMIT');

$count = $node->safe_psql('postgres', 'SELECT COUNT(*) FROM recno_rapid');
is($count, '150', 'All rapid concurrent inserts succeeded');

my $session_counts = $node->safe_psql('postgres',
    'SELECT COUNT(DISTINCT session_id) FROM recno_rapid');
is($session_counts, '3', 'All sessions contributed rows');

# ============================================================
# Test 11: Cleanup and final verification
# ============================================================

note "Cleanup and final verification";

# Drop all test tables
$node->safe_psql('postgres', 'DROP TABLE IF EXISTS recno_concurrent CASCADE');
$node->safe_psql('postgres', 'DROP TABLE IF EXISTS recno_mvcc_ts CASCADE');
$node->safe_psql('postgres', 'DROP TABLE IF EXISTS recno_inplace_concurrent CASCADE');
$node->safe_psql('postgres', 'DROP TABLE IF EXISTS recno_vacuum_concurrent CASCADE');
$node->safe_psql('postgres', 'DROP TABLE IF EXISTS recno_rapid CASCADE');

# Verify cleanup
my $tables = $node->safe_psql('postgres',
    "SELECT COUNT(*) FROM pg_class WHERE relname LIKE 'recno_%' AND relkind = 'r'");
is($tables, '0', 'All test tables cleaned up');

# Close background psql sessions
eval { $conn1->quit; };
eval { $conn2->quit; };
eval { $conn3->quit; };

done_testing();
