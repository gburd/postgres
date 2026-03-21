# Copyright (c) 2024-2026, PostgreSQL Global Development Group
#
# Test crash recovery for per-relation UNDO operations.
#
# These tests verify that the per-relation UNDO subsystem (OVUndo*)
# handles crashes gracefully:
#   - Server starts up cleanly after a crash with per-relation UNDO tables
#   - Tables remain accessible after recovery
#   - New operations work after crash recovery
#
# NOTE: The test_undo_tam does not WAL-log its data page modifications,
# so data inserted since the last checkpoint may be lost after a crash.
# These tests verify crash safety (no corruption, clean restart) rather
# than crash durability of individual rows.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('relundo_crash');
$node->init;
$node->append_conf(
	"postgresql.conf", qq(
autovacuum = off
log_min_messages = warning
shared_preload_libraries = ''
));
$node->start;

# Install the test_undo_tam extension
$node->safe_psql("postgres", "CREATE EXTENSION test_undo_tam");

# ================================================================
# Test 1: Server starts cleanly after crash with per-relation UNDO tables
# ================================================================

$node->safe_psql("postgres", qq(
CREATE TABLE relundo_t1 (id int, data text) USING test_undo_tam;
INSERT INTO relundo_t1 VALUES (1, 'before_crash');
INSERT INTO relundo_t1 VALUES (2, 'also_before_crash');
CHECKPOINT;
));

# Verify data exists before crash
my $result = $node->safe_psql("postgres",
	"SELECT count(*) FROM relundo_t1");
is($result, '2', 'data exists before crash');

# Crash the server
$node->stop('immediate');
$node->start;

# Server should start cleanly -- the table should be accessible
# (data may be present if checkpoint captured it)
$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM relundo_t1");
ok(defined $result, 'table is accessible after crash recovery');

# ================================================================
# Test 2: INSERT works after crash recovery
# ================================================================

# New inserts should work after crash recovery
$node->safe_psql("postgres",
	"INSERT INTO relundo_t1 VALUES (100, 'after_crash')");

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM relundo_t1 WHERE id = 100");
is($result, '1', 'INSERT works after crash recovery');

# ================================================================
# Test 3: UNDO chain introspection works after crash recovery
# ================================================================

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM test_undo_tam_dump_chain('relundo_t1')");
ok($result >= 0, 'UNDO chain dump works after crash recovery');

# ================================================================
# Test 4: Multiple tables survive crash
# ================================================================

$node->safe_psql("postgres", qq(
CREATE TABLE relundo_a (id int) USING test_undo_tam;
CREATE TABLE relundo_b (id int) USING test_undo_tam;
INSERT INTO relundo_a VALUES (1);
INSERT INTO relundo_b VALUES (10);
CHECKPOINT;
));

$node->stop('immediate');
$node->start;

# Both tables should be accessible
$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM relundo_a");
ok(defined $result, 'relundo_a accessible after crash');

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM relundo_b");
ok(defined $result, 'relundo_b accessible after crash');

# Can still insert into both
$node->safe_psql("postgres", qq(
INSERT INTO relundo_a VALUES (2);
INSERT INTO relundo_b VALUES (20);
));

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM relundo_a WHERE id = 2");
is($result, '1', 'INSERT into relundo_a works after crash');

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM relundo_b WHERE id = 20");
is($result, '1', 'INSERT into relundo_b works after crash');

# ================================================================
# Test 5: Coexistence with heap tables through crash
# ================================================================

$node->safe_psql("postgres", qq(
CREATE TABLE relundo_coexist (id int, data text) USING test_undo_tam;
CREATE TABLE heap_coexist (id int, data text);
INSERT INTO relundo_coexist VALUES (1, 'relundo_row');
INSERT INTO heap_coexist VALUES (1, 'heap_row');
CHECKPOINT;
));

$node->stop('immediate');
$node->start;

# Heap table data should survive (heap AM does WAL logging)
$result = $node->safe_psql("postgres",
	"SELECT data FROM heap_coexist WHERE id = 1");
is($result, 'heap_row', 'heap table data survives crash');

# Per-relation UNDO table should at least be accessible
$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM relundo_coexist");
ok(defined $result, 'per-relation UNDO table accessible after crash');

# ================================================================
# Test 6: VACUUM after crash
# ================================================================

$node->safe_psql("postgres", "VACUUM relundo_coexist");
pass('VACUUM on per-relation UNDO table after crash does not error');

# ================================================================
# Test 7: DROP TABLE after crash recovery
# ================================================================

$node->safe_psql("postgres", qq(
CREATE TABLE relundo_drop_test (id int) USING test_undo_tam;
INSERT INTO relundo_drop_test VALUES (1);
CHECKPOINT;
));

$node->stop('immediate');
$node->start;

# DROP should work after crash recovery
$node->safe_psql("postgres", "DROP TABLE relundo_drop_test");

# Verify it's gone
my ($ret, $stdout, $stderr) = $node->psql("postgres",
	"SELECT * FROM relundo_drop_test");
like($stderr, qr/does not exist/, 'table is dropped after crash recovery');

# ================================================================
# Test 8: Multiple sequential crashes
# ================================================================

$node->safe_psql("postgres", qq(
CREATE TABLE relundo_multi (id int) USING test_undo_tam;
INSERT INTO relundo_multi VALUES (1);
CHECKPOINT;
));

# First crash
$node->stop('immediate');
$node->start;

$node->safe_psql("postgres", qq(
INSERT INTO relundo_multi VALUES (2);
CHECKPOINT;
));

# Second crash
$node->stop('immediate');
$node->start;

$node->safe_psql("postgres",
	"INSERT INTO relundo_multi VALUES (3)");

# Table should be usable after multiple crashes
$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM relundo_multi WHERE id = 3");
is($result, '1', 'table usable after multiple sequential crashes');

# ================================================================
# Test 9: CREATE TABLE after crash recovery
# ================================================================

# Creating a new per-relation UNDO table after crash should work
$node->safe_psql("postgres", qq(
CREATE TABLE relundo_post_crash (id int) USING test_undo_tam;
INSERT INTO relundo_post_crash VALUES (42);
));

$result = $node->safe_psql("postgres",
	"SELECT id FROM relundo_post_crash");
is($result, '42', 'new table created and populated after crash');

# Cleanup
$node->stop;

done_testing();
