# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Verify crash recovery and WAL replay for RECNO tables.
#
# Tests:
#   - INSERT data, crash, restart, verify no data loss
#   - UPDATE/DELETE crash recovery
#   - WAL consistency (RECNO WAL records present and valid)
#   - Multiple crash/restart cycles
#   - Overflow data crash recovery
#   - Uncommitted transaction rollback after crash

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;

# Initialize node with recovery-friendly settings
my $node = PostgreSQL::Test::Cluster->new('recno_recovery');
$node->init;
$node->append_conf('postgresql.conf', 'fsync = on');
$node->append_conf('postgresql.conf', 'wal_level = replica');
$node->append_conf('postgresql.conf', 'max_wal_senders = 5');
$node->start;

# Enable WAL inspection
$node->safe_psql('postgres', 'CREATE EXTENSION pg_walinspect');

# ============================================================
# Test 1: Basic INSERT crash recovery
# ============================================================

$node->safe_psql('postgres',
	'CREATE TABLE recno_recovery (id int PRIMARY KEY, val text, counter int) USING recno');

$node->safe_psql('postgres',
	"INSERT INTO recno_recovery SELECT i, 'initial_' || i, 0
	 FROM generate_series(1, 100) i");

my $count_before = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_recovery');
is($count_before, '100', "Initial data inserted");

# Crash the server immediately (no clean shutdown)
$node->stop('immediate');

# Restart -- this triggers WAL recovery
$node->start;

my $count_after = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_recovery');
is($count_after, '100', "All rows recovered after crash (INSERT)");

# Verify specific rows survived
my $first_row = $node->safe_psql('postgres',
	'SELECT val FROM recno_recovery WHERE id = 1');
is($first_row, 'initial_1', "First row value correct after recovery");

my $last_row = $node->safe_psql('postgres',
	'SELECT val FROM recno_recovery WHERE id = 100');
is($last_row, 'initial_100', "Last row value correct after recovery");

# ============================================================
# Test 2: Mixed DML crash recovery
# ============================================================

my $start_lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');

$node->safe_psql('postgres',
	"BEGIN;
	 INSERT INTO recno_recovery VALUES (101, 'new_row', 1);
	 UPDATE recno_recovery SET counter = counter + 1 WHERE id <= 50;
	 DELETE FROM recno_recovery WHERE id > 95 AND id <= 100;
	 COMMIT");

my $end_lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');

# Capture pre-crash state
$count_before = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_recovery');
my $sum_before = $node->safe_psql('postgres',
	'SELECT SUM(counter) FROM recno_recovery WHERE id <= 50');

# Crash
$node->stop('immediate');
$node->start;

# Verify complete recovery
$count_after = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_recovery');
is($count_after, $count_before, "Row count matches after mixed DML crash recovery");

my $sum_after = $node->safe_psql('postgres',
	'SELECT SUM(counter) FROM recno_recovery WHERE id <= 50');
is($sum_after, $sum_before, "Updated counter values match after recovery");

my $new_row = $node->safe_psql('postgres',
	'SELECT val FROM recno_recovery WHERE id = 101');
is($new_row, 'new_row', "Inserted row recovered correctly");

my $deleted_count = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_recovery WHERE id > 95 AND id <= 100');
is($deleted_count, '0', "Deleted rows remain absent after recovery");

# ============================================================
# Test 3: Verify WAL contains RECNO records
# ============================================================

# Re-create extensions after crash recovery (WAL inspection)
$node->safe_psql('postgres', 'CREATE EXTENSION IF NOT EXISTS pg_walinspect');

# Generate fresh WAL records for inspection
# Query database directly for LSN to avoid ordering issues after recovery
$start_lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');

$node->safe_psql('postgres',
	"INSERT INTO recno_recovery VALUES (200, 'wal_test_insert', 42)");
$node->safe_psql('postgres',
	'UPDATE recno_recovery SET counter = 99 WHERE id = 200');
$node->safe_psql('postgres',
	'DELETE FROM recno_recovery WHERE id = 200');

$end_lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');

my $wal_records = $node->safe_psql('postgres',
	"SELECT COUNT(*) FROM pg_get_wal_records_info('$start_lsn', '$end_lsn')
	 WHERE resource_manager = 'RECNO'");
cmp_ok($wal_records, '>', '0', "WAL contains RECNO records");

# Check for specific WAL record types
my $insert_records = $node->safe_psql('postgres',
	"SELECT COUNT(*) FROM pg_get_wal_records_info('$start_lsn', '$end_lsn')
	 WHERE resource_manager = 'RECNO' AND record_type LIKE '%INSERT%'");
cmp_ok($insert_records, '>=', '1', "WAL contains RECNO INSERT records");

my $update_records = $node->safe_psql('postgres',
	"SELECT COUNT(*) FROM pg_get_wal_records_info('$start_lsn', '$end_lsn')
	 WHERE resource_manager = 'RECNO' AND record_type LIKE '%UPDATE%'");
cmp_ok($update_records, '>=', '1', "WAL contains RECNO UPDATE records");

my $delete_records = $node->safe_psql('postgres',
	"SELECT COUNT(*) FROM pg_get_wal_records_info('$start_lsn', '$end_lsn')
	 WHERE resource_manager = 'RECNO' AND record_type LIKE '%DELETE%'");
cmp_ok($delete_records, '>=', '1', "WAL contains RECNO DELETE records");

# ============================================================
# Test 4: Multiple crash/restart cycles
# ============================================================

$node->safe_psql('postgres',
	"INSERT INTO recno_recovery VALUES (301, 'cycle1', 1)");

$node->stop('immediate');
$node->start;

my $cycle1 = $node->safe_psql('postgres',
	'SELECT counter FROM recno_recovery WHERE id = 301');
is($cycle1, '1', "Data survives first crash cycle");

# Operations after first recovery
$node->safe_psql('postgres',
	"INSERT INTO recno_recovery VALUES (302, 'cycle2', 2)");
$node->safe_psql('postgres',
	'UPDATE recno_recovery SET counter = 10 WHERE id = 301');

# Second crash
$node->stop('immediate');
$node->start;

my $cycle2_insert = $node->safe_psql('postgres',
	'SELECT counter FROM recno_recovery WHERE id = 302');
is($cycle2_insert, '2', "Insert after first recovery survives second crash");

my $cycle2_update = $node->safe_psql('postgres',
	'SELECT counter FROM recno_recovery WHERE id = 301');
is($cycle2_update, '10', "Update after first recovery survives second crash");

# Third crash -- tests accumulated WAL replay
$node->safe_psql('postgres',
	"INSERT INTO recno_recovery VALUES (303, 'cycle3', 3)");

$node->stop('immediate');
$node->start;

my $cycle3 = $node->safe_psql('postgres',
	'SELECT counter FROM recno_recovery WHERE id = 303');
is($cycle3, '3', "Data survives third crash cycle");

# ============================================================
# Test 5: VACUUM recovery
# ============================================================

$start_lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');
$node->safe_psql('postgres', 'VACUUM recno_recovery');
$end_lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');

my $pre_vacuum_count = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_recovery');

# Crash after VACUUM
$node->stop('immediate');
$node->start;

my $post_vacuum_count = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_recovery');
is($post_vacuum_count, $pre_vacuum_count,
	"Data consistent after VACUUM + crash recovery");

# ============================================================
# Test 6: Overflow data crash recovery
# ============================================================

diag("Starting Test 6: Overflow data crash recovery");

diag("Creating overflow table WITHOUT PRIMARY KEY for debugging");
$node->safe_psql('postgres',
	'CREATE TABLE recno_overflow_recovery (
		id int,
		small_col text,
		large_col text
	) USING recno');

# Insert rows with overflow-sized data
diag("Inserting row 1 with 10KB data");
$node->safe_psql('postgres',
	"INSERT INTO recno_overflow_recovery VALUES (1, 'small', repeat('X', 10000))");
diag("Inserting row 2 with 20KB data");
$node->safe_psql('postgres',
	"INSERT INTO recno_overflow_recovery VALUES (2, 'another', repeat('Y', 20000))");
diag("Inserting row 3 with 50KB data");
$node->safe_psql('postgres',
	"INSERT INTO recno_overflow_recovery VALUES (3, 'mixed', repeat('Z', 50000))");

# Verify before crash
diag("Fetching length of row 3");
my $ov_len_before = $node->safe_psql('postgres',
	'SELECT length(large_col) FROM recno_overflow_recovery WHERE id = 3');
is($ov_len_before, '50000', "Overflow data stored correctly before crash");

# Modify overflow data (update overflow -> different overflow size)
diag("About to UPDATE row 1 with new overflow data");
eval {
	$node->safe_psql('postgres',
		"UPDATE recno_overflow_recovery SET large_col = repeat('W', 30000) WHERE id = 1");
	diag("UPDATE completed successfully");
};
if ($@) {
	diag("UPDATE FAILED with error: $@");
	die "Server crashed during UPDATE of overflow data";
}

# Delete an overflow row
diag("About to DELETE row 2");
eval {
	$node->safe_psql('postgres',
		'DELETE FROM recno_overflow_recovery WHERE id = 2');
	diag("DELETE completed successfully");
};
if ($@) {
	diag("DELETE FAILED with error: $@");
	die "Server crashed during DELETE of overflow data";
}

# Add CHECKPOINT to ensure data is flushed (temporary debug)
diag("About to CHECKPOINT");
eval {
	$node->safe_psql('postgres', 'CHECKPOINT');
	diag("CHECKPOINT completed successfully");
};
if ($@) {
	diag("CHECKPOINT FAILED with error: $@");
	die "Server crashed during CHECKPOINT";
}

# Crash
diag("Stopping server (immediate)");
$node->stop('immediate');
diag("Server stopped, starting recovery");
$node->start;
diag("Server restarted after crash recovery");

# Verify overflow data integrity after recovery
diag("Checking row count after recovery");

# Try to fetch each row individually to see which one fails
diag("Trying to fetch row 1");
eval {
	my $row1 = $node->safe_psql('postgres',
		'SELECT id FROM recno_overflow_recovery WHERE id = 1');
	diag("Row 1 fetch OK: id=$row1");
};
if ($@) {
	diag("Row 1 FAILED: $@");
}

diag("Trying to fetch row 3");
eval {
	my $row3 = $node->safe_psql('postgres',
		'SELECT id FROM recno_overflow_recovery WHERE id = 3');
	diag("Row 3 fetch OK: id=$row3");
};
if ($@) {
	diag("Row 3 FAILED: $@");
}

diag("Now trying COUNT(*)");
my $ov_count;
eval {
	$ov_count = $node->safe_psql('postgres',
		'SELECT COUNT(*) FROM recno_overflow_recovery');
	diag("Got count: $ov_count");
};
if ($@) {
	diag("Query FAILED after recovery: $@");
	die "Failed to query table after crash recovery";
}
is($ov_count, '2', "Correct row count after overflow crash recovery");

my $ov_updated = $node->safe_psql('postgres',
	'SELECT length(large_col) FROM recno_overflow_recovery WHERE id = 1');
is($ov_updated, '30000', "Updated overflow data length correct after recovery");

my $ov_content = $node->safe_psql('postgres',
	"SELECT large_col = repeat('W', 30000) FROM recno_overflow_recovery WHERE id = 1");
is($ov_content, 't', "Updated overflow data content matches after recovery");

my $ov_original = $node->safe_psql('postgres',
	"SELECT large_col = repeat('Z', 50000) FROM recno_overflow_recovery WHERE id = 3");
is($ov_original, 't', "Untouched overflow data survives crash recovery");

my $ov_deleted = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_overflow_recovery WHERE id = 2');
is($ov_deleted, '0', "Deleted overflow row not present after recovery");

# ============================================================
# Test 7: Uncommitted transaction rollback after crash
# ============================================================

# Start a background session with an uncommitted transaction
my $bg = $node->background_psql('postgres');
$bg->query_safe('BEGIN');
$bg->query_safe(
	"INSERT INTO recno_overflow_recovery VALUES (10, 'uncommitted', repeat('U', 15000))");
# Do NOT commit -- crash the server

$node->stop('immediate');

# The background process may have already terminated when the server crashed.
# Attempt to reconnect/clear, but don't fail if it's already dead.
eval { $bg->reconnect_and_clear; };

$node->start;

# Uncommitted overflow insert should not be visible after crash recovery
my $uncommitted = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_overflow_recovery WHERE id = 10');
is($uncommitted, '0', "Uncommitted overflow data rolled back after crash");

# Quit the background session if it's still alive
eval { $bg->quit; };

# ============================================================
# Test 8: Checkpoint + crash recovery
# ============================================================

# Insert data, checkpoint, insert more, then crash
$node->safe_psql('postgres',
	"INSERT INTO recno_recovery SELECT i, 'pre_ckpt_' || i, i
	 FROM generate_series(400, 450) i");

$node->safe_psql('postgres', 'CHECKPOINT');

$node->safe_psql('postgres',
	"INSERT INTO recno_recovery SELECT i, 'post_ckpt_' || i, i
	 FROM generate_series(451, 500) i");

my $total_before = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_recovery');

$node->stop('immediate');
$node->start;

my $total_after = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_recovery');
is($total_after, $total_before,
	"Both pre- and post-checkpoint data recovered after crash");

# Verify specific rows from both sides of checkpoint
my $pre_ckpt = $node->safe_psql('postgres',
	'SELECT val FROM recno_recovery WHERE id = 425');
is($pre_ckpt, 'pre_ckpt_425', "Pre-checkpoint row recovered correctly");

my $post_ckpt = $node->safe_psql('postgres',
	'SELECT val FROM recno_recovery WHERE id = 475');
is($post_ckpt, 'post_ckpt_475', "Post-checkpoint row recovered correctly");

# ============================================================
# Test 9: Uncommitted in-place UPDATE rollback after crash
# ============================================================
#
# A committed row is updated in place (fixed-width counter column, so the
# new value overwrites the old bytes on the page) by an uncommitted
# transaction.  The server then crashes.  After recovery the row must show
# its ORIGINAL committed value: the uncommitted in-place update must not be
# visible.  This is the in-place-UPDATE analogue of Test 7's uncommitted
# INSERT rollback.

# Seed a committed row with a known counter value.
$node->safe_psql('postgres',
	"INSERT INTO recno_recovery VALUES (600, 'inplace_seed', 11)");

my $seed = $node->safe_psql('postgres',
	'SELECT counter FROM recno_recovery WHERE id = 600');
is($seed, '11', "In-place UPDATE seed row committed");

# Uncommitted in-place update of the fixed-width counter column.
my $bg2 = $node->background_psql('postgres');
$bg2->query_safe('BEGIN');
$bg2->query_safe('UPDATE recno_recovery SET counter = 99 WHERE id = 600');
# Do NOT commit.  Force the dirty page (carrying the uncommitted new value)
# to disk via a CHECKPOINT from a separate connection, so crash recovery
# starts from an on-disk page that already holds counter = 99.  Without
# this the dirty buffer is simply lost on immediate crash and the on-disk
# page trivially retains the old value.
$node->safe_psql('postgres', 'CHECKPOINT');

# Crash the server with the update still in flight.
$node->stop('immediate');

eval { $bg2->reconnect_and_clear; };

$node->start;

my $inplace = $node->safe_psql('postgres',
	'SELECT counter FROM recno_recovery WHERE id = 600');
is($inplace, '11',
	"Uncommitted in-place UPDATE rolled back after crash (old value visible)");

eval { $bg2->quit; };

done_testing();
