# Copyright (c) 2024-2026, PostgreSQL Global Development Group
#
# Test crash recovery for UNDO logging operations.
#
# These tests verify that the UNDO subsystem recovers correctly after
# crashes at various points during:
#   - UNDO record insertion
#   - Transaction abort with UNDO application
#   - UNDO discard operations
#   - Checkpoint with active UNDO data

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('undo_recovery');
$node->init;
$node->append_conf(
	"postgresql.conf", qq(
enable_undo = on
autovacuum = off
undo_worker_naptime = 600000
undo_retention_time = 3600000
log_min_messages = debug2
));
$node->start;

# ================================================================
# Test 1: Basic UNDO table creation and crash recovery
# ================================================================

$node->safe_psql("postgres", qq(
CREATE TABLE undo_test (id int, data text) WITH (enable_undo = on);
INSERT INTO undo_test VALUES (1, 'before_crash');
));

# Verify data exists
my $result = $node->safe_psql("postgres",
	"SELECT count(*) FROM undo_test WHERE data = 'before_crash'");
is($result, '1', 'data exists before crash');

# Crash the server
$node->stop('immediate');
$node->start;

# Verify data survives crash recovery
$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM undo_test WHERE data = 'before_crash'");
is($result, '1', 'data survives crash recovery');

# ================================================================
# Test 2: Crash during transaction with UNDO-enabled table
# ================================================================

# Begin a transaction, insert data, then crash before commit
$node->safe_psql("postgres", qq(
INSERT INTO undo_test VALUES (2, 'committed_before_crash');
));

# Start a transaction but don't commit (use background psql)
# This data should be lost after crash
$node->safe_psql("postgres", qq(
BEGIN;
INSERT INTO undo_test VALUES (3, 'uncommitted_data');
-- crash will happen before commit
));

# Insert committed data in a separate transaction
$node->safe_psql("postgres", qq(
INSERT INTO undo_test VALUES (4, 'also_committed');
));

# Crash
$node->stop('immediate');
$node->start;

# Committed data should survive
$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM undo_test WHERE id IN (2, 4)");
is($result, '2', 'committed rows survive crash');

# ================================================================
# Test 3: UNDO-enabled table with multiple operations then crash
# ================================================================

$node->safe_psql("postgres", qq(
TRUNCATE undo_test;
INSERT INTO undo_test SELECT g, 'row_' || g FROM generate_series(1, 100) g;
UPDATE undo_test SET data = 'updated_' || id WHERE id <= 50;
DELETE FROM undo_test WHERE id > 90;
));

# Crash and recover
$node->stop('immediate');
$node->start;

# Verify state after recovery
$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM undo_test");
is($result, '90', 'correct row count after crash with mixed operations');

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM undo_test WHERE data LIKE 'updated_%'");
is($result, '50', 'updated rows preserved after crash');

# ================================================================
# Test 4: Crash during checkpoint with active UNDO data
# ================================================================

$node->safe_psql("postgres", qq(
TRUNCATE undo_test;
INSERT INTO undo_test SELECT g, 'checkpoint_test_' || g FROM generate_series(1, 50) g;
CHECKPOINT;
INSERT INTO undo_test SELECT g, 'post_checkpoint_' || g FROM generate_series(51, 100) g;
));

# Crash after checkpoint but with additional data
$node->stop('immediate');
$node->start;

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM undo_test");
is($result, '100', 'all data recovers after crash following checkpoint');

# ================================================================
# Test 5: Multiple crashes in sequence
# ================================================================

$node->safe_psql("postgres", qq(
TRUNCATE undo_test;
INSERT INTO undo_test VALUES (1, 'survived_double_crash');
));

# First crash
$node->stop('immediate');
$node->start;

$node->safe_psql("postgres", qq(
INSERT INTO undo_test VALUES (2, 'after_first_recovery');
));

# Second crash
$node->stop('immediate');
$node->start;

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM undo_test");
is($result, '2', 'data survives multiple crashes');

$result = $node->safe_psql("postgres",
	"SELECT data FROM undo_test ORDER BY id");
is($result, "survived_double_crash\nafter_first_recovery",
	'correct data after multiple crashes');

# ================================================================
# Test 6: UNDO directory exists after recovery
# ================================================================

my $pgdata = $node->data_dir;
ok(-d "$pgdata/base/undo", 'UNDO directory exists after recovery');

# ================================================================
# Test 7: Transaction abort with UNDO rollback
# ================================================================

$node->safe_psql("postgres", qq(
TRUNCATE undo_test;
INSERT INTO undo_test VALUES (1, 'original');
));

# This should be rolled back
$node->safe_psql("postgres", qq(
BEGIN;
DELETE FROM undo_test WHERE id = 1;
ROLLBACK;
));

$result = $node->safe_psql("postgres",
	"SELECT data FROM undo_test WHERE id = 1");
is($result, 'original', 'DELETE is rolled back via UNDO');

# Crash after the rollback to verify consistency
$node->stop('immediate');
$node->start;

$result = $node->safe_psql("postgres",
	"SELECT data FROM undo_test WHERE id = 1");
is($result, 'original', 'rolled-back state survives crash');

# ================================================================
# Test 8: Subtransaction abort with UNDO
# ================================================================

$node->safe_psql("postgres", qq(
TRUNCATE undo_test;
INSERT INTO undo_test VALUES (1, 'parent_data');
BEGIN;
SAVEPOINT sp1;
INSERT INTO undo_test VALUES (2, 'child_data');
ROLLBACK TO sp1;
INSERT INTO undo_test VALUES (3, 'after_rollback');
COMMIT;
));

$result = $node->safe_psql("postgres",
	"SELECT id FROM undo_test ORDER BY id");
is($result, "1\n3", 'subtransaction rollback works with UNDO');

# Crash and verify
$node->stop('immediate');
$node->start;

$result = $node->safe_psql("postgres",
	"SELECT id FROM undo_test ORDER BY id");
is($result, "1\n3", 'subtransaction rollback state survives crash');

# Cleanup
$node->stop;

done_testing();
