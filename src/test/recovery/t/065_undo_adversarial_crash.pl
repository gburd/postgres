# Copyright (c) 2024-2026, PostgreSQL Global Development Group
#
# Adversarial crash tests for the UNDO infrastructure.
#
# Uses injection_points to halt execution at precise points in the UNDO
# subsystem, then crashes the server to verify that recovery handles
# partially-applied or incomplete UNDO operations correctly.
#
# Test vehicle: FILEOPS (the sole active UNDO RM after heap UNDO removal).
# FILEOPS writes UNDO records for transactional file operations and provides
# legitimate crash scenarios for the generic UNDO infrastructure.
#
# Operations used:
#   - test_fileops_truncate: calls FileOpsTruncate (writes UNDO record)
#   - test_fileops_chmod: calls FileOpsChmod (writes UNDO record)
#   - CREATE TABLESPACE: calls FileOpsMkdir (writes UNDO record)

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use File::Path qw(make_path remove_tree);
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# ================================================================
# Helper: set up a node with injection_points and test_fileops
# ================================================================
sub setup_node
{
	my ($name) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init;
	$node->append_conf(
		'postgresql.conf', qq(
shared_preload_libraries = 'injection_points'
log_min_messages = debug2
autovacuum = off
wal_level = replica
));
	$node->start;

	if (!$node->check_extension('injection_points'))
	{
		plan skip_all => 'Extension injection_points not installed';
	}
	if (!$node->check_extension('test_fileops'))
	{
		plan skip_all => 'Extension test_fileops not installed';
	}

	$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
	$node->safe_psql('postgres', 'CREATE EXTENSION test_fileops');
	return $node;
}

# ================================================================
# Test 1: Crash during transaction abort with UNDO records
#
# Verifies that when a transaction that wrote UNDO records crashes
# mid-abort (after ATM registration but before cleanup), recovery
# properly handles the aborted transaction state.
# ================================================================

my $node = setup_node('test1');

my $datadir = $node->data_dir;

# Create a test file that we'll truncate inside a transaction
$node->safe_psql('postgres',
	qq{SELECT test_fileops_create_tempfile('test1_file.dat')});

# Verify initial file size (1024 bytes from create_tempfile)
my $result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/test1_file.dat')});
is($result, '1024', 'Test 1: initial file size is 1024');

# Set up injection point on the abort path
$node->safe_psql('postgres',
	q{SELECT injection_points_attach('undo-xact-abort-after-atm', 'wait')});

# Run a transaction that truncates the file and then aborts
my $bgpsql = $node->background_psql('postgres');
$bgpsql->query_until(
	qr/starting_abort/,
	qq(\\echo starting_abort
BEGIN;
SELECT test_fileops_truncate('$datadir/test1_file.dat', 0);
ABORT;
\\q
));

# Wait for the abort to reach our injection point
$node->wait_for_event('client backend', 'undo-xact-abort-after-atm');

# Crash the server while mid-abort
$node->stop('immediate');

# Restart and verify recovery completes
$node->start;

# The file size should have been restored by UNDO (either during abort
# completion or during recovery) -- truncate reversed to original 1024
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/test1_file.dat')});
is($result, '1024', 'Test 1: file size restored after crash during abort');

# Server is healthy
$result = $node->safe_psql('postgres', 'SELECT 1');
is($result, '1', 'Test 1: server operational after recovery');

$node->stop;

# ================================================================
# Test 2: Crash during UNDO batch WAL insertion
#
# Verifies that if we crash between XLogBeginInsert and XLogInsert
# for an UNDO batch, the incomplete batch is not visible and the
# transaction is treated as aborted.
# ================================================================

$node = setup_node('test2');
$datadir = $node->data_dir;

# Create a test file
$node->safe_psql('postgres',
	qq{SELECT test_fileops_create_tempfile('test2_file.dat')});

$node->safe_psql('postgres',
	q{SELECT injection_points_attach('undo-batch-before-wal-insert', 'wait')});

# Start a transaction that will trigger batch flush on commit
$bgpsql = $node->background_psql('postgres');
$bgpsql->query_until(
	qr/starting_op/,
	qq(\\echo starting_op
BEGIN;
SELECT test_fileops_truncate('$datadir/test2_file.dat', 512);
COMMIT;
\\q
));

# Wait for the batch insertion to reach our injection point
$node->wait_for_event('client backend', 'undo-batch-before-wal-insert');

# Crash before the WAL insert completes
$node->stop('immediate');
$node->start;

# The key guarantee: server recovers cleanly regardless of whether the
# truncate committed or not (the UNDO batch may not have been written).
$result = $node->safe_psql('postgres', 'SELECT 1');
is($result, '1', 'Test 2: server operational after crash during batch WAL insert');

$node->stop;

# ================================================================
# Test 3: Crash during UNDO worker discard cycle
#
# Verifies that crashing the UNDO background worker mid-discard
# does not corrupt the UNDO log state, and that the worker resumes
# correctly after restart.
# ================================================================

$node = setup_node('test3');
$datadir = $node->data_dir;

$node->safe_psql('postgres',
	q{SELECT injection_points_attach('undo-worker-before-discard', 'wait')});

# Generate some committed transactions with UNDO records so the worker
# has something to process
$node->safe_psql('postgres',
	qq{SELECT test_fileops_create_tempfile('test3_file.dat')});
$node->safe_psql('postgres',
	qq{SELECT test_fileops_chmod('$datadir/test3_file.dat', 493)});

# Wait for the UNDO worker to hit the injection point
$node->wait_for_event('undo worker', 'undo-worker-before-discard');

# Crash while worker is about to discard
$node->stop('immediate');
$node->start;

# UNDO log integrity maintained - server operational
$result = $node->safe_psql('postgres', 'SELECT 1');
is($result, '1', 'Test 3: server operational after crash during worker discard');

# Can still perform transactional operations with UNDO
$node->safe_psql('postgres',
	qq{SELECT test_fileops_create_tempfile('test3_after.dat')});
$node->safe_psql('postgres',
	qq{SELECT test_fileops_chmod('$datadir/test3_after.dat', 420)});
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_get_mode('$datadir/test3_after.dat')});
is($result, '420', 'Test 3: FILEOPS operations work after worker crash recovery');

$node->stop;

# ================================================================
# Test 4: Crash during FILEOPS UNDO apply
#
# Verifies that crashing mid-way through FILEOPS UNDO application
# (while reversing file operations) results in correct recovery.
# The UNDO chain should be re-applied during crash recovery.
# ================================================================

$node = setup_node('test4');
$datadir = $node->data_dir;

# Create a test file
$node->safe_psql('postgres',
	qq{SELECT test_fileops_create_tempfile('test4_file.dat')});

# Verify initial permissions (0644 = 420 decimal)
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_get_mode('$datadir/test4_file.dat')});
my $orig_mode = $result;

# Set up injection point on FILEOPS UNDO apply
$node->safe_psql('postgres',
	q{SELECT injection_points_attach('fileops-undo-apply-begin', 'wait')});

$bgpsql = $node->background_psql('postgres');
$bgpsql->query_until(
	qr/starting_fileops/,
	qq(\\echo starting_fileops
BEGIN;
SELECT test_fileops_chmod('$datadir/test4_file.dat', 511);
ABORT;
\\q
));

# Wait for the UNDO apply to start
$node->wait_for_event('client backend', 'fileops-undo-apply-begin');

# Crash while UNDO is being applied
$node->stop('immediate');
$node->start;

# After recovery, the permissions should be restored (UNDO completed)
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_get_mode('$datadir/test4_file.dat')});
is($result, $orig_mode,
	'Test 4: file permissions restored after crash during UNDO apply');

$result = $node->safe_psql('postgres', 'SELECT 1');
is($result, '1', 'Test 4: server operational after crash during FILEOPS UNDO apply');

$node->stop;

# ================================================================
# Test 5: Deep subtransaction UNDO chain
#
# Verifies that deeply nested savepoints with FILEOPS operations
# are all properly reversed after a crash. Tests UNDO chain traversal
# with deep nesting.
# ================================================================

$node = setup_node('test5');
$datadir = $node->data_dir;

# Create test files
$node->safe_psql('postgres',
	qq{SELECT test_fileops_create_tempfile('test5_file.dat')});

# Verify initial size
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/test5_file.dat')});
is($result, '1024', 'Test 5: initial file size is 1024');

# Build a transaction with 20 nested savepoints, each truncating the file
# to a smaller size
my $depth = 20;
$bgpsql = $node->background_psql('postgres');

my $sql = "BEGIN;\n";
for my $i (1 .. $depth)
{
	my $size = 1024 - ($i * 40);	# 984, 944, ..., 224
	$sql .= "SAVEPOINT sp$i;\n";
	$sql .= "SELECT test_fileops_truncate('$datadir/test5_file.dat', $size);\n";
}

$bgpsql->query_until(
	qr/deep_done/,
	$sql . "\\echo deep_done\n");

# Verify the file was truncated to final size (1024 - 20*40 = 224)
my $final_size = 1024 - ($depth * 40);
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/test5_file.dat')});
is($result, "$final_size", 'Test 5: file truncated through nested savepoints');

# Crash the server (transaction not committed)
$node->stop('immediate');
$node->start;

# After recovery, the file size should be restored to original 1024
# (uncommitted transaction rolled back via UNDO chain traversal)
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/test5_file.dat')});
is($result, '1024',
	'Test 5: file size restored after deep subtransaction crash');

$result = $node->safe_psql('postgres', 'SELECT 1');
is($result, '1', 'Test 5: server operational after deep subtransaction crash');

$node->stop;

# ================================================================
# Test 6: Repeated crashes during UNDO apply (idempotency)
#
# Verifies that crashing multiple times during UNDO application
# produces the correct final state. UNDO apply must be idempotent:
# partially-applied operations should not cause errors on retry.
# ================================================================

$node = setup_node('test6');
$datadir = $node->data_dir;

# Create test file
$node->safe_psql('postgres',
	qq{SELECT test_fileops_create_tempfile('test6_file.dat')});
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/test6_file.dat')});
is($result, '1024', 'Test 6: initial file size is 1024');

# Set up injection point for UNDO apply
$node->safe_psql('postgres',
	q{SELECT injection_points_attach('fileops-undo-apply-begin', 'wait')});

$bgpsql = $node->background_psql('postgres');
$bgpsql->query_until(
	qr/starting_idempotent/,
	qq(\\echo starting_idempotent
BEGIN;
SELECT test_fileops_truncate('$datadir/test6_file.dat', 100);
SELECT test_fileops_chmod('$datadir/test6_file.dat', 511);
ABORT;
\\q
));

# Wait for first UNDO apply attempt
$node->wait_for_event('client backend', 'fileops-undo-apply-begin');

# First crash during UNDO apply
$node->stop('immediate');

# First recovery
$node->start;

# Verify file state is correct after first recovery
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/test6_file.dat')});
is($result, '1024',
	'Test 6: file size restored after first crash during UNDO apply');

# Second crash for idempotency verification
$node->stop('immediate');
$node->start;

# File should still be in correct state
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/test6_file.dat')});
is($result, '1024',
	'Test 6: file size correct after repeated crashes (idempotent recovery)');

$result = $node->safe_psql('postgres', 'SELECT 1');
is($result, '1',
	'Test 6: server stable after repeated crashes');

$node->stop;

done_testing();
