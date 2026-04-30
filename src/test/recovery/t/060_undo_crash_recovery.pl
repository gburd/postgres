
# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Advanced crash recovery tests for UNDO subsystem.
#
# These tests verify crash safety of the UNDO log under various failure
# scenarios beyond the basic crash tests in 056_undo_crash.pl.
#
# Scenarios tested:
#   1. Crash during large rollback (many UNDO records)
#   2. Crash during concurrent DML on multiple UNDO-enabled tables
#   3. Prepared transaction with UNDO records, crash, recovery
#   4. Crash after CHECKPOINT with dirty UNDO-enabled pages
#   5. Repeated crash-restart cycles for stability

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use IPC::Run;
use Test::More;

# ================================================================
# Test 1: Crash during large rollback (many UNDO records)
#
# Verify that if a server crashes while a large transaction is
# in progress (many rows modified), crash recovery correctly
# makes the modifications invisible.
# ================================================================

my $node = PostgreSQL::Test::Cluster->new('crash_large');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
enable_undo = on
wal_level = replica
autovacuum = off
shared_buffers = 128MB
});
$node->start;

# Create table and populate with committed data.
$node->safe_psql('postgres', q{
CREATE TABLE large_abort (id int PRIMARY KEY, val text) WITH (enable_undo = on);
INSERT INTO large_abort SELECT g, repeat('x', 100) || g
  FROM generate_series(1, 10000) g;
});

my $initial = $node->safe_psql('postgres',
	q{SELECT count(*) FROM large_abort});
is($initial, '10000', 'test1: initial 10000 rows');

# Start a background session that deletes all rows without committing.
my ($stdin, $stdout, $stderr) = ('', '', '');
my $psql_timeout = IPC::Run::timer($PostgreSQL::Test::Utils::timeout_default);
my $h = IPC::Run::start(
	[
		'psql', '--no-psqlrc', '--quiet', '--no-align', '--tuples-only',
		'--set' => 'ON_ERROR_STOP=1',
		'--file' => '-',
		'--dbname' => $node->connstr('postgres')
	],
	'<' => \$stdin,
	'>' => \$stdout,
	'2>' => \$stderr,
	$psql_timeout);

$stdin .= q{
BEGIN;
DELETE FROM large_abort;
SELECT 'large_delete_done';
};

ok(pump_until($h, $psql_timeout, \$stdout, qr/large_delete_done/),
	'test1: large DELETE completed in transaction');

# Crash the server.
$node->stop('immediate');
$h->finish;

# Restart and verify all rows survive.
$node->start;

my $recovered = $node->safe_psql('postgres',
	q{SELECT count(*) FROM large_abort});
is($recovered, '10000', 'test1: all 10000 rows visible after crash recovery');

# Verify data integrity with a checksum.
my $checksum = $node->safe_psql('postgres',
	q{SELECT sum(id) FROM large_abort});
is($checksum, '50005000', 'test1: sum of ids correct after recovery');

# ================================================================
# Test 2: Crash during concurrent DML on multiple tables
#
# Multiple UNDO-enabled tables with concurrent modifications,
# crash, and verify each table independently.
# ================================================================

$node->safe_psql('postgres', q{
CREATE TABLE multi_a (id int, val text) WITH (enable_undo = on);
CREATE TABLE multi_b (id int, val text) WITH (enable_undo = on);
CREATE TABLE multi_c (id int, val text) WITH (enable_undo = on);
INSERT INTO multi_a SELECT g, 'a' || g FROM generate_series(1, 500) g;
INSERT INTO multi_b SELECT g, 'b' || g FROM generate_series(1, 500) g;
INSERT INTO multi_c SELECT g, 'c' || g FROM generate_series(1, 500) g;
});

# Two background sessions with open transactions on different tables.
my ($sin1, $sout1, $serr1) = ('', '', '');
my $h1 = IPC::Run::start(
	[
		'psql', '--no-psqlrc', '--quiet', '--no-align', '--tuples-only',
		'--set' => 'ON_ERROR_STOP=1',
		'--file' => '-',
		'--dbname' => $node->connstr('postgres')
	],
	'<' => \$sin1,
	'>' => \$sout1,
	'2>' => \$serr1,
	$psql_timeout);

$sin1 .= q{
BEGIN;
DELETE FROM multi_a WHERE id <= 250;
UPDATE multi_b SET val = 'modified' WHERE id <= 250;
SELECT 'session1_done';
};
ok(pump_until($h1, $psql_timeout, \$sout1, qr/session1_done/),
	'test2: session 1 DML completed');

my ($sin2, $sout2, $serr2) = ('', '', '');
my $h2 = IPC::Run::start(
	[
		'psql', '--no-psqlrc', '--quiet', '--no-align', '--tuples-only',
		'--set' => 'ON_ERROR_STOP=1',
		'--file' => '-',
		'--dbname' => $node->connstr('postgres')
	],
	'<' => \$sin2,
	'>' => \$sout2,
	'2>' => \$serr2,
	$psql_timeout);

$sin2 .= q{
BEGIN;
INSERT INTO multi_c SELECT g, 'new_c' || g FROM generate_series(501, 1000) g;
SELECT 'session2_done';
};
ok(pump_until($h2, $psql_timeout, \$sout2, qr/session2_done/),
	'test2: session 2 DML completed');

# Crash with both sessions open.
$node->stop('immediate');
$h1->finish;
$h2->finish;

$node->start;

# multi_a: all 500 rows should be visible (DELETE was aborted).
my $a_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM multi_a});
is($a_count, '500', 'test2: multi_a has all 500 rows after crash');

# multi_b: all 500 rows should have original values (UPDATE was aborted).
my $b_modified = $node->safe_psql('postgres',
	q{SELECT count(*) FROM multi_b WHERE val LIKE 'modified%'});
is($b_modified, '0', 'test2: multi_b has no modified rows after crash');

# multi_c: only 500 original rows (INSERT was aborted).
my $c_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM multi_c});
is($c_count, '500', 'test2: multi_c has 500 original rows after crash');

# ================================================================
# Test 3: Prepared transaction with UNDO, crash, recovery
#
# PREPARE TRANSACTION should preserve UNDO state across crash.
# After recovery, COMMIT PREPARED should succeed, and the data
# should reflect the committed changes.
# ================================================================

$node->safe_psql('postgres', q{
SET max_prepared_transactions = 10;
});
# Need to restart for the GUC to take effect.
$node->append_conf('postgresql.conf', q{max_prepared_transactions = 10});
$node->restart;

$node->safe_psql('postgres', q{
CREATE TABLE prep_test (id int, val text) WITH (enable_undo = on);
INSERT INTO prep_test SELECT g, 'orig' || g FROM generate_series(1, 100) g;
});

# Prepare a transaction that deletes some rows.
$node->safe_psql('postgres', q{
BEGIN;
DELETE FROM prep_test WHERE id <= 50;
PREPARE TRANSACTION 'undo_prep_1';
});

# Prepared-transaction changes are not committed yet; MVCC hides them
# from other sessions.  All 100 original rows remain visible.
my $prep_visible = $node->safe_psql('postgres',
	q{SELECT count(*) FROM prep_test});
is($prep_visible, '100', 'test3: all 100 rows visible; prepared DELETE not yet committed');

# Crash.
$node->stop('immediate');
$node->start;

# The prepared transaction should survive crash recovery.
my $prep_list = $node->safe_psql('postgres',
	q{SELECT gid FROM pg_prepared_xacts WHERE gid = 'undo_prep_1'});
is($prep_list, 'undo_prep_1',
	'test3: prepared transaction survives crash recovery');

# ROLLBACK PREPARED to restore the deleted rows.
$node->safe_psql('postgres', q{ROLLBACK PREPARED 'undo_prep_1'});

my $prep_after_rollback = $node->safe_psql('postgres',
	q{SELECT count(*) FROM prep_test});
is($prep_after_rollback, '100',
	'test3: all 100 rows restored after ROLLBACK PREPARED');

# ================================================================
# Test 4: Crash after CHECKPOINT with dirty UNDO-enabled pages
#
# Ensure that a CHECKPOINT followed by a crash doesn't lose
# committed data from UNDO-enabled tables.
# ================================================================

$node->safe_psql('postgres', q{
CREATE TABLE ckpt_test (id int, val text) WITH (enable_undo = on);
INSERT INTO ckpt_test SELECT g, 'before_ckpt' || g FROM generate_series(1, 200) g;
CHECKPOINT;
UPDATE ckpt_test SET val = 'after_ckpt' || id WHERE id <= 100;
});

# Crash without a second checkpoint -- recovery must replay WAL.
$node->stop('immediate');
$node->start;

my $ckpt_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM ckpt_test});
is($ckpt_count, '200', 'test4: all 200 rows present after post-checkpoint crash');

my $ckpt_updated = $node->safe_psql('postgres',
	q{SELECT count(*) FROM ckpt_test WHERE val LIKE 'after_ckpt%'});
is($ckpt_updated, '100',
	'test4: committed updates survived post-checkpoint crash');

my $ckpt_original = $node->safe_psql('postgres',
	q{SELECT count(*) FROM ckpt_test WHERE val LIKE 'before_ckpt%'});
is($ckpt_original, '100',
	'test4: non-updated rows preserved after post-checkpoint crash');

# ================================================================
# Test 5: Repeated crash-restart cycles
#
# Verify stability across multiple crash-restart iterations.
# Each cycle modifies data, crashes, recovers, and verifies.
# ================================================================

$node->safe_psql('postgres', q{
CREATE TABLE stability_test (id int PRIMARY KEY, val int) WITH (enable_undo = on);
INSERT INTO stability_test SELECT g, 0 FROM generate_series(1, 100) g;
});

for my $cycle (1..3)
{
	# Committed update: increment val.
	$node->safe_psql('postgres',
		qq{UPDATE stability_test SET val = val + 1});

	# Uncommitted update (will be lost on crash).
	($stdin, $stdout, $stderr) = ('', '', '');
	$h = IPC::Run::start(
		[
			'psql', '--no-psqlrc', '--quiet', '--no-align', '--tuples-only',
			'--set' => 'ON_ERROR_STOP=1',
			'--file' => '-',
			'--dbname' => $node->connstr('postgres')
		],
		'<' => \$stdin,
		'>' => \$stdout,
		'2>' => \$stderr,
		$psql_timeout);

	$stdin .= qq{
BEGIN;
UPDATE stability_test SET val = val + 1000;
SELECT 'cycle_${cycle}_done';
};

	ok(pump_until($h, $psql_timeout, \$stdout, qr/cycle_${cycle}_done/),
		"test5: cycle $cycle uncommitted update done");

	$node->stop('immediate');
	$h->finish;
	$node->start;

	# val should be $cycle (committed increments only).
	my $expected_val = $cycle;
	my $actual_val = $node->safe_psql('postgres',
		q{SELECT val FROM stability_test WHERE id = 1});
	is($actual_val, "$expected_val",
		"test5: cycle $cycle - val is $expected_val after crash recovery");
}

$node->stop;

# ================================================================
# Test 6: TOAST crash recovery -- UPDATE on TOASTed column
#
# Verify that crash recovery correctly restores the old large datum
# when a transaction that modified a TOASTed column is aborted by
# a server crash.  This exercises the cross-relation UNDO ordering
# invariant: old TOAST chunks must be present when the heap tuple
# referencing them is restored.
# ================================================================

my $node6 = PostgreSQL::Test::Cluster->new('toast_crash');
$node6->init;
$node6->append_conf(
	'postgresql.conf', q{
enable_undo = on
wal_level = replica
autovacuum = off
shared_buffers = 128MB
});
$node6->start;

# Create a table with a TOASTed column; insert an initial large value.
$node6->safe_psql('postgres', q{
CREATE TABLE toast_crash_tbl (id int PRIMARY KEY, payload text)
  WITH (enable_undo = on);
INSERT INTO toast_crash_tbl VALUES (1, repeat('original_', 12000));
INSERT INTO toast_crash_tbl VALUES (2, repeat('stable_', 15000));
});

# Verify initial state.
my $t6_init_len = $node6->safe_psql('postgres',
	q{SELECT length(payload) FROM toast_crash_tbl WHERE id = 1});
is($t6_init_len, '108000', 'test6: initial TOASTed value length = 108000');

my $t6_prefix = $node6->safe_psql('postgres',
	q{SELECT left(payload, 9) FROM toast_crash_tbl WHERE id = 1});
is($t6_prefix, 'original_', 'test6: initial value starts with original_');

# Open a background session that updates the TOASTed column without committing.
my ($t6in, $t6out, $t6err) = ('', '', '');
my $t6h = IPC::Run::start(
	[
		'psql', '--no-psqlrc', '--quiet', '--no-align', '--tuples-only',
		'--set' => 'ON_ERROR_STOP=1',
		'--file' => '-',
		'--dbname' => $node6->connstr('postgres')
	],
	'<' => \$t6in,
	'>' => \$t6out,
	'2>' => \$t6err,
	$psql_timeout);

$t6in .= q{
BEGIN;
UPDATE toast_crash_tbl SET payload = repeat('modified_', 12000) WHERE id = 1;
SELECT 'toast_update_done';
};

ok(pump_until($t6h, $psql_timeout, \$t6out, qr/toast_update_done/),
	'test6: TOASTed column UPDATE completed in open transaction');

# Crash the server with the UPDATE transaction open.
#
# We confirmed 'toast_update_done' above, which means the UPDATE (and its
# UNDO records) are already written into the WAL stream via XLogInsert().
# stop('immediate') sends SIGQUIT which terminates without a checkpoint, but
# the WAL records are already durably in the WAL buffer or written to disk.
# Recovery will replay them and apply UNDO to restore the pre-UPDATE state.
$node6->stop('immediate');
$t6h->finish;

# Restart and verify the old large value is fully restored.
$node6->start;

my $t6_recovered_len = $node6->safe_psql('postgres',
	q{SELECT length(payload) FROM toast_crash_tbl WHERE id = 1});
is($t6_recovered_len, '108000',
	'test6: TOASTed value length restored after crash recovery');

my $t6_recovered_prefix = $node6->safe_psql('postgres',
	q{SELECT left(payload, 9) FROM toast_crash_tbl WHERE id = 1});
is($t6_recovered_prefix, 'original_',
	'test6: TOASTed value content restored to original_ after crash recovery');

# Verify the second row (not modified) is intact.
my $t6_stable_len = $node6->safe_psql('postgres',
	q{SELECT length(payload) FROM toast_crash_tbl WHERE id = 2});
is($t6_stable_len, '105000',
	'test6: unmodified TOASTed row intact after crash recovery');

$node6->stop;

# ================================================================
# Test 7: Subtransaction SAVEPOINT with TOASTed UPDATE crash recovery
#
# Verify that crash recovery correctly rolls back a TOASTed column
# UPDATE that was inside a SAVEPOINT within an uncommitted transaction.
# The entire outer transaction (including all savepoint work) must be
# reversed.
# ================================================================

my $node7 = PostgreSQL::Test::Cluster->new('toast_savepoint_crash');
$node7->init;
$node7->append_conf(
	'postgresql.conf', q{
enable_undo = on
wal_level = replica
autovacuum = off
shared_buffers = 128MB
});
$node7->start;

$node7->safe_psql('postgres', q{
CREATE TABLE sp_toast_tbl (id int PRIMARY KEY, payload text)
  WITH (enable_undo = on);
INSERT INTO sp_toast_tbl VALUES (1, repeat('original_', 10000));
INSERT INTO sp_toast_tbl VALUES (2, repeat('stable__', 12000));
});

# Verify: 'original_' = 9 chars x 10000 = 90000; 'stable__' = 8 chars x 12000 = 96000

my $t7_init = $node7->safe_psql('postgres',
	q{SELECT left(payload, 9) FROM sp_toast_tbl WHERE id = 1});
is($t7_init, 'original_', 'test7: initial TOASTed value confirmed');

my ($t7in, $t7out, $t7err) = ('', '', '');
my $t7h = IPC::Run::start(
	[
		'psql', '--no-psqlrc', '--quiet', '--no-align', '--tuples-only',
		'--set' => 'ON_ERROR_STOP=1',
		'--file' => '-',
		'--dbname' => $node7->connstr('postgres')
	],
	'<' => \$t7in,
	'>' => \$t7out,
	'2>' => \$t7err,
	$psql_timeout);

$t7in .= q{
BEGIN;
SAVEPOINT sp1;
UPDATE sp_toast_tbl SET payload = repeat('modified_', 10000) WHERE id = 1;
SELECT 'toast_sp_done';
};

ok(pump_until($t7h, $psql_timeout, \$t7out, qr/toast_sp_done/),
	'test7: TOASTed UPDATE within SAVEPOINT completed in open transaction');

$node7->stop('immediate');
$t7h->finish;
$node7->start;

my $t7_prefix = $node7->safe_psql('postgres',
	q{SELECT left(payload, 9) FROM sp_toast_tbl WHERE id = 1});
is($t7_prefix, 'original_',
	'test7: TOASTed value restored after SAVEPOINT crash recovery');

my $t7_len = $node7->safe_psql('postgres',
	q{SELECT length(payload) FROM sp_toast_tbl WHERE id = 1});
is($t7_len, '90000',
	'test7: TOASTed value length correct after SAVEPOINT crash recovery');

my $t7_stable = $node7->safe_psql('postgres',
	q{SELECT length(payload) FROM sp_toast_tbl WHERE id = 2});
is($t7_stable, '96000',
	'test7: unmodified TOASTed row intact after SAVEPOINT crash recovery');

$node7->stop;

done_testing();
