
# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test crash recovery for RECNO table access method with UNDO.
#
# These tests verify that RECNO tables (which use the per-relation UNDO
# subsystem) handle crash recovery correctly:
#
#   1. Committed data in a RECNO table survives a crash and is present
#      after recovery.
#   2. An aborted transaction's inserts are cleaned up after crash
#      recovery (ATM recovery via CLOG marks the transaction as aborted).
#   3. Multi-page UNDO chain recovery: inserting enough rows to span
#      multiple UNDO pages, then crashing, does not corrupt data.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('recno_undo_recovery');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
enable_undo = on
autovacuum = off
log_min_messages = warning
});
$node->start;

# Check whether the RECNO access method is available.  If the build does
# not include RECNO, skip the entire test gracefully.
my $has_recno = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_am WHERE amname = 'recno'});
if ($has_recno eq '0')
{
	plan skip_all => 'recno access method not available';
}

# ================================================================
# Test 1: Committed data in RECNO table survives crash
# ================================================================

$node->safe_psql('postgres', q{
CREATE TABLE recno_recovery_t1 (id int, val text) USING recno;
INSERT INTO recno_recovery_t1
    SELECT g, 'committed row ' || g FROM generate_series(1, 50) g;
});

# Force a checkpoint so the data is durable on disk.
$node->safe_psql('postgres', q{CHECKPOINT});

# Verify data before crash.
my $pre_crash_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM recno_recovery_t1});
is($pre_crash_count, '50', 'Test 1: 50 rows present before crash');

# Crash the server.
$node->stop('immediate');
$node->start;

# Verify data after recovery.
my $post_crash_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM recno_recovery_t1});
is($post_crash_count, '50',
	'Test 1: all 50 committed rows present after crash recovery');

# Verify data integrity by checking a specific row.
my $sample_val = $node->safe_psql('postgres',
	q{SELECT val FROM recno_recovery_t1 WHERE id = 25});
is($sample_val, 'committed row 25',
	'Test 1: tuple data intact after crash recovery');

# Verify aggregate integrity.
my $sum_ids = $node->safe_psql('postgres',
	q{SELECT sum(id) FROM recno_recovery_t1});
is($sum_ids, '1275', 'Test 1: sum of ids correct (1+2+...+50 = 1275)');

# ================================================================
# Test 2: Aborted transaction cleaned up after crash (ATM recovery)
# ================================================================

# Start a background psql session with an uncommitted INSERT, then crash
# the server.  After recovery the uncommitted rows should not be visible.

my ($stdin, $stdout, $stderr) = ('', '', '');
my $psql_timeout = IPC::Run::timer($PostgreSQL::Test::Utils::timeout_default);
my $h = IPC::Run::start(
	[
		'psql', '--no-psqlrc', '--quiet', '--no-align', '--tuples-only',
		'--set' => 'ON_ERROR_STOP=1',
		'--file' => '-',
		'--dbname' => $node->connstr('postgres')
	],
	'<'  => \$stdin,
	'>'  => \$stdout,
	'2>' => \$stderr,
	$psql_timeout);

$stdin .= q{
BEGIN;
INSERT INTO recno_recovery_t1
    SELECT g, 'aborted row ' || g FROM generate_series(100, 149) g;
SELECT 'insert_done';
};

ok(pump_until($h, $psql_timeout, \$stdout, qr/insert_done/),
	'Test 2: INSERT completed in uncommitted transaction');

# Verify the rows are visible within the open transaction.
$stdout = '';
$stdin .= q{
SELECT count(*) FROM recno_recovery_t1;
};
ok(pump_until($h, $psql_timeout, \$stdout, qr/^100$/m),
	'Test 2: 100 rows visible within uncommitted transaction');

# Crash the server while the transaction is still in progress.
$node->stop('immediate');
$h->finish;

# Restart - recovery should mark the uncommitted transaction as aborted
# via CLOG, making the inserted rows invisible.
$node->start;

my $recovered_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM recno_recovery_t1});
is($recovered_count, '50',
	'Test 2: only 50 committed rows visible after crash recovery of aborted INSERT');

# Verify none of the aborted rows leaked through.
my $aborted_rows = $node->safe_psql('postgres',
	q{SELECT count(*) FROM recno_recovery_t1 WHERE val LIKE 'aborted%'});
is($aborted_rows, '0',
	'Test 2: no aborted rows visible after crash recovery');

# ================================================================
# Test 3: Multi-page UNDO chain recovery
# ================================================================

# Insert enough rows to span multiple UNDO pages.  Each UNDO page is
# typically 8 kB, so inserting many rows with non-trivial data should
# create a multi-page UNDO chain.

$node->safe_psql('postgres', q{
CREATE TABLE recno_recovery_multi (id int, val text) USING recno;
INSERT INTO recno_recovery_multi
    SELECT g, 'multipage row ' || g || ' ' || repeat('x', 200)
    FROM generate_series(1, 500) g;
});

# Checkpoint so the data is durable.
$node->safe_psql('postgres', q{CHECKPOINT});

# Verify before crash.
my $multi_pre = $node->safe_psql('postgres',
	q{SELECT count(*) FROM recno_recovery_multi});
is($multi_pre, '500', 'Test 3: 500 rows present before crash');

# Also start an uncommitted transaction that adds more rows (creating
# additional UNDO chain entries) before crashing.
($stdin, $stdout, $stderr) = ('', '', '');
$h = IPC::Run::start(
	[
		'psql', '--no-psqlrc', '--quiet', '--no-align', '--tuples-only',
		'--set' => 'ON_ERROR_STOP=1',
		'--file' => '-',
		'--dbname' => $node->connstr('postgres')
	],
	'<'  => \$stdin,
	'>'  => \$stdout,
	'2>' => \$stderr,
	$psql_timeout);

$stdin .= q{
BEGIN;
INSERT INTO recno_recovery_multi
    SELECT g, 'uncommitted multi ' || g || ' ' || repeat('y', 200)
    FROM generate_series(1000, 1499) g;
SELECT 'multi_insert_done';
};

ok(pump_until($h, $psql_timeout, \$stdout, qr/multi_insert_done/),
	'Test 3: multi-page INSERT completed in uncommitted transaction');

# Crash while the large uncommitted transaction is in progress.
$node->stop('immediate');
$h->finish;

$node->start;

# Verify only committed rows are visible.
my $multi_post = $node->safe_psql('postgres',
	q{SELECT count(*) FROM recno_recovery_multi});
is($multi_post, '500',
	'Test 3: only 500 committed rows after multi-page UNDO crash recovery');

# Verify data integrity of committed rows.
my $multi_sample = $node->safe_psql('postgres',
	q{SELECT left(val, 20) FROM recno_recovery_multi WHERE id = 250});
like($multi_sample, qr/^multipage row 250 xx/,
	'Test 3: committed row data intact after multi-page UNDO recovery');

# Verify no uncommitted rows leaked.
my $uncommitted = $node->safe_psql('postgres',
	q{SELECT count(*) FROM recno_recovery_multi WHERE id >= 1000});
is($uncommitted, '0',
	'Test 3: no uncommitted rows visible after multi-page UNDO recovery');

# Verify the table is still fully operational after recovery.
$node->safe_psql('postgres', q{
INSERT INTO recno_recovery_multi VALUES (9999, 'post-recovery insert');
});

my $post_recovery_row = $node->safe_psql('postgres',
	q{SELECT val FROM recno_recovery_multi WHERE id = 9999});
is($post_recovery_row, 'post-recovery insert',
	'Test 3: INSERT works on RECNO table after multi-page UNDO recovery');

# Clean up.
$node->stop;

done_testing();
