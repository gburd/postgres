
# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test crash recovery with UNDO-enabled tables.
#
# This test verifies that if the server crashes while an UNDO-enabled
# table has in-progress transactions, crash recovery correctly restores
# data integrity via PostgreSQL's standard MVCC/CLOG-based recovery.
#
# With the current heap-based storage engine, crash recovery does not
# need to apply UNDO chains because PostgreSQL's MVCC already handles
# visibility of aborted transactions through CLOG.  The UNDO records
# are written to the WAL but are not applied during abort.
#
# Scenario:
#   1. Create an UNDO-enabled table with committed data.
#   2. Begin a transaction that DELETEs all rows (but do not commit).
#   3. Crash the server (immediate stop).
#   4. Restart the server - recovery should abort the in-progress
#      transaction via CLOG, making the deleted rows visible again.
#   5. Verify all original rows are present.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
enable_undo = on
autovacuum = off
});
$node->start;

# Create an UNDO-enabled table and populate it with committed data.
$node->safe_psql('postgres', q{
CREATE TABLE crash_test (id int PRIMARY KEY, val text) WITH (enable_undo = on);
INSERT INTO crash_test SELECT g, 'original row ' || g FROM generate_series(1, 100) g;
});

# Verify initial data.
my $initial_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM crash_test});
is($initial_count, '100', 'initial row count is 100');

# Use a background psql session to start a transaction that deletes all
# rows but does not commit.  We use a separate psql session so we can
# crash the server while the transaction is in progress.
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

# Start a transaction that deletes all rows.
$stdin .= q{
BEGIN;
DELETE FROM crash_test;
SELECT 'delete_done';
};

ok(pump_until($h, $psql_timeout, \$stdout, qr/delete_done/),
	'DELETE completed in transaction');

# Also verify within the session that the rows appear deleted.
$stdout = '';
$stdin .= q{
SELECT count(*) FROM crash_test;
};
ok(pump_until($h, $psql_timeout, \$stdout, qr/^0$/m),
	'rows appear deleted within open transaction');

# Crash the server while the DELETE transaction is still in progress.
# The 'immediate' stop sends SIGQUIT, simulating a crash.
$node->stop('immediate');

# The psql session should have been killed by the crash.
$h->finish;

# Start the server.  Recovery should detect the in-progress transaction
# and mark it as aborted via CLOG, making the deleted rows visible again.
$node->start;

# Verify that all rows are visible after crash recovery.
my $recovered_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM crash_test});
is($recovered_count, '100',
	'all 100 rows visible after crash recovery');

# Verify data integrity: check that values are correct.
my $sum_ids = $node->safe_psql('postgres',
	q{SELECT sum(id) FROM crash_test});
is($sum_ids, '5050', 'sum of ids correct (1+2+...+100 = 5050)');

# Verify a specific row to check tuple data integrity.
my $sample_row = $node->safe_psql('postgres',
	q{SELECT val FROM crash_test WHERE id = 42});
is($sample_row, 'original row 42', 'tuple data intact after recovery');

# Test a second scenario: crash during INSERT.
$node->safe_psql('postgres', q{
CREATE TABLE crash_insert_test (id int, val text) WITH (enable_undo = on);
});

# Start a background session with an uncommitted INSERT.
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

$stdin .= q{
BEGIN;
INSERT INTO crash_insert_test SELECT g, 'should not persist ' || g FROM generate_series(1, 50) g;
SELECT 'insert_done';
};

ok(pump_until($h, $psql_timeout, \$stdout, qr/insert_done/),
	'INSERT completed in transaction');

# Crash the server.
$node->stop('immediate');
$h->finish;

# Restart - recovery should mark the uncommitted transaction as aborted
# via CLOG, making the inserted rows invisible.
$node->start;

my $insert_recovered = $node->safe_psql('postgres',
	q{SELECT count(*) FROM crash_insert_test});
is($insert_recovered, '0',
	'no rows visible after crash recovery of uncommitted INSERT');

$node->stop;

done_testing();
