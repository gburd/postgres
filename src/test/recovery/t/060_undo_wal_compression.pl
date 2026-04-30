# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test WAL compression for cluster-wide UNDO operations.
#
# This test verifies that the wal_compression GUC works correctly with
# cluster-wide UNDO tables (enable_undo=on).  UNDO records are written
# to the WAL stream via XLOG_UNDO_BATCH, and WAL compression applies to
# Full Page Images (FPIs) within those records.
#
# Scenario:
#   1. Start a primary with wal_compression=pglz and enable_undo=on
#   2. Create an UNDO-enabled table, INSERT rows
#   3. Test DELETE rollback: BEGIN; DELETE FROM t; ROLLBACK; verify rows
#   4. Test UPDATE rollback: BEGIN; UPDATE t; ROLLBACK; verify original data
#   5. Crash-stop (kill -9), restart, verify data integrity

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('undo_walcomp');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
enable_undo = on
wal_compression = pglz
full_page_writes = on
autovacuum = off
});
$node->start;

# Create an UNDO-enabled table and populate it.
$node->safe_psql('postgres', q{
CREATE TABLE t (id int, val text) WITH (enable_undo = on);
INSERT INTO t SELECT g, 'original value ' || g FROM generate_series(1, 100) g;
});

my $initial_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM t});
is($initial_count, '100', 'initial row count is 100');

# ================================================================
# Phase 1: DELETE rollback with WAL compression active
# ================================================================

$node->safe_psql('postgres', q{
BEGIN;
DELETE FROM t;
ROLLBACK;
});

my $after_delete_rollback = $node->safe_psql('postgres',
	q{SELECT count(*) FROM t});
is($after_delete_rollback, '100',
	'all 100 rows visible after DELETE rollback with pglz compression');

# ================================================================
# Phase 2: UPDATE rollback with WAL compression active
# ================================================================

$node->safe_psql('postgres', q{
BEGIN;
UPDATE t SET val = repeat('x', 500) WHERE id <= 10;
ROLLBACK;
});

my $after_update_rollback = $node->safe_psql('postgres',
	q{SELECT count(*) FROM t WHERE val LIKE 'original value %'});
is($after_update_rollback, '100',
	'all 100 rows have original values after UPDATE rollback with pglz');

# Verify a specific row to confirm data integrity.
my $sample_row = $node->safe_psql('postgres',
	q{SELECT val FROM t WHERE id = 5});
is($sample_row, 'original value 5',
	'specific row data intact after UPDATE rollback');

# ================================================================
# Phase 3: Crash recovery with WAL compression
# ================================================================

# Force a checkpoint so recovery has a known starting point.
$node->safe_psql('postgres', q{CHECKPOINT});

# Start a background session with an uncommitted DELETE.
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
DELETE FROM t;
SELECT 'delete_done';
};

ok(pump_until($h, $psql_timeout, \$stdout, qr/delete_done/),
	'DELETE completed in open transaction before crash');

# Crash the server (immediate stop sends SIGQUIT).
$node->stop('immediate');
$h->finish;

# Restart -- recovery should abort the in-progress transaction.
$node->start;

# Verify all rows are visible after crash recovery.
my $recovered_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM t});
is($recovered_count, '100',
	'all 100 rows visible after crash recovery with pglz WAL compression');

# Verify data integrity after crash recovery.
my $sum_ids = $node->safe_psql('postgres',
	q{SELECT sum(id) FROM t});
is($sum_ids, '5050', 'sum of ids correct after crash recovery (1+2+...+100 = 5050)');

my $post_crash_val = $node->safe_psql('postgres',
	q{SELECT val FROM t WHERE id = 42});
is($post_crash_val, 'original value 42',
	'tuple data intact after crash recovery with compressed WAL');

# Verify the table is still usable after recovery.
$node->safe_psql('postgres',
	q{INSERT INTO t VALUES (999, 'post_crash_insert')});
my $post_insert = $node->safe_psql('postgres',
	q{SELECT val FROM t WHERE id = 999});
is($post_insert, 'post_crash_insert',
	'INSERT works after crash recovery with pglz WAL compression');

$node->stop;

# ================================================================
# Phase 4: lz4 WAL compression (if available)
# ================================================================

my $node_lz4 = PostgreSQL::Test::Cluster->new('undo_walcomp_lz4');
$node_lz4->init;
$node_lz4->append_conf(
	'postgresql.conf', q{
enable_undo = on
full_page_writes = on
autovacuum = off
});
$node_lz4->start;

# Check whether lz4 WAL compression is supported by attempting to SET it.
my ($ret, $stdout_lz4, $stderr_lz4) = $node_lz4->psql('postgres',
	"SET wal_compression = 'lz4'");
if ($ret != 0)
{
	$node_lz4->stop;
	note("lz4 WAL compression not available, skipping lz4 tests");
	pass("lz4 DELETE rollback skipped (not available)");
	pass("lz4 UPDATE rollback skipped (not available)");
	pass("lz4 crash recovery skipped (not available)");
	pass("lz4 data integrity skipped (not available)");
	pass("lz4 post-crash INSERT skipped (not available)");
}
else
{
	$node_lz4->append_conf('postgresql.conf', "wal_compression = lz4\n");
	$node_lz4->reload;

	# Create an UNDO-enabled table and populate it.
	$node_lz4->safe_psql('postgres', q{
	CREATE TABLE t (id int, val text) WITH (enable_undo = on);
	INSERT INTO t SELECT g, 'original value ' || g FROM generate_series(1, 100) g;
	});

	# DELETE rollback with lz4 compression.
	$node_lz4->safe_psql('postgres', q{
	BEGIN;
	DELETE FROM t;
	ROLLBACK;
	});
	my $lz4_delete = $node_lz4->safe_psql('postgres',
		q{SELECT count(*) FROM t});
	is($lz4_delete, '100',
		'all 100 rows visible after DELETE rollback with lz4 compression');

	# UPDATE rollback with lz4 compression.
	$node_lz4->safe_psql('postgres', q{
	BEGIN;
	UPDATE t SET val = repeat('x', 500) WHERE id <= 10;
	ROLLBACK;
	});
	my $lz4_update = $node_lz4->safe_psql('postgres',
		q{SELECT count(*) FROM t WHERE val LIKE 'original value %'});
	is($lz4_update, '100',
		'all 100 rows have original values after UPDATE rollback with lz4');

	# Crash recovery with lz4 compression.
	$node_lz4->safe_psql('postgres', q{CHECKPOINT});

	my ($in_lz4, $out_lz4, $err_lz4) = ('', '', '');
	my $lz4_timeout =
		IPC::Run::timer($PostgreSQL::Test::Utils::timeout_default);
	my $h_lz4 = IPC::Run::start(
		[
			'psql', '--no-psqlrc', '--quiet',
			'--no-align', '--tuples-only',
			'--set' => 'ON_ERROR_STOP=1',
			'--file' => '-',
			'--dbname' => $node_lz4->connstr('postgres')
		],
		'<' => \$in_lz4,
		'>' => \$out_lz4,
		'2>' => \$err_lz4,
		$lz4_timeout);

	$in_lz4 .= q{
	BEGIN;
	DELETE FROM t;
	SELECT 'delete_done';
	};

	ok(pump_until($h_lz4, $lz4_timeout, \$out_lz4, qr/delete_done/),
		'lz4: DELETE completed in open transaction before crash');

	$node_lz4->stop('immediate');
	$h_lz4->finish;

	$node_lz4->start;

	my $lz4_recovered = $node_lz4->safe_psql('postgres',
		q{SELECT count(*) FROM t});
	is($lz4_recovered, '100',
		'all 100 rows visible after crash recovery with lz4 WAL compression');

	my $lz4_sum = $node_lz4->safe_psql('postgres',
		q{SELECT sum(id) FROM t});
	is($lz4_sum, '5050',
		'sum of ids correct after lz4 crash recovery');

	$node_lz4->safe_psql('postgres',
		q{INSERT INTO t VALUES (999, 'post_crash_lz4')});
	my $lz4_post = $node_lz4->safe_psql('postgres',
		q{SELECT val FROM t WHERE id = 999});
	is($lz4_post, 'post_crash_lz4',
		'INSERT works after crash recovery with lz4 WAL compression');

	$node_lz4->stop;
}

done_testing();
