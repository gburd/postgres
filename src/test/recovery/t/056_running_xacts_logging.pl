# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Verify that xl_running_xacts records are periodically logged into WAL on a
# live primary, and that a streaming standby reaches and stays at consistency.
#
# This periodic logging used to be done by the background writer; it now lives
# in the walwriter (see src/backend/postmaster/walwriter.c).  The cadence is a
# hot-standby correctness feature: downstream replicas rely on these records to
# reach a consistent snapshot and to trim KnownAssignedXids.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# The walwriter logs a fresh xl_running_xacts every LOG_SNAPSHOT_INTERVAL_MS
# (15s) on an out-of-recovery primary with wal_level >= replica, but only if
# some other WAL was inserted since the last one.  Keep the test bounded but
# tolerant of scheduling jitter.
my $log_snapshot_interval_s = 15;

# Initialize primary node with streaming replication enabled (wal_level=replica).
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
# Keep WAL segments around so pg_walinspect can scan back over the window we
# measure without hitting a recycled segment.
$node_primary->append_conf('postgresql.conf', 'wal_keep_size = 128MB');
$node_primary->start;

# pg_walinspect lets us count RUNNING_XACTS records straight from SQL, avoiding
# a dependency on locating the pg_waldump binary.
$node_primary->safe_psql('postgres',
	'CREATE EXTENSION pg_walinspect');


# Create a streaming standby from a base backup.
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->start;

# (1) The standby must reach consistency and catch up to the primary.
$node_primary->safe_psql('postgres',
	'CREATE TABLE tab_int AS SELECT generate_series(1, 100) AS a');
$node_primary->wait_for_catchup($node_standby, 'replay',
	$node_primary->lsn('insert'));
my $result = $node_standby->safe_psql('postgres',
	'SELECT count(*) FROM tab_int');
is($result, '100', 'standby reached consistency and caught up');

# WAL position used as the lower bound for pg_walinspect scans; assigned below
# after all backup/checkpoint churn so its segment is still present.
my $scan_start;

# Helper: number of RUNNING_XACTS records in WAL from $scan_start to the
# current insert LSN (pg_walinspect clamps the end bound to the flush LSN).
my $count_running_xacts = sub {
	return $node_primary->safe_psql(
		'postgres', qq[
		SELECT count(*)
		FROM pg_get_wal_records_info('$scan_start', pg_current_wal_insert_lsn())
		WHERE resource_manager = 'Standby'
		  AND record_type = 'RUNNING_XACTS']);
};

# (2) With the primary otherwise idle, RUNNING_XACTS records must keep being
# emitted by the walwriter.  We insert one row per poll so that there is always
# "interesting" WAL since the last snapshot (the walwriter deliberately skips
# logging on a fully idle system), then require the count to grow by at least 2,
# which spans at least two logging intervals and proves periodic emission rather
# than a one-off record from some other code path (e.g. checkpoint).
#
# Capture the scan lower bound here, after all backup/checkpoint churn, so the
# starting segment is still present; wal_keep_size keeps it around for the
# duration of the measurement.  Use the flush position (a record boundary) and
# emit a record past it before the first scan, so pg_walinspect always has a
# valid record to start from.
$scan_start = $node_primary->safe_psql('postgres',
	'SELECT pg_current_wal_lsn()');
$node_primary->safe_psql('postgres', 'INSERT INTO tab_int VALUES (1)');
my $start = $count_running_xacts->();

# Poll for up to a few logging intervals.  Under normal scheduling two new
# records appear within ~2*15s; allow generous slack for slow CI.
my $deadline = time() + 5 * $log_snapshot_interval_s;
my $seen = $start;
while (time() < $deadline)
{
	# Insert a tiny bit of WAL so the "interesting records since last snapshot"
	# guard in the walwriter is satisfied, then wait a poll interval.
	$node_primary->safe_psql('postgres',
		'INSERT INTO tab_int VALUES (1)');
	sleep(2);
	$seen = $count_running_xacts->();
	last if $seen >= $start + 2;
}

cmp_ok($seen, '>=', $start + 2,
	'walwriter periodically logs xl_running_xacts on the primary');

# (3) Standby is still streaming and caught up after the activity above; this
# exercises that the relocated logging did not disturb replication.
$node_primary->wait_for_catchup($node_standby, 'replay',
	$node_primary->lsn('insert'));
$result = $node_standby->safe_psql('postgres',
	'SELECT count(*) > 100 FROM tab_int');
is($result, 't', 'standby stays caught up after running-xacts activity');

$node_standby->stop;
$node_primary->stop;

done_testing();
