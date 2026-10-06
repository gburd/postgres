
# Copyright (c) 2026, PostgreSQL Global Development Group

# BARK's own WAL records on a hot standby (patterned on
# src/test/recovery/t/031_recovery_conflict.pl):
#
# 1. Replaying VACUUM's record takes a cleanup lock, so a standby cursor
#    holding a pin on the leaf makes replay wait and then cancels it.
# 2. Reusing a deleted page logs a conflict horizon, which cancels a
#    standby snapshot that might still hold a link to the page.
# 3. VACUUM writes no BARK record for a leaf it does not change.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf(
	'postgresql.conf', qq[
max_standby_streaming_delay = 50ms
log_recovery_conflict_waits = on
deadlock_timeout = 10ms
autovacuum = off
]);
$node_primary->start;
$node_primary->backup('backup');

my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, 'backup',
	has_streaming => 1);
$node_standby->start;

my $db = 'postgres';
my $log_location;
my $sect;

$node_primary->safe_psql(
	$db, qq[
CREATE TABLE pin_t (a int);
INSERT INTO pin_t SELECT g FROM generate_series(1, 2000) g;
CREATE INDEX pin_t_idx ON pin_t USING bark (a);
VACUUM pin_t;
CREATE TABLE reuse_t (a int, pad text);
INSERT INTO reuse_t SELECT g, repeat('x', 100) FROM generate_series(1, 20000) g;
CREATE INDEX reuse_t_idx ON reuse_t USING bark (a);
VACUUM reuse_t;
]);
$node_primary->wait_for_replay_catchup($node_standby);

my $psql_standby = $node_standby->background_psql($db, on_error_stop => 0);


## 1: buffer pin conflict on VACUUM replay
$sect = 'buffer pin conflict';

# Dead index entries from an aborted insert, as 031_recovery_conflict.pl
# makes them.  They are dead to every snapshot, so the heap's prune record
# for them carries no conflict horizon, and the cursor below never visits
# their heap page: it fetches only key 1, whose heap page is all-visible,
# so it holds no heap pin.  The dead entries have key 5, on the same first
# leaf as key 1, so the BARK VACUUM record that removes them needs a cleanup
# lock on the leaf the cursor has pinned.
$node_primary->safe_psql(
	$db, qq[
BEGIN;
INSERT INTO pin_t SELECT 5 FROM generate_series(1, 50);
ROLLBACK;
BEGIN; LOCK pin_t; COMMIT;
]);
$node_primary->wait_for_replay_catchup($node_standby);

# An index-only cursor keeps the pin on its leaf between fetches (a plain
# index scan with an MVCC snapshot drops it once the page is read).
my $res = $psql_standby->query_safe(
	qq[
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
SET LOCAL enable_indexscan = off;
DECLARE c1 CURSOR FOR SELECT a FROM pin_t WHERE a > 0 ORDER BY a;
FETCH FORWARD FROM c1;
]);
like($res, qr/^1$/m, "$sect: index-only cursor holds its leaf");

$log_location = -s $node_standby->logfile;

# VACUUM removes the aborted entries from the cursor's leaf; replaying the
# BARK VACUUM record needs a cleanup lock on it.
$node_primary->safe_psql($db, 'VACUUM pin_t');
$node_primary->wait_for_replay_catchup($node_standby);

check_conflict_log("User was holding shared buffer pin for too long");
check_conflict_record('buffer pin', 'Bark/VACUUM');
$psql_standby->reconnect_and_clear();
check_conflict_stat('bufferpin');


## 2: snapshot conflict when a deleted page is reused
$sect = 'page reuse conflict';

$log_location = -s $node_standby->logfile;
my $lsn_before = $node_primary->safe_psql($db,
	'SELECT pg_current_wal_insert_lsn()');

# Empty a range of interior leaves.  The standby snapshot is taken after the
# DELETE commits, so the heap's prune records, whose horizon is the deleting
# transaction, do not conflict with it.  VACUUM then deletes the leaves with
# a safexid later than the snapshot's xmin, and a later VACUUM makes them
# reusable on the primary (no older snapshot there, and hot_standby_feedback
# is off, so the standby's does not count).
$node_primary->safe_psql($db, 'DELETE FROM reuse_t WHERE a BETWEEN 2000 AND 12000');
$node_primary->wait_for_replay_catchup($node_standby);

$res = $psql_standby->query_safe(
	qq[
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM reuse_t;
]);
like($res, qr/^9999$/m, "$sect: standby snapshot taken after the delete");

$node_primary->safe_psql($db, 'VACUUM reuse_t');
$node_primary->safe_psql($db, 'SELECT txid_current()');
$node_primary->safe_psql($db, 'VACUUM reuse_t');

# Splits now reuse the deleted pages, logging REUSE_PAGE with safexid as the
# conflict horizon.
$node_primary->safe_psql($db,
	"INSERT INTO reuse_t SELECT 2000 + g % 10000, repeat('y', 100) FROM generate_series(1, 20000) g"
);
$node_primary->wait_for_replay_catchup($node_standby);

my $lsn_after = $node_primary->safe_psql($db,
	'SELECT pg_current_wal_insert_lsn()');
my $reuse = waldump_count($lsn_before, $lsn_after, 'REUSE_PAGE');
cmp_ok($reuse, '>', 0, "$sect: primary logged Bark REUSE_PAGE records");

check_conflict_log(
	"User query might have needed to see row versions that must be removed");
check_conflict_record('snapshot', 'Bark/REUSE_PAGE');
$psql_standby->reconnect_and_clear();
ok( $node_standby->poll_query_until(
		$db,
		qq[SELECT confl_snapshot > 0 FROM pg_stat_database_conflicts WHERE datname = '$db';],
		't'),
	"$sect: stats show a snapshot conflict on the standby");

# The standby's view of the index agrees with the primary's.
my $primary_count = $node_primary->safe_psql($db,
	'SET enable_seqscan = off; SET enable_bitmapscan = off; SELECT count(*) FROM reuse_t');
my $standby_count = $node_standby->safe_psql($db,
	'SET enable_seqscan = off; SET enable_bitmapscan = off; SELECT count(*) FROM reuse_t');
is($standby_count, $primary_count, "$sect: standby index scan matches the primary");


## 3: no BARK record for a VACUUM that changes nothing
$sect = 'no needless records';

$node_primary->safe_psql(
	$db, qq[
CREATE TABLE quiet_t (a int);
INSERT INTO quiet_t SELECT g FROM generate_series(1, 20000) g;
CREATE INDEX quiet_t_idx ON quiet_t USING bark (a);
VACUUM quiet_t;
]);
$lsn_before = $node_primary->safe_psql($db, 'SELECT pg_current_wal_insert_lsn()');
$node_primary->safe_psql($db, 'VACUUM (INDEX_CLEANUP ON) quiet_t');
$lsn_after = $node_primary->safe_psql($db, 'SELECT pg_current_wal_insert_lsn()');
is(waldump_count($lsn_before, $lsn_after, 'VACUUM'), 0,
	"$sect: VACUUM of an unchanged index writes no Bark VACUUM record");

$psql_standby->quit;
$node_standby->stop;
$node_primary->stop;

done_testing();

# Bark records of the given type the primary wrote between two LSNs.
sub waldump_count
{
	my ($start, $end, $type) = @_;
	return 0 if $start eq $end;
	my ($stdout, $stderr) = run_command(
		[
			'pg_waldump', '--rmgr=Bark',
			'--path=' . $node_primary->data_dir . '/pg_wal',
			"--start=$start", "--end=$end"
		]);
	my @matches = ($stdout =~ /desc: $type /g);
	return scalar(@matches);
}

sub check_conflict_log
{
	my $message = shift;
	my $old_log_location = $log_location;

	$log_location = $node_standby->wait_for_log(qr/$message/, $log_location);

	cmp_ok($log_location, '>', $old_log_location,
		"$sect: logfile contains terminated connection due to recovery conflict"
	);
}

# The record whose replay waited on the conflict: the CONTEXT line after
# the "recovery still waiting" message for it names the record type.
sub check_conflict_record
{
	my ($kind, $record) = @_;
	my $log = slurp_file($node_standby->logfile);
	like(
		$log,
		qr/recovery conflict on $kind\n[^\n]*CONTEXT:  WAL redo at \S+ for \Q$record\E/,
		"$sect: the conflict comes from replaying $record");
}

sub check_conflict_stat
{
	my $conflict_type = shift;

	ok( $node_standby->poll_query_until(
			$db,
			qq[SELECT confl_$conflict_type > 0 FROM pg_stat_database_conflicts WHERE datname = '$db';],
			't'),
		"$sect: stats show conflict on standby");
}
