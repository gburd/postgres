
# Copyright (c) 2026, PostgreSQL Global Development Group

# BARK's dead-entry hints and simple deletion on a hot standby (patterned
# on 002_standby.pl):
#
# 1. Marking entries LP_DEAD writes no BARK record: with data checksums the
#    first hint on a page after a checkpoint logs a full-page image
#    (XLOG/FPI_FOR_HINT), and later hints are not logged at all, so the
#    standby's copy of the leaf has fewer marks than the primary's.  The
#    next BARK record on the page, checked with wal_consistency_checking,
#    must still match: bark_mask masks the line pointer flags and
#    BARK_HAS_GARBAGE.  Standby scans set no marks.
# 2. Simple deletion of the marked entries logs a Bark/DELETE record with
#    the deleted rows' conflict horizon; a standby snapshot taken after the
#    rows died does not conflict with it.
# 3. A standby snapshot that still sees the rows keeps seeing them while the
#    primary's marks reach the standby (in a page image), since standby
#    scans take no account of marks, and is cancelled by the Bark/DELETE
#    record's conflict.
#
# Each heap page holds one row with room to spare, so that no heap page is
# ever pruned: pruning would resolve the conflict first and leave the
# deletion record no horizon.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf(
	'postgresql.conf', qq[
max_standby_streaming_delay = 5s
log_recovery_conflict_waits = on
deadlock_timeout = 10ms
autovacuum = off
]);
$node_primary->start;

my $db = 'postgres';
$node_primary->safe_psql(
	$db, qq[
CREATE EXTENSION pageinspect;
CREATE EXTENSION amcheck;
CREATE TABLE flush_t (a int);
CREATE TABLE lpd (a int, pad text) WITH (fillfactor = 10);
CREATE INDEX lpd_a ON lpd USING bark (a) WITH (fillfactor = 100);
INSERT INTO lpd SELECT g, repeat('x', 500) FROM generate_series(1, 400) g;
]);
$node_primary->backup('backup');
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, 'backup', has_streaming => 1);
$node_standby->start;

my $checksums = $node_primary->safe_psql($db, 'SHOW data_checksums');
my $scan = 'SET enable_seqscan = off; SET enable_bitmapscan = off; '
  . 'SET enable_indexonlyscan = off; ';
my $sect;
my $log_location;

is( $node_primary->safe_psql(
		$db,
		"SELECT count(DISTINCT (ctid::text::point)[0]) = count(*) FROM lpd"),
	't',
	'each heap page holds one row');
is($node_primary->safe_psql($db, "SELECT pg_relation_size('lpd_a') / 8192"),
	'2', 'the index is the meta page and one leaf');

# The leaf's LP_DEAD entries, on a node.
sub ndead
{
	my ($node) = @_;
	return $node->safe_psql($db,
		"SELECT dead_items FROM bark_page_stats('lpd_a', 1)");
}

sub waldump_count
{
	my ($start, $end, $rmgr, $type) = @_;
	return 0 if $start eq $end;
	my ($stdout, $stderr) = run_command(
		[
			'pg_waldump', "--rmgr=$rmgr",
			'--path=' . $node_primary->data_dir . '/pg_wal',
			"--start=$start", "--end=$end"
		]);
	my @matches = ($stdout =~ /desc: $type/g);
	return scalar(@matches);
}

# A scan's hint page image is not flushed by its read-only transaction:
# commit a write so that the standby can catch up past it.
sub flush_wal
{
	$node_primary->safe_psql($db, 'INSERT INTO flush_t VALUES (1)');
}

sub lsn
{
	return $node_primary->safe_psql($db,
		'SELECT pg_current_wal_insert_lsn()');
}


## 1: the hints are not logged, and masked
$sect = 'hints';

$node_primary->safe_psql($db, 'DELETE FROM lpd WHERE a BETWEEN 101 AND 150');
$node_primary->wait_for_replay_catchup($node_standby);
is($node_standby->safe_psql($db, $scan . 'SELECT count(*) FROM lpd WHERE a > 0'),
	'350', "$sect: standby index scan skips the deleted rows");
is(ndead($node_standby), '0', "$sect: a standby scan marks nothing");

$node_primary->safe_psql($db, 'CHECKPOINT');
my $lsn0 = lsn();
is($node_primary->safe_psql($db, $scan . 'SELECT count(*) FROM lpd WHERE a > 0'),
	'350', "$sect: primary index scan");
my $lsn1 = lsn();
is(ndead($node_primary), '50', "$sect: the primary's scan marks 50 entries");
is(waldump_count($lsn0, $lsn1, 'Bark', '\S'),
	0, "$sect: marking writes no Bark record");
SKIP:
{
	skip 'data checksums are off', 1 unless $checksums eq 'on';
	cmp_ok(waldump_count($lsn0, $lsn1, 'XLOG', 'FPI_FOR_HINT'),
		'>', 0, "$sect: the first hint after a checkpoint logs a page image");
}

# More marks on the page, now unlogged: the page's LSN is past the
# checkpoint.  Then a Bark record on it, checked against the standby's
# replay.
$node_primary->safe_psql($db, 'DELETE FROM lpd WHERE a BETWEEN 151 AND 170');
$lsn0 = lsn();
$node_primary->safe_psql($db, $scan . 'SELECT count(*) FROM lpd WHERE a > 0');
$lsn1 = lsn();
is(ndead($node_primary), '70', "$sect: a later scan marks 20 more");
is(waldump_count($lsn0, $lsn1, 'XLOG', 'FPI_FOR_HINT'),
	0, "$sect: the later hints are not logged");
$lsn0 = lsn();
$node_primary->safe_psql($db,
	"SET wal_consistency_checking = 'Bark'; INSERT INTO lpd VALUES (1000, 'y')");
$lsn1 = lsn();
is(waldump_count($lsn0, $lsn1, 'Bark', 'INSERT_LEAF'),
	1, "$sect: the insert is a Bark/INSERT_LEAF record");
$node_primary->wait_for_replay_catchup($node_standby);
is(ndead($node_standby),
	$checksums eq 'on' ? '50' : '0',
	"$sect: the standby has only the logged marks");
is(ndead($node_primary), '70', "$sect: the primary keeps its marks");
is($node_standby->safe_psql($db, $scan . 'SELECT count(*) FROM lpd WHERE a > 0'),
	'331', "$sect: standby index scan after the insert");
unlike(
	slurp_file($node_standby->logfile),
	qr/inconsistent page/,
	"$sect: the masked pages are consistent");


## 2: simple deletion without a conflict
$sect = 'no conflict';

my $psql_standby = $node_standby->background_psql($db, on_error_stop => 0);
my $res = $psql_standby->query_safe(
	$scan . qq[
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM lpd WHERE a > 0;
]);
like($res, qr/^331$/m, "$sect: standby snapshot taken after the rows died");
$log_location = -s $node_standby->logfile;

# Fill the leaf: the insert that does not fit deletes the 70 marked entries
# instead of splitting it.
$lsn0 = lsn();
$node_primary->safe_psql($db,
	"INSERT INTO lpd SELECT g, 'y' FROM generate_series(1001, 1020) g");
$lsn1 = lsn();
is(ndead($node_primary), '0', "$sect: the marked entries are deleted");
is($node_primary->safe_psql($db, "SELECT pg_relation_size('lpd_a') / 8192"),
	'2', "$sect: the leaf did not split");
is(waldump_count($lsn0, $lsn1, 'Bark', 'DELETE snapshotConflictHorizon: [1-9]\d*, ndeleted: 70,'),
	1, "$sect: one Bark/DELETE record deletes them, with a conflict horizon");
$node_primary->wait_for_replay_catchup($node_standby);
$res = $psql_standby->query_safe('SELECT count(*) FROM lpd WHERE a > 0');
like($res, qr/^331$/m, "$sect: the standby snapshot is not cancelled");
$psql_standby->query_safe('COMMIT');
is(substr(slurp_file($node_standby->logfile), $log_location) =~
	  /recovery conflict/ ? 1 : 0,
	0, "$sect: no recovery conflict");


## 3: simple deletion conflicts with a snapshot that sees the rows
$sect = 'snapshot conflict';

$res = $psql_standby->query_safe(
	$scan . qq[
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM lpd WHERE a BETWEEN 251 AND 300;
]);
like($res, qr/^50$/m, "$sect: standby snapshot taken before the delete");

# The primary's snapshots do not include the standby's, so the rows are dead
# to every snapshot there and its scan marks them; after a checkpoint the
# marks reach the standby in a page image.
$node_primary->safe_psql($db, 'DELETE FROM lpd WHERE a BETWEEN 251 AND 300');
$node_primary->safe_psql($db, 'CHECKPOINT');
$node_primary->safe_psql($db, $scan . 'SELECT count(*) FROM lpd WHERE a > 0');
is(ndead($node_primary), '50', "$sect: the primary marks the 50 entries");
flush_wal();
$node_primary->wait_for_replay_catchup($node_standby);
SKIP:
{
	skip 'data checksums are off', 1 unless $checksums eq 'on';
	is(ndead($node_standby), '50', "$sect: the marks reach the standby");
}
$res = $psql_standby->query_safe(
	'SELECT count(*) FROM lpd WHERE a BETWEEN 251 AND 300');
like($res, qr/^50$/m,
	"$sect: the standby snapshot still sees the rows through the index");

$log_location = -s $node_standby->logfile;
$lsn0 = lsn();
$node_primary->safe_psql($db,
	"INSERT INTO lpd SELECT g, 'y' FROM generate_series(2001, 2100) g");
$lsn1 = lsn();
is(waldump_count($lsn0, $lsn1, 'Bark', 'DELETE snapshotConflictHorizon: [1-9]\d*, ndeleted: 50,'),
	1, "$sect: a Bark/DELETE record deletes the entries, with a conflict horizon");
$log_location = $node_standby->wait_for_log(
	qr/User query might have needed to see row versions that must be removed/,
	$log_location);
like(
	slurp_file($node_standby->logfile),
	qr/recovery conflict on snapshot\n(?:[^\n]*DETAIL:[^\n]*\n)?[^\n]*CONTEXT:  WAL redo at \S+ for Bark\/DELETE/,
	"$sect: the conflict comes from replaying Bark/DELETE");
$psql_standby->reconnect_and_clear();
ok( $node_standby->poll_query_until(
		$db,
		qq[SELECT confl_snapshot > 0 FROM pg_stat_database_conflicts WHERE datname = '$db';],
		't'),
	"$sect: stats show a snapshot conflict on the standby");

# Both indexes agree with the heap.
$node_primary->wait_for_replay_catchup($node_standby);
my $q = $scan . 'SELECT count(*), sum(a) FROM lpd WHERE a > 0';
is($node_standby->safe_psql($db, $q),
	$node_primary->safe_psql($db, $q),
	'standby index scan matches the primary');
$node_primary->safe_psql($db, "SELECT bark_index_check('lpd_a', true)");
$node_standby->safe_psql($db, "SELECT bark_index_check('lpd_a', true)");
pass('bark_index_check on the primary and the standby');

$psql_standby->quit;
$node_standby->stop;
$node_primary->stop;
done_testing();
