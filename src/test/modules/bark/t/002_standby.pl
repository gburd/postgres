
# Copyright (c) 2026, PostgreSQL Global Development Group

# BARK's own WAL records on a hot standby (patterned on
# src/test/recovery/t/031_recovery_conflict.pl):
#
# 1. Replaying VACUUM's record takes a cleanup lock, so a standby cursor
#    holding a pin on the leaf makes replay wait and then cancels it.
# 2. Reusing a deleted page logs a conflict horizon, which cancels a
#    standby snapshot that might still hold a link to the page.
# 3. VACUUM writes no BARK record for a leaf it does not change.
# 4. Inserts are logged as BARK INSERT_LEAF, INSERT_UPPER, OVERWRITE and
#    ADD_TID records, and the standby's index finds the same rows as the
#    primary's.
# 5. Splits of leaves and internal pages are logged as BARK SPLIT records,
#    and new levels as NEWROOT, and the standby's index finds the same rows
#    as the primary's.  The left half of a split is rebuilt from the
#    original page, with the new entry or a divided entry's left part when
#    those stay left, or logged whole when it takes a new prefix.
# 6. Bottom-up deletion logs a conflict horizon, which cancels a standby
#    snapshot that can still see the heap tuples whose entries it deletes.

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
wal_keep_size = 1GB
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


## 4: insert records
$sect = 'insert records';

# Unique keys add entries and split leaves under a parent with room
# (INSERT_LEAF, INSERT_UPPER); keys inserted round-robin and in runs form
# LIST and POSTING entries (OVERWRITE) and then grow them a TID at a time
# (ADD_TID).
$lsn_before = $node_primary->safe_psql($db, 'SELECT pg_current_wal_insert_lsn()');
$node_primary->safe_psql(
	$db, qq[
CREATE TABLE ins_t (a int, b int);
CREATE INDEX ins_t_idx ON ins_t USING bark (a);
INSERT INTO ins_t SELECT g, g FROM generate_series(1, 20000) g;
INSERT INTO ins_t SELECT 100000 + g % 100, g FROM generate_series(1, 20000) g;
INSERT INTO ins_t SELECT 200000 + (g - 1) / 1000, g FROM generate_series(1, 20000) g;
]);
$lsn_after = $node_primary->safe_psql($db, 'SELECT pg_current_wal_insert_lsn()');
$node_primary->wait_for_replay_catchup($node_standby);

foreach my $type ('INSERT_LEAF', 'INSERT_UPPER', 'OVERWRITE', 'ADD_TID')
{
	cmp_ok(waldump_count($lsn_before, $lsn_after, $type),
		'>', 0, "$sect: primary logged Bark $type records");
}

my $ins_query = qq[
SET enable_seqscan = off; SET enable_bitmapscan = off;
SELECT count(*), sum(a), sum(b) FROM ins_t WHERE a > 0];
$primary_count = $node_primary->safe_psql($db, $ins_query);
is($primary_count, '60000|6201190000|600030000',
	"$sect: primary index scan finds every row");
is($node_standby->safe_psql($db, $ins_query),
	$primary_count, "$sect: standby index scan matches the primary");


## 5: split and new-root records
$sect = 'split records';

# 300-byte keys fit about 25 to a page at every level, so 20000 of them,
# inserted in random order, split leaves and internal pages and grow the
# tree to three levels.
$lsn_before = $node_primary->safe_psql($db, 'SELECT pg_current_wal_insert_lsn()');
$node_primary->safe_psql(
	$db, qq[
CREATE TABLE split_t (k int, s text);
CREATE INDEX split_t_idx ON split_t USING bark (s);
SELECT setseed(0.5);
INSERT INTO split_t SELECT g, lpad(g::text, 300, '0')
  FROM generate_series(1, 20000) g ORDER BY random();
]);
$lsn_after = $node_primary->safe_psql($db, 'SELECT pg_current_wal_insert_lsn()');
$node_primary->wait_for_replay_catchup($node_standby);

cmp_ok(waldump_count($lsn_before, $lsn_after, 'SPLIT level: 0,'),
	'>', 0, "$sect: primary logged Bark SPLIT records for leaves");
cmp_ok(waldump_count($lsn_before, $lsn_after, 'SPLIT level: [1-9]\\d*,'),
	'>', 0, "$sect: primary logged Bark SPLIT records for internal pages");
cmp_ok(waldump_count($lsn_before, $lsn_after, 'NEWROOT root: \\d+, level: 2,'),
	'>', 0, "$sect: primary logged a Bark NEWROOT record for a third level");

my $split_query = qq[
SET enable_seqscan = off; SET enable_bitmapscan = off;
SELECT count(*), sum(k) FROM split_t WHERE s > ''];
$primary_count = $node_primary->safe_psql($db, $split_query);
is($primary_count, '20000|200010000', "$sect: primary index scan finds every row");
is($node_standby->safe_psql($db, $split_query),
	$primary_count, "$sect: standby index scan matches the primary");

cmp_ok(waldump_count($lsn_before, $lsn_after, 'SPLIT [^\n]* firstrightoff: \\d+, newitemoff:'),
	'>', 0, "$sect: primary logged SPLIT records with the new entry on the left");

# Prefix-coded leaves: the first run of keys shares a long prefix, which
# each left half keeps; the second run shares none, so a split among its
# keys takes the prefix away from the left half, which is logged whole.
# wal_consistency_checking makes the standby compare each half it rebuilds.
$lsn_before = $node_primary->safe_psql($db, 'SELECT pg_current_wal_insert_lsn()');
$node_primary->safe_psql(
	$db, qq[
SET wal_consistency_checking = 'Bark';
CREATE EXTENSION amcheck;
CREATE TABLE splitp_t (k int, s text);
CREATE INDEX splitp_t_idx ON splitp_t USING bark (s)
  WITH (prefix_compression = on);
SELECT setseed(0.7);
INSERT INTO splitp_t SELECT g, 'https://www.example.com/item/' || lpad(g::text, 8, '0')
  FROM generate_series(1, 5000) g ORDER BY random();
INSERT INTO splitp_t SELECT g, md5(g::text) || 'https://x/' || g
  FROM generate_series(1, 2000) g ORDER BY random();
]);
$lsn_after = $node_primary->safe_psql($db, 'SELECT pg_current_wal_insert_lsn()');

cmp_ok(waldump_count($lsn_before, $lsn_after, 'SPLIT [^\n]* prefix: L., firstrightoff:'),
	'>', 0, "$sect: primary logged SPLIT records keeping the left prefix");
cmp_ok(waldump_count($lsn_before, $lsn_after, 'SPLIT [^\n]* left logged'),
	'>', 0, "$sect: primary logged SPLIT records with the left half whole");

# One key: each split cuts the last POSTING entry of the left page, whose
# left part stays there (the replaced entry).  Too many records for
# wal_consistency_checking; the standby's index is checked instead.
$lsn_before = $lsn_after;
$node_primary->safe_psql(
	$db, qq[
CREATE TABLE splitv_t (k int);
CREATE INDEX splitv_t_idx ON splitv_t USING bark (k);
INSERT INTO splitv_t SELECT 1 FROM generate_series(1, 150000);
]);
$lsn_after = $node_primary->safe_psql($db, 'SELECT pg_current_wal_insert_lsn()');
$node_primary->wait_for_replay_catchup($node_standby);

cmp_ok(waldump_count($lsn_before, $lsn_after, 'SPLIT [^\n]* replaceoff:'),
	'>', 0, "$sect: primary logged SPLIT records replacing an entry on the left");

$split_query = qq[
SET enable_seqscan = off; SET enable_bitmapscan = off;
SELECT count(*), sum(k) FROM splitp_t WHERE s > ''
UNION ALL
SELECT count(*), sum(k) FROM splitv_t WHERE k = 1];
$primary_count = $node_primary->safe_psql($db, $split_query);
is($primary_count, "7000|14503500\n150000|150000",
	"$sect: primary index scans find every row");
is($node_standby->safe_psql($db, $split_query),
	$primary_count, "$sect: standby index scans match the primary");
is($node_standby->safe_psql($db,
		"SELECT bark_index_check('splitp_t_idx'), bark_index_check('splitv_t_idx')"),
	'|', "$sect: standby indexes pass bark_index_check");



## 6: snapshot conflict on bottom-up deletion
$sect = 'bottom-up deletion conflict';

# Every UPDATE of v is non-HOT and leaves k unchanged, so the k index takes
# each new version with indexUnchanged set, and a leaf that is full first has
# the entries of dead versions deleted, in a Bark/DELETE record whose conflict
# horizon is the transaction that left them dead.  The standby snapshot is
# taken before the first round of UPDATEs, so it can still see the versions
# that round leaves dead.  The first round's sequential scan prunes nothing,
# since the heap holds no dead tuples yet, and returns the new versions'
# TIDs; the second round updates the rows by TID, and a TID scan does not
# prune the pages it reads.  So the Bark/DELETE records of the second round
# are the only records the standby snapshot conflicts with.  The u index
# has prefix-coded leaves.  A tenth of the rows are deleted before the
# snapshot, so that some entries die whole (their deletion has a horizon the
# snapshot does not conflict with), while the updated rows' entries lose
# members.  The UPDATEs set wal_consistency_checking, so the standby checks
# each page that DELETE redo produces against the primary's.
$node_primary->safe_psql(
	$db, qq[
CREATE TABLE churn_t (k int, v int, u text)
  WITH (fillfactor = 100, autovacuum_enabled = off);
CREATE INDEX churn_t_k ON churn_t USING bark (k);
CREATE INDEX churn_t_v ON churn_t USING bark (v);
CREATE INDEX churn_t_u ON churn_t USING bark (u)
  WITH (prefix_compression = on);
INSERT INTO churn_t
  SELECT g, 0, 'https://www.example.com/item/' || lpad((g % 1250)::text, 8, '0')
  FROM generate_series(1, 5000) g;
DELETE FROM churn_t WHERE k % 10 = 0;
]);
$node_primary->wait_for_replay_catchup($node_standby);

$res = $psql_standby->query_safe(
	qq[
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM churn_t;
]);
like($res, qr/^4500$/m, "$sect: standby snapshot taken before the updates");

# The page images wal_consistency_checking adds make replay fall behind, and
# a standby more than max_standby_streaming_delay behind cancels a
# conflicting query at once, without the wait whose log line names the
# record.  So the standby waits longer in this section, and catches up
# between the rounds.
$node_standby->append_conf('postgresql.conf', 'max_standby_streaming_delay = 5s');
$node_standby->reload;

$log_location = -s $node_standby->logfile;
$lsn_before = $node_primary->safe_psql($db, 'SELECT pg_current_wal_insert_lsn()');
my $tids = $node_primary->safe_psql($db,
	"SET wal_consistency_checking = 'Bark'; WITH u AS (UPDATE churn_t SET v = v + 1 RETURNING ctid) SELECT array_agg(ctid) FROM u"
);
$node_primary->wait_for_replay_catchup($node_standby);
like(
	$node_primary->safe_psql(
		$db,
		"SET enable_seqscan = off; EXPLAIN (COSTS OFF) UPDATE churn_t SET v = v + 1 WHERE ctid = ANY ('$tids'::tid[])"
	),
	qr/Tid Scan/,
	"$sect: the second round updates by TID scan");
$node_primary->safe_psql($db,
	"SET wal_consistency_checking = 'Bark'; SET enable_seqscan = off; UPDATE churn_t SET v = v + 1 WHERE ctid = ANY ('$tids'::tid[])"
);
$node_primary->wait_for_replay_catchup($node_standby);
$lsn_after = $node_primary->safe_psql($db, 'SELECT pg_current_wal_insert_lsn()');

cmp_ok(waldump_count($lsn_before, $lsn_after, 'DELETE snapshotConflictHorizon: [1-9]\\d*,'),
	'>', 0, "$sect: primary logged Bark DELETE records with a conflict horizon");
cmp_ok(waldump_count($lsn_before, $lsn_after, 'DELETE [^\\n]* ndeleted: [1-9]\\d*,'),
	'>', 0, "$sect: primary logged Bark DELETE records that delete entries");
cmp_ok(waldump_count($lsn_before, $lsn_after, 'DELETE [^\\n]* nupdated: [1-9]\\d*,'),
	'>', 0, "$sect: primary logged Bark DELETE records that rewrite entries");

check_conflict_log(
	"User query might have needed to see row versions that must be removed");
check_conflict_record('snapshot', 'Bark/DELETE');
$psql_standby->reconnect_and_clear();

my $churn_query = qq[
SET enable_seqscan = off; SET enable_bitmapscan = off;
SELECT count(*), sum(k), sum(v) FROM churn_t WHERE k > 0
UNION ALL
SELECT count(*), count(DISTINCT u), 0 FROM churn_t WHERE u > 'h'];
$primary_count = $node_primary->safe_psql($db, $churn_query);
is($primary_count, "4500|11250000|9000\n4500|1125|0",
	"$sect: primary index scans find every row");
is($node_standby->safe_psql($db, $churn_query),
	$primary_count, "$sect: standby index scans match the primary");

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
