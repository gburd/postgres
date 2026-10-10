# Copyright (c) 2026, PostgreSQL Global Development Group

# Crash recovery replays BARK records of indexes that were dropped before the
# crash.  The drop truncated their files to zero, so redo finds no page for
# most of the records (XLogReadBufferForRedo returns BLK_NOTFOUND) until the
# replayed commit of the drop removes the files.  One index is not dropped:
# its bottom-up deletions and page reuse are replayed outside hot standby,
# and it must come back intact.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
# wal_level=replica: REUSE_PAGE is logged only for standbys.  As in 005, no
# full-page images, which would restore the pages redo must not find.  This
# controlled process crash does not simulate torn writes, for which disabling
# full_page_writes is unsafe.  No checkpoint may start during the workload.
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', qq[
autovacuum = off
full_page_writes = off
wal_consistency_checking = ''
checkpoint_timeout = 1h
max_wal_size = 8GB
]);
$node->start;

# The tables and indexes of bark_walinspect.sql, and keep_t, whose keep_t_k
# is the one index not dropped (keep_t_v makes its updates non-HOT).
$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE TABLE wi_split (k int, s text) WITH (autovacuum_enabled = off);
CREATE INDEX wi_split_idx ON wi_split USING bark (s);
CREATE TABLE wi_prefix (k int, s text) WITH (autovacuum_enabled = off);
CREATE INDEX wi_prefix_idx ON wi_prefix USING bark (s)
  WITH (prefix_compression = on);
CREATE TABLE wi_dup (k int) WITH (autovacuum_enabled = off);
CREATE INDEX wi_dup_idx ON wi_dup USING bark (k);
CREATE TABLE wi_hkl (k int, v int) WITH (fillfactor = 50, autovacuum_enabled = off);
CREATE INDEX wi_hkl_idx ON wi_hkl USING bark (k);
CREATE TABLE wi_hkp (k int, v int) WITH (fillfactor = 50, autovacuum_enabled = off);
CREATE INDEX wi_hkp_idx ON wi_hkp USING bark (k);
CREATE TABLE wi_big (id int, k text) WITH (autovacuum_enabled = off);
CREATE INDEX wi_big_idx ON wi_big USING bark (k);
CREATE TABLE wi_churn (k int, v int, u text)
  WITH (fillfactor = 100, autovacuum_enabled = off);
CREATE INDEX wi_churn_k ON wi_churn USING bark (k);
CREATE INDEX wi_churn_v ON wi_churn USING bark (v);
CREATE INDEX wi_churn_u ON wi_churn USING bark (u)
  WITH (prefix_compression = on);
CREATE TABLE wi_vac (a int, pad text) WITH (autovacuum_enabled = off);
INSERT INTO wi_vac SELECT g, repeat('x', 100) FROM generate_series(1, 20000) g;
INSERT INTO wi_vac SELECT 100000 + g % 20, 'y' FROM generate_series(1, 20000) g;
CREATE INDEX wi_vac_idx ON wi_vac USING bark (a);
CREATE TABLE keep_t (k int, v int, pad text)
  WITH (fillfactor = 100, autovacuum_enabled = off);
CREATE INDEX keep_t_k ON keep_t USING bark (k);
CREATE INDEX keep_t_v ON keep_t USING bark (v);
]);
my @dropped = qw(wi_split_idx wi_prefix_idx wi_dup_idx wi_hkl_idx wi_hkp_idx
  wi_big_idx wi_churn_k wi_churn_v wi_churn_u wi_vac_idx keep_t_v);
my @tables = qw(wi_split wi_prefix wi_dup wi_hkl wi_hkp wi_big wi_churn
  wi_vac keep_t);

# pg_waldump's --relation argument for an index's current file.
sub relpath
{
	my $idx = shift;
	return $node->safe_psql('postgres', qq[
SELECT format('%s/%s/%s',
  (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'),
  (SELECT oid FROM pg_database WHERE datname = current_database()),
  pg_relation_filenode('$idx'));
]);
}
my %files;    # every file of a dropped index, including those REINDEX replaces
$files{ relpath($_) } = $_ foreach @dropped;
my @at_checkpoint = sort keys %files;
my $keep_file = relpath('keep_t_k');

sub lsn { return $node->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()') }
sub lsn_ge
{
	my ($x, $y) = @_;
	return $node->safe_psql('postgres', "SELECT '$x'::pg_lsn >= '$y'::pg_lsn") eq 't';
}

$node->safe_psql('postgres', 'CHECKPOINT');
my $redo = $node->safe_psql('postgres',
	'SELECT redo_lsn FROM pg_control_checkpoint()');
my $start = lsn();
ok(lsn_ge($start, $redo), 'the workload starts after the redo pointer');

# The bark_walinspect.sql workload, without its checkpoints.  See there for
# which record types each part logs.
$node->safe_psql('postgres', q[
SELECT setseed(0.5);
INSERT INTO wi_split SELECT g, lpad(g::text, 300, '0')
  FROM generate_series(1, 20000) g ORDER BY random();
SELECT setseed(0.7);
INSERT INTO wi_prefix SELECT g, 'https://www.example.com/item/' || lpad(g::text, 8, '0')
  FROM generate_series(1, 5000) g ORDER BY random();
INSERT INTO wi_prefix SELECT g, md5(g::text) || 'https://x/' || g
  FROM generate_series(1, 2000) g ORDER BY random();
]);
$node->safe_psql('postgres', q[
INSERT INTO wi_dup SELECT 1 FROM generate_series(1, 150000);
INSERT INTO wi_dup SELECT 100000 + g % 100 FROM generate_series(1, 20000) g;
INSERT INTO wi_dup SELECT 200000 + (g - 1) / 1000 FROM generate_series(1, 20000) g;
]);
$node->safe_psql('postgres', q[
INSERT INTO wi_hkl SELECT g % 50, g FROM generate_series(1, 100000) g;
UPDATE wi_hkl SET k = 1 WHERE k = 2 AND v % 4 = 2;
]);
# REINDEX replaces the file; the old one is also dropped before the crash.
$files{ relpath('wi_hkl_idx') } = 'wi_hkl_idx (pre-REINDEX)';
$node->safe_psql('postgres', q[
REINDEX INDEX wi_hkl_idx;
UPDATE wi_hkl SET k = 1 WHERE k = 3 AND v % 4 = 3;
INSERT INTO wi_hkp SELECT g % 2, g FROM generate_series(1, 100000) g;
UPDATE wi_hkp SET k = 1 WHERE k = 0 AND v % 4 = 0;
]);
$files{ relpath('wi_hkp_idx') } = 'wi_hkp_idx (pre-REINDEX)';
$node->safe_psql('postgres', q[
REINDEX INDEX wi_hkp_idx;
UPDATE wi_hkp SET k = 1 WHERE k = 0 AND v % 4 = 2;
]);
$files{ relpath($_) } = $_ foreach qw(wi_hkl_idx wi_hkp_idx);
$node->safe_psql('postgres', q[
INSERT INTO wi_big SELECT g, (SELECT string_agg(md5(g::text || i::text), '')
                              FROM generate_series(1, 160 + (g % 3) * 300) i)
  FROM generate_series(1, 30) g;
DELETE FROM wi_big WHERE id % 2 = 0;
VACUUM wi_big;
INSERT INTO wi_churn
  SELECT g, 0, 'https://www.example.com/item/' || lpad((g % 1250)::text, 8, '0')
  FROM generate_series(1, 5000) g;
DELETE FROM wi_churn WHERE k % 10 = 0;
UPDATE wi_churn SET v = v + 1;
UPDATE wi_churn SET v = v + 1;
]);
$node->safe_psql('postgres', q[
DELETE FROM wi_vac WHERE a BETWEEN 2000 AND 12000 OR (a > 100000 AND ctid::text LIKE '%7)');
VACUUM wi_vac;
DELETE FROM wi_vac WHERE a % 3 = 0 OR (a > 100000 AND ctid::text LIKE '%3)');
VACUUM wi_vac;
SELECT 1 FROM txid_current();
VACUUM wi_vac;
INSERT INTO wi_vac SELECT 2000 + g % 10000, repeat('z', 100)
  FROM generate_series(1, 20000) g;
]);

# keep_t, as wi_churn then wi_vac: bottom-up deletion (DELETE), then emptied
# leaves unlinked and, once recyclable, reused (REUSE_PAGE).
$node->safe_psql('postgres', q[
INSERT INTO keep_t SELECT g, 0, repeat('k', 40) FROM generate_series(1, 20000) g;
DELETE FROM keep_t WHERE k % 10 = 0;
UPDATE keep_t SET v = v + 1;
UPDATE keep_t SET v = v + 1;
DELETE FROM keep_t WHERE k BETWEEN 4000 AND 14000;
VACUUM keep_t;
SELECT 1 FROM txid_current();
VACUUM keep_t;
]);
my $reuse_start = lsn();
$node->safe_psql('postgres', q[
INSERT INTO keep_t SELECT 4000 + g % 10000, 3, repeat('r', 40)
  FROM generate_series(1, 20000) g;
]);
my $reuse_end = lsn();

is($node->safe_psql('postgres', q[
SELECT count(*) FILTER (WHERE bark_index_check(i, true) IS NOT NULL)
  FROM unnest(ARRAY['wi_split_idx', 'wi_prefix_idx', 'wi_dup_idx', 'wi_hkl_idx',
                    'wi_hkp_idx', 'wi_big_idx', 'wi_churn_k', 'wi_churn_v',
                    'wi_churn_u', 'wi_vac_idx', 'keep_t_k', 'keep_t_v']::regclass[]) i;
]), '12', 'every index passes amcheck before the crash');

# The pre-crash snapshot of every table, read by sequential scans.
my $seq_settings = q[
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
];
my $snapshot_sql = $seq_settings . join("\n", map {
	"SELECT '$_', count(*), md5(string_agg(t::text, E'\\n' ORDER BY t::text)) FROM $_ t;"
} @tables);
my $snapshot = $node->safe_psql('postgres', $snapshot_sql);

my $drop_start = lsn();
$node->safe_psql('postgres', 'DROP INDEX ' . join(', ', @dropped));
my $end = lsn();
is($node->safe_psql('postgres',
		'SELECT redo_lsn FROM pg_control_checkpoint()'),
	$redo, 'no checkpoint moved the redo pointer past the workload');

sub waldump
{
	my @args = @_;
	my ($out, $err) = run_command(
		[
			'pg_waldump', '--rmgr=Bark',
			'--path=' . $node->data_dir . '/pg_wal', @args
		]);
	is($err, '', "pg_waldump @args reads the WAL without errors");
	return $out;
}
sub type_counts
{
	my %n = (shift =~ /^Bark\/(\w+)\s+(\d+)/mg);
	return %n;
}

# Every BARK record type in the replayed interval, and for each dropped file
# the records that refer to its pages.
my $all = waldump('--stats=record', "--start=$start", "--end=$end");
my %all = type_counts($all);
my $log = "redo $redo, WAL interval: $start .. $end\n$all\n";
my %dropped_types;
foreach my $file (sort keys %files)
{
	my $stats = waldump('--stats=record', "--relation=$file",
		"--start=$start", "--end=$end");
	my %n = type_counts($stats);
	$log .= "$files{$file} ($file)\n$stats\n";
	ok(scalar(%n), "$files{$file}: its records are in the replayed interval");
	$dropped_types{$_} += $n{$_} foreach keys %n;
}
# REUSE_PAGE changes no page: it names its index in the record, not in a
# block reference, so the type is checked over the whole interval.
foreach my $type (qw(INSERT_LEAF INSERT_UPPER SPLIT NEWROOT OVERWRITE ADD_TID
	INSERT_SWAP OVERFLOW VACUUM DELETE MERGE UNLINK_PAGE MARK_DELETED))
{
	ok($dropped_types{$type}, "$type records of dropped indexes are replayed");
}
ok($all{REUSE_PAGE}, 'REUSE_PAGE records are replayed');

# The kept index: bottom-up deletions and the reuse of its own pages.
my %keep = type_counts(waldump('--stats=record', "--relation=$keep_file",
	"--start=$start", "--end=$end"));
$log .= "keep_t_k ($keep_file)\n" . join(', ', map { "$_ $keep{$_}" } sort keys %keep) . "\n";
ok($keep{DELETE}, 'keep_t_k logged bottom-up deletions');
ok($keep{UNLINK_PAGE}, 'keep_t_k logged page deletions');
my $reuse = waldump("--start=$reuse_start", "--end=$reuse_end");
my $keep_reuse = () = $reuse =~ /desc: REUSE_PAGE rel: \Q$keep_file\E, /mg;
$log .= "keep_t_k REUSE_PAGE records: $keep_reuse\n";
ok($keep_reuse > 0, 'keep_t_k logged the reuse of its deleted pages');
append_to_file("$PostgreSQL::Test::Utils::log_path/redo_dropped_wal.log", $log);

# xlogutils.c reports each block redo cannot read at DEBUG1.
$node->append_conf('postgresql.conf', "log_min_messages = 'warning, startup:debug1'\n");
$node->stop('immediate');
$node->start;
ok($node->log_contains('database system was interrupted'),
	'server went through crash recovery');
# Files written after the checkpoint (REINDEX) were logged whole, so redo
# finds their pages; the ones the checkpoint saw were truncated by the drop.
foreach my $file (@at_checkpoint)
{
	my (undef, $db, $fn) = split m{/}, $file;
	ok($node->log_contains(
			"page \\d+ of relation base/$db/$fn (does not exist|is uninitialized)"),
		"$files{$file}: redo found no page for some of its records");
}
ok($node->log_contains("redo starts at $redo"),
	'recovery started at the redo pointer, before the workload');
my ($done) = (slurp_file($node->logfile) =~ /redo done at ([0-9A-F]+\/[0-9A-F]+)/);
ok(defined $done && lsn_ge($done, $drop_start),
	'recovery replayed the commit of the drop');

is($node->safe_psql('postgres', q[
SELECT count(*) FROM pg_class WHERE relname IN ('wi_split_idx', 'wi_prefix_idx',
  'wi_dup_idx', 'wi_hkl_idx', 'wi_hkp_idx', 'wi_big_idx', 'wi_churn_k',
  'wi_churn_v', 'wi_churn_u', 'wi_vac_idx', 'keep_t_v', 'keep_t_k');
]), '1', 'only the kept index remains');
is($node->safe_psql('postgres', $snapshot_sql), $snapshot,
	'every table matches its pre-crash snapshot');

# keep_t_k against a sequential scan, by index and index-only scans.
my $query = 'SELECT k, v, pad FROM keep_t ORDER BY k';
my $index_settings = q[
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
];
my $ios_settings = q[
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = off;
];
my $expected = $node->safe_psql('postgres', "$seq_settings $query");
like($node->safe_psql('postgres', "$index_settings EXPLAIN (COSTS OFF) $query"),
	qr/Index Scan using keep_t_k/, 'recovered query uses keep_t_k');
# Rows of equal k come back in heap order from neither plan reliably.
is(join("\n", sort split /\n/, $node->safe_psql('postgres', "$index_settings $query")),
	join("\n", sort split /\n/, $expected),
	'index scan returns every row exactly once');
my $kquery = 'SELECT k FROM keep_t ORDER BY k';
like($node->safe_psql('postgres', "$ios_settings EXPLAIN (COSTS OFF) $kquery"),
	qr/Index Only Scan using keep_t_k/, 'key query uses an index-only scan');
is($node->safe_psql('postgres', "$ios_settings $kquery"),
	$node->safe_psql('postgres', "$seq_settings $kquery"),
	'index-only scan returns every key exactly once');
is($node->safe_psql('postgres', "SELECT bark_index_check('keep_t_k', true)"),
	'', 'recovered keep_t_k passes amcheck with heapallindexed');

# Let the recovery process save its coverage counters.
$node->stop;
done_testing();
