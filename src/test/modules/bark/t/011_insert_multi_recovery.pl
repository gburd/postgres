# Copyright (c) 2026, PostgreSQL Global Development Group

# Crash recovery of INSERT_MULTI, the record that adds several entries of
# one multikey row to a leaf.
#
# 1. After a checkpoint, a row's three keys go on the root leaf in one
#    record with an applied full-page image, and the next row's keys, which
#    fall between the existing entries, in one without.  Recovery restores
#    the image and then redoes the second record on it (BLK_NEEDS_REDO),
#    adding its entries at the logged offsets.
# 2. With full_page_writes off, two more rows are added, and the leaf is
#    flushed and evicted without a checkpoint, so the page on disk is newer
#    than both records and redo must leave it alone (BLK_DONE).
# 3. An unlogged multikey index takes the same path with a fake LSN.
#
# After each crash the index entries are the heap's (key, TID) pairs, scans
# by the index return a sequential scan's rows, and bark_index_check accepts
# the index.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', qq[
autovacuum = off
wal_keep_size = 1GB
full_page_writes = on
wal_consistency_checking = ''
checkpoint_timeout = 1h
max_wal_size = 1GB
]);
$node->start;
# The first row sets the multikey flag, so the workload logs no meta page.
$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE EXTENSION bark_multikey;
CREATE EXTENSION pageinspect;
CREATE EXTENSION pg_buffercache;
CREATE TABLE multi_t (id int, a int4[]) WITH (autovacuum_enabled = off);
INSERT INTO multi_t VALUES (1, '{1,2}');
CREATE INDEX multi_idx ON multi_t USING bark (a bark_int4_array_ops);
]);
is($node->safe_psql('postgres',
		"SELECT multikey FROM bark_multikey_meta('multi_idx')"),
	't', 'the index is multikey before the workload');

sub waldump
{
	my ($start, $end) = @_;
	my ($wal, $stderr) = run_command(
		[
			'pg_waldump', '--rmgr=Bark',
			'--path=' . $node->data_dir . '/pg_wal',
			"--start=$start", "--end=$end"
		]);
	# pg_waldump reports, on stderr, skipping a page header when a start LSN
	# falls on a page boundary; that is not an error.
	$stderr =~ s/^pg_waldump: first record is after .*\n//m;
	is($stderr, '', 'pg_waldump reads the workload WAL without errors');
	append_to_file("$PostgreSQL::Test::Utils::log_path/insert_multi_wal.log",
		"WAL interval: $start .. $end\n$wal\n");
	return $wal;
}
sub insert_lsn
{
	return $node->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');
}
sub redo_lsn
{
	return $node->safe_psql('postgres',
		'SELECT redo_lsn FROM pg_control_checkpoint()');
}

# The index against the heap after a crash: its entries are the rows'
# (key, TID) pairs, each once, and index and bitmap scans return the rows a
# sequential scan does.
my $seq_settings = q[
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
];
my $index_settings = q[
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
];
my $bitmap_settings = q[
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
];
sub check_index
{
	my ($table, $idx, $rows, $what) = @_;
	is($node->safe_psql('postgres', "SELECT count(*) FROM $table"),
		$rows, "$what: every committed row survived recovery");
	is($node->safe_psql('postgres', qq[
SELECT key, tid FROM bark_multikey_entries('$idx') WHERE NOT marker
ORDER BY key, tid]),
		$node->safe_psql('postgres', qq[
$seq_settings
SELECT DISTINCT k, ctid FROM $table, unnest(a) k ORDER BY k, ctid]),
		"$what: the index holds each row's keys exactly once");
	my $query = "SELECT id FROM $table WHERE a && '{0,2,10,21,24,50}' ORDER BY id";
	my $expected = $node->safe_psql('postgres', "$seq_settings $query");
	like($node->safe_psql('postgres', "$index_settings EXPLAIN (COSTS OFF) $query"),
		qr/Index Scan using $idx/, "$what: the query uses an index scan");
	is($node->safe_psql('postgres', "$index_settings $query"),
		$expected, "$what: the index scan returns the sequential scan's rows");
	like($node->safe_psql('postgres', "$bitmap_settings EXPLAIN (COSTS OFF) $query"),
		qr/Bitmap Index Scan on $idx/, "$what: the query uses a bitmap scan");
	is($node->safe_psql('postgres', "$bitmap_settings $query"),
		$expected, "$what: the bitmap scan returns the sequential scan's rows");
	is($node->safe_psql('postgres', "SELECT bark_index_check('$idx')"),
		'', "$what: the index passes bark_index_check");
}

## 1: an image, then a record redone on it
$node->safe_psql('postgres', 'CHECKPOINT');
my $redo = redo_lsn();
my $start = insert_lsn();
$node->safe_psql('postgres', "INSERT INTO multi_t VALUES (2, '{20,21,22}')");
my $mid = insert_lsn();
$node->safe_psql('postgres', "INSERT INTO multi_t VALUES (3, '{0,10,25}')");
my $end = insert_lsn();
my $wal = waldump($start, $end);
my @records = ($wal =~ /^.*desc:.*$/mg);
is(scalar @records, 2, 'the two rows logged exactly two BARK records');
like($wal,
	qr/desc: INSERT_MULTI nentries: 3, offsets: \[3, 4, 5\], blkref #0: rel \d+\/\d+\/\d+ blk 1 FPW$/m,
	'the first row is one INSERT_MULTI with an applied full-page image');
unlike($wal, qr/for WAL verification/,
	'the image is not just for WAL consistency checking');
# Keys 0, 10 and 25 go before entry 1, before entry 3 and after entry 5 of
# the page the record finds; each offset counts the entries before it.
like($wal,
	qr/desc: INSERT_MULTI nentries: 3, offsets: \[1, 4, 8\], blkref #0: rel \d+\/\d+\/\d+ blk 1$/m,
	'the second row is one INSERT_MULTI without an image');
is(redo_lsn(), $redo, 'no checkpoint moved the redo pointer past the workload');

$node->stop('immediate');
$node->start;
ok($node->log_contains('database system was interrupted'),
	'server went through crash recovery');
ok($node->log_contains("redo starts at $redo"),
	'recovery started before both records');
check_index('multi_t', 'multi_idx', 3, 'image then redo');

## 2: the page on disk is newer than both records
$node->safe_psql('postgres', 'ALTER SYSTEM SET full_page_writes = off');
$node->reload;
is($node->safe_psql('postgres', 'SHOW full_page_writes'), 'off',
	'full_page_writes is off');
$node->safe_psql('postgres', 'CHECKPOINT');
$redo = redo_lsn();
$start = insert_lsn();
# As in 005: no images, since an applied image would overwrite the newer
# on-disk leaf.  This controlled process crash does not simulate torn
# writes, for which disabling full_page_writes is unsafe.
$node->safe_psql('postgres', "INSERT INTO multi_t VALUES (4, '{5,23}')");
$node->safe_psql('postgres', "INSERT INTO multi_t VALUES (5, '{6,24,50}')");
$end = insert_lsn();
$wal = waldump($start, $end);
@records = ($wal =~ /^.*desc:.*$/mg);
is(scalar @records, 2, 'the two rows logged exactly two BARK records');
like($wal,
	qr/desc: INSERT_MULTI nentries: 2, offsets: \[4, 9\], blkref #0: rel \d+\/\d+\/\d+ blk 1$/m,
	'the third row is one INSERT_MULTI without an image');
my ($last) = ($wal =~
	/lsn: ([0-9A-F]+\/[0-9A-F]+), [^\n]*desc: INSERT_MULTI nentries: 3, offsets: \[5, 11, 13\], blkref #0: rel \d+\/\d+\/\d+ blk 1$/m);
ok(defined $last, 'the fourth row is one INSERT_MULTI without an image');
unlike($wal, qr/FPW/, 'neither applied nor consistency-only images were logged');

# Flush and evict without a checkpoint, as in 005 and 007, then read the
# leaf back from disk; a second eviction that flushes nothing shows the read
# was the on-disk page.
my $evict = q[
SELECT buffers_flushed > 0, buffers_skipped
FROM pg_buffercache_evict_relation('multi_idx');
];
is($node->safe_psql('postgres', $evict), 't|0',
	'dirty index pages were flushed and evicted, none skipped');
is($node->safe_psql('postgres', qq[
SELECT lsn >= '$last'::pg_lsn FROM page_header(get_raw_page('multi_idx', 1));
]), 't', 'the on-disk leaf is as new as the last record');
is($node->safe_psql('postgres', $evict), 'f|0',
	'nothing was dirty: the page read was the on-disk page');
is($node->safe_psql('postgres', q[
SELECT count(*) FROM pg_buffercache
WHERE relfilenode = pg_relation_filenode('multi_idx')
  AND reldatabase = (SELECT oid FROM pg_database WHERE datname = current_database());
]), '0', 'no index page remains in shared buffers');
is(redo_lsn(), $redo, 'no checkpoint moved the redo pointer past the workload');

$node->stop('immediate');
$node->start;
ok($node->log_contains("redo starts at $redo"),
	'recovery started before both records');
check_index('multi_t', 'multi_idx', 5, 'already applied');

## 3: an unlogged index
$node->safe_psql(
	'postgres', q[
CREATE UNLOGGED TABLE multi_u (id int, a int4[]);
INSERT INTO multi_u VALUES (1, '{1,2}');
CREATE INDEX multi_u_idx ON multi_u USING bark (a bark_int4_array_ops);
INSERT INTO multi_u VALUES (2, '{20,21,22}'), (3, '{0,10,25}'),
  (4, '{5,23}'), (5, '{6,24,50}');
]);
check_index('multi_u', 'multi_u_idx', 5, 'unlogged');

# Let the recovery process save its coverage counters.
$node->stop;
done_testing();
