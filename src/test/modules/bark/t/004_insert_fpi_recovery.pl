# Copyright (c) 2026, PostgreSQL Global Development Group

# The first change to an existing leaf after a checkpoint logs an applied
# full-page image.  INSERT_LEAF redo must not also insert the entry, which
# would duplicate it.  This is not a wal_consistency_checking-only image.

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
]);
$node->start;
$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE TABLE fpi_t (i int);
INSERT INTO fpi_t VALUES (1);
CREATE INDEX fpi_idx ON fpi_t USING bark (i);
CHECKPOINT;
]);

# The checkpoint flushes the existing leaf and advances the redo pointer.
# bark_insert_entry registers it with REGBUF_STANDARD; its old page LSN
# makes this first change require an image with BKPIMAGE_APPLY set.  Commit
# flushes the WAL.  Do not checkpoint again before the crash: recovery must
# start before this INSERT_LEAF, whether or not its data page was flushed.
my $start = $node->safe_psql('postgres',
	'SELECT pg_current_wal_insert_lsn()');
$node->safe_psql('postgres', 'INSERT INTO fpi_t VALUES (2)');
my $end = $node->safe_psql('postgres',
	'SELECT pg_current_wal_insert_lsn()');
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
append_to_file("$PostgreSQL::Test::Utils::log_path/insert_fpi_wal.log",
	"WAL interval: $start .. $end\n$wal\n");
my @records = ($wal =~ /^.*desc:.*$/mg);
is(scalar @records, 1, 'exactly one BARK record in the workload interval');
like($wal, qr/desc: INSERT_LEAF off: 2, blkref #0: rel \d+\/\d+\/\d+ blk 1 FPW$/m,
	'INSERT_LEAF has an ordinary applied full-page image');
unlike($wal, qr/for WAL verification/,
	'the image is not just for WAL consistency checking');

$node->stop('immediate');
$node->start;
ok($node->log_contains('database system was interrupted'),
	'server went through crash recovery');

my $query = 'SELECT i FROM fpi_t ORDER BY i';
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
like($node->safe_psql('postgres',
		"$seq_settings EXPLAIN (COSTS OFF) $query"),
	qr/Seq Scan on fpi_t/, 'reference uses a sequential scan');
my $expected = $node->safe_psql('postgres', "$seq_settings $query");
is($expected, "1\n2", 'both committed rows survived recovery');
like($node->safe_psql('postgres',
		"$index_settings EXPLAIN (COSTS OFF) $query"),
	qr/Index Scan using fpi_idx/, 'recovered query uses the BARK index');
is($node->safe_psql('postgres', "$index_settings $query"),
	$expected, 'image replay returns each indexed row exactly once');
is($node->safe_psql('postgres',
		"SELECT bark_index_check('fpi_idx', true)"),
	'', 'recovered index passes amcheck with heapallindexed');

# Let the recovery process save its coverage counters.
$node->stop;
done_testing();
