# Copyright (c) 2026, PostgreSQL Global Development Group

# Flush a leaf newer than its first INSERT_LEAF record without advancing
# the recovery checkpoint.  Redo must leave the already-applied entries alone.

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
full_page_writes = off
wal_consistency_checking = ''
checkpoint_timeout = 1h
]);
$node->start;
$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE EXTENSION pg_buffercache;
CREATE TABLE done_t (i int);
INSERT INTO done_t VALUES (1);
CREATE INDEX done_idx ON done_t USING bark (i);
CHECKPOINT;
]);

# No full-page images: replaying an earlier image would overwrite the newer
# on-disk leaf and make the later inserts need redo.  This controlled process
# crash does not simulate torn writes or a power failure, for which disabling
# full_page_writes is unsafe.  Two distinct keys fit on the existing leaf;
# after the second insert its LSN is strictly newer than the first record.
my $start = $node->safe_psql('postgres',
	'SELECT pg_current_wal_insert_lsn()');
$node->safe_psql('postgres', 'INSERT INTO done_t VALUES (2), (3)');
my $end = $node->safe_psql('postgres',
	'SELECT pg_current_wal_insert_lsn()');
my ($wal, $stderr) = run_command(
	[
		'pg_waldump', '--rmgr=Bark',
		'--path=' . $node->data_dir . '/pg_wal',
		"--start=$start", "--end=$end"
	]);
is($stderr, '', 'pg_waldump reads the workload WAL without errors');
append_to_file("$PostgreSQL::Test::Utils::log_path/insert_done_wal.log",
	"WAL interval: $start .. $end\n$wal\n");
my @records = ($wal =~ /^.*desc:.*$/mg);
is(scalar @records, 2, 'exactly two BARK records in the workload interval');
foreach my $off (2, 3)
{
	like($wal,
		qr/desc: INSERT_LEAF off: $off, blkref #0: rel \d+\/\d+\/\d+ blk 1$/m,
		"INSERT_LEAF at offset $off has no image");
}
unlike($wal, qr/FPW/, 'neither applied nor consistency-only images were logged');

# This documented developer function flushes dirty buffers before eviction.
# There are no other users of this index.  Unlike CHECKPOINT, eviction leaves
# the recovery start point before both inserts.  Verify that no pinned/dirty
# buffer was skipped and no index buffer remains before crashing.
is($node->safe_psql('postgres', q[
SELECT buffers_skipped FROM pg_buffercache_evict_relation('done_idx');
]), '0', 'no index buffer was skipped during flush and eviction');
is($node->safe_psql('postgres', q[
SELECT count(*) FROM pg_buffercache
WHERE relfilenode = pg_relation_filenode('done_idx')
  AND reldatabase = (SELECT oid FROM pg_database WHERE datname = current_database());
]), '0', 'the updated index is on disk, not in shared buffers');

$node->stop('immediate');
$node->start;
ok($node->log_contains('database system was interrupted'),
	'server went through crash recovery');

my $query = 'SELECT i FROM done_t ORDER BY i';
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
	qr/Seq Scan on done_t/, 'reference uses a sequential scan');
my $expected = $node->safe_psql('postgres', "$seq_settings $query");
is($expected, "1\n2\n3", 'all committed rows survived recovery');
like($node->safe_psql('postgres',
		"$index_settings EXPLAIN (COSTS OFF) $query"),
	qr/Index Scan using done_idx/, 'recovered query uses the BARK index');
is($node->safe_psql('postgres', "$index_settings $query"),
	$expected, 'already-applied inserts return each indexed row exactly once');
is($node->safe_psql('postgres',
		"SELECT bark_index_check('done_idx', true)"),
	'', 'recovered index passes amcheck with heapallindexed');

# Let the recovery process save its coverage counters.
$node->stop;
done_testing();
