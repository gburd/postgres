# Copyright (c) 2026, PostgreSQL Global Development Group

# A compact split that is the first change to a full leaf after a checkpoint
# restores the left page's image, but must still initialize the new right page.

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
$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE EXTENSION pageinspect;
CREATE TABLE split_fpi_t (id int, s text COLLATE "C");
ALTER TABLE split_fpi_t ALTER COLUMN s SET STORAGE PLAIN;
CREATE UNIQUE INDEX split_fpi_idx ON split_fpi_t USING bark (s)
  WITH (prefix_compression = off);
INSERT INTO split_fpi_t VALUES (1, lpad('1', 1000, '0'));
DO $$
DECLARE
  n int := 1;
BEGIN
  -- Equal-sized distinct entries: no coalescing or prefix changes.  Leave
  -- a margin for the leaf's TID reserve while filling without a split.
  WHILE (SELECT free_size >= avg_item_size + 16
         FROM bark_page_stats('split_fpi_idx', 1)) LOOP
    n := n + 1;
    INSERT INTO split_fpi_t VALUES (n, lpad(n::text, 1000, '0'));
  END LOOP;
END
$$;
]);
is($node->safe_psql('postgres', q[
SELECT root = 1 AND level = 0 FROM bark_metap('split_fpi_idx');
]), 't', 'the index still has a single root leaf');
is($node->safe_psql('postgres', q[
SELECT free_size < avg_item_size FROM bark_page_stats('split_fpi_idx', 1);
]), 't', 'the existing leaf cannot fit another equal-sized entry');
my $rows = 1 + $node->safe_psql('postgres', 'SELECT count(*) FROM split_fpi_t');
$node->safe_psql('postgres', 'CHECKPOINT');
my $redo = $node->safe_psql('postgres',
	'SELECT redo_lsn FROM pg_control_checkpoint()');
my $start = $node->safe_psql('postgres',
	'SELECT pg_current_wal_insert_lsn()');
$node->safe_psql('postgres',
	"INSERT INTO split_fpi_t VALUES ($rows, lpad('$rows', 1000, '0'))");
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
append_to_file("$PostgreSQL::Test::Utils::log_path/split_fpi_wal.log",
	"WAL interval: $start .. $end\n$wal\n");
my @splits = ($wal =~ /^.*desc: SPLIT .*$/mg);
is(scalar @splits, 1, 'the first post-checkpoint insert logged one split');
# Plain FPW means BKPIMAGE_APPLY; the verification-only form has a suffix.
# firstrightoff identifies a compact split, not a whole-left reconstruction.
like($wal,
	qr/desc: SPLIT level: 0, [^\n]*firstrightoff: \d+, [^\n]*blkref #0: rel \d+\/\d+\/\d+ blk 1 FPW, blkref #1: rel \d+\/\d+\/\d+ blk 2$/m,
	'compact split has an applied left image and no right image');
unlike($wal, qr/for WAL verification/, 'no consistency-only images');
is($node->safe_psql('postgres',
		'SELECT redo_lsn FROM pg_control_checkpoint()'),
	$redo, 'checkpoint redo pointer stayed before the split');

$node->stop('immediate');
$node->start;
ok($node->log_contains('database system was interrupted'),
	'server went through crash recovery');
my $query = 'SELECT id, s FROM split_fpi_t ORDER BY s';
my $seq_settings = 'SET enable_indexscan = off; SET enable_indexonlyscan = off; SET enable_bitmapscan = off;';
my $index_settings = 'SET enable_seqscan = off; SET enable_bitmapscan = off; SET enable_indexonlyscan = off;';
like($node->safe_psql('postgres', "$seq_settings EXPLAIN (COSTS OFF) $query"),
	qr/Seq Scan on split_fpi_t/, 'reference uses a sequential scan');
my $expected = $node->safe_psql('postgres', "$seq_settings $query");
is($node->safe_psql('postgres', "$seq_settings SELECT count(*) FROM split_fpi_t"),
	$rows, 'all committed rows survived recovery');
like($node->safe_psql('postgres', "$index_settings EXPLAIN (COSTS OFF) $query"),
	qr/Index Scan using split_fpi_idx/, 'recovered query uses the BARK index');
is($node->safe_psql('postgres', "$index_settings $query"),
	$expected, 'both recovered halves match sequential rows and values');
is($node->safe_psql('postgres',
		"SELECT bark_index_check('split_fpi_idx', true)"),
	'', 'recovered index passes amcheck with heapallindexed');
$node->stop;
done_testing();
