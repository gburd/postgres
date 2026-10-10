# Copyright (c) 2026, PostgreSQL Global Development Group

# Flush a root leaf after its compact split and the new root without
# advancing the recovery checkpoint.  The left page on disk is then newer
# than the SPLIT record: redo must skip it, yet still rebuild the right page.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
# As in 005: no full-page images, since an applied image would overwrite the
# newer on-disk page.  This controlled process crash does not simulate torn
# writes, for which disabling full_page_writes is unsafe.
$node->append_conf(
	'postgresql.conf', qq[
autovacuum = off
wal_keep_size = 1GB
full_page_writes = off
wal_consistency_checking = ''
checkpoint_timeout = 1h
max_wal_size = 1GB
]);
$node->start;
# The 006 fixture: a unique root leaf filled with equal-sized distinct keys,
# no prefix compression, so the next insert makes a compact split.
$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE EXTENSION pageinspect;
CREATE EXTENSION pg_buffercache;
CREATE TABLE split_done_t (id int, s text COLLATE "C");
ALTER TABLE split_done_t ALTER COLUMN s SET STORAGE PLAIN;
CREATE UNIQUE INDEX split_done_idx ON split_done_t USING bark (s)
  WITH (prefix_compression = off);
INSERT INTO split_done_t VALUES (1, lpad('1', 1000, '0'));
DO $$
DECLARE
  n int := 1;
BEGIN
  WHILE (SELECT free_size >= avg_item_size + 16
         FROM bark_page_stats('split_done_idx', 1)) LOOP
    n := n + 1;
    INSERT INTO split_done_t VALUES (n, lpad(n::text, 1000, '0'));
  END LOOP;
END
$$;
]);
is($node->safe_psql('postgres', q[
SELECT root = 1 AND level = 0 FROM bark_metap('split_done_idx');
]), 't', 'the index still has a single root leaf');
is($node->safe_psql('postgres', q[
SELECT free_size < avg_item_size FROM bark_page_stats('split_done_idx', 1);
]), 't', 'the existing leaf cannot fit another equal-sized entry');
my $rows = 1 + $node->safe_psql('postgres', 'SELECT count(*) FROM split_done_t');
$node->safe_psql('postgres', 'CHECKPOINT');
my $redo = $node->safe_psql('postgres',
	'SELECT redo_lsn FROM pg_control_checkpoint()');
my $start = $node->safe_psql('postgres',
	'SELECT pg_current_wal_insert_lsn()');
$node->safe_psql('postgres',
	"INSERT INTO split_done_t VALUES ($rows, lpad('$rows', 1000, '0'))");
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
append_to_file("$PostgreSQL::Test::Utils::log_path/split_done_wal.log",
	"WAL interval: $start .. $end\n$wal\n");
my @records = ($wal =~ /^.*desc:.*$/mg);
is(scalar @records, 2, 'the insert logged exactly two BARK records');
like($wal,
	qr/desc: SPLIT level: 0, leaf: T, left: 1, right: 2, [^\n]*firstrightoff: \d+, [^\n]*blkref #0: rel \d+\/\d+\/\d+ blk 1, blkref #1: rel \d+\/\d+\/\d+ blk 2$/m,
	'compact root-leaf split, no image on either page');
my ($newroot) = ($wal =~ /lsn: ([0-9A-F]+\/[0-9A-F]+), [^\n]*desc: NEWROOT root: 3, level: 1, /m);
ok(defined $newroot, 'the split is finished by a new root after it');
like($wal, qr/desc: NEWROOT [^\n]*blkref #1: rel \d+\/\d+\/\d+ blk 1,/m,
	'the new root record also changes the left page');
unlike($wal, qr/FPW/, 'neither applied nor consistency-only images were logged');

# Before the crash, record the new right page as the primary built it.
my $right_sql = q[
SELECT bark_prev, bark_next, bark_level, flags FROM bark_page_stats('split_done_idx', 2);
SELECT itemoffset, ctid, itemlen, data FROM bark_page_items('split_done_idx', 2);
];
my $right = $node->safe_psql('postgres', $right_sql);

# Flush and evict without a checkpoint, as in 005.  Then read the left page
# back from disk: its LSN is the new root record's, past the split.  A
# second eviction flushing nothing shows that read was the on-disk state.
my $evict = q[
SELECT buffers_flushed > 0, buffers_skipped
FROM pg_buffercache_evict_relation('split_done_idx');
];
is($node->safe_psql('postgres', $evict), 't|0',
	'dirty index pages were flushed and evicted, none skipped');
is($node->safe_psql('postgres', qq[
SELECT lsn > '$newroot'::pg_lsn FROM page_header(get_raw_page('split_done_idx', 1));
]), 't', 'the on-disk left page is newer than the split record');
is($node->safe_psql('postgres', q[
SELECT flags FROM bark_page_stats('split_done_idx', 1);
]), '{leaf}', 'the on-disk left page has its split finished');
is($node->safe_psql('postgres', $evict), 'f|0',
	'nothing was dirty: the pages read were the on-disk pages');
is($node->safe_psql('postgres', q[
SELECT count(*) FROM pg_buffercache
WHERE relfilenode = pg_relation_filenode('split_done_idx')
  AND reldatabase = (SELECT oid FROM pg_database WHERE datname = current_database());
]), '0', 'no index page remains in shared buffers');
is($node->safe_psql('postgres',
		'SELECT redo_lsn FROM pg_control_checkpoint()'),
	$redo, 'checkpoint redo pointer stayed before the split');

$node->stop('immediate');
$node->start;
ok($node->log_contains('database system was interrupted'),
	'server went through crash recovery');
is($node->safe_psql('postgres', $right_sql), $right,
	'the right page was rebuilt as the primary built it');
is($node->safe_psql('postgres', q[
SELECT root = 3 AND level = 1 FROM bark_metap('split_done_idx');
]), 't', 'the meta page points at the new root');
my $query = 'SELECT id, s FROM split_done_t ORDER BY s';
my $seq_settings = 'SET enable_indexscan = off; SET enable_indexonlyscan = off; SET enable_bitmapscan = off;';
my $index_settings = 'SET enable_seqscan = off; SET enable_bitmapscan = off; SET enable_indexonlyscan = off;';
like($node->safe_psql('postgres', "$seq_settings EXPLAIN (COSTS OFF) $query"),
	qr/Seq Scan on split_done_t/, 'reference uses a sequential scan');
my $expected = $node->safe_psql('postgres', "$seq_settings $query");
is($node->safe_psql('postgres', "$seq_settings SELECT count(*) FROM split_done_t"),
	$rows, 'all committed rows survived recovery');
like($node->safe_psql('postgres', "$index_settings EXPLAIN (COSTS OFF) $query"),
	qr/Index Scan using split_done_idx/, 'recovered query uses the BARK index');
is($node->safe_psql('postgres', "$index_settings $query"),
	$expected, 'both halves return each row and value exactly once');
is($node->safe_psql('postgres',
		"SELECT bark_index_check('split_done_idx', true)"),
	'', 'recovered index passes amcheck with heapallindexed');
# Let the recovery process save its coverage counters.
$node->stop;
done_testing();
