# Copyright (c) 2026, PostgreSQL Global Development Group

# Recover logged BARK leaf/internal splits and new roots after a checkpoint.
# Missing entries or broken links must be visible to an index scan or amcheck.

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
wal_consistency_checking = 'Bark'
checkpoint_timeout = 1h
max_wal_size = 1GB
]);
$node->start;
$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE TABLE split_t (id int, s text COLLATE "C");
CREATE INDEX split_idx ON split_t USING bark (s)
  WITH (prefix_compression = on);
CREATE TABLE wide_t (id int, s text COLLATE "C");
CREATE INDEX wide_idx ON wide_t USING bark (s);
CHECKPOINT;
]);

my $redo = $node->safe_psql('postgres',
	'SELECT redo_lsn FROM pg_control_checkpoint()');
my $start = $node->safe_psql('postgres',
	'SELECT pg_current_wal_insert_lsn()');

# As in 002_standby.pl: shared-prefix keys split prefix-coded leaves, then
# unrelated keys make some splits log the left half whole.  Keep the seed
# fixed, but assert the record forms rather than assume the workload hit them.
$node->safe_psql(
	'postgres', q[
SELECT setseed(0.7);
INSERT INTO split_t
  SELECT g, 'https://www.example.com/item/' || lpad(g::text, 8, '0')
  FROM generate_series(1, 5000) g ORDER BY random();
INSERT INTO split_t
  SELECT 5000 + g, md5(g::text) || 'https://x/' || g
  FROM generate_series(1, 2000) g ORDER BY random();
]);

# The wide-key workload from 002_standby.pl has room for only about 25 keys
# per page at each level.  It splits internal pages as well as leaves and
# needs a new root above them; ordinary short keys might only split leaves.
$node->safe_psql(
	'postgres', q[
SELECT setseed(0.5);
INSERT INTO wide_t
  SELECT g, lpad(g::text, 300, '0')
  FROM generate_series(1, 20000) g ORDER BY random();
]);
my $end = $node->safe_psql('postgres',
	'SELECT pg_current_wal_insert_lsn()');

# Retention above keeps this exact interval available after recovery too.
# A consistency-check image is not necessarily an image applied by redo;
# neither its presence nor this crash proves BLK_RESTORED or BLK_DONE.
my ($wal, $stderr) = run_command(
	[
		'pg_waldump', '--rmgr=Bark',
		'--path=' . $node->data_dir . '/pg_wal',
		"--start=$start", "--end=$end"
	]);
is($stderr, '', 'pg_waldump reads the workload WAL without errors');
append_to_file("$PostgreSQL::Test::Utils::log_path/split_wal.log",
	"WAL interval: $start .. $end\n$wal\n");
like($wal, qr/desc: SPLIT level: 0, [^\n]*prefix: L., firstrightoff:/,
	'workload logged compact splits retaining the left prefix');
like($wal, qr/desc: SPLIT level: 0, [^\n]*left logged whole/,
	'workload logged whole-left splits');
like($wal, qr/desc: SPLIT level: [1-9]\d*, leaf: F,/,
	'workload logged an internal-page split');
like($wal, qr/desc: NEWROOT root: \d+, level: 2,/,
	'workload logged a new root above the split internal root');

# Consistency images make this workload WAL-heavy.  Both the time and size
# limits above must keep automatic checkpoints from moving recovery past the
# root changes we intend to replay.
is($node->safe_psql('postgres',
		'SELECT redo_lsn FROM pg_control_checkpoint()'),
	$redo, 'checkpoint redo pointer stayed before the workload');

$node->stop('immediate');
$node->start;
ok($node->log_contains('database system was interrupted'),
	'server went through crash recovery');

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
foreach my $name ('split', 'wide')
{
	my $query = "SELECT id, s FROM ${name}_t ORDER BY s, id";
	my $rows = $name eq 'split' ? 7000 : 20000;

	like($node->safe_psql('postgres',
			"$seq_settings EXPLAIN (COSTS OFF) $query"),
		qr/Seq Scan on ${name}_t/, "$name: reference uses a sequential scan");
	my $expected = $node->safe_psql('postgres', "$seq_settings $query");
	is($node->safe_psql('postgres', "$seq_settings SELECT count(*) FROM ${name}_t"),
		$rows, "$name: all committed rows survived recovery");
	like($node->safe_psql('postgres',
			"$index_settings EXPLAIN (COSTS OFF) $query"),
		qr/Index Scan using ${name}_idx/, "$name: recovered query uses the BARK index");
	is($node->safe_psql('postgres', "$index_settings $query"),
		$expected, "$name: recovered BARK rows and values match the sequential scan");
	is($node->safe_psql('postgres',
			"SELECT bark_index_check('${name}_idx', true)"),
		'', "$name: recovered split index passes amcheck with heapallindexed");
}

# Shut down normally so an instrumented recovery process can save coverage.
$node->stop;
done_testing();
