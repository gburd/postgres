# Copyright (c) 2026, PostgreSQL Global Development Group

# VACUUM puts a BARK index's never-initialized pages in the free space map,
# as btvacuumpage does.  A page the index was extended by stays all-zero when
# the insert that extended it fails or crashes before writing it (a split's
# right half, an overflow chain's later blocks): nothing references it, and
# without VACUUM recording it, only REINDEX would get it back.  The test
# makes such pages by extending the index file with zeros while the server is
# stopped, as a crash after the extension would leave them.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;

$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE EXTENSION pageinspect;
CREATE EXTENSION pg_freespacemap;
CREATE TABLE t (id int, k int);
CREATE INDEX t_k ON t USING bark (k);
INSERT INTO t SELECT g, g FROM generate_series(1, 2000) g;
]);

my $blcksz = $node->safe_psql('postgres', 'SHOW block_size');
my $before = $node->safe_psql('postgres',
	"SELECT pg_relation_size('t_k') / $blcksz");
my $path = $node->data_dir . '/'
  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('t_k')");

# A build with small segments (--with-segsize-blocks) spreads the index over
# several files, and md.c wants every segment but the last one full.
my $segblocks = $node->safe_psql('postgres',
	"SELECT setting FROM pg_settings WHERE name = 'segment_size'");

# Three never-written pages at the end of the index.
$node->stop;
for my $b ($before .. $before + 2)
{
	my $seg = int($b / $segblocks);
	my $file = $seg == 0 ? $path : "$path.$seg";
	open(my $fh, '>>', $file) or die "open $file: $!";
	binmode $fh;
	print $fh "\0" x $blcksz;
	close($fh);
}
$node->start;

is( $node->safe_psql(
		'postgres', "SELECT pg_relation_size('t_k') / $blcksz"),
	$before + 3, 'the index has three more blocks');
is( $node->safe_psql(
		'postgres', "SELECT string_agg(type, ',' ORDER BY b)
		 FROM generate_series($before, $before + 2) b,
		   LATERAL bark_page_stats('t_k', b)"),
	'new,new,new', 'they are new pages');
is( $node->safe_psql(
		'postgres', "SELECT count(*) FROM generate_series($before, $before + 2) b
		 WHERE pg_freespace('t_k', b) > 0"),
	'0', 'the free space map does not have them');

$node->safe_psql('postgres', 'VACUUM t');
is( $node->safe_psql(
		'postgres', "SELECT count(*) FROM generate_series($before, $before + 2) b
		 WHERE pg_freespace('t_k', b) > 0"),
	'3', 'VACUUM records them as free');

# Splits take them before extending the index again.
$node->safe_psql('postgres',
	'INSERT INTO t SELECT g, g FROM generate_series(2001, 4000) g');
is( $node->safe_psql(
		'postgres', "SELECT count(*) FROM generate_series($before, $before + 2) b,
		 LATERAL bark_page_stats('t_k', b) WHERE type = 'new'"),
	'0', 'inserts reuse them');
is($node->safe_psql('postgres', "SELECT bark_index_parent_check('t_k')"),
	'', 'bark_index_parent_check passes');
is( $node->safe_psql(
		'postgres', q[SET enable_seqscan = off; SET enable_bitmapscan = off;
		 SELECT count(*) FROM t WHERE k > 0]),
	'4000', 'an index scan finds every row');

$node->stop;
done_testing();
