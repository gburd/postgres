
# Copyright (c) 2026, PostgreSQL Global Development Group

# A BARK index whose meta page says version 2, as pg_upgrade carries over
# from a server whose BARK predates version tags, keeps working: scans,
# inserts that split and coalesce, VACUUM, and amcheck, and it is left at
# version 2 until a rebuild writes version 3.  Version 3 lays out an index
# of a table whose locator is not versioned exactly as version 2 does (the
# meta page's BARK_META_VERSIONED is clear and no entry has a tag), so a
# version 2 index is made here by setting the version word of such an
# index's meta page back to 2 with the server stopped.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(no_data_checksums => 1);
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;

$node->safe_psql(
	'postgres', q{
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pageinspect;
	CREATE TABLE t (a int, b text);
	INSERT INTO t SELECT g % 500, 'v' || g FROM generate_series(1, 20000) g;
	CREATE INDEX t_a ON t USING bark (a);
	CREATE INDEX t_b ON t USING bark (b);
});
is( $node->safe_psql(
		'postgres',
		q{SELECT string_agg(version || ':' || flags, ',') FROM
		  (SELECT (bark_metap(i)).* FROM unnest(ARRAY['t_a', 't_b']) i)}),
	'3:0,3:0',
	'a new index on a heap table is version 3, not built for tags');

my $blksz = int($node->safe_psql('postgres', 'SHOW block_size'));
my %path;
$path{$_} = $node->data_dir . '/'
  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('$_')")
  for qw(t_a t_b);
$node->stop;

# BarkMetaPageData follows the 24-byte page header: magic, then version.
for my $i (qw(t_a t_b))
{
	open(my $fh, '+<:raw', $path{$i}) or die "open $path{$i}: $!";
	seek($fh, 24 + 4, 0) or die "seek: $!";
	print $fh pack('L', 2);
	close $fh;
}
$node->start;

is( $node->safe_psql(
		'postgres',
		q{SELECT string_agg(version::text, ',') FROM
		  (SELECT (bark_metap(i)).* FROM unnest(ARRAY['t_a', 't_b']) i)}),
	'2,2',
	'the indexes are version 2');

# The index's answers agree with a seqscan, and the indexes check.
sub check
{
	my ($what) = @_;
	my $q =
	  q{SELECT count(*), sum(a), count(DISTINCT b) FROM t WHERE a BETWEEN 10 AND 300 AND b > 'v1'};
	my $seq = $node->safe_psql('postgres',
		"SET enable_indexscan = off; SET enable_indexonlyscan = off; SET enable_bitmapscan = off; $q"
	);
	for my $m (qw(indexscan bitmapscan))
	{
		my $off = $m eq 'indexscan' ? 'enable_bitmapscan' : 'enable_indexscan';
		is( $node->safe_psql('postgres',
				"SET enable_seqscan = off; SET $off = off; $q"),
			$seq, "$what: $m matches seqscan");
	}
	is( $node->safe_psql(
			'postgres',
			q{SELECT bark_index_parent_check('t_a', true), bark_index_parent_check('t_b', true)}
		),
		'|',
		"$what: bark_index_parent_check");
}

check('read');

# Inserts into existing equal keys (coalescing), and enough to split leaves.
$node->safe_psql('postgres',
	q{INSERT INTO t SELECT g % 700, 'w' || g FROM generate_series(1, 30000) g});
check('write');

$node->safe_psql('postgres',
	q{DELETE FROM t WHERE a % 3 = 0; VACUUM t;
	  INSERT INTO t SELECT g % 300, 'x' || g FROM generate_series(1, 5000) g});
check('vacuum');

is( $node->safe_psql(
		'postgres',
		q{SELECT string_agg(version::text, ',') FROM
		  (SELECT (bark_metap(i)).* FROM unnest(ARRAY['t_a', 't_b']) i)}),
	'2,2',
	'reading and writing leave the indexes at version 2');

$node->safe_psql('postgres', 'REINDEX INDEX t_a');
is($node->safe_psql('postgres', q{SELECT version FROM bark_metap('t_a')}),
	'3', 'REINDEX writes version 3');
check('reindexed');

done_testing();
