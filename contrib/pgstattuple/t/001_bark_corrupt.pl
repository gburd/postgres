
# Copyright (c) 2026, PostgreSQL Global Development Group

# pgstatbarkindex on BARK pages that SQL cannot make: a meta page without
# the magic number, and a data page without BARK's special space.  The
# pages are patched on disk with the server stopped; each must be reported
# as corruption, the data page with its block number.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(no_data_checksums => 1);
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;
my $blksz = int($node->safe_psql('postgres', 'SHOW block_size'));
# Blocks per relation file (--with-segsize-blocks can make it small).
my $segblks = int($node->safe_psql('postgres',
	"SELECT setting FROM pg_settings WHERE name = 'segment_size'"));

$node->safe_psql(
	'postgres', q(
	CREATE EXTENSION pgstattuple;
	CREATE TABLE t AS SELECT g::int8 AS a FROM generate_series(1, 1000) g;
	CREATE INDEX t_meta ON t USING bark (a);
	CREATE INDEX t_page ON t USING bark (a);
));
my %path = map {
	$_ => $node->data_dir . '/'
	  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('$_')")
} qw(t_meta t_page);
$node->stop;

# Write $bytes at byte $off of block $blk of the index at $path.
sub patch
{
	my ($path, $blk, $off, $bytes) = @_;
	my $segno = int($blk / $segblks);
	my $file = $segno ? "$path.$segno" : $path;
	open(my $fh, '+<:raw', $file) or die "open $file: $!";
	seek($fh, ($blk % $segblks) * $blksz + $off, 0) or die "seek: $!";
	print $fh $bytes;
	close $fh;
}

# The magic number is BarkMetaPageData's first field, 24 bytes in.
patch($path{t_meta}, 0, 24, pack('L', 0));
# pd_special (offset 16) at the end of block 1, a leaf: no special space.
patch($path{t_page}, 1, 16, pack('S', $blksz));

$node->start;
my ($rc, $out, $err) =
  $node->psql('postgres', "SELECT * FROM pgstatbarkindex('t_meta')");
like(
	$err,
	qr/index "t_meta" has a corrupted meta page/,
	'a meta page without the magic number is reported');
($rc, $out, $err) =
  $node->psql('postgres', "SELECT * FROM pgstatbarkindex('t_page')");
like(
	$err,
	qr/index "t_page" contains corrupted page at block 1/,
	'a page without special space is reported at its block');

$node->stop;
done_testing();
