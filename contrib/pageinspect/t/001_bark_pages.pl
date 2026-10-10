
# Copyright (c) 2026, PostgreSQL Global Development Group

# BARK pages that SQL cannot make: a meta page written before its newer
# fields, a half-dead page, a flag bit without a name, a dead line pointer,
# a block that was never initialized, and another session's temporary
# index.  The pages are patched on disk with the server stopped; pageinspect
# must report each one.

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

$node->safe_psql(
	'postgres', q(
	CREATE EXTENSION pageinspect;
	CREATE TABLE t AS SELECT g::int8 AS a FROM generate_series(1, 10000) g;
	CREATE INDEX t_idx ON t USING bark (a);
));

# Another session's temporary index.
my $bg = $node->background_psql('postgres');
$bg->query_safe(
	'CREATE TEMP TABLE tt (a int8); CREATE INDEX tt_idx ON tt USING bark (a)');
my $tmpidx = $node->safe_psql('postgres',
	"SELECT relnamespace::regnamespace || '.tt_idx' FROM pg_class WHERE relname = 'tt_idx'"
);
my ($ret, $out, $err) =
  $node->psql('postgres', "SELECT * FROM bark_metap('$tmpidx')");
like(
	$err,
	qr/cannot access temporary tables of other sessions/,
	'another session\'s temporary index is refused');
$bg->quit;

my $path = $node->data_dir . '/'
  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('t_idx')");
my @leaves = split /\n/,
  $node->safe_psql('postgres',
	"SELECT blkno FROM bark_multi_page_stats('t_idx', 1, -1) WHERE type = 'leaf' ORDER BY blkno LIMIT 2"
  );
my $nblocks = $node->safe_psql('postgres',
	"SELECT pg_relation_size('t_idx') / $blksz");
is(scalar @leaves, 2, 't_idx has two leaves to patch');

$node->stop;

sub patch
{
	my ($blk, $f) = @_;
	open(my $fh, '+<:raw', $path) or die "open $path: $!";
	seek($fh, $blk * $blksz, 0) or die "seek: $!";
	read($fh, my $p, $blksz) == $blksz or die "short read of $path";
	$f->(\$p);
	seek($fh, $blk * $blksz, 0) or die "seek: $!";
	print $fh $p;
	close $fh;
}
sub g16 { unpack('S', substr(${ $_[0] }, $_[1], 2)) }
sub s16 { substr(${ $_[0] }, $_[1], 2) = pack('S', $_[2]) }

# A meta page from before bark_allequalimage, bark_flags and bark_nkeys:
# pd_lower (offset 12) ends after bark_level, 24 + 16 bytes in.
patch(0, sub { s16($_[0], 12, 24 + 16) });

# A half-dead leaf (BARK_HALF_DEAD 0x10), with bit 0x100, which has no
# name; bark_flags is 12 bytes into the special space (pd_special at 16).
patch(
	$leaves[0],
	sub {
		my $f = g16($_[0], 16) + 12;
		s16($_[0], $f, g16($_[0], $f) | 0x10 | 0x100);
	});

# The first line pointer LP_DEAD: lp_flags is bits 15-16 of the 32-bit
# ItemIdData in either byte order.
patch(
	$leaves[1],
	sub {
		substr(${ $_[0] }, 24, 4) =
		  pack('L', unpack('L', substr(${ $_[0] }, 24, 4)) | (3 << 15));
	});

# A block the relation was extended by but nothing initialized.
open(my $fh, '>>:raw', $path) or die "open $path: $!";
print $fh "\0" x $blksz;
close $fh;

$node->start;

is( $node->safe_psql(
		'postgres',
		"SELECT allequalimage IS NULL, flags, nkeys FROM bark_metap('t_idx')"),
	't|0|0',
	'a meta page without the newer fields reads them as absent');
is( $node->safe_psql(
		'postgres',
		"SELECT type, flags FROM bark_page_stats('t_idx', $leaves[0])"),
	'half-dead|{leaf,half_dead,100}',
	'a half-dead page, and a flag bit without a name in hex');
is( $node->safe_psql(
		'postgres',
		"SELECT dead_items, live_items > 0 FROM bark_page_stats('t_idx', $leaves[1])"
	),
	'1|t',
	'a dead line pointer is counted');
is( $node->safe_psql(
		'postgres',
		"SELECT type, num_nulls(live_items, dead_items, avg_item_size, page_size, free_size, bark_prev, bark_next, bark_level, bark_cycleid, flags) FROM bark_page_stats('t_idx', $nblocks)"
	),
	'new|10',
	'a never-initialized block is type new, its other columns null');
is( $node->safe_psql(
		'postgres',
		"SELECT string_agg(type, ',') FROM bark_multi_page_stats('t_idx', $nblocks, -1)"
	),
	'new',
	'bark_multi_page_stats reports the new block');
is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM bark_page_items('t_idx', $nblocks)"),
	'0',
	'a never-initialized block has no items');

$node->stop;
done_testing();
