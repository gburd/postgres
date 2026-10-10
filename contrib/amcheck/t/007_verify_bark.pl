
# Copyright (c) 2026, PostgreSQL Global Development Group

# Corrupt BARK index pages on disk with the server stopped, and check that
# bark_index_check and bark_index_parent_check report each corruption with
# the expected message and never crash.  Every corruption gets its own
# index, so the reports do not mask each other.  Offsets come from the
# page itself (line pointers, special space); pageinspect finds the
# blocks and items to corrupt before the server stops.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my $node = PostgreSQL::Test::Cluster->new('test');
$node->init(no_data_checksums => 1);
$node->append_conf('postgresql.conf', 'autovacuum=off');
$node->start;
my $blksz = int($node->safe_psql('postgres', 'SHOW block_size'));

$node->safe_psql(
	'postgres', q(
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pageinspect;
	CREATE FUNCTION bigstr(s int, n int) RETURNS text LANGUAGE sql IMMUTABLE AS
	  $$ SELECT substr(string_agg(md5(s::text || g::text), ''), 1, n)
	     FROM generate_series(1, (n + 31) / 32) g $$;
	CREATE TABLE t_small (a int);
	CREATE TABLE t_multi (a int);
	CREATE TABLE t_list (a int);
	CREATE TABLE t_post (a int);
	CREATE TABLE t_big (k text COLLATE "C");
	-- Uncompressed in the index too (it copies the column's storage), so the
	-- oversized entry keeps an inline prefix of the key's own bytes.
	ALTER TABLE t_big ALTER COLUMN k SET STORAGE EXTERNAL;
));

# name => table, column
my %idx = (
	i_order => 't_small', i_marker => 't_small', i_version => 't_small',
	i_noroot => 't_small',
	map({ $_ => 't_multi' }
		qw(i_hikey i_rlink i_llink i_leftmost i_level i_incomplete i_deleted
		  i_hknotpivot i_hknatts i_rfirst i_dlbad i_dlswap i_dlskip i_dlkey
		  i_metaroot i_rootflag)),
	i_list_order => 't_list', i_list_count => 't_list',
	i_post_len => 't_post', i_post_size => 't_post',
	map({ $_ => 't_big' }
		qw(i_big_out i_big_nonovf i_big_trunc i_big_prefix i_big_complete
		  i_big_chain)));
for my $i (sort keys %idx)
{
	my $col = $idx{$i} eq 't_big' ? 'k' : 'a';
	$node->safe_psql('postgres',
		"CREATE INDEX $i ON $idx{$i} USING bark ($col)");
}

# Inserted after the indexes exist, so duplicates form LIST and POSTING
# entries and the leaves split as inserts split them.
$node->safe_psql(
	'postgres', q(
	INSERT INTO t_small SELECT g FROM generate_series(1, 10) g;
	INSERT INTO t_multi SELECT g FROM generate_series(1, 3000) g;
	INSERT INTO t_list SELECT g % 20 FROM generate_series(1, 200) g;
	INSERT INTO t_post SELECT g / 1000 FROM generate_series(0, 4999) g;
	INSERT INTO t_big VALUES ('A' || bigstr(1, 40000)), ('b1'), ('b2'), ('b3');
));

# Every index passes before it is corrupted.
for my $i (sort keys %idx)
{
	$node->safe_psql('postgres', "SELECT bark_index_parent_check('$i', true)");
}

# Facts about each index, read before the server stops.
my %info;
for my $i (sort keys %idx)
{
	my $c = $info{$i} = {};
	$c->{path} = $node->data_dir . '/'
	  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('$i')");
	($c->{root}, $c->{level}) = split /\|/,
	  $node->safe_psql('postgres', "SELECT root, level FROM bark_metap('$i')");
	if ($c->{level} > 0)
	{
		# The root is rightmost, so it has no high key: all downlinks.
		$c->{kids} = [
			split /\n/,
			$node->safe_psql('postgres',
				"SELECT downlink FROM bark_page_items('$i', $c->{root}) ORDER BY itemoffset"
			) ];
	}
	for my $shape (qw(LIST POSTING OVERSIZED))
	{
		my $r = $node->safe_psql('postgres',
			"SELECT itemoffset, overflow_blkno FROM bark_page_items('$i', $c->{root}) WHERE shape = '$shape' ORDER BY itemoffset LIMIT 1"
		);
		($c->{"off_$shape"}, $c->{ovf}) = split /\|/, $r if $r ne '';
	}
}
is($info{i_order}{level}, 0, 't_small is a single leaf');
cmp_ok(scalar @{ $info{i_hikey}{kids} }, '>=', 4, 't_multi has 4+ leaves');
is($info{i_list_order}{level}, 0, 't_list is a single leaf');
ok(defined $info{i_list_order}{off_LIST}, 't_list has a LIST entry');
is($info{i_post_len}{level}, 0, 't_post is a single leaf');
ok(defined $info{i_post_len}{off_POSTING}, 't_post has a POSTING entry');
is($info{i_big_out}{level}, 0, 't_big is a single leaf');
is($info{i_big_out}{off_OVERSIZED}, 1, 't_big has an OVERSIZED first entry');

$node->stop;

# Page access.  A page is a string; offsets are bytes from its start.
sub rd
{
	my ($path, $blk) = @_;
	open(my $fh, '<:raw', $path) or die "open $path: $!";
	seek($fh, $blk * $blksz, 0) or die "seek: $!";
	read($fh, my $p, $blksz) == $blksz or die "short read of $path";
	close $fh;
	return $p;
}

sub patch
{
	my ($i, $blk, $f) = @_;
	my $path = $info{$i}{path};
	my $p = rd($path, $blk);
	$f->(\$p);
	open(my $fh, '+<:raw', $path) or die "open $path: $!";
	seek($fh, $blk * $blksz, 0) or die "seek: $!";
	print $fh $p;
	close $fh;
}
sub g16 { unpack('S', substr(${ $_[0] }, $_[1], 2)) }
sub s16 { substr(${ $_[0] }, $_[1], 2) = pack('S', $_[2]) }
sub g32 { unpack('L', substr(${ $_[0] }, $_[1], 4)) }
sub s32 { substr(${ $_[0] }, $_[1], 4) = pack('L', $_[2]) }
sub gi32 { unpack('l', substr(${ $_[0] }, $_[1], 4)) }
sub si32 { substr(${ $_[0] }, $_[1], 4) = pack('l', $_[2]) }
# Byte offset of item $off (ItemIdData lp_off: low 15 bits).
sub item { g32($_[0], 24 + 4 * ($_[1] - 1)) & 0x7fff }
sub maxoff { (g16($_[0], 12) - 24) / 4 }
# BarkPageOpaqueData: prev 0, next 4, level 8, cycleid 10, flags 12.
sub opq { g16($_[0], 16) + $_[1] }
# IndexTupleData: t_tid block (bi_hi, bi_lo) 0, posid 4, t_info 6; data 8.
sub set_tid_blk { s16($_[0], $_[1], $_[2] >> 16); s16($_[0], $_[1] + 2, $_[2] & 0xffff) }
sub tid_blk { (g16($_[0], $_[1]) << 16) | g16($_[0], $_[1] + 2) }
sub key_bump { my ($p, $it, $v) = @_; si32($p, $it + 8, $v) }
# An int4 key column sits right after the 8-byte header.

my ($sroot) = $info{i_order}{root};
my ($mroot, $L0, $L1, $L2) = ($info{i_hikey}{root}, @{ $info{i_hikey}{kids} });

# Keys out of order within a page: the first key of a 10-key leaf becomes 1000.
patch('i_order', $sroot, sub { key_bump($_[0], item($_[0], 1), 1000) });
# The reserved marker heap TID on an index without a marker column.
patch('i_marker', $sroot,
	sub { my $it = item($_[0], maxoff($_[0])); s16($_[0], $it, 0xffff); s16($_[0], $it + 2, 0xffff); s16($_[0], $it + 4, 1) });
# Meta page: BarkMetaPageData at 24 (magic, version, root, level).
patch('i_version', 0, sub { s32($_[0], 28, 1) });
patch('i_noroot', 0, sub { s32($_[0], 32, 0) });
patch('i_metaroot', 0, sub { s32($_[0], 32, 999999) });

# Leaves of t_multi.  L0 is leftmost; its high key is item 1.
patch('i_hikey', $info{i_hikey}{kids}[0],
	sub { key_bump($_[0], item($_[0], maxoff($_[0])), 2000000000) });
patch('i_rlink', $info{i_rlink}{kids}[0],
	sub { s32($_[0], opq($_[0], 4), $info{i_rlink}{kids}[2]) });
patch('i_llink', $info{i_llink}{kids}[1],
	sub { s32($_[0], opq($_[0], 0), $info{i_llink}{kids}[2]) });
patch('i_leftmost', $info{i_leftmost}{kids}[0],
	sub { s32($_[0], opq($_[0], 0), $info{i_leftmost}{kids}[1]) });
patch('i_level', $info{i_level}{kids}[1], sub { s16($_[0], opq($_[0], 8), 1) });
patch('i_incomplete', $info{i_incomplete}{kids}[0],
	sub { s16($_[0], opq($_[0], 12), g16($_[0], opq($_[0], 12)) | 0x20) });
patch('i_deleted', $info{i_deleted}{kids}[1],
	sub { s16($_[0], opq($_[0], 12), g16($_[0], opq($_[0], 12)) | 0x04) });
patch('i_hknotpivot', $info{i_hknotpivot}{kids}[0],
	sub { my $it = item($_[0], 1); s16($_[0], $it + 6, g16($_[0], $it + 6) & ~0x2000) });
patch('i_hknatts', $info{i_hknatts}{kids}[0],
	sub { s16($_[0], item($_[0], 1) + 4, 0x1000 | 5) });
patch('i_rfirst', $info{i_rfirst}{kids}[1],
	sub { key_bump($_[0], item($_[0], 2), -1) });

# The root of t_multi: downlinks at items 1.. (item 1 is minus infinity).
patch('i_dlbad', $info{i_dlbad}{root}, sub { set_tid_blk($_[0], item($_[0], 2), 999999) });
patch('i_dlswap', $info{i_dlswap}{root},
	sub {
		my ($a, $b) = (item($_[0], 2), item($_[0], 3));
		my ($x, $y) = (tid_blk($_[0], $a), tid_blk($_[0], $b));
		set_tid_blk($_[0], $a, $y);
		set_tid_blk($_[0], $b, $x);
	});
patch('i_dlskip', $info{i_dlskip}{root},
	sub { set_tid_blk($_[0], item($_[0], 2), $info{i_dlskip}{kids}[2]) });
patch('i_dlkey', $info{i_dlkey}{root},
	sub { my $it = item($_[0], 2); key_bump($_[0], $it, gi32($_[0], $it + 8) - 1) });
patch('i_rootflag', $info{i_rootflag}{root},
	sub { s16($_[0], opq($_[0], 12), g16($_[0], opq($_[0], 12)) & ~0x02) });

# LIST: count in the low 12 bits of posid; locators at the body offset,
# which the t_tid block field holds.
patch('i_list_order', $info{i_list_order}{root},
	sub {
		my $it = item($_[0], $info{i_list_order}{off_LIST});
		my $body = $it + tid_blk($_[0], $it);
		my ($t0, $t1) = (substr(${ $_[0] }, $body, 6), substr(${ $_[0] }, $body + 6, 6));
		substr(${ $_[0] }, $body, 12) = $t1 . $t0;
	});
patch('i_list_count', $info{i_list_count}{root},
	sub { my $it = item($_[0], $info{i_list_count}{off_LIST}); s16($_[0], $it + 4, (g16($_[0], $it + 4) & 0xf000) | 1) });

# POSTING: a uint16 serialization length at the body offset, then the sbm.
patch('i_post_len', $info{i_post_len}{root},
	sub { my $it = item($_[0], $info{i_post_len}{off_POSTING}); s16($_[0], $it + tid_blk($_[0], $it), 0xffff) });
patch('i_post_size', $info{i_post_size}{root},
	sub {
		my $it = item($_[0], $info{i_post_size}{off_POSTING});
		my $body = tid_blk($_[0], $it);
		my $need = $body + 2 + g16($_[0], $it + $body);
		s16($_[0], $it + 6, (g16($_[0], $it + 6) & 0xe000) | $need);
	});

# OVERSIZED: first overflow block in t_tid; BarkOverflowRef at 8: fulllen 0,
# locator 4, natts 10, pivottid 12, prefixlen 18, prefixcomplete 20, prefix 21.
my $broot = $info{i_big_out}{root};
# The prefix tests need one: prefixlen at 18 of the reference is nonzero.
for my $i (qw(i_big_prefix i_big_complete))
{
	my $p = rd($info{$i}{path}, $broot);
	cmp_ok(g16(\$p, item(\$p, 1) + 8 + 18), '>', 0, "$i has an inline prefix");
}
patch('i_big_out', $broot, sub { set_tid_blk($_[0], item($_[0], 1), 999999) });
# A zeroed first overflow page: valid as a new page, but not an overflow page.
patch('i_big_nonovf', $info{i_big_nonovf}{ovf}, sub { ${ $_[0] } = "\0" x $blksz });
patch('i_big_trunc', $broot,
	sub { my $it = item($_[0], 1); s32($_[0], $it + 8, g32($_[0], $it + 8) + 100000) });
patch('i_big_prefix', $broot, sub { substr(${ $_[0] }, item($_[0], 1) + 8 + 21, 1) = '@' });
patch('i_big_complete', $broot, sub { substr(${ $_[0] }, item($_[0], 1) + 8 + 20, 1) = "\x01" });
patch('i_big_chain', $info{i_big_chain}{ovf}, sub { s32($_[0], opq($_[0], 4), 999999) });

$node->start;

sub check
{
	my ($fn, $i, $re) = @_;
	my ($rc, $out, $err) = $node->psql('postgres', "SELECT $fn('$i')");
	if (defined $re)
	{
		like($err, qr/index "$i" .*$re|ERROR:  $re/m, "$fn reports $i");
	}
	else
	{
		is($rc, 0, "$fn passes $i") or diag($err);
	}
}

sub both { check('bark_index_check', $_[0], $_[1]); check('bark_index_parent_check', $_[0], $_[1]) }
sub parent_only { check('bark_index_check', $_[0], undef); check('bark_index_parent_check', $_[0], $_[1]) }

my ($lroot, $loff) = ($info{i_list_order}{root}, $info{i_list_order}{off_LIST});
my ($proot, $poff) = ($info{i_post_len}{root}, $info{i_post_len}{off_POSTING});

both('i_order', "has out-of-order keys on page $sroot at offset 2");
both('i_marker', "has a marker on page $sroot at offset 10 but no column with markers");
both('i_version', 'was built by an older BARK version');
parent_only('i_noroot', 'has no root but has 1 tree pages');
parent_only('i_metaroot', 'has a meta page naming invalid root block 999999');
both('i_hikey', "has a key past the high key on page $L0 at offset \\d+");
both('i_rlink', "broken sibling link: page ${L0}'s right sibling $L2 does not link back");
both('i_llink', "broken sibling link: page ${L0}'s right sibling $L1 does not link back");
parent_only('i_leftmost', "has page $L0 at level 0 whose left link does not match its place in the level");
both('i_level',
	"(has a level mismatch across the sibling link from page $L0 to $L1|has a downlink on page $mroot at level 1 to page $L1, which is not at level 0)");
both('i_incomplete', "has an unfinished split on page $L0");
both('i_deleted', "has a deleted page $L1 that does not have the deleted-page layout");
both('i_hknotpivot', "has a high key on page $L0 that is not a pivot");
both('i_hknatts', "has a high key with 5 key attributes on page $L0");
both('i_rfirst',
	"has a first key on page $L1 that is less than (the high key of its left sibling $L0|its downlink on page $mroot)");
both('i_dlbad', "has a downlink on page $mroot to invalid block 999999");
both('i_dlswap', "has a first key on page $L1 that is less than its downlink on page $mroot");
parent_only('i_dlskip',
	"has a downlink on page $mroot at offset 2 to page $L2, where the level below continues at page $L1");
parent_only('i_dlkey',
	"has a downlink on page $mroot at offset 2 that is not the high key of the page to its child's left");
parent_only('i_rootflag', "has page $mroot flagged as root inconsistently");
both('i_list_order', "has out-of-order list locators on page $lroot at offset $loff");
both('i_list_count', "has a list entry with 1 locators on page $lroot at offset $loff");
# The entry's shape is checked before anything reads its TIDs, so a corrupt
# set is reported as corruption of that entry.
both('i_post_len', "has a corrupt posting set on page $proot at offset $poff");
both('i_post_size', "posting entry on page $proot at offset $poff, smaller than the \\d+ bytes");
both('i_big_out', "oversized entry on page $broot at offset 1 points at out-of-range overflow block 999999");
both('i_big_nonovf', "oversized entry on page $broot at offset 1 references non-overflow block $info{i_big_nonovf}{ovf}");
both('i_big_trunc', "oversized entry on page $broot at offset 1 has a truncated overflow chain");
both('i_big_prefix', "oversized entry on page $broot at offset 1 has a prefix that does not match its first key column");
both('i_big_complete', "oversized entry on page $broot at offset 1 claims a complete 32-byte prefix");
both('i_big_chain', "oversized entry on page $broot at offset 1 points at out-of-range overflow block 999999");

# No check crashed the server.
is($node->safe_psql('postgres', 'SELECT 1'), '1', 'server is up');
unlike(slurp_file($node->logfile), qr/terminated by signal|TRAP:/, 'no backend crashed');

$node->stop;
done_testing();
