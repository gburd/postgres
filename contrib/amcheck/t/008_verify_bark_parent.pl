
# Copyright (c) 2026, PostgreSQL Global Development Group

# Corrupt BARK index pages on disk with the server stopped, as
# 007_verify_bark.pl does, for the corruptions 007 does not plant: the
# tree-walk checks only bark_index_parent_check makes (bark_check_levels),
# the per-page entry limits, deleted-page and prefix-page layouts, posting
# sets of one member, and the marker rules of a multikey operator class.
# Each corruption gets its own index and must be reported with the
# expected message, never crash, never pass.

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
	CREATE EXTENSION bark_multikey;
	CREATE TABLE t_small (a int);
	CREATE TABLE t_multi (a int);
	CREATE TABLE t_post (a int);
	CREATE TABLE t_pfx (k text COLLATE "C");
	CREATE TABLE t_mk (id int, a int4[]);
	CREATE TABLE t_empty (a int);
	CREATE TABLE t_deep (a int);
	INSERT INTO t_deep SELECT g FROM generate_series(1, 40000) g;
	INSERT INTO t_pfx SELECT 'a shared prefix of the keys ' || g FROM generate_series(1, 50) g;
));

# name => [table, column list, reloptions]
my %idx = (
	map({ $_ => [ 't_small', 'a' ] }
		qw(i_toobig i_shouldovf i_tidorder i_leaflevel i_orphan i_pfx_empty)),
	map({ $_ => [ 't_multi', 'a' ] }
		qw(i_tidhikey i_del_upper i_del_leaf i_pfx_notleaf i_metalevel
		  i_rootnew i_roothalf i_rootprev i_firstdl i_emptyleaf i_newleaf
		  i_halfleaf i_lastdl i_nodl i_rootnext i_rootmeta i_alllevel i_dlmeta
		  i_del_ok i_del_left)),
	i_post1 => [ 't_post', 'a' ],
	i_pfx_bad => [ 't_pfx', 'k', 'prefix_compression = on' ],
	i_pfx_info => [ 't_pfx', 'k', 'prefix_compression = on' ],
	i_empty => [ 't_empty', 'a' ],
	i_mk_null => [ 't_mk', 'a bark_int4_array_marked_ops' ],
	i_mk_list => [ 't_mk', 'a bark_int4_array_marked_ops' ],
	i_mk_notnull => [ 't_mk', 'id, a bark_int4_array_marked_ops' ],
	i_mk_nomark => [ 't_mk', 'a bark_int4_array_ops' ],
	map({ $_ => [ 't_deep', 'a', 'fillfactor = 10' ] }
		qw(i_deep_notpivot i_deep_hikey i_deep_hktid i_deep_lastempty)));
for my $i (sort keys %idx)
{
	my ($t, $cols, $opt) = @{ $idx{$i} };
	$node->safe_psql('postgres',
		"CREATE INDEX $i ON $t USING bark ($cols)"
		  . (defined $opt ? " WITH ($opt)" : ''));
}
$node->safe_psql('postgres', 'CREATE UNIQUE INDEX i_empty_u ON t_empty USING bark (a)');

# Inserted after the indexes exist, so the leaves split as inserts split
# them and duplicates form LIST and POSTING entries.  t_deep's indexes are
# built at a low fillfactor to reach three levels, and t_pfx's by CREATE
# INDEX, which forms prefix-compressed leaves.
$node->safe_psql(
	'postgres', q(
	INSERT INTO t_small SELECT g FROM generate_series(1, 10) g;
	INSERT INTO t_multi SELECT g FROM generate_series(1, 3000) g;
	INSERT INTO t_post SELECT g / 1000 FROM generate_series(0, 4999) g;
	INSERT INTO t_mk VALUES (1, '{1,2}'), (2, '{3}'), (3, '{4}');
	INSERT INTO t_mk SELECT g, '{5}' FROM generate_series(10, 19) g;
));

for my $i (sort keys %idx)
{
	$node->safe_psql('postgres', "SELECT bark_index_parent_check('$i', true)")
	  unless $idx{$i}[0] eq 't_mk';
	$node->safe_psql('postgres', "SELECT bark_index_parent_check('$i')");
}

sub items
{
	my ($i, $blk, $col, $where) = @_;
	return [
		split /\n/,
		$node->safe_psql('postgres',
			"SELECT $col FROM bark_page_items('$i', $blk) "
			  . ($where // '') . " ORDER BY itemoffset")
	];
}

my %info;
for my $i ('i_empty_u', sort keys %idx)
{
	my $c = $info{$i} = {};
	$c->{path} = $node->data_dir . '/'
	  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('$i')");
	($c->{root}, $c->{level}) = split /\|/,
	  $node->safe_psql('postgres', "SELECT root, level FROM bark_metap('$i')");
	$c->{kids} = items($i, $c->{root}, 'downlink') if $c->{level} > 0;
}
for my $i (qw(i_mk_null i_mk_notnull))
{
	($info{$i}{off_marker}) = @{
		items($i, $info{$i}{root}, 'itemoffset',
			"WHERE ctid::text = '(4294967295,1)'") };
}
($info{i_mk_list}{off_LIST}) =
  @{ items('i_mk_list', $info{i_mk_list}{root}, 'itemoffset', "WHERE shape = 'LIST'") };
($info{i_post1}{off_POSTING}) =
  @{ items('i_post1', $info{i_post1}{root}, 'itemoffset', "WHERE shape = 'POSTING'") };
# Level 1 of t_deep: the leftmost internal page, and its last child.
for my $i (qw(i_deep_lastempty))
{
	my $c = $info{$i};
	$c->{lastkid} = items($i, $c->{kids}[0], 'downlink')->[-1];
}

is($info{i_toobig}{level}, 0, 't_small is a single leaf');
cmp_ok(scalar @{ $info{i_tidhikey}{kids} }, '>=', 4, 't_multi has 4+ leaves');
is($info{i_deep_hikey}{level}, 2, 't_deep has three levels');
cmp_ok(scalar @{ $info{i_deep_hikey}{kids} }, '>=', 2, 't_deep has 2+ internal pages below its root');
is($info{i_post1}{level}, 0, 't_post is a single leaf');
ok(defined $info{i_post1}{off_POSTING}, 't_post has a POSTING entry');
is($info{i_pfx_bad}{level}, 0, 't_pfx is a single leaf');
like(
	$node->safe_psql('postgres',
		"SELECT flags FROM bark_page_stats('i_pfx_bad', $info{i_pfx_bad}{root})"),
	qr/prefix/, 't_pfx is prefix-compressed');
is($info{i_empty}{level}, 0, 't_empty is a single leaf');
ok(defined $info{$_}{off_marker}, "$_ has a marker") for qw(i_mk_null i_mk_notnull);
ok(defined $info{i_mk_list}{off_LIST}, 'i_mk_list has a LIST entry');

$node->stop;

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
sub s32 { substr(${ $_[0] }, $_[1], 4) = pack('L', $_[2]) }
sub gi32 { unpack('l', substr(${ $_[0] }, $_[1], 4)) }
sub si32 { substr(${ $_[0] }, $_[1], 4) = pack('l', $_[2]) }
# Byte offset of item $off (ItemIdData lp_off: low 15 bits).
sub item { unpack('L', substr(${ $_[0] }, 24 + 4 * ($_[1] - 1), 4)) & 0x7fff }
sub maxoff { (g16($_[0], 12) - 24) / 4 }
# PageHeaderData: pd_lower 12, pd_upper 14, pd_special 16.
sub set_lower { s16($_[0], 12, $_[1]) }
# BarkPageOpaqueData: prev 0, next 4, level 8, cycleid 10, flags 12.
sub opq { g16($_[0], 16) + $_[1] }
sub flags_or { s16($_[0], opq($_[0], 12), g16($_[0], opq($_[0], 12)) | $_[1]) }
# IndexTupleData: t_tid block (bi_hi, bi_lo) 0, posid 4, t_info 6; an int4
# key column right after the 8-byte header.
sub tid_blk { (g16($_[0], $_[1]) << 16) | g16($_[0], $_[1] + 2) }
sub set_marker_tid { s16($_[0], $_[1], 0xffff); s16($_[0], $_[1] + 2, 0xffff); s16($_[0], $_[1] + 4, 1) }
sub set_size { s16($_[0], $_[1] + 6, (g16($_[0], $_[1] + 6) & 0xe000) | $_[2]) }
sub key { gi32($_[0], $_[1] + 8) }
sub set_key { si32($_[0], $_[1] + 8, $_[2]) }

my $sroot = $info{i_toobig}{root};
my ($mroot, $L0, $L1) = ($info{i_tidhikey}{root}, @{ $info{i_tidhikey}{kids} });
my $nkids = scalar @{ $info{i_tidhikey}{kids} };
my ($droot, $I0, $I1) = ($info{i_deep_hikey}{root}, @{ $info{i_deep_hikey}{kids} });

# BarkMaxItemSize, from the page's special space as bark.h computes it.
my $special = do { my $p = rd($info{i_toobig}{path}, $sroot); g16(\$p, 16) };
my $maxitem = int(($blksz - 40 - ($blksz - $special)) / 3) & ~7;

# Entry limits on a leaf.  An entry above the ceiling, and a SINGLE entry
# too large to stay inline.
patch('i_toobig', $sroot, sub { set_size($_[0], item($_[0], 1), $maxitem + 8) });
patch('i_shouldovf', $sroot, sub { set_size($_[0], item($_[0], 1), $maxitem - 2) });
# Two entries of key 2, the first with the higher heap TID.
patch('i_tidorder', $sroot,
	sub { my $it = item($_[0], 1); set_key($_[0], $it, 2); s16($_[0], $it + 4, 3) });
# The last entry of L0 equal to its high key, which has no heap TID: it
# belongs on L1.
patch('i_tidhikey', $L0,
	sub { set_key($_[0], item($_[0], maxoff($_[0])), key($_[0], item($_[0], 1))) });

# Deleted pages: items removed but pd_upper not at the safexid, and the
# deleted-page layout on a page still flagged as a leaf.
patch('i_del_upper', $info{i_del_upper}{kids}[1],
	sub { set_lower($_[0], 24); flags_or($_[0], 0x04) });
patch('i_del_leaf', $info{i_del_leaf}{kids}[1],
	sub { set_lower($_[0], 24); s16($_[0], 14, g16($_[0], 16) - 8); flags_or($_[0], 0x04) });

# Prefix pages: the flag on an internal page, and a PREFIX item with a
# nonzero t_tid.
patch('i_pfx_notleaf', $info{i_pfx_notleaf}{root}, sub { flags_or($_[0], 0x400) });
patch('i_pfx_bad', $info{i_pfx_bad}{root}, sub { s16($_[0], item($_[0], 1) + 2, 1) });
# A PREFIX item whose size disagrees with its line pointer, and the flag on
# a leaf with no items.
patch('i_pfx_info', $info{i_pfx_info}{root}, sub { set_size($_[0], item($_[0], 1), g16($_[0], item($_[0], 1) + 6) + 1) });
patch('i_pfx_empty', $sroot, sub { set_lower($_[0], 24); flags_or($_[0], 0x400) });

# A POSTING entry whose set is a valid sbm of one member: a uint16 length,
# then the 16-byte wire header (magic "sm10", version 2, little-endian), and
# a small-mode body of one word holding bit 0.
patch('i_post1', $info{i_post1}{root},
	sub {
		my $it = item($_[0], $info{i_post1}{off_POSTING});
		my $body = $it + tid_blk($_[0], $it);
		substr(${ $_[0] }, $body, 34) =
		  pack('S', 32)
		  . pack('L C C C C', 0x30316d73, 2, 1, 1, 0) . ("\0" x 8)
		  . pack('Q', (1 << 63) | 1) . pack('Q', 1);
	});

# Markers.  The marker of a one-column marked index made NULL (the null
# bitmap then reads the low byte of its key, which is 0), the NULL fixed
# column of a two-column one's marker made not NULL, the reserved TID as the
# highest locator of a LIST, and on an index whose class has no markers.
patch('i_mk_null', $info{i_mk_null}{root},
	sub { my $it = item($_[0], $info{i_mk_null}{off_marker}); s16($_[0], $it + 6, g16($_[0], $it + 6) | 0x8000) });
patch('i_mk_notnull', $info{i_mk_notnull}{root},
	sub { my $it = item($_[0], $info{i_mk_notnull}{off_marker}); s16($_[0], $it + 6, g16($_[0], $it + 6) & ~0x8000) });
patch('i_mk_list', $info{i_mk_list}{root},
	sub {
		my $it = item($_[0], $info{i_mk_list}{off_LIST});
		my $n = g16($_[0], $it + 4) & 0xfff;
		set_marker_tid($_[0], $it + tid_blk($_[0], $it) + 6 * ($n - 1));
	});
patch('i_mk_nomark', $info{i_mk_nomark}{root}, sub { set_marker_tid($_[0], item($_[0], 1)) });

# The tree walk.  The meta page's root level one too high (BarkMetaPageData
# at 24: magic, version, root, level); a single leaf at level 1 named as a
# level-1 root.
patch('i_metalevel', 0, sub { s32($_[0], 36, 2) });
patch('i_leaflevel', 0, sub { s32($_[0], 36, 1) });
patch('i_leaflevel', $sroot, sub { s16($_[0], opq($_[0], 8), 1) });
# A root that is a new page, a half-dead page, or has a left sibling.
patch('i_rootnew', $info{i_rootnew}{root}, sub { ${ $_[0] } = "\0" x $blksz });
patch('i_roothalf', $info{i_roothalf}{root}, sub { flags_or($_[0], 0x10) });
patch('i_rootprev', $info{i_rootprev}{root},
	sub { s32($_[0], opq($_[0], 0), $info{i_rootprev}{kids}[0]) });
# The root's first downlink a copy of its second, so it has a key.
patch('i_firstdl', $info{i_firstdl}{root},
	sub { substr(${ $_[0] }, 24, 4) = substr(${ $_[0] }, 28, 4) });
# The leftmost leaf emptied: it has no high key for the next downlink.
patch('i_emptyleaf', $info{i_emptyleaf}{kids}[0], sub { set_lower($_[0], 24) });
# The second leaf a new page, or half-dead: the root links to it.
patch('i_newleaf', $info{i_newleaf}{kids}[1], sub { ${ $_[0] } = "\0" x $blksz });
patch('i_halfleaf', $info{i_halfleaf}{kids}[1], sub { flags_or($_[0], 0x10) });
# The root's last downlink dropped, and all of them.
patch('i_lastdl', $info{i_lastdl}{root}, sub { set_lower($_[0], g16($_[0], 12) - 4) });
patch('i_nodl', $info{i_nodl}{root}, sub { set_lower($_[0], 24) });
# The root flagged as the meta page.
patch('i_rootmeta', $info{i_rootmeta}{root}, sub { flags_or($_[0], 0x08) });
# Every leaf one level up: the sibling links agree, the downlinks do not.
patch('i_alllevel', $_, sub { s16($_[0], opq($_[0], 8), 1) }) for @{ $info{i_alllevel}{kids} };
# A downlink to the meta page.
patch('i_dlmeta', $info{i_dlmeta}{root}, sub { s16($_[0], item($_[0], 2), 0); s16($_[0], item($_[0], 2) + 2, 0) });
# A leaf deleted with the right layout but still in the tree: the second
# (heapallindexed walks past it and misses its rows) and the leftmost (the
# descent to the leftmost leaf moves right through it).
for my $i (qw(i_del_ok i_del_left))
{
	patch($i, $info{$i}{kids}[ $i eq 'i_del_ok' ? 1 : 0 ],
		sub {
			set_lower($_[0], 24);
			s16($_[0], 14, g16($_[0], 16) - 8);
			s16($_[0], opq($_[0], 12), (g16($_[0], opq($_[0], 12)) & ~0x01) | 0x04);
		});
}
# The root given a right sibling that is a new page, appended to the index.
{
	my $path = $info{i_rootnext}{path};
	my $nblocks = (-s $path) / $blksz;
	open(my $fh, '>>:raw', $path) or die "open: $!";
	print $fh "\0" x $blksz;
	close $fh;
	patch('i_rootnext', $info{i_rootnext}{root}, sub { s32($_[0], opq($_[0], 4), $nblocks) });
}
# An index with no root and no tree pages, the meta page naming none and
# its one leaf a new page, as before its first insert: not corrupt.
for my $i (qw(i_empty i_empty_u))
{
	patch($i, 0, sub { s32($_[0], 32, 0) });
	patch($i, $info{$i}{root}, sub { ${ $_[0] } = "\0" x $blksz });
}
# A copy of the only leaf appended to the index: live, but in no tree.
{
	my $p = rd($info{i_orphan}{path}, $info{i_orphan}{root});
	open(my $fh, '>>:raw', $info{i_orphan}{path}) or die "open: $!";
	print $fh $p;
	close $fh;
}

# Three levels.  The root's second downlink made a SINGLE entry (alt-TID bit
# cleared); its key and child do not change.
patch('i_deep_notpivot', $info{i_deep_notpivot}{root},
	sub { my $it = item($_[0], 2); s16($_[0], $it + 6, g16($_[0], $it + 6) & ~0x2000) });
# The leftmost internal page's high key lowered by one, and the root's
# downlink to its right sibling with it: still above the page's keys and
# below its sibling's, but no longer its last child's high key.
patch('i_deep_hikey', $I0, sub { my $it = item($_[0], 1); set_key($_[0], $it, key($_[0], $it) - 1) });
patch('i_deep_hikey', $droot, sub { my $it = item($_[0], 2); set_key($_[0], $it, key($_[0], $it) - 1) });
# The high key given a heap TID (the padding after its key, zero), which
# the root's downlink to the page's right sibling does not have.
patch('i_deep_hktid', $info{i_deep_hktid}{kids}[0],
	sub { my $it = item($_[0], 1); s16($_[0], $it + 4, g16($_[0], $it + 4) | 0x4000) });
# That page's last child emptied: it has no high key to match the page's.
patch('i_deep_lastempty', $info{i_deep_lastempty}{lastkid}, sub { set_lower($_[0], 24) });

$node->start;

sub check
{
	my ($fn, $i, $re, $args) = @_;
	my ($rc, $out, $err) =
	  $node->psql('postgres', "SELECT $fn('$i'" . ($args // '') . ")");
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

my ($proot, $poff) = ($info{i_post1}{root}, $info{i_post1}{off_POSTING});
my $toobig = $maxitem + 8;
my $ovf = $maxitem - 2;
both('i_toobig', "has a $toobig-byte leaf entry on page $sroot at offset 1, larger than the $maxitem-byte limit");
both('i_shouldovf', "has a $ovf-byte SINGLE entry on page $sroot at offset 1 that should be OVERSIZED");
both('i_tidorder', "has out-of-order heap TIDs among equal keys on page $sroot at offset 2");
both('i_tidhikey', "has a heap TID past the high key on page $L0 at offset \\d+");
both('i_del_upper', "has a deleted page $info{i_del_upper}{kids}[1] that does not have the deleted-page layout");
both('i_del_leaf', "has a deleted page $info{i_del_leaf}{kids}[1] that does not have the deleted-page layout");
both('i_pfx_notleaf', "has a prefix-compressed page $mroot that is not a leaf with a prefix item");
both('i_pfx_bad', "has a malformed prefix item on page $info{i_pfx_bad}{root}");
both('i_post1', "has an invalid posting set \\(1 members\\) on page $proot at offset $poff");
both('i_mk_null',
	"has a marker on page $info{i_mk_null}{root} at offset $info{i_mk_null}{off_marker} whose key column 1 is NULL");
both('i_mk_notnull',
	"has a marker on page $info{i_mk_notnull}{root} at offset $info{i_mk_notnull}{off_marker} whose key column 1 is not NULL");
both('i_mk_list',
	"has the reserved marker heap TID in an entry that is not SINGLE on page $info{i_mk_list}{root} at offset $info{i_mk_list}{off_LIST}");
both('i_mk_nomark', "has a marker on page $info{i_mk_nomark}{root} at offset 1 but no column with markers");

parent_only('i_metalevel', "has a page $mroot at level 2 of the tree that is not a live page of that level");
parent_only('i_leaflevel', "has a page $sroot at level 1 of the tree that is not a live page of that level");
parent_only('i_rootnew', "has a page $mroot at level 1 of the tree that is not a live page of that level");
parent_only('i_roothalf', "has a page $mroot at level 1 of the tree that is not a live page of that level");
parent_only('i_rootprev', "has page $mroot flagged as root inconsistently");
parent_only('i_firstdl', "has a first downlink at level 1 on page $mroot that is not minus infinity");
parent_only('i_emptyleaf',
	"has a downlink on page $mroot at offset 2 that is not the high key of the page to its child's left");
parent_only('i_newleaf', "has a downlink on page $mroot to page $L1, which is not a live page");
parent_only('i_halfleaf', "has a downlink on page $mroot to page $L1, which is not a live page");
parent_only('i_lastdl', 'has pages at level 0 that no downlink at level 1 names');
parent_only('i_nodl', 'has pages at level 0 that no downlink at level 1 names');
# heapallindexed descends to the leftmost leaf, and finds no downlink.
check('bark_index_check', 'i_nodl', "has an internal page $mroot with no downlinks", ', true');
parent_only('i_orphan', 'has 2 live pages, but only 1 are reachable from its root');
parent_only('i_deep_notpivot', "has an item on internal page $droot at offset 2 that is not a pivot");
parent_only('i_deep_hikey', "has a high key on page $I0 that is not the high key of its last child");
parent_only('i_deep_hktid',
	"has a downlink on page $droot at offset 2 that is not the high key of the page to its child's left");
both('i_pfx_info', "has a malformed prefix item on page $info{i_pfx_info}{root}");
both('i_pfx_empty', "has a prefix-compressed page $sroot that is not a leaf with a prefix item");
parent_only('i_rootmeta', "has a page $mroot at level 1 of the tree that is not a live page of that level");
# The root's first downlink becomes its high key, which only a downlink may
# be: minus infinity.
both('i_rootnext', "has a high key with 0 key attributes on page $mroot");
both('i_alllevel', "has a downlink on page $mroot at level 1 to page $L0, which is not at level 0");
both('i_dlmeta', "has a downlink on page $mroot to invalid block 0");
parent_only('i_del_ok', "has a downlink on page $mroot to page $info{i_del_ok}{kids}[1], which is not a live page");
parent_only('i_del_left', "has a downlink on page $mroot to page $info{i_del_left}{kids}[0], which is not a live page");
for my $i (qw(i_del_ok i_del_left))
{
	check('bark_index_check', $i,
		"heap tuple \\(\\d+,\\d+\\) from table \"t_multi\" lacks matching index tuple within index \"$i\"",
		', true');
}
# An empty index has no root and no pages; every check passes.
check($_, 'i_empty', undef, ', true') for qw(bark_index_check bark_index_parent_check);
check('bark_index_check', 'i_empty_u', undef, ', true, true');
parent_only('i_deep_lastempty', "has a high key on page $I0 that is not the high key of its last child");

# No check crashed the server.
is($node->safe_psql('postgres', 'SELECT 1'), '1', 'server is up');
unlike(slurp_file($node->logfile), qr/terminated by signal|TRAP:/, 'no backend crashed');

$node->stop;
done_testing();
