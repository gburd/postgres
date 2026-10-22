
# Copyright (c) 2026, PostgreSQL Global Development Group

# amcheck's and pageinspect's checks of BARK's version tags, on pages
# patched on disk with the server stopped: a tagged SINGLE with offset bits
# set or a misplaced trailer, a tagged OVERSIZED entry of the wrong size, a
# LIST with BARK_HAS_TAG, a tagged entry or pivot in an index that does not
# store tags, a pivot with a tag of 0, shortened under its tag or without
# every key attribute, BARK_LOWBOUND_TAGGED on the leftmost leaf or missing
# on a leaf whose lower bound has a tag, and (with the page validator) a
# version 2 meta page claiming BARK_META_VERSIONED.  Also pg_walinspect's
# descriptions of the records that carry tags.  Each corruption gets its
# own index, or the index is rebuilt clean first.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(no_data_checksums => 1);
$node->append_conf(
	'postgresql.conf', q{
autovacuum = off
bark_verify_pages = off
});
$node->start;
my $blksz = int($node->safe_psql('postgres', 'SHOW block_size'));

$node->safe_psql(
	'postgres', q{
	CREATE EXTENSION test_versioned_tableam;
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pageinspect;
	CREATE EXTENSION pg_walinspect;
});

# (TID, tag) of every leaf entry, by block and offset, as pageinspect reads it.
sub item
{
	my ($idx, $blk, $off) = @_;
	return $node->safe_psql('postgres',
		"SELECT shape || ' ' || coalesce(tag::text, '-') FROM bark_page_items('$idx', $blk) WHERE itemoffset = $off");
}

# The byte offset in its block of the item at `off` (its line pointer's lp_off).
sub item_offset
{
	my ($idx, $blk, $off) = @_;
	my $lp = $node->safe_psql('postgres',
		"SELECT lp_off FROM heap_page_items(get_raw_page('$idx', $blk)) WHERE lp = $off");
	return int($lp);
}

# Stop the server, apply `f` to block `blk` of `idx`, start it again.
sub patch
{
	my ($idx, $blk, $f) = @_;
	my $path = $node->data_dir . '/'
	  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('$idx')");
	$node->stop;
	open(my $fh, '+<:raw', $path) or die "open $path: $!";
	seek($fh, $blk * $blksz, 0) or die "seek: $!";
	read($fh, my $p, $blksz) == $blksz or die "short read of $path";
	$f->(\$p);
	seek($fh, $blk * $blksz, 0) or die "seek: $!";
	print $fh $p;
	close $fh;
	$node->start;
}
sub g16 { unpack('S', substr(${ $_[0] }, $_[1], 2)) }
sub s16 { substr(${ $_[0] }, $_[1], 2) = pack('S', $_[2]) }
sub s32 { substr(${ $_[0] }, $_[1], 4) = pack('L', $_[2]) }

# amcheck's report on `idx`, and pageinspect's listing of block `blk`.
sub reports
{
	my ($idx, $blk) = @_;
	my ($r1, $o1, $e1) =
	  $node->psql('postgres', "SELECT bark_index_check('$idx')");
	my ($r2, $o2, $e2) = $node->psql('postgres',
		"SELECT count(*) FROM bark_page_items(get_raw_page('$idx', $blk))");
	return ($e1, $e2);
}

# A one-leaf index on a versioned table: rows 1..20, every entry tagged
# (the build tags each row, its xmin).
sub fresh
{
	my ($name, $extra) = @_;
	$extra //= '';
	$node->safe_psql(
		'postgres', qq{
		DROP TABLE IF EXISTS $name;
		CREATE TABLE $name (id int, k int, t text) USING heap_versioned;
		ALTER TABLE $name ALTER COLUMN t SET STORAGE EXTERNAL;
		INSERT INTO $name SELECT g, g, 'x' FROM generate_series(1, 20) g;
		$extra
		SET test_versioned_tableam.build_tag_percent = 100;
		CREATE INDEX ${name}_k ON $name USING bark (k);
	});
}

my ($e1, $e2);

# (1) A tagged SINGLE whose low offset bits are not zero (the status bits
# are BARK_HAS_TAG alone, so it still reads as a SINGLE).
fresh('c1');
like(item('c1_k', 1, 3), qr/^SINGLE \d+$/, 'c1: offset 3 is a tagged SINGLE');
my $o = item_offset('c1_k', 1, 3);
patch('c1_k', 1, sub { s16($_[0], $o + 4, g16($_[0], $o + 4) | 0x0001) });
($e1, $e2) = reports('c1_k', 1);
like($e1, qr/BARK index "c1_k" has a malformed tagged entry on page 1 at offset 3/,
	'c1: amcheck reports a tagged SINGLE with offset bits set');

# (2) A tagged SINGLE whose trailer offset (t_tid block) is not MAXALIGNed.
fresh('c2');
$o = item_offset('c2_k', 1, 3);
patch('c2_k', 1, sub { s16($_[0], $o + 2, g16($_[0], $o + 2) + 1) });
($e1, $e2) = reports('c2_k', 1);
like($e1, qr/BARK index "c2_k" has a malformed tagged entry on page 1 at offset 3/,
	'c2: amcheck reports a misplaced trailer');

# (3) An untagged LIST with BARK_HAS_TAG: rows 1..20 all k = 5, untagged.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE c3 (id int, k int) USING heap_versioned;
	INSERT INTO c3 SELECT g, 5 FROM generate_series(1, 20) g;
	SET test_versioned_tableam.build_tag_percent = 0;
	CREATE INDEX c3_k ON c3 USING bark (k);
});
like(item('c3_k', 1, 1), qr/^LIST -$/, 'c3: one untagged LIST');
$o = item_offset('c3_k', 1, 1);
patch('c3_k', 1, sub { s16($_[0], $o + 4, g16($_[0], $o + 4) | 0x4000) });
($e1, $e2) = reports('c3_k', 1);
like($e1, qr/BARK index "c3_k" has a tagged LIST or POSTING entry on page 1 at offset 1/,
	'c3: amcheck reports a tagged LIST');

# (4) A tagged OVERSIZED entry whose size leaves out its tag.
fresh('c4', q{INSERT INTO c4 VALUES (21, 21,
  (SELECT string_agg(md5(i::text), '') FROM generate_series(1, 300) i));});
$node->safe_psql('postgres', 'CREATE INDEX c4_t ON c4 USING bark (t)');
my $ooff = $node->safe_psql('postgres',
	q{SELECT itemoffset FROM bark_page_items('c4_t', 1) WHERE shape = 'OVERSIZED' AND tag IS NOT NULL});
ok($ooff ne '', 'c4: a tagged OVERSIZED entry');
$o = item_offset('c4_t', 1, $ooff);
patch('c4_t', 1, sub { s16($_[0], $o + 6, (g16($_[0], $o + 6) & ~0x1fff) | ((g16($_[0], $o + 6) & 0x1fff) - 8)) })
  if $ooff ne '';
($e1, $e2) = reports('c4_t', 1);
like($e1, qr/BARK index "c4_t" has a malformed oversized entry on page 1 at offset $ooff/,
	'c4: amcheck reports an OVERSIZED entry without room for its tag');
like($e2, qr/invalid OVERSIZED entry at offset number $ooff/,
	'c4: pageinspect refuses to read past the entry');

# (5) A tagged entry, and a tagged pivot, in an index that does not store
# tags: clear BARK_META_VERSIONED.  The leaf of a one-run index splits, so
# its pivots have tags.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE c5 (id int, k int) USING heap_versioned;
	INSERT INTO c5 VALUES (1, 7);
	SET test_versioned_tableam.build_tag_percent = 0;
	CREATE INDEX c5_k ON c5 USING bark (k);
	SET test_versioned_tableam.stale_tags = live;
	SELECT count(test_versioned_insert_entry('c5_k', ctid, 2000000 + g))
	  FROM c5, generate_series(1, 500) g;
});
my $root = $node->safe_psql('postgres', q{SELECT root FROM bark_metap('c5_k')});
cmp_ok($root, '>', 1, 'c5: the run split');
# bark_flags of the meta page: 24 (page header) + 20
patch('c5_k', 0, sub { s32($_[0], 24 + 20, 0) });
($e1, $e2) = reports('c5_k', 1);
like($e1, qr/BARK index "c5_k" has a (tagged entry|pivot with a version tag) on page \d+ at offset \d+ but does not store tags/,
	'c5: amcheck reports a tag in an index that does not store tags');
my ($r, $out, $err) =
  $node->psql('postgres', "SELECT bark_index_parent_check('c5_k')");
like($err, qr/does not store tags/, 'c5: so does the parent check');

# (6) Internal pivots of c5, restored: a tag of 0 with BARK_PIVOT_TAG, too
# short for its tag, and without every key attribute.
for my $case ('zero', 'short', 'natts', 'other')
{
	$node->safe_psql('postgres', 'REINDEX INDEX c5_k');
	$node->safe_psql(
		'postgres', q{
		SET test_versioned_tableam.stale_tags = live;
		SELECT count(test_versioned_insert_entry('c5_k', ctid, 3000000 + g))
		  FROM c5, generate_series(1, 500) g;
	});
	$root = $node->safe_psql('postgres', q{SELECT root FROM bark_metap('c5_k')});
	my $poff = $node->safe_psql('postgres',
		"SELECT min(itemoffset) FROM bark_page_items('c5_k', $root) WHERE tag IS NOT NULL");
	ok($poff ne '', "c6 $case: the root has a tagged pivot");
	$o = item_offset('c5_k', $root, $poff);
	my $len = $node->safe_psql('postgres',
		"SELECT itemlen FROM bark_page_items('c5_k', $root) WHERE itemoffset = $poff");
	if ($case eq 'zero')
	{
		patch('c5_k', $root, sub { s32($_[0], $o + $len - 6 - 4, 0) });
		($e1, $e2) = reports('c5_k', $root);
		like($e1, qr/has a pivot with version tag 0 on page $root at offset $poff/,
			'c6: amcheck reports a pivot tag of 0');
	}
	elsif ($case eq 'short')
	{
		# t_info's size down by 8: the heap TID moves into the tag's place
		patch('c5_k', $root, sub {
			my $ti = g16($_[0], $o + 6);
			s16($_[0], $o + 6, ($ti & ~0x1fff) | (($ti & 0x1fff) - 8)); });
		($e1, $e2) = reports('c5_k', $root);
		like($e1, qr/has a pivot (too short for its version tag|with version tag 0) on page $root at offset $poff/,
			'c6: amcheck reports a pivot shortened under its tag');
	}
	elsif ($case eq 'other')
	{
		# another nonzero tag, one below: the downlink no longer equals the
		# high key of the page to its child's left
		patch('c5_k', $root, sub {
			my $t = unpack('L', substr(${ $_[0] }, $o + $len - 6 - 4, 4));
			s32($_[0], $o + $len - 6 - 4, $t - 1); });
		($r, $out, $err) =
		  $node->psql('postgres', "SELECT bark_index_parent_check('c5_k')");
		like($err, qr/has a downlink on page $root at offset $poff that is not the high key of the page to its child's left/,
			'c6: the parent check reports a downlink whose tag is not its left page\'s high key\'s');
	}
	else
	{
		# the attribute count (low bits of t_tid offset) to 0
		patch('c5_k', $root, sub {
			s16($_[0], $o + 4, g16($_[0], $o + 4) & ~0x07ff); });
		($e1, $e2) = reports('c5_k', $root);
		like($e1, qr/has a pivot with a version tag but not every key attribute and a heap TID on page $root at offset $poff/,
			'c6: amcheck reports a tagged pivot without every key attribute');
	}
}

# (7) BARK_LOWBOUND_TAGGED (0x100 of bark_flags, 12 bytes into the special
# space) on the leftmost leaf, and cleared on a leaf whose lower bound has
# a tag.
$node->safe_psql('postgres', 'REINDEX INDEX c5_k');
$node->safe_psql(
	'postgres', q{
	SET test_versioned_tableam.stale_tags = live;
	SELECT count(test_versioned_insert_entry('c5_k', ctid, 4000000 + g))
	  FROM c5, generate_series(1, 500) g;
});
my $leftmost = $node->safe_psql('postgres',
	q{SELECT blkno FROM bark_multi_page_stats('c5_k', 1, -1) WHERE type = 'leaf' AND bark_prev = 0});
my $flagged = $node->safe_psql('postgres',
	q{SELECT min(blkno) FROM bark_multi_page_stats('c5_k', 1, -1) WHERE 'lowbound_tagged' = ANY (flags)});
ok($flagged ne '', 'c7: a flagged leaf');
patch('c5_k', $leftmost, sub { my $f = g16($_[0], 16) + 12; s16($_[0], $f, g16($_[0], $f) | 0x100) });
($e1, $e2) = reports('c5_k', $leftmost);
like($e1, qr/has BARK_LOWBOUND_TAGGED on page $leftmost, which is not a leaf with a left sibling/,
	'c7: amcheck reports the flag on the leftmost leaf');
patch('c5_k', $leftmost, sub { my $f = g16($_[0], 16) + 12; s16($_[0], $f, g16($_[0], $f) & ~0x100) });
patch('c5_k', $flagged, sub { my $f = g16($_[0], 16) + 12; s16($_[0], $f, g16($_[0], $f) & ~0x100) });
($e1, $e2) = reports('c5_k', $flagged);
like($e1, qr/has a leaf $flagged whose lower bound, the high key of \d+, has a version tag, but no BARK_LOWBOUND_TAGGED/,
	'c7: amcheck reports a tagged lower bound without the flag');

# (8) A version 2 meta page with BARK_META_VERSIONED.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE c8 (id int, k int) USING heap_versioned;
	INSERT INTO c8 SELECT g, g FROM generate_series(1, 20) g;
	SET test_versioned_tableam.build_tag_percent = 0;
	CREATE INDEX c8_k ON c8 USING bark (k);
});
patch('c8_k', 0, sub { s32($_[0], 24 + 4, 2) });
# The meta page is checked by the page validator after a change to it
# (cassert builds only): VACUUM stores the key count there.
SKIP:
{
	skip 'the page validator exists only in cassert builds', 1
	  if !check_pg_config('#define USE_ASSERT_CHECKING 1');
	($r, $out, $err) =
	  $node->psql('postgres',
		'INSERT INTO c8 VALUES (21, 21); SET bark_verify_pages = on; VACUUM c8');
	like($err, qr/BARK index "c8_k" has a version 2 meta page 0 built for version tags/,
		'c8: the page validator reports a version 2 meta page built for tags');
}

# (9) pg_walinspect's descriptions of the records that carry tags: a split
# and a page deletion with BARK_LOWBOUND_TAGGED (the run's last pages,
# emptied), and an INSERT_SWAP that divides a LIST for a tagged entry.
my $start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$node->safe_psql(
	'postgres', q{
	CREATE TABLE c9 (id int, k int) USING heap_versioned;
	INSERT INTO c9 SELECT g, CASE WHEN g = 1 THEN 5 ELSE 5 + g END FROM generate_series(1, 300) g;
	SET test_versioned_tableam.build_tag_percent = 0;
	CREATE INDEX c9_k ON c9 USING bark (k);
	SET test_versioned_tableam.stale_tags = live;
	SELECT count(test_versioned_insert_entry('c9_k', ctid, 1000000 + g))
	  FROM c9, generate_series(1, 700) g WHERE id = 1;
	CREATE TABLE c9d (id int, k int) USING heap_versioned;
	INSERT INTO c9d SELECT g, 5 FROM generate_series(1, 10) g;
	CREATE INDEX c9d_k ON c9d USING bark (k);
	SELECT test_versioned_insert_entry('c9d_k', ctid, 7) FROM c9d WHERE id = 5;
});
$node->safe_psql(
	'postgres', qq{
	SET test_versioned_tableam.stale_tags = live;
	SET test_versioned_tableam.stale_dead_min = 1000001;
	SET test_versioned_tableam.stale_dead_max = 1000700;
	VACUUM c9;
	CHECKPOINT;
});
my $descs = $node->safe_psql(
	'postgres', q{
	SELECT bool_or(record_type = 'SPLIT' AND description LIKE '%, lower bound tagged: R%') || ' ' ||
	       bool_or(record_type = 'UNLINK_PAGE' AND description LIKE '%, lower bound tagged') || ' ' ||
	       bool_or(record_type = 'INSERT_SWAP' AND description LIKE '%, divide for tag: 7') || ' ' ||
	       bool_or(record_type = 'VACUUM' AND description LIKE '%nuntagged: %')
	  FROM pg_get_wal_records_info('} . $start . q{', pg_current_wal_flush_lsn())
	 WHERE resource_manager = 'Bark'});
is($descs, 'true true true true',
	'walinspect: split, unlink, divide and vacuum records describe their tags')
  or diag($node->safe_psql('postgres', q{
	SELECT record_type || ': ' || max(description) FROM pg_get_wal_records_info('} . $start . q{', pg_current_wal_flush_lsn())
	 WHERE resource_manager = 'Bark' GROUP BY record_type}));

# a clean stop, so that backends exit normally (coverage counts)
$node->stop;
done_testing();
