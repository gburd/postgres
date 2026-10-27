
# Copyright (c) 2026, PostgreSQL Global Development Group

# Versioned multikey BARK indexes: BARK_META_MKTAGS and its build rule,
# tagged key entries, untagged markers, and the checks of the meta page,
# with the conversion bits BARK_META_CONVERTING and BARK_META_SETTLED.
# A multikey index built on a versioned table that is neither GIN-interface,
# unique nor exclusion gets BARK_META_MKTAGS (4); any other gets none, and
# keeps moving a row whose column an update changes.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(allows_streaming => 1, no_data_checksums => 1);
$node->append_conf(
	'postgresql.conf', q{
autovacuum = off
});
$node->start;

$node->safe_psql(
	'postgres', q{
	CREATE EXTENSION test_versioned_tableam;
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pageinspect;
	CREATE EXTENSION bark_multikey;
	-- every leaf entry of a BARK index, in index order
	CREATE FUNCTION bark_entries(idx regclass)
	  RETURNS TABLE (blk int, off int, shape text, htid tid, tids tid[], tag int8)
	  LANGUAGE sql AS $$
	  SELECT b::int, i.itemoffset, i.shape, i.htid, i.tids, i.tag
	    FROM generate_series(1, pg_relation_size(idx) / current_setting('block_size')::int - 1) b,
	         LATERAL bark_page_items(idx::text, b) i
	   WHERE (SELECT type FROM bark_page_stats(idx::text, b)) = 'leaf'
	     AND i.shape IN ('SINGLE', 'LIST', 'POSTING', 'OVERSIZED')
	     AND NOT (i.itemoffset = 1 AND (SELECT bark_next FROM bark_page_stats(idx::text, b)) <> 0)
	   ORDER BY 1, 2 $$;
});

# bark_flags & (VERSIONED | MKTAGS) of an index's meta page
sub mkflags
{
	my ($idx) = @_;
	return $node->safe_psql('postgres',
		"SELECT flags & 6 FROM bark_metap('$idx')");
}

# can_version is the AM's; whether this index can is what heap_versioned's
# update sees, so it is shown by the update itself below.

# --- 4a: the build rule ---

$node->safe_psql(
	'postgres', q{
	CREATE TABLE tm (id int, s int, a int4[]) USING heap_versioned;
	INSERT INTO tm SELECT g, g, ARRAY[g, g + 1] FROM generate_series(1, 50) g;
	CREATE INDEX tm_a ON tm USING bark (a bark_int4_array_ops);
	CREATE INDEX tm_s ON tm USING bark (s);
	CREATE INDEX tm_g ON tm USING bark (a bark_gin_int4_array_ops);
	CREATE TABLE tu (id int, a int4[]) USING heap_versioned;
	INSERT INTO tu SELECT g, ARRAY[2 * g, 2 * g + 1] FROM generate_series(1, 50) g;
	CREATE UNIQUE INDEX tm_u ON tu USING bark (a bark_int4_array_ops);
	CREATE INDEX tm_m ON tm USING bark (a bark_int4_array_marked_ops);
	CREATE TABLE tmx (id int, a int4[],
	  CONSTRAINT tmx_a EXCLUDE USING bark (a bark_int4_array_ops WITH &&))
	  USING heap_versioned;
	CREATE TABLE th (id int, a int4[]);
	CREATE INDEX th_a ON th USING bark (a bark_int4_array_ops);
});
is(mkflags('tm_a'), '6', 'plain multikey on heap_versioned: VERSIONED and MKTAGS');
is(mkflags('tm_m'), '6', 'multikey with markers: VERSIONED and MKTAGS');
is(mkflags('tm_s'), '2', 'scalar: VERSIONED only');
is(mkflags('tm_g'), '2', 'GIN-interface: VERSIONED only');
is(mkflags('tm_u'), '2', 'unique multikey: VERSIONED only');
is(mkflags('tmx_a'), '2', 'exclusion multikey: VERSIONED only');
is(mkflags('th_a'), '0', 'multikey on heap: neither');

# The build tags every row of heap_versioned (each row's xmin), and the
# multikey index's key entries carry it; markers stay untagged.
is( $node->safe_psql(
		'postgres', q{
		SELECT count(*) FILTER (WHERE tag IS NOT NULL) || ' ' || count(*)
		  FROM bark_entries('tm_a')}),
	'100 100', 'MKTAGS build: every key entry is tagged');
is( $node->safe_psql(
		'postgres', q{
		SELECT count(*) FROM bark_entries('tm_g') WHERE tag IS NOT NULL}),
	'0', 'GIN-interface build: no entry is tagged');
is( $node->safe_psql(
		'postgres', q{
		SELECT count(*) FROM bark_multikey_entries('tm_m') WHERE marker}),
	'1', 'marked: one marker');
is( $node->safe_psql(
		'postgres', q{
		SELECT count(*) FILTER (WHERE tag IS NOT NULL) || ' ' || count(*)
		  FROM bark_entries('tm_m')}),
	'100 101', 'marked: key entries tagged, the marker not');

# REINDEX, and the rebuilds that clear ii_Unique, recompute MKTAGS from the
# catalog.
$node->safe_psql('postgres', 'REINDEX INDEX tm_a; REINDEX INDEX tm_u');
is(mkflags('tm_a') . ' ' . mkflags('tm_u'), '6 2', 'REINDEX recomputes MKTAGS');
$node->safe_psql('postgres', 'VACUUM FULL tm; VACUUM FULL tu');
is(mkflags('tm_a') . ' ' . mkflags('tm_u'), '6 2',
	'VACUUM FULL: MKTAGS from the catalog, not ii_Unique');

# Inserts and an update that changes only the multikey column tag the key
# entries, and the update keeps the locator (TU_Affected): with MKTAGS the
# index can version.  Markers stay untagged.
$node->safe_psql(
	'postgres', q{
	DROP INDEX tm_g;
	INSERT INTO tm VALUES (51, 51, '{51,52,53}');
});
my $kept = q{SELECT n FROM test_versioned_counts() WHERE counter = 'update_kept'};
is( $node->safe_psql(
		'postgres', qq{
		DO \$\$BEGIN PERFORM test_versioned_reset(); END\$\$;
		SET test_versioned_tableam.keep_locator = pretend;
		UPDATE tm SET a = a || 1000 WHERE id = 2;
		$kept}),
	'1', 'update of the multikey column only: the locator is kept');
is( $node->safe_psql(
		'postgres', q{
		SELECT count(*) || ' ' || count(*) FILTER (WHERE tag IS NOT NULL)
		  FROM bark_entries('tm_m') WHERE htid >= '(4294967295,0)'}),
	'1 0', 'after an insert and an update the marker is still untagged');
for my $idx (qw(tm_a tm_m tm_s))
{
	is($node->safe_psql('postgres', "SELECT bark_index_check('$idx')"),
		'', "$idx checks");
}

# --- 4a: the meta page checks ---

# Set bark_flags of `idx`'s meta page to `flags`, with the server stopped.
sub set_meta_flags
{
	my ($idx, $flags) = @_;
	my $path = $node->data_dir . '/'
	  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('$idx')");
	$node->stop;
	open(my $fh, '+<:raw', $path) or die "open $path: $!";
	seek($fh, 24 + 20, 0) or die "seek: $!";
	print $fh pack('L', $flags);
	close $fh;
	$node->start;
}

sub amcheck_err
{
	my ($idx) = @_;
	my ($ret, $out, $err) =
	  $node->psql('postgres', "SELECT bark_index_check('$idx')");
	return $err;
}

$node->safe_psql(
	'postgres', q{
	CREATE INDEX th_a2 ON th USING bark (a bark_int4_array_ops);
	CREATE INDEX tm_s2 ON tm USING bark (s);
	CREATE INDEX tm_g2 ON tm USING bark (a bark_gin_int4_array_ops);
});
set_meta_flags('th_a2', 4);
like(amcheck_err('th_a2'),
	qr/with version tags on its keys but not built for version tags/,
	'amcheck: MKTAGS without VERSIONED');
set_meta_flags('tm_s2', 6);
like(amcheck_err('tm_s2'),
	qr/with version tags on its keys but no extracted column/,
	'amcheck: MKTAGS on a scalar index');
set_meta_flags('tm_g2', 6);
like(amcheck_err('tm_g2'),
	qr/with version tags on its keys but answers scans through GIN's interface/,
	'amcheck: MKTAGS on a GIN-interface index');

# "Conversion" in the README: a conversion runs only on an index with
# MKTAGS (8 = CONVERTING) and settles only after it began (16 = SETTLED).
$node->safe_psql(
	'postgres', q{
	CREATE INDEX tm_c1 ON tm USING bark (a bark_int4_array_ops);
	CREATE INDEX tm_c2 ON tm USING bark (a bark_int4_array_ops);
	CREATE INDEX tm_c3 ON tm USING bark (a bark_int4_array_ops);
});
set_meta_flags('tm_c1', 2 | 8);
like(amcheck_err('tm_c1'),
	qr/has a meta page 0 converting an index without version tags on its keys/,
	'amcheck: CONVERTING without MKTAGS');
set_meta_flags('tm_c2', 2 | 4 | 16);
like(amcheck_err('tm_c2'),
	qr/has a meta page 0 with a settled conversion that has not begun/,
	'amcheck: SETTLED without CONVERTING');
set_meta_flags('tm_c3', 2 | 4 | 8 | 16);
is(amcheck_err('tm_c3'), '', 'amcheck: a settled conversion is sound');

# A tagged key entry in an index whose meta page claims no MKTAGS is
# corruption (bark_verify_tag follows canversion).
set_meta_flags('tm_a', 2);
like(amcheck_err('tm_a'),
	qr/has a tagged entry on page \d+ at offset \d+ but does not store tags/,
	'amcheck: a tagged key entry without MKTAGS');

SKIP:
{
	skip 'injection points not supported by this build', 6
	  if ($ENV{enable_injection_points} // '') ne 'yes';
	$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');

	# A tagged marker: the insert path is made to tag the marker of a new
	# key set.  amcheck reports it (with the validator off, which would
	# report it first).
	$node->safe_psql(
		'postgres', q{
		CREATE TABLE tq (id int, a int4[]) USING heap_versioned;
		CREATE INDEX tq_m ON tq USING bark (a bark_int4_array_marked_ops);
		SET bark_verify_pages = off;
		SELECT injection_points_attach('bark-insert-tag-markers', 'notice');
		INSERT INTO tq VALUES (1, '{1,2}');
		SELECT injection_points_detach('bark-insert-tag-markers');
	});
	like(amcheck_err('tq_m'), qr/has a tagged marker on page \d+ at offset \d+/,
		'amcheck: a tagged marker');

	# The page validator (cassert): with the fault planted in a meta page
	# a change writes (VACUUM storing a new key count, bark_set_nkeys), the
	# change reports MKTAGS without VERSIONED, and on a versioned table
	# MKTAGS on a scalar index.
	skip 'the page validator exists only in cassert builds', 9
	  unless check_pg_config('#define USE_ASSERT_CHECKING 1');
	my ($ret, $out, $err) = $node->psql(
		'postgres', q{
		CREATE TABLE tq2 (a int4[]);
		CREATE INDEX tq2_a ON tq2 USING bark (a bark_int4_array_ops);
		INSERT INTO tq2 SELECT ARRAY[g, g + 1] FROM generate_series(1, 100) g;
		SELECT injection_points_attach('bark-verify-fault-mktags', 'notice');
		VACUUM tq2;});
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('bark-verify-fault-mktags')");
	like($err,
		qr/ERROR:  BARK index "tq2_a" has a meta page 0 with version tags on its keys but not built for version tags/,
		'validator: MKTAGS without VERSIONED');
	($ret, $out, $err) = $node->psql(
		'postgres', q{
		CREATE TABLE tq3 (k int) USING heap_versioned;
		CREATE INDEX tq3_k ON tq3 USING bark (k);
		INSERT INTO tq3 SELECT g FROM generate_series(1, 100) g;
		SELECT injection_points_attach('bark-verify-fault-mktags', 'notice');
		VACUUM tq3;});
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('bark-verify-fault-mktags')");
	like($err,
		qr/ERROR:  BARK index "tq3_k" has a meta page 0 with version tags on its keys but no extracted column/,
		'validator: MKTAGS on a scalar index');
	# The conversion bits, planted in the meta page VACUUM writes,
	# each in an index of its own (a planted fault stays in the page)
	foreach my $c (
		[ 'converting', 'converting an index without version tags on its keys',
		  'CONVERTING without MKTAGS' ],
		[ 'settled', 'with a settled conversion that has not begun',
		  'SETTLED without CONVERTING' ])
	{
		my ($point, $msg, $what) = @$c;
		($ret, $out, $err) = $node->psql(
			'postgres', qq{
			CREATE TABLE tv_$point (a int4[]);
			CREATE INDEX tv_${point}_a ON tv_$point USING bark (a bark_int4_array_ops);
			INSERT INTO tv_$point SELECT ARRAY[g, g + 1] FROM generate_series(1, 100) g;
			SELECT injection_points_attach('bark-verify-fault-$point', 'notice');
			VACUUM tv_$point;});
		$node->safe_psql('postgres',
			"SELECT injection_points_detach('bark-verify-fault-$point')");
		like($err, qr/ERROR:  BARK index "tv_${point}_a" has a meta page 0 $msg/,
			"validator: $what");
	}

	# Redo has no relation, so it checks only what the page shows: MKTAGS
	# without VERSIONED.  The standby plants the fault as it replays the
	# meta page of the NEWROOT record of the root's first split; the first
	# insert's CREATE_ROOT logged the meta page whole, which redo restores
	# without replaying.
	$node->backup('b');
	my $standby = PostgreSQL::Test::Cluster->new('standby');
	$standby->init_from_backup($node, 'b', has_streaming => 1);
	$standby->start;
	$node->safe_psql(
		'postgres', q{
		CREATE TABLE tq4 (a int4[]);
		CREATE INDEX tq4_a ON tq4 USING bark (a bark_int4_array_ops);
		INSERT INTO tq4 VALUES ('{1,2}');
	});
	$node->wait_for_replay_catchup($standby);
	$standby->safe_psql('postgres',
		"SELECT injection_points_attach('bark-verify-fault-mktags', 'notice')");
	my $off = -s $standby->logfile;
	$node->safe_psql('postgres',
		'INSERT INTO tq4 SELECT ARRAY[g, g + 1] FROM generate_series(1, 2000) g');
	ok( $standby->wait_for_log(
			qr/PANIC:  BARK index "\d+" has a meta page 0 with version tags on its keys but not built for version tags/,
			$off),
		'redo: the standby PANICs on MKTAGS without VERSIONED');
	$standby->stop('immediate', fail_ok => 1);

	# The same in redo: a standby that replays a meta page with SETTLED and no
	# CONVERTING, or CONVERTING and no MKTAGS, PANICs.  One standby each,
	# with the fault planted as above, in the meta page of a root's first
	# split.
	foreach my $c (
		[ 'settled', 'with a settled conversion that has not begun' ],
		[ 'converting', 'converting an index without version tags on its keys' ])
	{
		my ($point, $msg) = @$c;
		$node->backup("b_$point");
		my $sb = PostgreSQL::Test::Cluster->new("standby_$point");
		$sb->init_from_backup($node, "b_$point", has_streaming => 1);
		$sb->start;
		$node->safe_psql('postgres', qq{
			CREATE TABLE tq_$point (a int4[]);
			CREATE INDEX tq_${point}_a ON tq_$point USING bark (a bark_int4_array_ops);
			INSERT INTO tq_$point VALUES ('{1,2}');
		});
		$node->wait_for_replay_catchup($sb);
		$sb->safe_psql('postgres',
			"SELECT injection_points_attach('bark-verify-fault-$point', 'notice')");
		my $soff = -s $sb->logfile;
		$node->safe_psql('postgres',
			"INSERT INTO tq_$point SELECT ARRAY[g, g + 1] FROM generate_series(1, 2000) g");
		ok($sb->wait_for_log(qr/PANIC:  BARK index "\d+" has a meta page 0 $msg/, $soff),
			"redo: the standby PANICs on $point");
		$sb->stop('immediate', fail_ok => 1);
	}
	is($node->safe_psql('postgres', "SELECT bark_index_check('tq4_a')"),
		'', 'redo: the primary meta page is sound');
	pass('validator checks done');
}

$node->stop;
done_testing();
