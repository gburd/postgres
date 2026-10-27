
# Copyright (c) 2026, PostgreSQL Global Development Group

# VACUUM and deletion of BARK's tagged entries on heap_versioned: splits that keep tags out of pivots, the
# BARK_LOWBOUND_TAGGED flag and its propagation by page deletion, untagging
# in place (and what the lower-bound rule prevents), LP_DEAD marks on
# tagged entries, the cleanup of stale tagged entries before a split, and
# an entry kept tagged because its untagged position is in its twin.
# The primary runs with wal_consistency_checking = bark and a standby
# replays everything.
#
# test_versioned_insert_entry(index, tid, tag) adds an entry for the row at
# tid with any tag.  With test_versioned_tableam.stale_tags = live, entries
# whose tag is not the row's xmin are kept by VACUUM (LIVE), and with
# untag_tag only entries of that tag are answered LIVE_UNTAG.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', q{
autovacuum = off
wal_consistency_checking = 'bark'
});
$node->start;
$node->backup('b');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'b', has_streaming => 1);
$standby->start;

my $inj = $ENV{enable_injection_points} eq 'yes';

$node->safe_psql(
	'postgres', q{
	CREATE EXTENSION test_versioned_tableam;
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pageinspect;
	-- each leaf: its block, siblings, flags, live items and its first data
	-- entry as "TID tag"
	CREATE FUNCTION leaves(idx regclass)
	  RETURNS TABLE (blk int, prev int8, next int8, flags text[], n int, first text)
	  LANGUAGE sql AS $$
	  SELECT b::int, s.bark_prev, s.bark_next, s.flags, s.live_items,
	         (SELECT i.htid::text || ' ' || coalesce(i.tag::text, '0')
	            FROM bark_page_items(idx::text, b) i
	           WHERE i.itemoffset = CASE WHEN s.bark_next <> 0 THEN 2 ELSE 1 END)
	    FROM generate_series(1, pg_relation_size(idx) / 8192 - 1) b,
	         LATERAL bark_page_stats(idx::text, b) s
	   WHERE s.type = 'leaf' ORDER BY 1 $$;
	CREATE FUNCTION tagged_pivots(idx regclass) RETURNS int8 LANGUAGE sql AS $$
	  SELECT count(*) FROM generate_series(1, pg_relation_size(idx) / 8192 - 1) b,
	         LATERAL bark_page_items(idx::text, b) i
	   WHERE i.tag IS NOT NULL AND i.shape NOT IN ('SINGLE', 'LIST', 'POSTING', 'OVERSIZED') $$;
	CREATE FUNCTION tagged(idx regclass) RETURNS int8 LANGUAGE sql AS $$
	  SELECT count(*) FROM generate_series(1, pg_relation_size(idx) / 8192 - 1) b,
	         LATERAL bark_page_items(idx::text, b) i
	   WHERE i.tag IS NOT NULL AND i.shape IN ('SINGLE', 'OVERSIZED') $$;
});
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points') if $inj;

sub check_index
{
	my ($idx, $what) = @_;
	is( $node->safe_psql(
			'postgres', "SELECT bark_index_parent_check('$idx', true)"),
		'', "$what: bark_index_parent_check");
	$node->wait_for_catchup($standby);
	is($standby->safe_psql('postgres', "SELECT bark_index_check('$idx')"),
		'', "$what: bark_index_check on the standby");
}

# VACUUM `tab` with heap_versioned's version pass at DEBUG2, after `sets`;
# returns BARK's untagging line for `idx`.
sub vacuum
{
	my ($tab, $idx, $sets) = @_;
	$sets //= '';
	my ($ret, $out, $err) = $node->psql(
		'postgres', qq{
		SET test_versioned_tableam.stale_tags = live; $sets
		SET client_min_messages = debug2;
		VACUUM $tab});
	die "VACUUM $tab failed: $err" if $ret != 0;
	return $err =~ /index "$idx": (untagged \d+ entries in place, kept \d+ tagged whose page's lower bound may carry a tag and \d+ kept \(dup, AM gave LIVE to an older entry\))/
	  ? $1 : '';
}

# A run of `n` versions of row `id`'s entry, tagged base+1 .. base+n.
sub run_of
{
	my ($tab, $idx, $id, $base, $n) = @_;
	$node->safe_psql(
		'postgres', qq{
		SET test_versioned_tableam.stale_tags = live;
		SELECT count(test_versioned_insert_entry('$idx', ctid, $base + g))
		  FROM $tab, generate_series(1, $n) g WHERE id = $id});
}

# (1) A split near a run of one row's versions, among other keys, does not
# cut the run: no pivot gets a tag, and no leaf BARK_LOWBOUND_TAGGED.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE t1 (id int, k int) USING heap_versioned;
	INSERT INTO t1 SELECT g, g FROM generate_series(1, 600) g;
	SET test_versioned_tableam.build_tag_percent = 0;
	CREATE INDEX t1_k ON t1 USING bark (k);
});
my $before = $node->safe_psql('postgres', q{SELECT count(*) FROM leaves('t1_k')});
run_of('t1', 't1_k', 300, 1000000, 120);
cmp_ok($node->safe_psql('postgres', q{SELECT count(*) FROM leaves('t1_k')}),
	'>', $before, 'split near a run: the leaf split');
is($node->safe_psql('postgres', q{SELECT tagged_pivots('t1_k')}),
	'0', 'split near a run: no pivot has a tag');
is( $node->safe_psql(
		'postgres', q{SELECT count(*) FROM leaves('t1_k') WHERE 'lowbound_tagged' = ANY (flags)}),
	'0', 'split near a run: no leaf has BARK_LOWBOUND_TAGGED');
check_index('t1_k', 'split near a run');

# A leaf of one key, whose entries are runs of ten versions of each row's
# entry, splits between two rows: still no tagged pivot.  With the rule off
# (an injection point) the same inserts split inside runs.
for my $rule (1, 0)
{
	next if !$rule && !$inj;
	my $what = $rule ? 'one key, runs of ten' : 'one key, runs of ten, rule off';
	$node->safe_psql(
		'postgres', q{
		DROP TABLE IF EXISTS t1b;
		CREATE TABLE t1b (id int, k int) USING heap_versioned;
		INSERT INTO t1b SELECT g, 5 FROM generate_series(1, 60) g;
		SET test_versioned_tableam.build_tag_percent = 100;
		CREATE INDEX t1b_k ON t1b USING bark (k);
	});
	$node->safe_psql('postgres',
		q{SELECT injection_points_attach('bark-split-ignore-runs', 'notice')})
	  if !$rule;
	$node->safe_psql(
		'postgres', q{
		SET test_versioned_tableam.stale_tags = live;
		SELECT count(test_versioned_insert_entry('t1b_k', ctid, -g))
		  FROM t1b, generate_series(1, 9) g});
	$node->safe_psql('postgres',
		q{SELECT injection_points_detach('bark-split-ignore-runs')})
	  if !$rule;
	cmp_ok($node->safe_psql('postgres', q{SELECT count(*) FROM leaves('t1b_k')}),
		'>', 1, "$what: the leaf split");
	if ($rule)
	{
		is($node->safe_psql('postgres', q{SELECT tagged_pivots('t1b_k')}),
			'0', "$what: no pivot has a tag");
	}
	else
	{
		cmp_ok($node->safe_psql('postgres', q{SELECT tagged_pivots('t1b_k')}),
			'>', 0, "$what: a pivot has a tag");
		is( $node->safe_psql(
				'postgres', q{
				SELECT (SELECT count(*) FROM leaves('t1b_k') WHERE 'lowbound_tagged' = ANY (flags)) =
				       (SELECT count(*) FROM leaves('t1b_k') l, bark_page_items('t1b_k', l.blk) i
				         WHERE l.next <> 0 AND i.itemoffset = 1 AND i.tag IS NOT NULL)}),
			't', "$what: each right page of a tagged leaf high key has BARK_LOWBOUND_TAGGED");
	}
	check_index('t1b_k', $what);
}

# (2) A leaf that is all one run splits inside it: the pivots get tags, and
# each right page BARK_LOWBOUND_TAGGED (amcheck checks that every leaf whose
# lower bound has a tag has the flag).  Row 1 (k = 5) gets 700 versions of
# its entry after its untagged one; rows 2..300 have k = 5 + id.
sub one_run
{
	my ($tab) = @_;
	$node->safe_psql(
		'postgres', qq{
		CREATE TABLE $tab (id int, k int) USING heap_versioned;
		INSERT INTO $tab SELECT g, 5 + g - 1 FROM generate_series(1, 300) g;
		SET test_versioned_tableam.build_tag_percent = 0;
		CREATE INDEX ${tab}_k ON $tab USING bark (k);
	});
	run_of($tab, "${tab}_k", 1, 1000000, 700);
}
one_run('t2');
cmp_ok($node->safe_psql('postgres', q{SELECT tagged_pivots('t2_k')}),
	'>=', 2, 'one run: pivots have tags');
my @flagged = split /\n/,
  $node->safe_psql('postgres',
	q{SELECT blk FROM leaves('t2_k') WHERE 'lowbound_tagged' = ANY (flags) ORDER BY first});
cmp_ok(scalar(@flagged), '>=', 2, 'one run: right pages have BARK_LOWBOUND_TAGGED');
check_index('t2_k', 'one run');

# VACUUM may untag the first entry of a flagged page (the table AM says
# LIVE_UNTAG) but keeps it tagged: (5, (0,1), 0) would sort below the
# page's lower bound.
my $firsttag = $node->safe_psql('postgres',
	qq{SELECT split_part(first, ' ', 2) FROM leaves('t2_k') WHERE blk = $flagged[0]});
is(vacuum('t2', 't2_k', "SET test_versioned_tableam.untag_tag = $firsttag;"),
	"untagged 0 entries in place, kept 1 tagged whose page's lower bound may carry a tag and 0 kept (dup, AM gave LIVE to an older entry)",
	'one run: VACUUM keeps the flagged page\'s first entry tagged');
is($node->safe_psql('postgres', qq{SELECT first FROM leaves('t2_k') WHERE blk = $flagged[0]}),
	"(0,1) $firsttag", 'one run: the entry is still tagged');
check_index('t2_k', 'one run, VACUUM');

# An entry of another row on a flagged page goes in place: an earlier entry
# of the page sorts below its untagged position.  Row 300 moves to k = 5,
# after row 1's run, on the run's last page.
$node->safe_psql('postgres', q{UPDATE t2 SET k = 5 WHERE id = 300});
my $xmin300 = $node->safe_psql('postgres', q{SELECT xmin FROM t2 WHERE id = 300});
my $page300 = $node->safe_psql(
	'postgres', q{
	SELECT b FROM generate_series(1, pg_relation_size('t2_k') / 8192 - 1) b,
	       LATERAL bark_page_items('t2_k', b) i
	 WHERE i.htid = (SELECT ctid FROM t2 WHERE id = 300) AND i.shape = 'SINGLE'});
like($node->safe_psql('postgres', qq{SELECT flags FROM leaves('t2_k') WHERE blk = $page300}),
	qr/lowbound_tagged/, 'one run: row 300\'s new entry is on a flagged page');
is(vacuum('t2', 't2_k', "SET test_versioned_tableam.untag_tag = $xmin300;"),
	"untagged 1 entries in place, kept 0 tagged whose page's lower bound may carry a tag and 0 kept (dup, AM gave LIVE to an older entry)",
	'one run: VACUUM untags it in place');
is( $node->safe_psql(
		'postgres', qq{
		SELECT count(*) FROM bark_page_items('t2_k', $page300) i
		 WHERE i.htid = (SELECT ctid FROM t2 WHERE id = 300) AND i.tag IS NULL}),
	'1', 'one run: row 300\'s entry is untagged, on the same page');
check_index('t2_k', 'one run, untag in place');

# What the rule prevents (R3.2): with it off, VACUUM untags the flagged
# page's first entry in place, and the page then holds an entry below its
# lower bound.
SKIP:
{
	skip 'injection points not supported by this build', 2 if !$inj;
	$node->safe_psql('postgres',
		q{SELECT injection_points_attach('bark-untag-ignore-bound', 'notice')});
	is(vacuum('t2', 't2_k', "SET test_versioned_tableam.untag_tag = $firsttag;"),
		"untagged 1 entries in place, kept 0 tagged whose page's lower bound may carry a tag and 0 kept (dup, AM gave LIVE to an older entry)",
		'rule off: VACUUM untags the entry');
	$node->safe_psql('postgres',
		q{SELECT injection_points_detach('bark-untag-ignore-bound')});
	my ($ret, $out, $err) = $node->psql('postgres',
		q{SELECT bark_index_parent_check('t2_k')});
	like($err, qr/has a first key on page \d+ that is less than the high key of its left sibling/,
		'rule off: amcheck finds the entry below the page\'s lower bound');
	$node->safe_psql('postgres', 'REINDEX INDEX t2_k');
}
check_index('t2_k', 'after REINDEX');

# (3) Untagging in place on a right page whose lower bound is untagged: a
# build that tags every row, then VACUUM, which untags every entry, the
# first entries of right pages included.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE t3 (id int, k int) USING heap_versioned;
	INSERT INTO t3 SELECT g, g FROM generate_series(1, 2000) g;
	SET test_versioned_tableam.build_tag_percent = 100;
	CREATE INDEX t3_k ON t3 USING bark (k);
});
is($node->safe_psql('postgres', q{SELECT tagged('t3_k')}), '2000', 'untag: every entry tagged');
cmp_ok($node->safe_psql('postgres', q{SELECT count(*) FROM leaves('t3_k') WHERE prev <> 0}),
	'>=', 3, 'untag: right pages, none flagged') ;
is(vacuum('t3', 't3_k'),
	"untagged 2000 entries in place, kept 0 tagged whose page's lower bound may carry a tag and 0 kept (dup, AM gave LIVE to an older entry)",
	'untag: VACUUM untags every entry in place');
is($node->safe_psql('postgres', q{SELECT tagged('t3_k')}), '0', 'untag: no entry is tagged');
is( $node->safe_psql(
		'postgres', q{
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		SELECT count(*), sum(id) FROM t3 WHERE k > 0}),
	'2000|2001000', 'untag: index scan');
check_index('t3_k', 'untag');

# (4) Page deletion: the flagged last page of a run, once empty, is deleted,
# and its right sibling takes over its lower bound and its flag.
one_run('t4');
my ($last, $right) = split /\|/,
  $node->safe_psql('postgres',
	q{SELECT blk, next FROM leaves('t4_k') WHERE 'lowbound_tagged' = ANY (flags) ORDER BY first DESC LIMIT 1});
unlike($node->safe_psql('postgres', qq{SELECT flags FROM leaves('t4_k') WHERE blk = $right}),
	qr/lowbound_tagged/, 'deletion: the right sibling has no flag yet');
my $lo = $node->safe_psql('postgres',
	qq{SELECT min(tag) FROM bark_page_items('t4_k', $last) WHERE shape = 'SINGLE'});
my $hi = $node->safe_psql('postgres',
	qq{SELECT max(tag) FROM bark_page_items('t4_k', $last) WHERE shape = 'SINGLE'});
vacuum('t4', 't4_k',
	"SET test_versioned_tableam.stale_dead_min = $lo; SET test_versioned_tableam.stale_dead_max = $hi;");
is( $node->safe_psql(
		'postgres', qq{SELECT type FROM bark_page_stats('t4_k', $last)}),
	'deleted', 'deletion: the emptied page is deleted');
like($node->safe_psql('postgres', qq{SELECT flags FROM leaves('t4_k') WHERE blk = $right}),
	qr/lowbound_tagged/, 'deletion: its right sibling has BARK_LOWBOUND_TAGGED');
check_index('t4_k', 'deletion');

# (5) LP_DEAD on tagged entries: a scan that finds a row dead marks each
# of its tagged entries it returned, and no other entry.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE t5 (id int, k int) USING heap_versioned;
	INSERT INTO t5 SELECT g, g FROM generate_series(1, 100) g;
	SET test_versioned_tableam.build_tag_percent = 100;
	CREATE INDEX t5_k ON t5 USING bark (k);
});
run_of('t5', 't5_k', 50, 1000000, 1);
$node->safe_psql('postgres', q{DELETE FROM t5 WHERE id = 50});
my $dead = q{SELECT dead_items FROM bark_page_stats('t5_k', 1)};
is($node->safe_psql('postgres', $dead), '0', 'LP_DEAD: none yet');
$node->safe_psql('postgres',
	q{SET enable_seqscan = off; SET enable_bitmapscan = off; SELECT * FROM t5 WHERE k = 50});
is($node->safe_psql('postgres', $dead), '2', 'LP_DEAD: both tagged entries of the row are marked');
# simple deletion then removes both before the page would split: insert
# until it has split, however many entries a page holds on this platform
$node->safe_psql(
	'postgres', q{
	DO $$
	DECLARE g int := 100;
	BEGIN
	  WHILE pg_relation_size('t5_k') / 8192 < 3 AND g < 5000 LOOP
	    INSERT INTO t5 SELECT n, n FROM generate_series(g + 1, g + 10) n;
	    g := g + 10;
	  END LOOP;
	END $$});
is( $node->safe_psql(
		'postgres', q{
		SELECT count(*) FROM generate_series(1, pg_relation_size('t5_k') / 8192 - 1) b,
		       LATERAL bark_page_items('t5_k', b) i
		 WHERE i.htid = '(0,50)' AND i.shape = 'SINGLE'}),
	'0', 'LP_DEAD: simple deletion removed them');
check_index('t5_k', 'LP_DEAD');

# (6) Before a leaf splits, its stale tagged entries go: a page full of
# entries of live rows tagged with versions valid for none (the table AM's
# index_delete_tuples says so), and inserts that would split it.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE t6 (id int, k int) USING heap_versioned;
	INSERT INTO t6 SELECT g, g FROM generate_series(1, 100) g;
	SET test_versioned_tableam.build_tag_percent = 0;
	CREATE INDEX t6_k ON t6 USING bark (k);
	SET test_versioned_tableam.stale_tags = live;
	SELECT count(test_versioned_insert_entry('t6_k', ctid, -1)) FROM t6;
	SELECT count(test_versioned_insert_entry('t6_k', ctid, -2)) FROM t6 WHERE id <= 60;
});
is($node->safe_psql('postgres', q{SELECT count(*) FROM leaves('t6_k')}),
	'1', 'pre-split: one leaf');
is($node->safe_psql('postgres', q{SELECT tagged('t6_k')}), '160', 'pre-split: 160 stale entries');
# Inserts fill the leaf until it would split (the number depends on the
# platform's entry size), which deletes the stale entries instead.
$node->safe_psql(
	'postgres', q{
	SELECT test_versioned_reset();
	DO $$
	DECLARE g int := 100;
	BEGIN
	  WHILE tagged('t6_k') > 0 AND g < 5000 LOOP
	    INSERT INTO t6 SELECT n, n FROM generate_series(g + 1, g + 10) n;
	    g := g + 10;
	  END LOOP;
	END $$;
});
is($node->safe_psql('postgres', q{SELECT count(*) FROM leaves('t6_k')}),
	'1', 'pre-split: still one leaf');
is($node->safe_psql('postgres', q{SELECT tagged('t6_k')}), '0', 'pre-split: the stale entries are gone');
cmp_ok($node->safe_psql('postgres',
		q{SELECT n FROM test_versioned_counts() WHERE counter = 'delete_stale'}),
	'>=', 160, 'pre-split: index_delete_tuples judged them by tag');
is( $node->safe_psql(
		'postgres', q{
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		SELECT count(*), sum(id) FROM t6 WHERE k > 0}),
	$node->safe_psql('postgres', q{SELECT count(*), sum(id) FROM t6 WHERE k > 0}),
	'pre-split: index scan');
check_index('t6_k', 'pre-split');

# (7) An entry whose untagged position (K, L, 0) is in another entry: the
# row's own untagged entry, its twin, which is a SINGLE or the last member
# of a LIST or POSTING (an entry of its key never holds L strictly inside
# its range: a tagged insert divides it at L first).  The table AM answers
# LIVE_UNTAG for (K, L, t) and LIVE for the twin, which under the
# executor's insert rule cannot both be right: t's update changed the
# index's key.  VACUUM keeps the tag, which is right whichever is wrong.
# Scans run as if the twin's LIVE was the wrong one (invalid_untagged
# rejects it), so the tagged entry is the row's only valid one; an
# injection point that deletes the tagged entry instead shows the row lost.
# Each table: rows with keys from `keys`, then (k, L, xmin) for row `id`.
sub twin_case
{
	my ($tab, $opts, $rows, $keys, $id) = @_;
	$node->safe_psql(
		'postgres', qq{
		CREATE TABLE $tab (id int, k int, pad text) USING heap_versioned $opts;
		INSERT INTO $tab SELECT g, $keys, $rows FROM generate_series(1, 400) g WHERE $keys IS NOT NULL;
		SET test_versioned_tableam.build_tag_percent = 0;
		CREATE INDEX ${tab}_k ON $tab USING bark (k);
		SELECT test_versioned_insert_entry('${tab}_k', ctid, xmin::text::int8)
		  FROM $tab WHERE id = $id;
	});
	return $node->safe_psql('postgres', "SELECT ctid FROM $tab WHERE id = $id");
}
$node->safe_psql(
	'postgres', q{
	-- the entry before the tagged entry of `l`, as "shape count holds-l",
	-- a LIST as plain or coded (an int4 key's plain LIST is 16 bytes, 12
	-- where MAXALIGN is 4, and 6 a locator), and the tagged entry's tag
	CREATE FUNCTION twin(idx regclass, l tid) RETURNS text LANGUAGE sql AS $$
	  WITH e AS (
	    SELECT b, i.itemoffset AS off, i.shape, i.ntids, i.itemlen, i.htid, i.tag,
	           coalesce(i.tids[i.ntids], i.htid) AS last
	      FROM generate_series(1, pg_relation_size(idx) / 8192 - 1) b,
	           LATERAL bark_page_stats(idx::text, b) s,
	           LATERAL bark_page_items(idx::text, b) i
	     WHERE s.type = 'leaf' AND i.shape IN ('SINGLE', 'LIST', 'POSTING'))
	  SELECT coalesce(max(p.shape ||
	           CASE WHEN p.shape <> 'LIST' OR p.ntids = 4 THEN ''
	                WHEN p.itemlen = (SELECT ((8 + 4 + max_data_alignment - 1) / max_data_alignment) * max_data_alignment FROM pg_control_init()) + 6 * p.ntids
	                THEN ' plain' ELSE ' coded' END ||
	           ' ' || p.ntids || ' ' || (p.last = l) || ' tag ' || t.tag), 'no tagged entry')
	    FROM e t JOIN e p ON p.b = t.b AND p.off = t.off - 1
	   WHERE t.htid = l AND t.tag IS NOT NULL $$;
});
my $xminq = q{SELECT xmin FROM %s WHERE id = %d};
my %twins = (
	# (a) the row's own SINGLE: distinct keys
	td_a => [ '', 'NULL', 'CASE WHEN g <= 20 THEN 10 * g END', 7 ],
	# a LIST of rows on four heap pages: plain where MAXALIGN is 8, coded
	# where it is 4 (MAXALIGN rounds the coded form below the plain one), so
	# its form is not shown
	td_l => [ 'WITH (fillfactor = 10)', "repeat('x', 700)",
		'CASE WHEN g <= 4 THEN 5 END', 4 ],
	# a coded LIST: twenty consecutive rows
	td_c => [ '', 'NULL', 'CASE WHEN g <= 20 THEN 5 END', 20 ],
	# a POSTING: four hundred
	td_p => [ '', 'NULL', '5', 400 ]);
my %twinwant = (
	td_a => 'SINGLE 1 true',
	td_l => 'LIST 4 true',
	td_c => 'LIST coded 20 true',
	td_p => 'POSTING 400 true');
my %l;
for my $tab (sort keys %twins)
{
	my ($opts, $rows, $keys, $id) = @{ $twins{$tab} };
	$l{$tab} = twin_case($tab, $opts, $rows, $keys, $id);
	my $xmin = $node->safe_psql('postgres', sprintf($xminq, $tab, $id));
	is($node->safe_psql('postgres', "SELECT twin('${tab}_k', '$l{$tab}')"),
		"$twinwant{$tab} tag $xmin", "twin $tab: the tagged entry follows its twin");
	is(vacuum($tab, "${tab}_k"),
		"untagged 0 entries in place, kept 0 tagged whose page's lower bound may carry a tag and 1 kept (dup, AM gave LIVE to an older entry)",
		"twin $tab: VACUUM keeps the tag");
	is($node->safe_psql('postgres', "SELECT twin('${tab}_k', '$l{$tab}')"),
		"$twinwant{$tab} tag $xmin", "twin $tab: the entry is still tagged");
	# A range qual, so that ORDER BY k DESC makes a backward scan.
	my $k = $node->safe_psql('postgres', "SELECT k FROM $tab WHERE id = $id");
	my $q = "FROM $tab WHERE k >= $k AND k <= $k";
	my $want = $node->safe_psql('postgres',
		"SET enable_indexscan = off; SET enable_bitmapscan = off; SELECT count(*), sum(id) $q");
	for my $scan (
		[ 'index', 'enable_bitmapscan = off; SET enable_indexonlyscan = off', "id $q ORDER BY k" ],
		[ 'backward index', 'enable_bitmapscan = off; SET enable_indexonlyscan = off', "id $q ORDER BY k DESC" ],
		[ 'index-only', 'enable_bitmapscan = off', "$id AS id $q ORDER BY k" ],
		[ 'bitmap', 'enable_indexscan = off; SET enable_indexonlyscan = off', "id $q" ])
	{
		my ($what, $sets, $sel) = @$scan;
		my $wantscan = $what eq 'index-only'
		  ? $node->safe_psql('postgres',
			"SET enable_indexscan = off; SET enable_bitmapscan = off; SELECT count(*), count(*) * $id $q")
		  : $want;
		is( $node->safe_psql(
				'postgres', qq{
				SET test_versioned_tableam.invalid_untagged = '$l{$tab}';
				SET enable_seqscan = off; SET $sets;
				SELECT count(*), sum(id) FROM (SELECT $sel) s}),
			$wantscan, "twin $tab: $what scan, the twin rejected, matches seqscan");
	}
	check_index("${tab}_k", "twin $tab");
}

# L inside a LIST's range without being a member: the tagged insert divided
# the LIST at L, so VACUUM untags it in place, between the parts.  Rows 1,
# 2, 5, 6 have k = 7, rows 3 and 4 k = 8, two to a heap page; the update
# keeps row 3 on its page (keep_locator = pretend), so its tagged entry
# (7, (1,3), xmin) falls between (0,2) and (2,1).
$node->safe_psql(
	'postgres', q{
	CREATE TABLE td_n (id int, k int, pad text) USING heap_versioned
	  WITH (fillfactor = 50);
	CREATE INDEX td_n_k ON td_n USING bark (k);
	INSERT INTO td_n SELECT g, CASE WHEN g IN (3, 4) THEN 8 ELSE 7 END,
	  repeat('x', 1800) FROM generate_series(1, 6) g;
	SET test_versioned_tableam.keep_locator = pretend;
	UPDATE td_n SET k = 7 WHERE id = 3;
});
my $nentries = q{
	SELECT string_agg(i.shape || ' ' || coalesce(i.tids::text, i.htid::text) ||
	                  CASE WHEN i.tag IS NULL THEN '' ELSE ' tagged' END, ' / ' ORDER BY i.itemoffset)
	  FROM bark_page_items('td_n_k', 1) i WHERE i.shape <> 'PIVOT'};
like($node->safe_psql('postgres', $nentries),
	qr{^LIST \{"\(0,1\)","\(0,2\)"\} / SINGLE \(1,3\) tagged / LIST \{"\(2,1\)","\(2,2\)"\} / },
	'not a member: the LIST is divided around the tagged entry');
is(vacuum('td_n', 'td_n_k'),
	"untagged 1 entries in place, kept 0 tagged whose page's lower bound may carry a tag and 0 kept (dup, AM gave LIVE to an older entry)",
	'not a member: VACUUM untags it in place');
like($node->safe_psql('postgres', $nentries),
	qr{^LIST \{"\(0,1\)","\(0,2\)"\} / SINGLE \(1,3\) / LIST \{"\(2,1\)","\(2,2\)"\} / },
	'not a member: the entry is untagged between the parts');
is( $node->safe_psql(
		'postgres', q{
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		SELECT string_agg(id::text, ' ' ORDER BY id) FROM td_n WHERE k = 7}),
	'1 2 3 5 6', 'not a member: index scan');
check_index('td_n_k', 'not a member');

# What keeping protects: VACUUM deletes the tagged entry instead (an
# injection point), and a scan that rejects the twin misses the row.
# amcheck's heapallindexed cannot tell: the twin still indexes (K, L).
SKIP:
{
	skip 'injection points not supported by this build', 4 if !$inj;
	$node->safe_psql('postgres',
		q{SELECT injection_points_attach('bark-untag-delete', 'notice')});
	vacuum('td_a', 'td_a_k');
	$node->safe_psql('postgres',
		q{SELECT injection_points_detach('bark-untag-delete')});
	is($node->safe_psql('postgres', "SELECT twin('td_a_k', '$l{td_a}')"),
		'no tagged entry', 'twin deleted: the tagged entry is gone');
	is( $node->safe_psql(
			'postgres', qq{
			SET test_versioned_tableam.invalid_untagged = '$l{td_a}';
			SET enable_seqscan = off; SET enable_bitmapscan = off;
			SELECT count(*) FROM td_a WHERE k = 70}),
		'0', 'twin deleted: an index scan misses the row');
	is($node->safe_psql('postgres', q{SELECT count(*) FROM td_a WHERE k = 70}),
		'1', 'twin deleted: a seqscan finds it');
	is($node->safe_psql('postgres', q{SELECT bark_index_parent_check('td_a_k', true)}),
		'', 'twin deleted: heapallindexed does not see it');
}

# Crash recovery replays all of it; the standby checked every record.
my $twinsig = q{SELECT md5(string_agg(c.relname || ' ' || b || ' ' || i.itemoffset || ' ' || i.shape || ' ' ||
  coalesce(i.htid::text, '') || ' ' || coalesce(i.tag::text, '') || ' ' || i.data, ',' ORDER BY c.relname, b, i.itemoffset))
  FROM pg_class c, generate_series(1, pg_relation_size(c.oid) / 8192 - 1) b, LATERAL bark_page_items(c.relname, b) i
  WHERE c.relname IN ('td_a_k', 'td_l_k', 'td_c_k', 'td_p_k', 'td_n_k')
    AND (SELECT type FROM bark_page_stats(c.relname, b)) = 'leaf'};
my $twinbefore = $node->safe_psql('postgres', $twinsig);
$node->wait_for_catchup($standby);
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', $twinsig), $twinbefore, 'twin: crash recovery replays the entries');
check_index("${_}_k", "twin $_ after crash") for qw(td_a td_l td_c td_p td_n);

$node->wait_for_catchup($standby);
unlike(slurp_file($standby->logfile), qr/inconsistent page found/,
	'no inconsistent page in replay');

# a clean stop, so that backends exit normally (coverage counts)
$standby->stop;
$node->stop;
done_testing();
