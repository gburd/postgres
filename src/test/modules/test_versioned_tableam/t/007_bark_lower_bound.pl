
# Copyright (c) 2026, PostgreSQL Global Development Group

# Lowering a BARK leaf's tagged lower bound (bark_lower_bound), so that
# VACUUM can untag the first entry of a page whose lower bound, the left
# sibling's high key, is a tagged (K, L, t0): the high key and the parent's
# downlink become (K, L) with no tag, the page loses BARK_LOWBOUND_TAGGED,
# and the entry is untagged in place.  Also the flag-only form (page
# deletion gave the page an untagged lower bound), the cases that decline
# (the left page still holds an entry of (K, L), a busy lock, a page that
# is its parent's first child), crash recovery, the WAL record's
# description, and an injection point that lowers the parent's downlink
# only, which amcheck must catch.  wal_consistency_checking = bark, and a
# standby replays everything.
#
# A run of row L's entries, tags 2, 4, .., 1200 (below the row's xmin, so the
# row's own entry, tagged with its xmin, sorts after them), fills whole
# leaves, which split inside the run.  test_versioned_tableam.stale_dead_min
# and _max make a range of tags dead, and untag_tag answers LIVE_UNTAG for
# one tag.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', q{
autovacuum = off
wal_consistency_checking = 'bark'
});
$node->start;
$node->safe_psql('postgres', q{SELECT pg_create_physical_replication_slot('s')});
$node->backup('b');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'b', has_streaming => 1);
$standby->append_conf('postgresql.conf', "primary_slot_name = 's'\n");
$standby->start;

$node->safe_psql(
	'postgres', q{
	CREATE EXTENSION test_versioned_tableam;
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pageinspect;
	CREATE EXTENSION pg_walinspect;
	CREATE EXTENSION injection_points;
	-- each live leaf: block, siblings, flags, its high key's tag, and the
	-- tag of its first entry and the least and greatest
	CREATE FUNCTION leaves(idx regclass)
	  RETURNS TABLE (blk int, prev int8, next int8, flags text[], hitag int8,
	                 firsttag int8, mintag int8, maxtag int8, n int8)
	  LANGUAGE sql AS $$
	  SELECT b::int, s.bark_prev, s.bark_next, s.flags,
	         (SELECT i.tag FROM bark_page_items(idx::text, b) i
	           WHERE s.bark_next <> 0 AND i.itemoffset = 1),
	         (SELECT i.tag FROM bark_page_items(idx::text, b) i
	           WHERE i.shape IN ('SINGLE', 'OVERSIZED') AND i.itemoffset > (s.bark_next <> 0)::int
	           ORDER BY i.itemoffset LIMIT 1),
	         (SELECT min(i.tag) FROM bark_page_items(idx::text, b) i
	           WHERE i.shape IN ('SINGLE', 'OVERSIZED') AND i.itemoffset > (s.bark_next <> 0)::int),
	         (SELECT max(i.tag) FROM bark_page_items(idx::text, b) i
	           WHERE i.shape IN ('SINGLE', 'OVERSIZED') AND i.itemoffset > (s.bark_next <> 0)::int),
	         (SELECT count(*) FROM bark_page_items(idx::text, b) i
	           WHERE i.shape IN ('SINGLE', 'OVERSIZED') AND i.itemoffset > (s.bark_next <> 0)::int)
	    FROM generate_series(1, pg_relation_size(idx) / 8192 - 1) b,
	         LATERAL bark_page_stats(idx::text, b) s
	   WHERE s.type = 'leaf' $$;
	-- the tag of the downlink to `child` (NULL for none), and whether
	-- there is one
	CREATE FUNCTION downlink(idx regclass, child int, OUT tag int8, OUT found bool)
	  LANGUAGE sql AS $$
	  SELECT max(i.tag), count(*) = 1
	    FROM generate_series(1, pg_relation_size(idx) / 8192 - 1) b,
	         LATERAL bark_page_stats(idx::text, b) s,
	         LATERAL bark_page_items(idx::text, b) i
	   WHERE s.bark_level > 0 AND s.type IN ('internal', 'root')
	     AND i.downlink = child AND NOT (s.bark_next <> 0 AND i.itemoffset = 1) $$;
	CREATE FUNCTION tagged(idx regclass) RETURNS int8 LANGUAGE sql AS $$
	  SELECT count(*) FROM generate_series(1, pg_relation_size(idx) / 8192 - 1) b,
	         LATERAL bark_page_items(idx::text, b) i
	   WHERE i.tag IS NOT NULL AND i.shape IN ('SINGLE', 'OVERSIZED') $$;
	-- xmins above the run's tags
	DO $$ BEGIN FOR i IN 1..1300 LOOP PERFORM pg_current_xact_id(); COMMIT; END LOOP; END $$;
});

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

# VACUUM `tab` after `sets`, at DEBUG2; returns BARK's untagging line for
# `idx` and the reasons it gave for keeping lower bounds.
sub vacuum
{
	my ($tab, $idx, $sets) = @_;
	my ($ret, $out, $err) = $node->psql(
		'postgres', qq{
		SET test_versioned_tableam.stale_tags = live; $sets
		SET client_min_messages = debug2;
		VACUUM $tab});
	die "VACUUM $tab failed: $err" if $ret != 0;
	return vacuum_line($idx, $err);
}

sub vacuum_line
{
	my ($idx, $err) = @_;
	my $line =
	  $err =~ /index "$idx": untagged (\d+) entries in place, kept (\d+) tagged whose page's lower bound may carry a tag and (\d+) whose untagged position is in another entry, after lowering (\d+) lower bounds/
	  ? "untagged $1, kept $2, dup $3, lowered $4"
	  : '';
	my @why = $err =~ /index "$idx": kept the tagged lower bound of block \d+: ([^\n]*)/g;
	return ($line, join('; ', @why));
}

# A table whose row `id` (key `k`) has a run of 600 entries tagged 2, 4,
# .., 1200 before its own, tagged with its xmin.  The other rows have
# k = 10 * id.
sub make_run
{
	my ($tab, $id, $k) = @_;
	$node->safe_psql(
		'postgres', qq{
		CREATE TABLE $tab (id int, k int) USING heap_versioned;
		INSERT INTO $tab SELECT g, CASE WHEN g = $id THEN $k ELSE 10 * g END
		  FROM generate_series(1, 300) g;
		SET test_versioned_tableam.build_tag_percent = 100;
		CREATE INDEX ${tab}_k ON $tab USING bark (k);
		SET test_versioned_tableam.stale_tags = live;
		SELECT count(test_versioned_insert_entry('${tab}_k', ctid, 2 * g))
		  FROM $tab, generate_series(1, 600) g WHERE id = $id;
	});
}

# The flagged leaf (P2) whose left sibling (P1) is `p1`, and P1's tags.
sub pages
{
	my ($idx, $where) = @_;
	my $row = $node->safe_psql(
		'postgres', qq{
		SELECT p2.blk, p1.blk, p1.mintag, p1.maxtag, p2.firsttag, p1.hitag
		  FROM leaves('$idx') p2 JOIN leaves('$idx') p1 ON p1.blk = p2.prev
		 WHERE 'lowbound_tagged' = ANY (p2.flags) AND $where
		 ORDER BY p2.firsttag LIMIT 1});
	return split /\|/, $row;
}

# Entries of key 5 an index scan returns, forward and backward.
sub scan_count
{
	my ($tab, $dir) = @_;
	return $node->safe_psql(
		'postgres', qq{
		SET test_versioned_tableam.stale_tags = live;
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		SELECT count(*) FROM (SELECT k FROM $tab WHERE k = 5 ORDER BY k $dir) s});
}

# (1) Decline: P1 still holds a live entry of (K, L).  P1 is the leftmost
# leaf, which page deletion never takes, so it stays even when empty.
make_run('tl', 1, 5);
my ($p2, $p1, $lo, $hi, $first, $t0) = pages('tl_k', 'p1.prev = 0');
ok(defined $t0 && $t0 > 0, "run: P1's high key has a tag ($t0)");
is_deeply(
	[ vacuum('tl', 'tl_k',
		"SET test_versioned_tableam.stale_dead_min = $lo; SET test_versioned_tableam.stale_dead_max = "
		  . ($hi - 2) . "; SET test_versioned_tableam.untag_tag = $first;") ],
	[ 'untagged 0, kept 1, dup 0, lowered 0',
	  'its left sibling holds an entry at or above the new bound' ],
	'P1 holds a live entry of (K, L): the bound is kept');
is($node->safe_psql('postgres', qq{SELECT firsttag FROM leaves('tl_k') WHERE blk = $p2}),
	$first, 'P1 holds an entry: P2\'s first entry is still tagged');
like($node->safe_psql('postgres', qq{SELECT flags FROM leaves('tl_k') WHERE blk = $p2}),
	qr/lowbound_tagged/, 'P1 holds an entry: P2 keeps BARK_LOWBOUND_TAGGED');
check_index('tl_k', 'P1 holds an entry');

# (2) The lowering: P1's last entry dies too, and VACUUM lowers P1's high
# key and the downlink to P2, clears P2's flag and untags its first entry.
my $nfwd = scan_count('tl', 'ASC');
my $lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');
is_deeply(
	[ vacuum('tl', 'tl_k',
		"SET test_versioned_tableam.stale_dead_min = $lo; SET test_versioned_tableam.stale_dead_max = $hi; SET test_versioned_tableam.untag_tag = $first;") ],
	[ 'untagged 1, kept 0, dup 0, lowered 1', '' ],
	'lowered: VACUUM lowers the bound and untags the entry');
is( $node->safe_psql(
		'postgres', qq{
		SELECT l1.hitag IS NULL, (downlink('tl_k', $p2)).tag IS NULL, (downlink('tl_k', $p2)).found,
		       'lowbound_tagged' <> ALL (l2.flags), l2.firsttag IS NULL, l1.n
		  FROM leaves('tl_k') l1, leaves('tl_k') l2 WHERE l1.blk = $p1 AND l2.blk = $p2}),
	't|t|t|t|t|0',
	'lowered: P1\'s high key and P2\'s downlink untagged, P2\'s flag clear, its first entry untagged');
is( $node->safe_psql(
		'postgres', qq{
		CHECKPOINT;
		SELECT string_agg(record_type || ': ' || description, ', ')
		  FROM pg_get_wal_records_info('$lsn', pg_current_wal_flush_lsn())
		 WHERE resource_manager = 'Bark' AND description ~ 'lower separator'}),
	"UNLINK_PAGE: lower separator, left: $p1, right: $p2, parent: "
	  . $node->safe_psql('postgres',
		qq{SELECT b FROM generate_series(1, pg_relation_size('tl_k') / 8192 - 1) b,
		          LATERAL bark_page_items('tl_k', b) i, LATERAL bark_page_stats('tl_k', b) s
		    WHERE s.bark_level = 1 AND i.downlink = $p2})
	  . ", poffset: "
	  . $node->safe_psql('postgres',
		qq{SELECT i.itemoffset FROM generate_series(1, pg_relation_size('tl_k') / 8192 - 1) b,
		          LATERAL bark_page_items('tl_k', b) i, LATERAL bark_page_stats('tl_k', b) s
		    WHERE s.bark_level = 1 AND i.downlink = $p2}),
	'lowered: one UNLINK_PAGE record, described as a lowered separator');
is(scan_count('tl', 'ASC'), $nfwd - 1, 'lowered: a forward scan returns each entry once');
is(scan_count('tl', 'DESC'), $nfwd - 1, 'lowered: a backward scan returns each entry once');
check_index('tl_k', 'lowered');

# Crash recovery replays the lowering and the untagging.
my $sig = q{SELECT md5(string_agg(b || ' ' || i.itemoffset || ' ' || i.shape || ' ' ||
  coalesce(i.htid::text, '') || ' ' || coalesce(i.tag::text, '') || ' ' || i.data, ',' ORDER BY b, i.itemoffset))
  FROM generate_series(1, pg_relation_size('tl_k') / 8192 - 1) b, LATERAL bark_page_items('tl_k', b) i};
my $sigbefore = $node->safe_psql('postgres', $sig);
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', $sig), $sigbefore, 'crash: the lowering is replayed');
check_index('tl_k', 'crash');

# (3) The flag only: in the middle of the key space, P1 is an interior
# leaf, so once empty it is deleted, and P2 takes over its lower bound, the
# untagged high key of the leaf before it, keeping its own flag, which
# VACUUM then clears.
make_run('tm', 150, 1495);
($p2, $p1, $lo, $hi, $first, $t0) = pages('tm_k', 'p1.mintag = 2');
ok(defined $p2, 'flag only: P1 starts the run, P2 is flagged');
$lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');
is_deeply(
	[ vacuum('tm', 'tm_k',
		"SET test_versioned_tableam.stale_dead_min = $lo; SET test_versioned_tableam.stale_dead_max = $hi; SET test_versioned_tableam.untag_tag = $first;") ],
	[ 'untagged 1, kept 0, dup 0, lowered 1', '' ],
	'flag only: VACUUM clears the flag and untags the entry');
is($node->safe_psql('postgres', qq{SELECT type FROM bark_page_stats('tm_k', $p1)}),
	'deleted', 'flag only: P1 was deleted');
is( $node->safe_psql(
		'postgres', qq{SELECT 'lowbound_tagged' <> ALL (flags), firsttag IS NULL FROM leaves('tm_k') WHERE blk = $p2}),
	't|t', 'flag only: P2\'s flag is clear, its first entry untagged');
like( $node->safe_psql(
		'postgres', qq{
		CHECKPOINT;
		SELECT string_agg(description, ', ')
		  FROM pg_get_wal_records_info('$lsn', pg_current_wal_flush_lsn())
		 WHERE resource_manager = 'Bark' AND description ~ 'lower separator'}),
	qr/^lower separator, left: \d+, right: $p2, flag only$/,
	'flag only: described as such');
check_index('tm_k', 'flag only');

# (4) Decline: a busy lock.  VACUUM stops before it locks the parent, an
# insert splits P2 and stops holding it, and VACUUM, finding P2 locked,
# keeps the bound; the next VACUUM lowers it.
make_run('tb', 1, 5);
($p2, $p1, $lo, $hi, $first, $t0) = pages('tb_k', 'p1.prev = 0');
$node->safe_psql('postgres',
	q{SELECT injection_points_attach('bark-lower-bound-start', 'wait')});
my $vac = $node->background_psql('postgres', on_error_stop => 0);
my $p2max;
$vac->query_until(qr/start/, qq{
	SET test_versioned_tableam.stale_tags = live;
	SET test_versioned_tableam.stale_dead_min = $lo;
	SET test_versioned_tableam.stale_dead_max = $hi;
	SET test_versioned_tableam.untag_tag = $first;
	SET client_min_messages = debug2;
	\\echo start
	VACUUM tb;
});
$node->wait_for_event('client backend', 'bark-lower-bound-start');
$node->safe_psql('postgres',
	q{SELECT injection_points_attach('bark-leave-leaf-split-incomplete', 'wait')});
my $ins = $node->background_psql('postgres', on_error_stop => 0);
$p2max = $node->safe_psql('postgres', qq{SELECT maxtag FROM leaves('tb_k') WHERE blk = $p2});
$ins->query_until(qr/start/, qq{
	SET test_versioned_tableam.stale_tags = live;
	\\echo start
	SELECT count(test_versioned_insert_entry('tb_k', ctid, g))
	  FROM tb, generate_series($first + 1, $p2max, 2) g WHERE id = 1;
});
$node->wait_for_event('client backend', 'bark-leave-leaf-split-incomplete');
$node->safe_psql('postgres', q{
	SELECT injection_points_detach('bark-lower-bound-start');
	SELECT injection_points_wakeup('bark-lower-bound-start')});
$vac->query('SELECT 1');
is_deeply([ vacuum_line('tb_k', $vac->{stderr}) ],
	[ 'untagged 0, kept 1, dup 0, lowered 0', 'a lock was busy' ],
	'busy: VACUUM keeps the bound while P2 is locked');
$vac->quit;
$node->safe_psql('postgres', q{
	SELECT injection_points_detach('bark-leave-leaf-split-incomplete');
	SELECT injection_points_wakeup('bark-leave-leaf-split-incomplete')});
$ins->query('SELECT 1');
$ins->quit;
check_index('tb_k', 'busy');
is_deeply(
	[ vacuum('tb', 'tb_k',
		"SET test_versioned_tableam.stale_dead_min = $lo; SET test_versioned_tableam.stale_dead_max = $hi; SET test_versioned_tableam.untag_tag = $first;") ],
	[ 'untagged 1, kept 0, dup 0, lowered 1', '' ],
	'busy: the next VACUUM lowers the bound');
check_index('tb_k', 'busy, then lowered');

# Run VACUUM `tab` in the background, stopped at bark-lower-bound-start,
# after its scan, holding no lock; `between` runs while it waits, then it
# carries on.  Returns its untagging line and reasons.  (Once VACUUM holds
# the parent, which every descent reads, no other backend can reach P1 or
# P2 that has not already.)
sub vacuum_around
{
	my ($tab, $idx, $sets, $between) = @_;
	$node->safe_psql('postgres',
		q{SELECT injection_points_attach('bark-lower-bound-start', 'wait')});
	my $bg = $node->background_psql('postgres', on_error_stop => 0);
	$bg->query_until(qr/start/, qq{
		SET test_versioned_tableam.stale_tags = live; $sets
		SET client_min_messages = debug2;
		\\echo start
		VACUUM $tab;
	});
	$node->wait_for_event('client backend', 'bark-lower-bound-start');
	$between->();
	$node->safe_psql('postgres', q{
		SELECT injection_points_detach('bark-lower-bound-start');
		SELECT injection_points_wakeup('bark-lower-bound-start')});
	$bg->query('SELECT 1');
	my @line = vacuum_line($idx, $bg->{stderr});
	$bg->quit;
	return @line;
}

# Entries of row 1 tagged 1 .. t0 - 1 go on P1, emptied by VACUUM's scan,
# and split it (it holds about 217).
sub split_p1
{
	my ($tab, $t0) = @_;
	return qq{
		SET test_versioned_tableam.stale_tags = live;
		SELECT count(test_versioned_insert_entry('${tab}_k', ctid, g))
		  FROM $tab, generate_series(1, $t0 - 1) g WHERE id = 1;
	};
}

# (4b) Decline: P1 splits before VACUUM locks the parent, and the split
# fails halfway (an error at bark-leave-leaf-split-incomplete), leaving P1
# BARK_INCOMPLETE_SPLIT with a right link to the new page, which has no
# downlink yet.  So the parent's downlinks still match (P1's, then P2's),
# and VACUUM finds the split only under P1's lock.
make_run('ts', 1, 5);
($p2, $p1, $lo, $hi, $first, $t0) = pages('ts_k', 'p1.prev = 0');
is_deeply(
	[ vacuum_around('ts', 'ts_k',
		"SET test_versioned_tableam.stale_dead_min = $lo; SET test_versioned_tableam.stale_dead_max = $hi; SET test_versioned_tableam.untag_tag = $first;",
		sub {
			$node->safe_psql('postgres',
				q{SELECT injection_points_attach('bark-leave-leaf-split-incomplete', 'error')});
			my ($ret, $out, $err) = $node->psql('postgres', split_p1('ts', $t0));
			like($err, qr/bark-leave-leaf-split-incomplete/, 'P1 splits: the split stops halfway');
			$node->safe_psql('postgres',
				q{SELECT injection_points_detach('bark-leave-leaf-split-incomplete')});
		}) ],
	[ 'untagged 0, kept 1, dup 0, lowered 0', 'its left sibling split' ],
	'P1 splits: VACUUM keeps the bound');
like($node->safe_psql('postgres', qq{SELECT flags FROM leaves('ts_k') WHERE blk = $p1}),
	qr/incomplete_split/, 'P1 splits: P1 is the left half of an incomplete split');
# an insert into P1's range, untagged, finishes the split
$node->safe_psql('postgres', q{
	SET test_versioned_tableam.stale_tags = live;
	SELECT test_versioned_insert_entry('ts_k', '(0,1)', 0)});
unlike($node->safe_psql('postgres', qq{SELECT flags FROM leaves('ts_k') WHERE blk = $p1}),
	qr/incomplete_split/, 'P1 splits: an insert finished the split');
check_index('ts_k', 'P1 splits');

# (4c) Decline: P1 splits, completely, before VACUUM locks the parent: P2's
# left sibling is then the split's right half, whose downlink is just
# before P2's, so the parent's downlinks do not match the left sibling
# VACUUM read.
make_run('tc', 1, 5);
($p2, $p1, $lo, $hi, $first, $t0) = pages('tc_k', 'p1.prev = 0');
is_deeply(
	[ vacuum_around('tc', 'tc_k',
		"SET test_versioned_tableam.stale_dead_min = $lo; SET test_versioned_tableam.stale_dead_max = $hi; SET test_versioned_tableam.untag_tag = $first;",
		sub { $node->safe_psql('postgres', split_p1('tc', $t0)); }) ],
	[ 'untagged 0, kept 1, dup 0, lowered 0', "its parent's downlinks do not match its left sibling" ],
	'P1 split: VACUUM keeps the bound');
isnt($node->safe_psql('postgres', qq{SELECT prev FROM leaves('tc_k') WHERE blk = $p2}),
	$p1, 'P1 split: P2 has a new left sibling');
check_index('tc_k', 'P1 split');

# (4d) Decline: an empty P1 whose own lower bound is in the run.  The run
# spans the leftmost leaf Pa, then P1 and P2, both flagged.  VACUUM
# empties P1 (every tag on it dead) but cannot delete it: an insert into
# P2 stopped mid-split (odd tags after P2's first, below the row's own
# entry, which is on P2 here) holds P2 when the deletion tries its lock.  The
# insert goes on once VACUUM waits for P2 in bark_lower_bound, which then
# finds P1's own downlink, Pa's high key, tagged in the run, which (K, L)
# would not sort above.  INDEX_CLEANUP off leaves heap_versioned's version
# pass the only scan of the index, which stops after its last block
# (bark-bulkdelete-after-page) for the insert.
make_run('te', 1, 5);
($p2, $p1, $lo, $hi, $first, $t0) = pages('te_k',
	q{p1.prev <> 0 AND (SELECT l.prev FROM leaves('te_k') l WHERE l.blk = p1.prev) = 0});
like($node->safe_psql('postgres', qq{SELECT flags FROM leaves('te_k') WHERE blk = $p1}),
	qr/lowbound_tagged/, 'empty P1: P1\'s own bound is tagged too');
my $last = $node->safe_psql('postgres', q{SELECT pg_relation_size('te_k') / 8192 - 1});
$node->safe_psql('postgres',
	qq{SELECT injection_points_attach('bark-bulkdelete-after-page', 'wait', '$last')});
$vac = $node->background_psql('postgres', on_error_stop => 0);
$vac->query_until(qr/start/, qq{
	SET test_versioned_tableam.stale_tags = live;
	SET test_versioned_tableam.stale_dead_min = $lo;
	SET test_versioned_tableam.stale_dead_max = $hi;
	SET test_versioned_tableam.untag_tag = $first;
	SET client_min_messages = debug2;
	\\echo start
	VACUUM (INDEX_CLEANUP OFF) te;
});
$node->wait_for_event('client backend', 'bark-bulkdelete-after-page');
$node->safe_psql('postgres',
	q{SELECT injection_points_attach('bark-leave-leaf-split-incomplete', 'wait')});
$ins = $node->background_psql('postgres', on_error_stop => 0);
$ins->query_until(qr/start/, qq{
	SET test_versioned_tableam.stale_tags = live;
	\\echo start
	SELECT count(test_versioned_insert_entry('te_k', ctid, g))
	  FROM te, generate_series($first + 1, $first + 401, 2) g WHERE id = 1;
});
$node->wait_for_event('client backend', 'bark-leave-leaf-split-incomplete');
$node->safe_psql('postgres', q{
	SELECT injection_points_detach('bark-bulkdelete-after-page');
	SELECT injection_points_wakeup('bark-bulkdelete-after-page')});
$node->poll_query_until('postgres', q{
	SELECT wait_event_type = 'Buffer' FROM pg_stat_activity WHERE query LIKE 'VACUUM (INDEX_CLEANUP OFF) te%'})
  or die 'VACUUM did not wait for P2';
$node->safe_psql('postgres', q{
	SELECT injection_points_detach('bark-leave-leaf-split-incomplete');
	SELECT injection_points_wakeup('bark-leave-leaf-split-incomplete')});
$ins->query('SELECT 1');
$ins->quit;
$vac->query('SELECT 1');
is_deeply([ vacuum_line('te_k', $vac->{stderr}) ],
	[ 'untagged 0, kept 1, dup 0, lowered 0',
	  "its left sibling's own lower bound is not below the new bound" ],
	'empty P1: VACUUM keeps the bound');
$vac->quit;
is($node->safe_psql('postgres', qq{SELECT n FROM leaves('te_k') WHERE blk = $p1}),
	'0', 'empty P1: P1 is empty and still linked');
check_index('te_k', 'empty P1');

# (4e) Decline: an OVERSIZED bound.  A run of one row's entries whose key
# is too long for a pivot inline leaves OVERSIZED tagged high keys.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE to1 (id int, k text) USING heap_versioned;
	ALTER TABLE to1 ALTER COLUMN k SET STORAGE EXTERNAL;
	INSERT INTO to1 SELECT g, CASE WHEN g = 1 THEN 'a' ELSE 'b' || g END ||
	  (SELECT string_agg(md5(g::text || i::text), '') FROM generate_series(1, 120) i)
	  FROM generate_series(1, 3) g;
	SET test_versioned_tableam.build_tag_percent = 100;
	CREATE INDEX to1_k ON to1 USING bark (k);
	SET test_versioned_tableam.stale_tags = live;
	SELECT count(test_versioned_insert_entry('to1_k', ctid, 2 * g))
	  FROM to1, generate_series(1, 600) g WHERE id = 1;
});
my ($op2, $op1, $olo, $ohi, $ofirst) = pages('to1_k', 'p1.prev = 0');
is( $node->safe_psql(
		'postgres', qq{SELECT shape FROM bark_page_items('to1_k', $op1) WHERE itemoffset = 1}),
	'OVERSIZED', 'OVERSIZED bound: P1\'s high key is OVERSIZED');
is_deeply(
	[ vacuum('to1', 'to1_k',
		"SET test_versioned_tableam.stale_dead_min = $olo; SET test_versioned_tableam.stale_dead_max = $ohi; SET test_versioned_tableam.untag_tag = $ofirst;") ],
	[ 'untagged 0, kept 1, dup 0, lowered 0', 'its lower bound is OVERSIZED' ],
	'OVERSIZED bound: VACUUM keeps it');
check_index('to1_k', 'OVERSIZED bound');

# (4f) An unlogged index: the lowering takes a fake LSN instead of a record.
$node->safe_psql(
	'postgres', q{
	CREATE UNLOGGED TABLE tu (id int, k int) USING heap_versioned;
	INSERT INTO tu SELECT g, CASE WHEN g = 1 THEN 5 ELSE 10 * g END FROM generate_series(1, 300) g;
	SET test_versioned_tableam.build_tag_percent = 100;
	CREATE INDEX tu_k ON tu USING bark (k);
	SET test_versioned_tableam.stale_tags = live;
	SELECT count(test_versioned_insert_entry('tu_k', ctid, 2 * g))
	  FROM tu, generate_series(1, 600) g WHERE id = 1;
});
($p2, $p1, $lo, $hi, $first, $t0) = pages('tu_k', 'p1.prev = 0');
is_deeply(
	[ vacuum('tu', 'tu_k',
		"SET test_versioned_tableam.stale_dead_min = $lo; SET test_versioned_tableam.stale_dead_max = $hi; SET test_versioned_tableam.untag_tag = $first;") ],
	[ 'untagged 1, kept 0, dup 0, lowered 1', '' ],
	'unlogged: VACUUM lowers the bound');
is($node->safe_psql('postgres', qq{SELECT bark_index_parent_check('tu_k', true)}),
	'', 'unlogged: bark_index_parent_check');

# (5) Decline: P2 is its parent's first child, so the bound is also the
# parent's lower bound, the high key of the parent's left sibling.  Long
# keys give a tree with several level-1 pages over one run.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tp (id int, k text) USING heap_versioned;
	INSERT INTO tp SELECT g, (SELECT string_agg(md5((CASE WHEN g = 1 THEN 0 ELSE g END * 100 + i)::text), '')
	                           FROM generate_series(1, 30) i)
	  FROM generate_series(1, 20) g;
	SET test_versioned_tableam.build_tag_percent = 100;
	CREATE INDEX tp_k ON tp USING bark (k);
	SET test_versioned_tableam.stale_tags = live;
	SELECT count(test_versioned_insert_entry('tp_k', ctid, 2 * g))
	  FROM tp, generate_series(1, 200) g WHERE id = 1;
});
my $firstchild = q{
	SELECT i.downlink FROM generate_series(1, pg_relation_size('tp_k') / 8192 - 1) b,
	       LATERAL bark_page_stats('tp_k', b) s, LATERAL bark_page_items('tp_k', b) i
	 WHERE s.bark_level = 1 AND s.bark_prev <> 0
	   AND i.itemoffset = CASE WHEN s.bark_next <> 0 THEN 2 ELSE 1 END};
($p2, $p1, $lo, $hi, $first, $t0) = pages('tp_k', "p2.blk IN ($firstchild) AND p1.mintag IS NOT NULL");
ok(defined $p2, 'first child: a flagged leaf that is its parent\'s first child');
is_deeply(
	[ vacuum('tp', 'tp_k',
		"SET test_versioned_tableam.stale_dead_min = $lo; SET test_versioned_tableam.stale_dead_max = $hi; SET test_versioned_tableam.untag_tag = $first;") ],
	[ 'untagged 0, kept 1, dup 0, lowered 0', "it is its parent's first child" ],
	'first child: VACUUM keeps the bound');
is($node->safe_psql('postgres', qq{SELECT firsttag FROM leaves('tp_k') WHERE blk = $p2}),
	$first, 'first child: P2\'s first entry is still tagged');
check_index('tp_k', 'first child');
is_deeply(
	[ vacuum('tp', 'tp_k', "SET test_versioned_tableam.untag_tag = $first;") ],
	[ 'untagged 0, kept 1, dup 0, lowered 0', "it is its parent's first child" ],
	'first child: a later VACUUM keeps it too');

# (6) The negative control: lowering the parent's downlink alone, leaving
# P1's high key and P2 as they were, makes the downlink unequal to P1's
# high key, which only the parent check sees.
make_run('tn', 1, 5);
($p2, $p1, $lo, $hi, $first, $t0) = pages('tn_k', 'p1.prev = 0');
$node->safe_psql('postgres',
	q{SELECT injection_points_attach('bark-lower-bound-skip-hikey', 'notice')});
is_deeply(
	[ vacuum('tn', 'tn_k',
		"SET test_versioned_tableam.stale_dead_min = $lo; SET test_versioned_tableam.stale_dead_max = $hi; SET test_versioned_tableam.untag_tag = $first;") ],
	[ 'untagged 0, kept 1, dup 0, lowered 0', "a test lowered its parent's downlink alone" ],
	'parent only: VACUUM lowers the downlink alone');
$node->safe_psql('postgres',
	q{SELECT injection_points_detach('bark-lower-bound-skip-hikey')});
is( $node->safe_psql(
		'postgres', qq{
		SELECT l1.hitag = $t0, (downlink('tn_k', $p2)).tag IS NULL
		  FROM leaves('tn_k') l1 WHERE l1.blk = $p1}),
	't|t', 'parent only: P1\'s high key keeps its tag, the downlink has none');
is($node->safe_psql('postgres', q{SELECT bark_index_check('tn_k', true)}),
	'', 'parent only: bark_index_check, which does not compare levels, passes');
my ($ret, $out, $err) = $node->psql('postgres', q{SELECT bark_index_parent_check('tn_k')});
like($err, qr/has a downlink on page \d+ at offset \d+ that is not the high key of the page to its child's left/,
	'parent only: bark_index_parent_check finds the downlink unequal to P1\'s high key');
$node->safe_psql('postgres', 'REINDEX INDEX tn_k');
check_index('tn_k', 'parent only, after REINDEX');

$node->wait_for_catchup($standby);
unlike(slurp_file($standby->logfile), qr/inconsistent page found/,
	'no inconsistent page in replay');

$standby->stop;
$node->stop;
done_testing();
