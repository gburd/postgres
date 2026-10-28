
# Copyright (c) 2026, PostgreSQL Global Development Group

# bark_reclaim_orphans frees the overflow pages of a BARK index that no
# entry references, and nothing else.
#
# 1. A crash between writing an oversized key's chain and placing its entry
#    (bark-overflow-chain-written) orphans the chain.  The reclaim frees
#    exactly its pages, bark_index_parent_check then reports no orphan, and
#    a standby that replays the frees under wal_consistency_checking finds
#    every page consistent and answers as the primary does.  A later VACUUM
#    makes the pages reusable, and oversized inserts reuse them.
# 2. A long chain whose insert stops after the first of its records
#    (bark-overflow-chain-partial, with an error) leaves an orphan whose
#    last page links to a block no record wrote.  That block, a deleted page
#    the free space map handed out, is reused by a live chain.  The reclaim
#    frees the orphan's pages and not the live page its dangling link names,
#    and every row still reads back as a sequential scan reads it.
# 3. When the unwritten blocks of such a chain came from extending the
#    index, they are new pages, which the reclaim leaves alone.
# 4. The reclaim is refused on the standby.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
wal_consistency_checking = 'Bark'
wal_keep_size = 1GB
]);
$node->start;

$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE EXTENSION injection_points;
CREATE EXTENSION pageinspect;
CREATE FUNCTION big(s int, n int) RETURNS text LANGUAGE sql IMMUTABLE AS
$$ SELECT substr(string_agg(md5(s::text || ':' || g::text), ''), 1, n)
   FROM generate_series(1, (n + 31) / 32) g $$;
CREATE FUNCTION pages(idx text, t text) RETURNS SETOF int8 LANGUAGE sql AS
$$ SELECT blkno FROM bark_multi_page_stats(idx, 1, -1)
   WHERE type = t ORDER BY 1 $$;
-- The md5 of every row's key, read by an index-only scan and by a
-- sequential scan, as two sorted lists.
CREATE FUNCTION scans(t regclass) RETURNS text LANGUAGE plpgsql AS $$
DECLARE a text; b text;
BEGIN
  SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off;
  EXECUTE format('SELECT string_agg(md5(k), '','' ORDER BY k) FROM %s WHERE k > ''''', t) INTO a;
  SET LOCAL enable_seqscan = on; SET LOCAL enable_indexscan = off;
  SET LOCAL enable_indexonlyscan = off;
  EXECUTE format('SELECT string_agg(md5(k), '','' ORDER BY k) FROM %s', t) INTO b;
  RETURN CASE WHEN a = b THEN 'same' ELSE 'differ' END;
END $$;
]);

$node->backup('backup');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'backup', has_streaming => 1);
$standby->start;

# The WARNINGs bark_index_parent_check gives for $idx.
sub orphan_warnings
{
	my ($idx) = @_;
	my ($rc, $out, $err) = $node->psql('postgres',
		"SELECT bark_index_parent_check('$idx')");
	is($rc, 0, "the parent check passes $idx");
	return $err =~ /WARNING:  (.*orphaned.*)$/mg;
}

## 1: a crash orphans a chain, and the reclaim frees it
$node->safe_psql(
	'postgres', q[
CREATE TABLE crash_t (id int, k text);
CREATE INDEX crash_idx ON crash_t USING bark (k);
INSERT INTO crash_t SELECT g, big(g, 30000) FROM generate_series(1, 3) g;
]);
my %before = map { $_ => 1 } split /\n/,
  $node->safe_psql('postgres', "SELECT pages('crash_idx', 'overflow')");

my $s1 = $node->background_psql('postgres', on_error_stop => 0);
$s1->query_safe(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('bark-overflow-chain-written', 'wait');
]);
$s1->query_until(
	qr/starting_insert/, q[
\echo starting_insert
INSERT INTO crash_t VALUES (100, big(100, 30000));
]);
$node->wait_for_event('client backend', 'bark-overflow-chain-written');
# Another commit flushes the chain's WAL; then kill the inserter.
$node->safe_psql('postgres', 'CREATE TABLE flush_wal ()');
my $pid = $node->safe_psql('postgres',
	"SELECT pid FROM pg_stat_activity WHERE wait_event = 'bark-overflow-chain-written'");
is(PostgreSQL::Test::Utils::system_log('pg_ctl', 'kill', 'KILL', $pid),
	0, 'killed the inserting backend');
$s1->{run}->finish;
$node->stop('immediate', fail_ok => 1);
$node->start;

my @orphans = grep { !$before{$_} } split /\n/,
  $node->safe_psql('postgres', "SELECT pages('crash_idx', 'overflow')");
cmp_ok(scalar @orphans, '>=', 4, 'the crash left an orphaned chain');
my @w = orphan_warnings('crash_idx');
is(scalar @w, 1, 'the parent check reports one orphaned chain')
  or diag(join("\n", @w));

is($node->safe_psql('postgres', "SELECT bark_reclaim_orphans('crash_idx')"),
	scalar @orphans, 'the reclaim frees the orphan\'s pages');
is( $node->safe_psql(
		'postgres', "SELECT string_agg(blkno::text, ',' ORDER BY blkno)
		 FROM pages('crash_idx', 'deleted') blkno"),
	join(',', @orphans), 'exactly those pages are now deleted');
@w = orphan_warnings('crash_idx');
is(scalar @w, 0, 'the parent check reports no orphan after the reclaim')
  or diag(join("\n", @w));
is($node->safe_psql('postgres', "SELECT bark_reclaim_orphans('crash_idx')"),
	'0', 'a second reclaim finds nothing');
is($node->safe_psql('postgres', "SELECT scans('crash_t')"),
	'same', 'scans of crash_idx return the sequential scan\'s keys');

$node->wait_for_replay_catchup($standby);
is( $standby->safe_psql(
		'postgres', "SELECT string_agg(blkno::text, ',' ORDER BY blkno)
		 FROM pages('crash_idx', 'deleted') blkno"),
	join(',', @orphans), 'the standby replayed the frees');
is( $standby->safe_psql(
		'postgres', q[
SET enable_seqscan = off; SET enable_bitmapscan = off;
SELECT string_agg(md5(k), ',' ORDER BY k) FROM crash_t WHERE k > '']),
	$node->safe_psql('postgres',
		"SELECT string_agg(md5(k), ',' ORDER BY k) FROM crash_t"),
	'an index scan on the standby returns the primary\'s keys');

# VACUUM records the freed pages in the free space map once they are
# recyclable, and a new oversized key's chain takes them.
my $size = $node->safe_psql('postgres',
	"SELECT pg_relation_size('crash_idx')");
$node->safe_psql(
	'postgres', q[
SELECT pg_current_xact_id();
VACUUM crash_t;
INSERT INTO crash_t VALUES (200, big(200, 30000));
]);
is($node->safe_psql('postgres', "SELECT pg_relation_size('crash_idx')"),
	$size, 'a new chain reuses the freed pages');
is($node->safe_psql('postgres', "SELECT count(*) FROM pages('crash_idx', 'deleted')"),
	'0', 'no freed page is left');

## 2: a mid-chain orphan whose last page links to a block now in use
# 40 keys of 30 kB, 4 overflow pages each; deleting and vacuuming half of
# them leaves 80 deleted pages in the free space map.  A 30 kB key takes 4
# pages, so a 60 kB one takes 8, in two records; an error after the first
# leaves 4 orphaned pages, the last linked to the fifth block the insert
# reserved, whose record was never written.  That block is still a deleted
# page.
$node->safe_psql(
	'postgres', q[
CREATE TABLE mid_t (id int, k text);
CREATE INDEX mid_idx ON mid_t USING bark (k);
INSERT INTO mid_t SELECT g, big(g, 30000) FROM generate_series(1, 40) g;
DELETE FROM mid_t WHERE id % 2 = 0;
SELECT pg_current_xact_id();
VACUUM mid_t;
SELECT pg_current_xact_id();
VACUUM mid_t;
]);
my %del = map { $_ => 1 } split /\n/,
  $node->safe_psql('postgres', "SELECT pages('mid_idx', 'deleted')");
cmp_ok(scalar keys %del, '>=', 16, 'mid_idx has free deleted pages');

my ($rc, $out, $err) = $node->psql(
	'postgres', q[
SELECT injection_points_set_local();
SELECT injection_points_attach('bark-overflow-chain-partial', 'error');
INSERT INTO mid_t VALUES (1000, big(1000, 60000));
]);
like($err, qr/error triggered for injection point bark-overflow-chain-partial/,
	'the long insert stops after its first record');

# The orphan, found as the census finds it: the overflow pages that no
# chain an OVERSIZED item starts reaches.
sub orphan_pages
{
	my %next = map { split /\|/ } split /\n/, $node->safe_psql(
		'postgres', q[
SELECT blkno, bark_next FROM bark_multi_page_stats('mid_idx', 1, -1)
WHERE type = 'overflow']);
	my @firsts = split /\n/, $node->safe_psql(
		'postgres', q[
SELECT DISTINCT i.overflow_blkno FROM bark_multi_page_stats('mid_idx', 1, -1) s,
  bark_page_items('mid_idx', s.blkno) i
WHERE s.type IN ('leaf', 'root', 'internal') AND i.shape = 'OVERSIZED']);
	my %reached;
	for my $blk (@firsts)
	{
		for (; defined $next{$blk} && !$reached{$blk}; $blk = $next{$blk})
		{
			$reached{$blk} = 1;
		}
	}
	return map { [ $_, $next{$_} ] } sort { $a <=> $b }
	  grep { !$reached{$_} } keys %next;
}
my @orph = orphan_pages();
is(scalar @orph, 4, 'the error left the four pages of the first record');
# Each links to the next, the last to a block that is not one of them.
my %orphset = map { $_->[0] => 1 } @orph;
my @dangling = grep { !$orphset{ $_->[1] } } @orph;
is(scalar @dangling, 1, 'one orphaned page links out of the orphan');
my $target = $dangling[0][1];
ok($del{$target}, "its link names block $target, a deleted page");

# VACUUM puts the reserved pages the error left unwritten back in the free
# space map, and 20 more oversized keys, 80 pages, take every free page,
# the dangling link's target among them.
$node->safe_psql(
	'postgres', q[
VACUUM mid_t;
INSERT INTO mid_t SELECT g, big(g, 30000) FROM generate_series(2001, 2020) g;
]);
my $ttype = $node->safe_psql('postgres',
	"SELECT type FROM bark_page_stats('mid_idx', $target)");
isnt($ttype, 'deleted', "block $target is in use again (type $ttype)");
is(scalar(orphan_pages()), 4, 'the orphan is still the same four pages');
@w = orphan_warnings('mid_idx');
is(scalar @w, 1, 'the parent check reports the orphan') or diag(join("\n", @w));

is($node->safe_psql('postgres', "SELECT bark_reclaim_orphans('mid_idx')"),
	'4', 'the reclaim frees the orphan\'s pages');
is( $node->safe_psql(
		'postgres', "SELECT type FROM bark_page_stats('mid_idx', $target)"),
	$ttype, "block $target, which the orphan links to, is not freed");
is( $node->safe_psql(
		'postgres', "SELECT count(*) FROM bark_multi_page_stats('mid_idx', 1, -1)
		 WHERE type = 'deleted' AND blkno IN (" . join(',', map { $_->[0] } @orph) . ")"),
	'4', 'the orphan\'s pages are deleted');
@w = orphan_warnings('mid_idx');
is(scalar @w, 0, 'the parent check reports no orphan after the reclaim')
  or diag(join("\n", @w));
is($node->safe_psql('postgres', "SELECT scans('mid_t')"),
	'same', 'scans of mid_idx return the sequential scan\'s keys');
is($node->safe_psql('postgres', "SELECT bark_index_check('mid_idx', true)"),
	'', 'bark_index_check with heapallindexed passes mid_idx');

## 3: an orphan whose unwritten pages were new
# With no free page, a long chain's pages extend the index; an error after
# its first record leaves the rest all-zero, which the census passes over
# and the reclaim leaves alone.
$node->safe_psql(
	'postgres', q[
CREATE TABLE new_t (id int, k text);
CREATE INDEX new_idx ON new_t USING bark (k);
INSERT INTO new_t VALUES (1, big(1, 30000));
]);
my $nblocks = $node->safe_psql('postgres',
	"SELECT pg_relation_size('new_idx') / current_setting('block_size')::int");
($rc, $out, $err) = $node->psql(
	'postgres', q[
SELECT injection_points_set_local();
SELECT injection_points_attach('bark-overflow-chain-partial', 'error');
INSERT INTO new_t VALUES (2, big(2, 60000));
]);
like($err, qr/error triggered for injection point bark-overflow-chain-partial/,
	'the insert stops after its first record');
# The chain's blocks follow the old end of the index: the first record's
# four overflow pages, then the blocks no record wrote.
my $grown = $node->safe_psql('postgres',
	"SELECT pg_relation_size('new_idx') / current_setting('block_size')::int") - $nblocks;
cmp_ok($grown, '>', 4, "the index grew by the chain's $grown blocks");
is( $node->safe_psql(
		'postgres', "SELECT string_agg(type, ',' ORDER BY b)
		 FROM generate_series($nblocks, $nblocks + $grown - 1) b,
		   LATERAL bark_page_stats('new_idx', b)"),
	join(',', ('overflow') x 4, ('new') x ($grown - 4)),
	'four overflow pages, and new pages for the blocks no record wrote');
is($node->safe_psql('postgres', "SELECT bark_reclaim_orphans('new_idx')"),
	'4', 'the reclaim frees the four written pages');
@w = orphan_warnings('new_idx');
is(scalar @w, 0, 'the parent check reports no orphan') or diag(join("\n", @w));
is($node->safe_psql('postgres', "SELECT scans('new_t')"),
	'same', 'scans of new_idx return the sequential scan\'s keys');

## 4: not on a standby
$node->wait_for_replay_catchup($standby);
($rc, $out, $err) = $standby->psql('postgres',
	"SELECT bark_reclaim_orphans('mid_idx')");
like($err, qr/cannot acquire lock mode ShareRowExclusiveLock on database objects while recovery is in progress/,
	'the reclaim is refused on a standby');
is($standby->safe_psql('postgres', "SELECT scans('mid_t')"),
	'same', 'scans of mid_idx on the standby return its sequential scan\'s keys');

$standby->stop;
ok(!$standby->log_contains('inconsistent page found'),
	'wal_consistency_checking found every replayed page consistent');

$node->stop;
done_testing();
