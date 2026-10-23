
# Copyright (c) 2026, PostgreSQL Global Development Group

# bark_index_parent_check follows every OVERSIZED entry's overflow chain,
# leaf entries' and pivots', and reports with a WARNING the overflow pages
# no chain reaches.
#
# 1. A crash between writing an oversized entry's chain and inserting the
#    entry orphans the chain's pages: the check reports exactly them, as
#    one chain, and bark_index_check, which does not follow chains, passes.
# 2. An index whose pivots are OVERSIZED, as a single 20 kB-key column
#    keeps them, shares one chain between a high key and the downlinks
#    copied from it.  The check reports nothing; with its pivot marks
#    skipped (the bark-check-chains-skip-pivots injection point) it reports
#    the pivots' chains as orphans, which shows that it marks them.
# 3. VACUUM deletes an empty leaf of an index whose pivots are OVERSIZED.
#    The right sibling's downlink, which goes, shared its chain only with
#    the deleted leaf's high key: VACUUM frees the chain, and the check
#    reports no orphaned pages, on the primary and on a standby that
#    replays the deletion under wal_consistency_checking.
# 4. A multikey row whose first key goes on the rightmost leaf an insert
#    cached, and whose oversized second key does not fit there: the fast
#    path cannot split, so the insert writes the second key's chain, finds
#    no room, and descends again.  The chain it wrote is freed, and the
#    check reports no orphaned pages.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
wal_consistency_checking = 'Bark'
]);
$node->start;

# Keys of $n bytes that do not compress: a chain of md5s.
$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE EXTENSION injection_points;
CREATE EXTENSION pageinspect;
CREATE EXTENSION bark_multikey;
CREATE FUNCTION big(s int, n int) RETURNS text LANGUAGE sql IMMUTABLE AS
$$ SELECT substr(string_agg(md5(s::text || ':' || g::text), ''), 1, n)
   FROM generate_series(1, (n + 31) / 32) g $$;
-- The index's overflow pages.
CREATE FUNCTION overflow_pages(idx text) RETURNS SETOF int8 LANGUAGE sql AS
$$ SELECT blkno FROM bark_multi_page_stats(idx, 1, -1)
   WHERE type = 'overflow' ORDER BY 1 $$;
]);

# The WARNINGs bark_index_parent_check gives for $idx, and whether it ended
# without an ERROR.
sub parent_check
{
	my ($idx) = @_;
	my ($rc, $out, $err) = $node->psql('postgres',
		"SELECT bark_index_parent_check('$idx')");
	my @w = $err =~ /WARNING:  (.*)$/mg;
	return ($rc, @w);
}

## 1: a crash between writing a chain and inserting its entry
$node->safe_psql(
	'postgres', q[
CREATE TABLE crash_t (id int, k text);
CREATE INDEX crash_idx ON crash_t USING bark (k);
INSERT INTO crash_t SELECT g, big(g, 30000) FROM generate_series(1, 3) g;
]);
my @before = split /\n/,
  $node->safe_psql('postgres', "SELECT * FROM overflow_pages('crash_idx')");
# Each of the three keys has a chain of the same length.
my $nchunks = scalar(@before) / 3;
cmp_ok($nchunks, '>=', 4, 'a 30 kB key takes a chain of 4 pages or more');
my ($rc, @w) = parent_check('crash_idx');
is($rc, 0, 'the parent check passes the index before the crash');
is(scalar @w, 0, 'and reports no orphaned pages') or diag(join("\n", @w));

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

# The chain is written and logged; no entry references it.  Another
# transaction's commit flushes the chain's WAL, so that recovery rebuilds
# its pages.  Then kill the backend; TAP clusters run with
# restart_after_crash off, so the server stops and the start below
# recovers it.
$node->safe_psql('postgres', 'CREATE TABLE flush_wal ()');
my $pid = $node->safe_psql('postgres',
	"SELECT pid FROM pg_stat_activity WHERE wait_event = 'bark-overflow-chain-written'");
is(PostgreSQL::Test::Utils::system_log('pg_ctl', 'kill', 'KILL', $pid),
	0, 'killed the inserting backend');
$s1->{run}->finish;
$node->stop('immediate', fail_ok => 1);
$node->start;
ok($node->log_contains('database system was interrupted'),
	'the server went through crash recovery');

my %old = map { $_ => 1 } @before;
my @orphans = grep { !$old{$_} } split /\n/,
  $node->safe_psql('postgres', "SELECT * FROM overflow_pages('crash_idx')");
is(scalar @orphans, $nchunks,
	"the crash left the new chain's $nchunks overflow pages");

is($node->safe_psql('postgres', "SELECT bark_index_check('crash_idx')"),
	'', 'bark_index_check passes the index');
($rc, @w) = parent_check('crash_idx');
is($rc, 0, 'the parent check reports no corruption');
is_deeply(
	\@w,
	[
		"BARK index \"crash_idx\" has an orphaned overflow chain of $nchunks pages at block $orphans[0]"
	],
	'the parent check reports the chain, by its first block and its pages');

## 2: OVERSIZED pivots share their chains
# Every key is oversized, so every high key and downlink is too.
$node->safe_psql(
	'postgres', q[
CREATE TABLE piv_t (id int, k text);
CREATE INDEX piv_idx ON piv_t USING bark (k);
INSERT INTO piv_t SELECT g, big(g, 20000) FROM generate_series(1, 200) g;
]);
is( $node->safe_psql(
		'postgres', q[
SELECT count(*) > 0 FROM bark_multi_page_stats('piv_idx', 1, -1) s,
  bark_page_items('piv_idx', s.blkno) i
WHERE s.type IN ('internal', 'root') AND i.shape = 'OVERSIZED'
  AND i.downlink IS NOT NULL]),
	't', 'piv_idx has OVERSIZED downlinks');
($rc, @w) = parent_check('piv_idx');
is($rc, 0, 'the parent check passes piv_idx');
is(scalar @w, 0, 'and reports no orphaned pages') or diag(join("\n", @w));

# Negative control: skip the pivots' chains when marking, and the check
# reports them as orphans.
my $pivchains = $node->safe_psql(
	'postgres', q[
SELECT count(DISTINCT i.overflow_blkno)
FROM bark_multi_page_stats('piv_idx', 1, -1) s,
  bark_page_items('piv_idx', s.blkno) i
WHERE s.type IN ('leaf', 'root', 'internal') AND i.shape = 'OVERSIZED'
  AND i.ntids IS NULL]);
cmp_ok($pivchains, '>', 0, 'piv_idx has pivot chains');
($rc, $_, my $err) = $node->psql(
	'postgres', q[
SELECT injection_points_set_local();
SELECT injection_points_attach('bark-check-chains-skip-pivots', 'notice');
SELECT bark_index_parent_check('piv_idx');]);
@w = $err =~ /WARNING:  (.*orphaned overflow chain.*)$/mg;
is(scalar @w, $pivchains,
	'with the pivot marks skipped, the check reports each pivot chain');

## 3: VACUUM deletes a leaf whose high key is OVERSIZED
$node->backup('backup');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'backup', has_streaming => 1);
$standby->start;

# 600 keys of 20 kB: six leaves under one root, every downlink but the
# first OVERSIZED.
$node->safe_psql(
	'postgres', q[
CREATE TABLE vac_t (id int, k text);
CREATE INDEX vac_idx ON vac_t USING bark (k);
INSERT INTO vac_t SELECT g, lpad(g::text, 4, '0') || big(g, 20000)
  FROM generate_series(1, 600) g;
]);
my $root = $node->safe_psql('postgres', "SELECT root FROM bark_metap('vac_idx')");
my ($leaf, $rightdl) = split /\|/, $node->safe_psql(
	'postgres', qq[
SELECT downlink, (SELECT overflow_blkno FROM bark_page_items('vac_idx', $root)
                  WHERE itemoffset = 4)
FROM bark_page_items('vac_idx', $root) WHERE itemoffset = 3]);
is( $node->safe_psql(
		'postgres', qq[
SELECT count(*) FROM bark_page_items('vac_idx', $root)
WHERE shape = 'OVERSIZED' AND itemoffset IN (3, 4)]),
	'2', 'the root has OVERSIZED downlinks to the second leaf and the next');

# Empty the second leaf (an interior one) and VACUUM, which deletes it.
$node->safe_psql(
	'postgres', qq[
DELETE FROM vac_t WHERE ctid IN
  (SELECT htid FROM bark_page_items('vac_idx', $leaf) WHERE ntids = 1);
VACUUM vac_t;
]);
is( $node->safe_psql(
		'postgres', "SELECT type FROM bark_page_stats('vac_idx', $leaf)"),
	'deleted', 'VACUUM deleted the emptied leaf');
is( $node->safe_psql(
		'postgres', "SELECT type FROM bark_page_stats('vac_idx', $rightdl)"),
	'deleted',
	"VACUUM freed the chain of the right sibling's downlink");
($rc, @w) = parent_check('vac_idx');
is($rc, 0, 'the parent check passes vac_idx');
is(scalar @w, 0, 'and reports no orphaned pages') or diag(join("\n", @w));

$node->wait_for_replay_catchup($standby);
is( $standby->safe_psql(
		'postgres', "SELECT type FROM bark_page_stats('vac_idx', $rightdl)"),
	'deleted', 'the standby replayed the freeing of the chain');
is( $standby->safe_psql(
		'postgres', q[
SET enable_seqscan = off; SET enable_bitmapscan = off;
SELECT count(*) FROM vac_t WHERE k > '0']),
	$node->safe_psql('postgres', 'SELECT count(*) FROM vac_t'),
	'an index scan on the standby finds every row');

## 4: the fast path gives up a leaf after writing an oversized key's chain
# Keys of 2.4 kB inserted in ascending order, enough for the levels above
# the leaves that make an insert cache the rightmost leaf
# (BARK_FASTPATH_MIN_LEVEL).  The cache is the backend's, so the inserts
# that fill the leaf and the row that tries it share one session.
my $pages = q[SELECT count(*) FILTER (WHERE type = 'overflow') || ',' ||
  count(*) FILTER (WHERE type = 'deleted') FROM bark_multi_page_stats('fp_idx', 1, -1)];
my ($level, $free, $before, $after) = split /\n/, $node->safe_psql(
	'postgres', qq[
CREATE TABLE fp_t (id int, a text[] COLLATE "C");
CREATE INDEX fp_idx ON fp_t USING bark (a bark_text_prefix_ops);
INSERT INTO fp_t SELECT g, ARRAY['k' || lpad(g::text, 6, '0') || big(g, 2400)]
  FROM generate_series(1, 30) g;
CREATE FUNCTION fp_free() RETURNS int LANGUAGE sql AS
\$\$ SELECT free_size FROM bark_multi_page_stats('fp_idx', 1, -1)
   WHERE type = 'leaf' AND bark_next = 0 \$\$;
-- Fill the rightmost leaf, in ascending order, to 48 bytes free.
DO \$\$
DECLARE i int := 0; f int;
BEGIN
  LOOP
    f := fp_free();
    EXIT WHEN f < 140;
    INSERT INTO fp_t VALUES (1000 + i,
      ARRAY['l' || lpad(i::text, 6, '0') || big(i, least(f - 140, 2400))]);
    i := i + 1;
  END LOOP;
  INSERT INTO fp_t VALUES (5000, ARRAY['lz' || repeat('x', f - 66)]);
END \$\$;
SELECT level FROM bark_metap('fp_idx');
SELECT fp_free();
$pages;
INSERT INTO fp_t VALUES (9999, ARRAY['m', 'n' || big(7, 20000)]);
$pages;
]);
cmp_ok($level, '>=', 2, 'fp_idx has two levels above its leaves');
# Room for the 16-byte entry of 'm' and the 8-byte reserve, not for an
# OVERSIZED entry's 64 bytes.
ok($free >= 24 && $free < 72, "the rightmost leaf has $free bytes free");
is($before, '0,0', 'fp_idx has no overflow pages before the row');
# A 20 kB key takes a chain of 3 pages: the insert wrote one on the cached
# leaf, gave the leaf up, and wrote another after its descent.
is($after, '3,3',
	'the insert freed the chain it wrote before giving up the cached leaf');
($rc, @w) = parent_check('fp_idx');
is($rc, 0, 'the parent check passes fp_idx');
is(scalar @w, 0, 'and reports no orphaned pages') or diag(join("\n", @w));
is( $node->safe_psql(
		'postgres', q[
SET enable_seqscan = off;
SELECT count(*) FROM fp_t WHERE a %^% 'n']),
	'1', 'an index scan finds the oversized key');

$node->wait_for_replay_catchup($standby);
$standby->stop;
ok(!$standby->log_contains('inconsistent page found'),
	'wal_consistency_checking found every replayed page consistent');

$node->stop;
done_testing();
