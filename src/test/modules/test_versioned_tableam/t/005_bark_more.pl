
# Copyright (c) 2026, PostgreSQL Global Development Group

# More of BARK's tagged entries on heap_versioned: OVERSIZED tagged entries through insert, scan, VACUUM and untag;
# a tagged pivot seen through pageinspect; crash recovery of VACUUM's
# untagging and of BARK_LOWBOUND_TAGGED; and a random sequence of updates,
# extra entries and VACUUMs with bark_index_parent_check after each step.
# wal_consistency_checking = bark, and a standby replays everything.

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
# a slot, so the primary keeps the WAL the standby has yet to replay
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
	CREATE FUNCTION entries(idx regclass)
	  RETURNS TABLE (blk int, off int, shape text, htid tid, tag int8)
	  LANGUAGE sql AS $$
	  SELECT b::int, i.itemoffset, i.shape, i.htid, i.tag
	    FROM generate_series(1, pg_relation_size(idx) / 8192 - 1) b,
	         LATERAL bark_page_items(idx::text, b) i
	   WHERE (SELECT type FROM bark_page_stats(idx::text, b)) = 'leaf' $$;
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

# (1) OVERSIZED tagged entries: long keys whose rows move (the update
# changes the key), so each new entry is tagged.  Scans return them with
# their tags (heap_versioned checks each), VACUUM untags them in place,
# and the overflow chains stay with them.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tov (id int, k text) USING heap_versioned;
	ALTER TABLE tov ALTER COLUMN k SET STORAGE EXTERNAL;
	SET test_versioned_tableam.build_tag_percent = 0;
	CREATE INDEX tov_k ON tov USING bark (k);
	INSERT INTO tov SELECT g, lpad(g::text, 4, '0') ||
	  (SELECT string_agg(md5(g::text || i::text), '') FROM generate_series(1, 120) i)
	  FROM generate_series(1, 40) g;
	UPDATE tov SET k = 'z' || k WHERE id % 4 = 0;
});
is( $node->safe_psql(
		'postgres', q{
		SELECT count(*) FILTER (WHERE tag IS NOT NULL) || '/' || count(*)
		  FROM entries('tov_k') WHERE shape = 'OVERSIZED'}),
	'10/50', 'oversized: ten tagged OVERSIZED entries');
is( $node->safe_psql(
		'postgres', q{
		SELECT count(*) FROM entries('tov_k') e JOIN tov t ON t.ctid = e.htid
		 WHERE e.tag IS NOT NULL AND e.tag = t.xmin::text::int8}),
	'10', 'oversized: their tags are the rows\' xmins');
$node->safe_psql('postgres', 'SELECT test_versioned_reset()');
is( $node->safe_psql(
		'postgres', q{
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		SELECT count(*) FROM tov WHERE k >= 'z'}),
	'10', 'oversized: index scan of the tagged ones');
is( $node->safe_psql(
		'postgres', q{SELECT n FROM test_versioned_counts() WHERE counter = 'scan_tagged'}),
	'10', 'oversized: the scan handed their tags to the table AM');
check_index('tov_k', 'oversized');
$node->safe_psql('postgres', 'VACUUM tov');
is( $node->safe_psql(
		'postgres', q{
		SELECT count(*) FILTER (WHERE tag IS NOT NULL) || '/' || count(*)
		  FROM entries('tov_k') WHERE shape = 'OVERSIZED'}),
	'0/40', 'oversized: VACUUM removed the dead ones and untagged the rest');
is( $node->safe_psql(
		'postgres', q{
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		SELECT count(*) FROM tov WHERE k >= 'z'}),
	'10', 'oversized: index scan after untagging');
check_index('tov_k', 'oversized, VACUUM');

# (2) A tagged pivot, seen through pageinspect: a leaf of one run of one
# row's entries splits inside it.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tpv (id int, k int) USING heap_versioned;
	INSERT INTO tpv VALUES (1, 7);
	SET test_versioned_tableam.build_tag_percent = 0;
	CREATE INDEX tpv_k ON tpv USING bark (k);
	SET test_versioned_tableam.stale_tags = live;
	SELECT count(test_versioned_insert_entry('tpv_k', ctid, 2000000 + g))
	  FROM tpv, generate_series(1, 500) g;
});
my $pivots = $node->safe_psql(
	'postgres', q{
	SELECT count(*) FROM generate_series(1, pg_relation_size('tpv_k') / 8192 - 1) b,
	       LATERAL bark_page_items('tpv_k', b) i
	 WHERE i.shape = 'PIVOT' AND i.tag IS NOT NULL AND i.htid = '(0,1)'});
cmp_ok($pivots, '>=', 2, "tagged pivot: pageinspect shows tagged pivots ($pivots)");
check_index('tpv_k', 'tagged pivot');

# (3) Crash recovery: VACUUM's in-place untagging, the split and page
# deletion records with BARK_LOWBOUND_TAGGED, replayed after an immediate
# stop; the primary's pages after recovery must pass amcheck, and match
# the standby's.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tcr (id int, k int) USING heap_versioned;
	INSERT INTO tcr SELECT g, g FROM generate_series(1, 3000) g;
	SET test_versioned_tableam.build_tag_percent = 100;
	CREATE INDEX tcr_k ON tcr USING bark (k);
	CHECKPOINT;
	VACUUM tcr;
	SET test_versioned_tableam.stale_tags = live;
	SELECT count(test_versioned_insert_entry('tcr_k', ctid, 3000000 + g))
	  FROM tcr, generate_series(1, 600) g WHERE id = 1500;
	UPDATE tcr SET k = k + 10000 WHERE id % 7 = 0;
});
my $flagged = q{
	SELECT count(*) FROM generate_series(1, pg_relation_size('tcr_k') / 8192 - 1) b,
	       LATERAL bark_page_stats('tcr_k', b) s WHERE 'lowbound_tagged' = ANY (s.flags)};
my $before = $node->safe_psql('postgres', $flagged);
cmp_ok($before, '>', 0, 'crash: leaves with BARK_LOWBOUND_TAGGED');
my $sig = q{SELECT md5(string_agg(blk || ' ' || off || ' ' || shape || ' ' ||
  coalesce(htid::text, '') || ' ' || coalesce(tag::text, ''), ',' ORDER BY blk, off)) FROM entries('tcr_k')};
my $sigbefore = $node->safe_psql('postgres', $sig);
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', $flagged), $before, 'crash: the flags are replayed');
is($node->safe_psql('postgres', $sig), $sigbefore, 'crash: the entries are replayed');
check_index('tcr_k', 'crash');
$node->safe_psql('postgres', 'SET test_versioned_tableam.stale_tags = live; VACUUM tcr');
$sigbefore = $node->safe_psql('postgres', $sig);
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', $sig), $sigbefore, 'crash: VACUUM\'s untagging is replayed');
check_index('tcr_k', 'crash after VACUUM');

# (4) A random sequence: updates that move rows (tagged entries in every
# index), updates of unindexed columns, extra entries of other tags for a
# few rows (live or stale), deletes, inserts and VACUUMs, which untag in
# place and remove dead entries, with every index checked after each step.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE trn (id int, k int, j int, pad text) USING heap_versioned;
	INSERT INTO trn SELECT g, g % 50, g, 'x' FROM generate_series(1, 2000) g;
	SET test_versioned_tableam.build_tag_percent = 50;
	CREATE INDEX trn_k ON trn USING bark (k);
	CREATE INDEX trn_kj ON trn USING bark (k, j);
	CREATE UNIQUE INDEX trn_id ON trn USING bark (id);
});
$node->safe_psql('postgres', 'SELECT test_versioned_reset()');
srand(20261022);
my $extra = 0;
my @steps = (
	sub { my $m = 2 + int(rand(9)); "UPDATE trn SET k = (k + 1) % 50 WHERE id % $m = 0" },
	sub { my $m = 2 + int(rand(9)); "UPDATE trn SET pad = pad || 'y' WHERE id % $m = 1" },
	sub { my $m = 3 + int(rand(20)); "UPDATE trn SET j = j + 1 WHERE id % $m = 2" },
	sub { my $id = 1 + int(rand(2000)); my $n = 1 + int(rand(30)); my $b = 30 * (++$extra);
		"SET test_versioned_tableam.stale_tags = live;
		 SELECT count(test_versioned_insert_entry('trn_k', ctid, -($b + g))) FROM trn, generate_series(1, $n) g WHERE id = $id" },
	sub { my $m = 20 + int(rand(40)); "DELETE FROM trn WHERE id % $m = 3" },
	sub { "INSERT INTO trn SELECT g, g % 50, g, 'n' FROM generate_series((SELECT max(id) + 1 FROM trn), (SELECT max(id) + 50 FROM trn)) g" },
	sub { "VACUUM trn" },
	sub { "SET test_versioned_tableam.stale_tags = untag; VACUUM trn" },
	sub { "SET test_versioned_tableam.stale_tags = live; VACUUM trn" },
);
my $bad = '';
for my $i (1 .. 40)
{
	my $sql = $steps[ int(rand(@steps)) ]->();
	$node->safe_psql('postgres', $sql);
	for my $idx (qw(trn_k trn_kj trn_id))
	{
		my ($ret, $out, $err) = $node->psql('postgres',
			"SELECT bark_index_parent_check('$idx', true)");
		if ($ret != 0)
		{
			$bad = "step $i ($sql), $idx: $err";
			last;
		}
	}
	last if $bad;
}
is($bad, '', 'random: every index passes after every step');
is( $node->safe_psql(
		'postgres', q{
		SELECT string_agg(counter || ' ' || (n > 0), ', ' ORDER BY counter) FROM test_versioned_counts()
		 WHERE counter IN ('vacuum_dead', 'vacuum_untag', 'vacuum_live', 'delete_status')}),
	'delete_status true, vacuum_dead true, vacuum_live true, vacuum_untag true',
	'random: deletion passes, VACUUM deletions, keeps and untagging all happened');
is( $node->safe_psql(
		'postgres', q{
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		SELECT count(*), sum(k) FROM trn WHERE k >= 0}),
	$node->safe_psql(
		'postgres', q{
		SET enable_indexscan = off; SET enable_indexonlyscan = off; SET enable_bitmapscan = off;
		SELECT count(*), sum(k) FROM trn WHERE k >= 0}),
	'random: index scan and seq scan agree');
check_index($_, "random: $_") for qw(trn_k trn_kj trn_id);

$node->wait_for_catchup($standby);
unlike(slurp_file($standby->logfile), qr/inconsistent page found/,
	'no inconsistent page in replay');

# a clean stop, so that backends exit normally (coverage counts)
$standby->stop;
$node->stop;
done_testing();
