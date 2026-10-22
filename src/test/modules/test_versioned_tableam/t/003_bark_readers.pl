
# Copyright (c) 2026, PostgreSQL Global Development Group

# BARK's readers hand heap_versioned each entry's tag.  heap_versioned's index scan checks every (TID, tag) it is given
# against the tuple at that TID (the tag is 0 or its raw xmin) and counts
# tagged and untagged ones, and its fetch_tid_version does the same for
# unique checks.  An index that stores no tags (multikey) must report 0, or
# heap_versioned raises an error.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
autovacuum = off
max_worker_processes = 8
max_parallel_workers = 4
});
$node->start;

$node->safe_psql(
	'postgres', q{
	CREATE EXTENSION test_versioned_tableam;
	CREATE EXTENSION bark_multikey;
	CREATE TABLE tr (id int, k int) USING heap_versioned;
	INSERT INTO tr SELECT g, g FROM generate_series(1, 10000) g;
	-- untagged entries from the build
	SET test_versioned_tableam.build_tag_percent = 0;
	CREATE INDEX tr_k ON tr USING bark (k);
	CREATE UNIQUE INDEX tr_id ON tr USING bark (id);
	-- the rows move (k is indexed), and every index gets an entry tagged
	-- with the new version's xmin
	UPDATE tr SET k = k + 100000 WHERE id % 10 = 0;
	CREATE TABLE tr_outer (k int);
	INSERT INTO tr_outer VALUES (5), (5), (100010), (100010), (100020);
	ANALYZE tr, tr_outer;
});

# Run `sql` after `sets`, as a prepared statement executed twice, and return
# the scan counters of the second execution, as "tagged/untagged", and its
# output.  The second execution is not planned again, so the planner's own
# index probes (get_actual_variable_range) are not counted.
my $noscans =
  'SET enable_seqscan = off; SET enable_bitmapscan = off; SET enable_indexonlyscan = off;';

sub scan
{
	my ($sql, $sets) = @_;
	$sets //= '';
	my @out = split /\n/,
	  $node->safe_psql(
		'postgres', qq{
		$noscans $sets
		PREPARE q AS $sql;
		EXECUTE q;
		SELECT test_versioned_reset();
		EXECUTE q});
	my $counts = $node->safe_psql(
		'postgres', q{
		SELECT string_agg(n::text, '/' ORDER BY counter)
		  FROM test_versioned_counts() WHERE counter IN ('scan_tagged', 'scan_untagged')});
	return ($counts, $out[-1]);
}

sub plan_of
{
	my ($sql, $sets) = @_;
	$sets //= '';
	return $node->safe_psql('postgres', "$noscans $sets EXPLAIN (COSTS OFF) $sql");
}

my ($c, $o);

($c, $o) = scan(q{SELECT count(*), sum(id) FROM tr WHERE k > 0});
is($o, '10000|50005000', 'forward: rows');
is($c, '1000/9000', 'forward: tags reach the table AM');

($c, $o) = scan(
	q{SELECT string_agg(id::text, ',') FROM (SELECT id FROM tr WHERE k > 99000 ORDER BY k DESC LIMIT 3) s});
is($o, '10000,9990,9980', 'backward: rows');
is($c, '3/0', 'backward: tags');
($c, $o) = scan(q{SELECT count(*) FROM (SELECT id FROM tr ORDER BY k DESC) s});
is($c, '1000/9000', 'backward, whole index: tags');

($c, $o) = scan(
	q{SELECT string_agg(id::text, ',' ORDER BY id) FROM tr WHERE k = ANY ('{5, 100010, 7, 100020, 100030}')});
is($o, '5,7,10,20,30', 'SAOP: rows');
is($c, '3/2', 'SAOP: tags');

($c, $o) = scan(
	q{SELECT string_agg(id::text, ',' ORDER BY id) FROM (SELECT id FROM tr ORDER BY k <~> 100500 LIMIT 3) s});
is($o, '490,500,510', 'KNN: rows');
is($c, '3/0', 'KNN: tags');
($c, $o) = scan(
	q{SELECT string_agg(id::text, ',' ORDER BY id) FROM (SELECT id FROM tr ORDER BY k <~> 15 LIMIT 3) s});
is($o, '14,15,16', 'KNN, untagged: rows');
is($c, '0/3', 'KNN, untagged: tags');

($c, $o) = scan(q{SELECT count(*) FROM tr WHERE k > 100000},
	'SET enable_indexonlyscan = on;');
is($o, '1000', 'IOS: rows');
is($c, '1000/0', 'IOS: tags');

my $mjsets =
  'SET enable_hashjoin = off; SET enable_nestloop = off; SET enable_material = off; SET enable_sort = on;';
my $mj = q{SELECT count(*) FROM tr_outer o JOIN tr ON tr.k = o.k};
like(plan_of($mj, $mjsets), qr/Merge Join.*Index Scan using tr_k/s,
	'mark/restore: a merge join over tr_k');
($c, $o) = scan($mj, $mjsets);
is($o, '5', 'mark/restore: rows');
# k 1..9999 untagged, then 100010 (tagged) and 100020, and 100030 to end
# the join; each outer duplicate restores the mark and returns its item
# again, 5 untagged and 100010 tagged.
is($c, '4/9001', 'mark/restore: tags, restored items too');

my $parsets = q{
	SET parallel_setup_cost = 0; SET parallel_tuple_cost = 0;
	SET min_parallel_index_scan_size = 0; SET min_parallel_table_scan_size = 0;
	SET max_parallel_workers_per_gather = 2; SET parallel_leader_participation = off;};
my $par = q{SELECT count(*), sum(id) FROM tr WHERE k > 0};
like(plan_of($par, $parsets), qr/Parallel Index Scan using tr_k/,
	'parallel: a parallel index scan');
($c, $o) = scan($par, $parsets);
is($o, '10000|50005000', 'parallel: rows');
is($c, '1000/9000', 'parallel: tags, from the workers');

# Bitmap scans drop tags: the TIDBitmap holds TIDs only.
is( $node->safe_psql(
		'postgres', q{
		SET enable_seqscan = off; SET enable_indexscan = off; SET enable_indexonlyscan = off;
		SELECT count(*) FROM tr WHERE k > 100000}),
	'1000', 'bitmap: rows');

# The unique check asks fetch_tid_version about each equal entry with its
# tag: id 10's live entry is tagged, its dead old one untagged.
$node->safe_psql('postgres', 'SELECT test_versioned_reset()');
my ($ret, $out, $err) =
  $node->psql('postgres', 'INSERT INTO tr VALUES (10, 0)');
like($err, qr/duplicate key value violates unique constraint "tr_id"/,
	'unique: the conflict is found');
is( $node->safe_psql(
		'postgres', q{
		SELECT string_agg(counter || '=' || n, ',' ORDER BY counter)
		  FROM test_versioned_counts() WHERE counter LIKE 'fetch%'}),
	'fetch_tagged=1,fetch_untagged=1', 'unique: the entries\' tags reach fetch_tid_version');

# A deferred unique constraint on a btree implemented by BARK.  Its recheck
# (UNIQUE_CHECK_EXISTING) is untagged: it recognizes the row's own entries
# by locator, an extra one of another tag too.
$node->safe_psql(
	'postgres', q{
	SET default_btree_implementation = bark;
	CREATE TABLE td (id int, u int) USING heap_versioned;
	ALTER TABLE td ADD CONSTRAINT td_u UNIQUE (u) DEFERRABLE INITIALLY DEFERRED;
	INSERT INTO td SELECT g, g FROM generate_series(1, 10) g;
});
is( $node->safe_psql(
		'postgres', q{SELECT a.amname FROM pg_index x JOIN pg_am a ON a.oid = x.indimplam
		  WHERE x.indexrelid = 'td_u'::regclass}),
	'bark', 'deferred: td_u is BARK');
# Two updates that move every row: the second one's transient conflicts
# meet the first one's tagged entries.
$node->safe_psql('postgres', 'UPDATE td SET u = u + 1');
$node->safe_psql('postgres', 'SELECT test_versioned_reset()');
$node->safe_psql('postgres', 'UPDATE td SET u = u + 1');
is( $node->safe_psql(
		'postgres', q{SELECT n > 0 FROM test_versioned_counts() WHERE counter = 'fetch_tagged'}),
	't', 'deferred: the checks ask about tagged entries');
($ret, $out, $err) =
  $node->psql('postgres', 'UPDATE td SET u = 5 WHERE id = 1');
like($err, qr/duplicate key value violates unique constraint "td_u"/,
	'deferred: a real conflict is found at commit');
# Row 1 meets row 2's entry of key 1000, so its insert is rechecked at
# commit, after row 2 has left the key.  Row 1 then has two entries of key
# 1000, tagged and untagged (as a stale one would be): the recheck skips
# both by locator, or the untagged one, whose locator reaches the live
# row 1, would be a conflict.
$node->safe_psql(
	'postgres', q{
	BEGIN;
	UPDATE td SET u = 1000 WHERE id = 2;
	UPDATE td SET u = 1000 WHERE id = 1;
	SELECT test_versioned_insert_entry('td_u', ctid, 0) FROM td WHERE id = 1;
	UPDATE td SET u = 2000 WHERE id = 2;
	COMMIT;
});
is($node->safe_psql('postgres', q{SET enable_indexscan = off; SET enable_bitmapscan = off;
	  SELECT id FROM td WHERE u = 1000}),
	'1', 'deferred: the row\'s own entries of two tags are no conflict');

# Multikey indexes store no tags; their scans report 0 (heap_versioned
# raises an error otherwise), a GIN-interface scan's too.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tmk (id int, a int4[]) USING heap_versioned;
	INSERT INTO tmk SELECT g, ARRAY[g, g + 1] FROM generate_series(1, 1000) g;
});
for my $opc (qw(bark_int4_array_ops bark_gin_int4_array_ops))
{
	$node->safe_psql('postgres', qq{
		CREATE INDEX tmk_a ON tmk USING bark (a $opc);
		UPDATE tmk SET a = a || 0 WHERE id % 10 = 0;
	});
	my $q = q{SELECT count(*) FROM tmk WHERE a && '{0}'};
	like(plan_of($q), qr/Index Scan using tmk_a/, "multikey $opc: an index scan");
	is($node->safe_psql('postgres', "$noscans $q"), '100',
		"multikey $opc: rows, all untagged");
	$node->safe_psql('postgres', q{
		DROP INDEX tmk_a;
		UPDATE tmk SET a = a[1:2] WHERE id % 10 = 0;
	});
}

# a clean stop, so that backends exit normally (coverage counts)
$node->stop;
done_testing();
