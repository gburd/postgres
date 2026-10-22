
# Copyright (c) 2026, PostgreSQL Global Development Group

# BARK storing the version tags heap_versioned hands it (DO NOT MERGE, with
# the module): tagged entries from UPDATE, from a helper that adds a tagged
# entry at a TID the index already has, and from CREATE INDEX, serial and
# parallel.  Each step is checked with amcheck, against the expected
# entries and tags (tag = the xmin of the tuple at the entry's TID), and on
# a standby that replays the WAL with wal_consistency_checking = bark.

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
max_parallel_maintenance_workers = 2
max_worker_processes = 8
});
$node->start;
$node->backup('b');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'b', has_streaming => 1);
$standby->start;

$node->safe_psql(
	'postgres', q{
	CREATE EXTENSION test_versioned_tableam;
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pageinspect;
	-- every leaf entry of a BARK index, in index order
	CREATE FUNCTION bark_entries(idx regclass)
	  RETURNS TABLE (blk int, off int, shape text, htid tid, tids tid[], tag int8)
	  LANGUAGE sql AS $$
	  SELECT b::int, i.itemoffset, i.shape, i.htid, i.tids, i.tag
	    FROM generate_series(1, pg_relation_size(idx) / 8192 - 1) b,
	         LATERAL bark_page_items(idx::text, b) i
	   WHERE (SELECT type FROM bark_page_stats(idx::text, b)) = 'leaf'
	     AND i.shape IN ('SINGLE', 'LIST', 'POSTING', 'OVERSIZED')
	     AND NOT (i.itemoffset = 1 AND (SELECT bark_next FROM bark_page_stats(idx::text, b)) <> 0)
	   ORDER BY 1, 2 $$;
});

# Check an index with amcheck, on the primary and, after catching up, on
# the standby, whose replay has checked every BARK record's page images.
sub check_index
{
	my ($idx, $what) = @_;
	is( $node->safe_psql(
			'postgres',
			"SELECT bark_index_parent_check('$idx', true)"),
		'',
		"$what: bark_index_parent_check");
	$node->wait_for_catchup($standby);
	is($standby->safe_psql('postgres', "SELECT bark_index_check('$idx', true)"),
		'', "$what: bark_index_check on the standby");
	is( $standby->safe_psql('postgres', "SELECT * FROM bark_entries('$idx')"),
		$node->safe_psql('postgres', "SELECT * FROM bark_entries('$idx')"),
		"$what: the standby's entries are the primary's");
}

# The entries of `idx`, (shape, htid or tids, tagged) in index order.
sub entries
{
	my ($idx) = @_;
	return $node->safe_psql(
		'postgres', qq{
		SELECT string_agg(shape || ' ' || coalesce(tids::text, htid::text) ||
		                  CASE WHEN tag IS NULL THEN '' ELSE ' tagged' END,
		                  E'\\n' ORDER BY blk, off)
		  FROM bark_entries('$idx')});
}

# Every tagged entry's tag is the xmin of the tuple at its TID.
sub tags_right
{
	my ($idx, $tab) = @_;
	return $node->safe_psql(
		'postgres', qq{
		SELECT count(*) FILTER (WHERE e.tag = t.xmin::text::int8) || '/' || count(*)
		  FROM bark_entries('$idx') e JOIN $tab t ON t.ctid = e.htid
		 WHERE e.tag IS NOT NULL});
}

# (b1) A tagged entry whose TID lies inside an untagged LIST's TID range,
# without being a member: rows 1, 2, 5, 6 have k = 7 and rows 3, 4 k = 8,
# two to a page; an UPDATE that gives row 3 k = 7 keeps it on its page
# (fillfactor leaves room), with keep_locator = pretend, and its tagged
# entry (7, (1,3), xid) falls between (0,2) and (2,1).
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tl (id int, k int, pad text) USING heap_versioned
	  WITH (fillfactor = 50);
	CREATE INDEX tl_k ON tl USING bark (k);
	INSERT INTO tl SELECT g, CASE WHEN g IN (3, 4) THEN 8 ELSE 7 END,
	  repeat('x', 1800) FROM generate_series(1, 6) g;
});
is($node->safe_psql('postgres', q{SELECT string_agg(ctid::text, ' ' ORDER BY id) FROM tl}),
	'(0,1) (0,2) (1,1) (1,2) (2,1) (2,2)', 'b1: two rows a page');
is(entries('tl_k'),
	"LIST {\"(0,1)\",\"(0,2)\",\"(2,1)\",\"(2,2)\"}\nLIST {\"(1,1)\",\"(1,2)\"}",
	'b1: untagged LISTs');
$node->safe_psql(
	'postgres', q{
	SET test_versioned_tableam.keep_locator = pretend;
	UPDATE tl SET k = 7 WHERE id = 3;
});
is($node->safe_psql('postgres', q{SELECT ctid FROM tl WHERE id = 3}),
	'(1,3)', 'b1: the new version stays on its page');
is(entries('tl_k'),
	"LIST {\"(0,1)\",\"(0,2)\"}\nSINGLE (1,3) tagged\nLIST {\"(2,1)\",\"(2,2)\"}\nLIST {\"(1,1)\",\"(1,2)\"}",
	'b1: the LIST is divided around the tagged entry');
is(tags_right('tl_k', 'tl'), '1/1', 'b1: the tag is the new version\'s xmin');
check_index('tl_k', 'b1');
is( $node->safe_psql(
		'postgres', q{
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		SELECT string_agg(id::text, ' ' ORDER BY id) FROM tl WHERE k = 7}),
	'1 2 3 5 6', 'b1: index scan');

# (b2) A tagged entry at a TID the LIST holds: the row's own old untagged
# entry.  test_versioned_insert_entry adds (5, tid, xmin), as an in-place
# table AM would for the row's new version.  The LIST is divided after the
# TID, so (5, L, 0) is followed by (5, L, t); at the LIST's last TID nothing
# is divided.  Neither is a duplicate-TID error, and an index scan returns
# the row once for each entry.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tm (id int, k int) USING heap_versioned;
	CREATE INDEX tm_k ON tm USING bark (k);
	INSERT INTO tm SELECT g, 5 FROM generate_series(1, 10) g;
	SELECT test_versioned_insert_entry('tm_k', ctid, xmin::text::int8)
	  FROM tm WHERE id = 4;
});
is(entries('tm_k'),
	"LIST {\"(0,1)\",\"(0,2)\",\"(0,3)\",\"(0,4)\"}\nSINGLE (0,4) tagged\nLIST {\"(0,5)\",\"(0,6)\",\"(0,7)\",\"(0,8)\",\"(0,9)\",\"(0,10)\"}",
	'b2: (K, L, 0) then (K, L, t), the LIST divided');
check_index('tm_k', 'b2');
$node->safe_psql('postgres',
	q{SELECT test_versioned_insert_entry('tm_k', ctid, xmin::text::int8) FROM tm WHERE id = 10});
is(entries('tm_k'),
	"LIST {\"(0,1)\",\"(0,2)\",\"(0,3)\",\"(0,4)\"}\nSINGLE (0,4) tagged\nLIST {\"(0,5)\",\"(0,6)\",\"(0,7)\",\"(0,8)\",\"(0,9)\",\"(0,10)\"}\nSINGLE (0,10) tagged",
	'b2: at the LIST\'s last TID the tagged entry follows it');
is(tags_right('tm_k', 'tm'), '2/2', 'b2: tags are the rows\' xmin');
check_index('tm_k', 'b2 last');
is( $node->safe_psql(
		'postgres', q{
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		SELECT string_agg(ctid::text, ' ') FROM
		  (SELECT ctid FROM tm WHERE k = 5 GROUP BY ctid HAVING count(*) = 2) s}),
	'(0,4) (0,10)', 'b2: an index scan returns both entries of each row');

# (b3) The same at a member in the middle of a POSTING entry, and of a
# coded LIST, on a page that has no room for the parts in place: the leaf
# splits with them, and the tagged entry goes in after.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tp (id int, k int) USING heap_versioned;
	CREATE INDEX tp_k ON tp USING bark (k) WITH (fillfactor = 100);
	INSERT INTO tp SELECT g, g % 3 FROM generate_series(1, 6000) g;
	SELECT test_versioned_insert_entry('tp_k', ctid, xmin::text::int8)
	  FROM tp WHERE id IN (301, 2999, 4501);
});
is(tags_right('tp_k', 'tp'), '3/3', 'b3: tags are the rows\' xmin');
is( $node->safe_psql(
		'postgres', q{
		SELECT count(*) FROM bark_entries('tp_k') e
		 WHERE e.htid IN (SELECT ctid FROM tp WHERE id IN (301, 2999, 4501))
		   AND e.tag IS NULL}),
	'0', 'b3: the old untagged entries are LIST or POSTING members');
check_index('tp_k', 'b3');
is( $node->safe_psql(
		'postgres', q{
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		SELECT count(*) FROM tp WHERE k >= 0}),
	'6003', 'b3: an index scan returns every entry');

# (a) CREATE INDEX of a table whose build scan tags every row (100), half
# of them (50), or none (0), serial and parallel, with duplicate keys (so
# that untagged rows form LISTs that tagged ones divide) and some keys too
# large to stay inline (tagged OVERSIZED entries).
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tb (id int, k int, t text) USING heap_versioned
	  WITH (parallel_workers = 2);
	ALTER TABLE tb ALTER COLUMN t SET STORAGE EXTERNAL;
	INSERT INTO tb SELECT g, g % 97,
	  CASE WHEN g % 500 = 0
	       THEN (SELECT string_agg(md5(g::text || i::text), '') FROM generate_series(1, 120) i)
	       ELSE 'v' || (g % 37) END
	  FROM generate_series(1, 30000) g;
});
for my $pct (100, 50, 0)
{
	for my $par (0, 2)
	{
		my $what = "a: $pct% tagged, $par workers";
		my ($ret, $out, $err) = $node->psql(
			'postgres', qq{
			SET test_versioned_tableam.build_tag_percent = $pct;
			SET max_parallel_maintenance_workers = $par;
			SET min_parallel_table_scan_size = 0;
			SET client_min_messages = debug1;
			CREATE INDEX tb_k ON tb USING bark (k);
			CREATE INDEX tb_t ON tb USING bark (t);
		});
		is($ret, 0, "$what: CREATE INDEX");
		like($err, qr/with request for $par parallel workers/, "$what: workers")
		  if $par > 0;
		my $ntagged = $node->safe_psql('postgres',
			q{SELECT count(*) FROM tb WHERE ((ctid::text::point)[0]::int * 7 + (ctid::text::point)[1]::int) % 100 < }
			  . $pct);
		for my $idx (qw(tb_k tb_t))
		{
			is(tags_right($idx, 'tb'), "$ntagged/$ntagged",
				"$what: $idx tags are the rows' xmin");
			check_index($idx, "$what: $idx");
		}
		is( $node->safe_psql(
				'postgres', q{
				SELECT count(*) FROM bark_entries('tb_t') WHERE shape = 'OVERSIZED' AND tag IS NOT NULL}),
			$pct == 0 ? '0' : $node->safe_psql(
				'postgres', qq{
				SELECT count(*) FROM tb WHERE id % 500 = 0 AND
				  ((ctid::text::point)[0]::int * 7 + (ctid::text::point)[1]::int) % 100 < $pct}),
			"$what: tagged OVERSIZED entries");
		is( $node->safe_psql(
				'postgres', q{
				SET enable_seqscan = off; SET enable_bitmapscan = off;
				SELECT count(*), sum(k) FROM tb WHERE k >= 0}),
			'30000|1439082', "$what: index scan");
		$node->safe_psql('postgres', 'DROP INDEX tb_k, tb_t');
	}
}

# The meta page says whether an index may hold tags: set when the table's
# locator is versioned at build time, by CREATE INDEX, ALTER TABLE ... SET
# ACCESS METHOD and REINDEX.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE th (k int);
	INSERT INTO th SELECT g FROM generate_series(1, 100) g;
	CREATE INDEX th_k ON th USING bark (k);
});
my $flags = q{SELECT flags & 2 FROM bark_metap('th_k')};
is($node->safe_psql('postgres', $flags), '0', 'heap: not built for tags');
$node->safe_psql('postgres', 'ALTER TABLE th SET ACCESS METHOD heap_versioned');
is($node->safe_psql('postgres', $flags), '2', 'SET ACCESS METHOD heap_versioned: built for tags');
$node->safe_psql('postgres', 'REINDEX INDEX th_k');
is($node->safe_psql('postgres', $flags), '2', 'REINDEX: built for tags');
$node->safe_psql('postgres', 'ALTER TABLE th SET ACCESS METHOD heap');
is($node->safe_psql('postgres', $flags), '0', 'SET ACCESS METHOD heap: not built for tags');
# btree under default_btree_implementation = bark is BARK, built for tags
$node->safe_psql('postgres',
	q{SET default_btree_implementation = bark; ALTER TABLE th SET ACCESS METHOD heap_versioned;
	  CREATE INDEX th_b ON th (k)});
is( $node->safe_psql(
		'postgres', q{SELECT a.amname, (bark_metap('th_b')).flags & 2
		  FROM pg_index x JOIN pg_am a ON a.oid = x.indimplam WHERE x.indexrelid = 'th_b'::regclass}),
	'bark|2', 'btree implemented by bark on heap_versioned');

# Crash recovery replays the same records.
$node->stop('immediate');
$node->start;
for my $idx (qw(tl_k tm_k tp_k))
{
	is($node->safe_psql('postgres', "SELECT bark_index_parent_check('$idx', true)"),
		'', "after crash recovery: $idx");
}

$node->wait_for_catchup($standby);
my $log = slurp_file($standby->logfile);
unlike($log, qr/inconsistent page found/, 'no inconsistent page in replay');

done_testing();
