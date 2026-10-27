
# Copyright (c) 2026, PostgreSQL Global Development Group

# BARK storing the version tags heap_versioned hands it: tagged entries from UPDATE, from a helper that adds a tagged
# entry at a TID the index already has, and from CREATE INDEX, serial and
# parallel, including runs of one row's entries of one key in a build and
# where the load may end a leaf in one.  Each step is checked with amcheck,
# against the expected entries and tags (tag = the xmin of the tuple at the
# entry's TID), and on a standby that replays the WAL with
# wal_consistency_checking = bark.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1, no_data_checksums => 1);
$node->append_conf(
	'postgresql.conf', q{
autovacuum = off
wal_consistency_checking = 'bark'
# the page images make much WAL; keep it until the standby has replayed it
wal_keep_size = 1GB
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

# Row by row: each row of `tab` has one entry in `idx`, tagged with the
# row's xmin when the build scan tagged the row (build_tag_percent `pct`,
# by TID), untagged otherwise.  Returns "mismatches/rows".
sub tags_per_row
{
	my ($idx, $tab, $pct) = @_;
	return $node->safe_psql(
		'postgres', qq{
		WITH e AS (SELECT coalesce(m.tid, e.htid) AS tid, e.tag
		             FROM bark_entries('$idx') e
		             LEFT JOIN LATERAL unnest(e.tids) m(tid) ON true),
		     r AS (SELECT ctid, CASE WHEN ((ctid::text::point)[0]::int * 7 +
		                                   (ctid::text::point)[1]::int) % 100 < $pct
		                             THEN xmin::text::int8 END AS want
		             FROM $tab)
		SELECT count(*) FILTER (WHERE r.ctid IS NULL OR e.tid IS NULL OR
		                              e.tag IS DISTINCT FROM r.want) || '/' || count(*)
		  FROM e FULL JOIN r ON r.ctid = e.tid});
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
			is(tags_per_row($idx, 'tb', $pct), '0/30000',
				"$what: $idx has one entry per row, tagged as the scan tagged it");
			check_index($idx, "$what: $idx");
		}
		# tagged SINGLEs go through the sort and are packed as untagged
		# entries are, not split about 50/50 by inserts after the load
		cmp_ok( $node->safe_psql(
				'postgres', q{
				SELECT avg(s.free_size)::int
				  FROM generate_series(1, pg_relation_size('tb_k') / current_setting('block_size')::int - 1) b,
				       LATERAL bark_page_stats('tb_k', b) s
				 WHERE s.type = 'leaf' AND s.bark_next <> 0}),
			'<', 2048, "$what: tb_k's leaves are packed")
		  if $pct > 0;
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

# (a2) Tagged entries inserted into a packed build of untagged LISTs, as a
# table AM that keeps locators adds them after CREATE INDEX: each divides
# the LIST holding its TID, and on full leaves the leaf splits with the
# divided parts in place.  (CREATE INDEX itself did this for tagged rows
# before it sorted them.)
$node->safe_psql(
	'postgres', q{
	SET test_versioned_tableam.build_tag_percent = 0;
	CREATE INDEX tb_k ON tb USING bark (k) WITH (fillfactor = 100);
	SELECT count(test_versioned_insert_entry('tb_k', ctid, x))
	  FROM (SELECT ctid, xmin::text::int8 AS x FROM tb
	         WHERE ((ctid::text::point)[0]::int * 7 + (ctid::text::point)[1]::int) % 100 < 50
	         ORDER BY ctid) s;
});
my $nhalf = $node->safe_psql('postgres',
	q{SELECT count(*) FROM tb WHERE ((ctid::text::point)[0]::int * 7 + (ctid::text::point)[1]::int) % 100 < 50});
is(tags_right('tb_k', 'tb'), "$nhalf/$nhalf", 'a2: the tags are the rows\' xmin');
is( $node->safe_psql(
		'postgres', q{
		SELECT count(*) FILTER (WHERE tag IS NULL) || ' ' || count(*) FILTER (WHERE tag IS NOT NULL)
		  FROM (SELECT coalesce(m.tid, e.htid), e.tag FROM bark_entries('tb_k') e
		          LEFT JOIN LATERAL unnest(e.tids) m(tid) ON true) s}),
	"30000 $nhalf", 'a2: every row keeps its untagged entry');
check_index('tb_k', 'a2');
is( $node->safe_psql(
		'postgres', q{
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		SELECT count(*) FROM tb WHERE k >= 0}),
	30000 + $nhalf, 'a2: an index scan returns every entry');
$node->safe_psql('postgres', 'DROP INDEX tb_k; RESET test_versioned_tableam.build_tag_percent');

# (b) Runs of entries equal on key and heap TID in one build.  With
# build_history = n, heap_versioned's build scan hands each row it tags n
# more times, tagged xmin - n .. xmin - 1 (see tvh_build_callback): one
# row's versions with one key.  bark_load does not end a leaf inside such a
# run, but moves the run's entries on the page to the next leaf, unless they
# fill the page or take more than a quarter of it (bark_build_run_tail);
# a leaf whose lower bound keeps a tag has BARK_LOWBOUND_TAGGED.  The extra
# tags are stale, which stale_tags = live accepts.
my $inj = ($ENV{enable_injection_points} // '') eq 'yes';
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points') if $inj;
$node->safe_psql(
	'postgres', q{
	-- each leaf: its high key's tag, its first and last data entries'
	-- (TID, key), and the run of entries of the last one's (TID, key) that
	-- ends the page: entries and bytes, as bark_build_run_tail counts them
	CREATE FUNCTION leaf_runs(idx regclass)
	  RETURNS TABLE (blk int, next int8, flags text[], hitag int8,
	                 ftid tid, fdata text, ftag int8, ltid tid, ldata text,
	                 tailn int8, tailbytes int8, ndata int8)
	  LANGUAGE sql AS $$
	  WITH l AS (SELECT b::int AS blk, s.bark_next AS next, s.flags
	               FROM generate_series(1, pg_relation_size(idx) / current_setting('block_size')::int - 1) b,
	                    LATERAL bark_page_stats(idx::text, b) s
	              WHERE s.type = 'leaf'),
	       i AS MATERIALIZED (SELECT l.blk, l.next, it.* FROM l, LATERAL bark_page_items(idx::text, l.blk) it),
	       d AS MATERIALIZED (SELECT * FROM i
	              WHERE shape IN ('SINGLE', 'LIST', 'POSTING', 'OVERSIZED')
	                AND NOT (itemoffset = 1 AND next <> 0)),
	       f AS (SELECT DISTINCT ON (blk) blk, htid, data, tag FROM d ORDER BY blk, itemoffset),
	       x AS (SELECT DISTINCT ON (blk) blk, htid, data FROM d ORDER BY blk, itemoffset DESC),
	       c AS (SELECT d.blk, max(d.itemoffset) AS off FROM d JOIN x USING (blk)
	              WHERE (d.htid, d.data) IS DISTINCT FROM (x.htid, x.data) GROUP BY d.blk),
	       t AS (SELECT d.blk, count(*) AS n, sum((d.itemlen + 7) / 8 * 8 + 4) AS bytes
	               FROM d LEFT JOIN c USING (blk)
	              WHERE d.itemoffset > coalesce(c.off, 0) GROUP BY d.blk)
	  SELECT l.blk, l.next, coalesce(l.flags, '{}'), h.tag, f.htid, f.data, f.tag, x.htid, x.data,
	         t.n, t.bytes, (SELECT count(*) FROM d WHERE d.blk = l.blk)
	    FROM l JOIN f USING (blk) JOIN x USING (blk) JOIN t USING (blk)
	    LEFT JOIN i h ON h.blk = l.blk AND h.itemoffset = 1 AND l.next <> 0 $$;
	-- leaf boundaries inside a run, and those of them where the run's part
	-- on the left leaf neither fills it nor takes over a quarter of a page
	CREATE FUNCTION run_cuts(idx regclass, OUT cuts int8, OUT bad int8)
	  LANGUAGE sql AS $$
	  WITH r AS MATERIALIZED (SELECT * FROM leaf_runs(idx))
	  SELECT count(*),
	         count(*) FILTER (WHERE a.tailn < a.ndata AND
	                          a.tailbytes <= current_setting('block_size')::int / 4)
	    FROM r a JOIN r b ON b.blk = a.next
	   WHERE b.ftid = a.ltid AND b.fdata = a.ldata AND b.ftag IS NOT NULL $$;
	-- leaf high keys with a tag, and whether the flagged leaves are
	-- exactly those whose left sibling's high key has one
	CREATE FUNCTION tag_bounds(idx regclass, OUT tagged int8, OUT flags_right bool)
	  LANGUAGE sql AS $$
	  WITH r AS MATERIALIZED (SELECT * FROM leaf_runs(idx))
	  SELECT count(*) FILTER (WHERE a.hitag IS NOT NULL),
	         bool_and((a.hitag IS NOT NULL) = ('lowbound_tagged' = ANY (b.flags)))
	    FROM r a JOIN r b ON b.blk = a.next $$;
});

# Every row of `tab` has n + 1 entries in `idx`, tagged xmin - n .. xmin.
sub history_right
{
	my ($idx, $tab, $n) = @_;
	return $node->safe_psql(
		'postgres', qq{
		SELECT count(*) FILTER (WHERE e.n = $n + 1 AND e.lo = r.x - $n AND e.hi = r.x
		                          AND e.d = $n + 1) || '/' || count(*)
		  FROM (SELECT ctid, xmin::text::int8 AS x FROM $tab) r
		  LEFT JOIN (SELECT htid, count(*) AS n, count(DISTINCT tag) AS d,
		                    min(tag) AS lo, max(tag) AS hi
		               FROM bark_entries('$idx') GROUP BY htid) e ON e.htid = r.ctid});
}

# (b1) Short runs: 2000 rows, each with 21 entries (about 750 bytes), so
# that nearly every leaf would end inside one.  Each run moves whole to the
# next leaf: no high key keeps a tag, no leaf is flagged.  With the rule off
# (an injection point) the same build ends leaves inside runs, which is
# what the first checks would catch.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tr1 (id int, k int) USING heap_versioned
	  WITH (parallel_workers = 2);
	INSERT INTO tr1 SELECT g, g % 500 FROM generate_series(1, 2000) g;
});
for my $par (0, 2)
{
	for my $rule (0, 1)
	{
		next if !$rule && !$inj;
		my $what = "b1: runs of 21, $par workers" . ($rule ? '' : ', rule off');
		$node->safe_psql('postgres',
			q{SELECT injection_points_attach('bark-build-ignore-runs', 'notice')})
		  if !$rule;
		my ($ret, $out, $err) = $node->psql(
			'postgres', qq{
			SET test_versioned_tableam.build_tag_percent = 100;
			SET test_versioned_tableam.build_history = 20;
			SET max_parallel_maintenance_workers = $par;
			SET min_parallel_table_scan_size = 0;
			SET client_min_messages = debug1;
			CREATE INDEX tr1_k ON tr1 USING bark (k);
		});
		is($ret, 0, "$what: CREATE INDEX");
		like($err, qr/with request for $par parallel workers/, "$what: workers")
		  if $par > 0;
		$node->safe_psql('postgres',
			q{SELECT injection_points_detach('bark-build-ignore-runs')})
		  if !$rule;
		is(history_right('tr1_k', 'tr1', 20), '2000/2000',
			"$what: every row has its 21 entries, tagged xmin - 20 .. xmin");
		cmp_ok($node->safe_psql('postgres', q{SELECT count(*) FROM leaf_runs('tr1_k')}),
			'>', 100, "$what: leaves");
		my ($cuts, $bad) = split /\|/,
		  $node->safe_psql('postgres', q{SELECT * FROM run_cuts('tr1_k')});
		my ($tagged, $flagsok) = split /\|/,
		  $node->safe_psql('postgres', q{SELECT * FROM tag_bounds('tr1_k')});
		if ($rule)
		{
			is($cuts, '0', "$what: no leaf ends inside a run");
			is($tagged, '0', "$what: no high key has a tag");
		}
		else
		{
			cmp_ok($bad, '>', 50, "$what: leaves end inside short runs");
			is($tagged, $cuts, "$what: each such leaf's high key has a tag");
		}
		is($flagsok, 't', "$what: the leaves after them, and only those, are flagged");
		is( $node->safe_psql(
				'postgres', q{
				SET test_versioned_tableam.stale_tags = live;
				SET enable_seqscan = off; SET enable_bitmapscan = off;
				SELECT count(*), count(DISTINCT id) FROM tr1 WHERE k >= 0}),
			'42000|2000', "$what: an index scan returns every entry");
		check_index('tr1_k', $what);
		$node->safe_psql('postgres', 'DROP INDEX tr1_k') if !$rule || !$par;	# keep the last
	}
}

# (b1p) The same with prefix-coded leaves (prefix_compression, a text key
# whose values share a long prefix): the run's entries leave a coded page
# and are coded again for the next leaf's prefix.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tr3 (id int, t text) USING heap_versioned;
	INSERT INTO tr3 SELECT g, 'a-prefix-every-key-shares-' || lpad((g % 500)::text, 4, '0')
	  FROM generate_series(1, 2000) g;
	SET test_versioned_tableam.build_tag_percent = 100;
	SET test_versioned_tableam.build_history = 20;
	CREATE INDEX tr3_t ON tr3 USING bark (t) WITH (prefix_compression = on);
	RESET test_versioned_tableam.build_history;
});
is(history_right('tr3_t', 'tr3', 20), '2000/2000',
	'b1p: every row has its 21 entries, tagged xmin - 20 .. xmin');
cmp_ok( $node->safe_psql(
		'postgres', q{
		SELECT count(*) FROM leaf_runs('tr3_t') WHERE 'prefix' = ANY (flags)}),
	'>', 50, 'b1p: the leaves are prefix-coded');
is($node->safe_psql('postgres', q{SELECT * FROM run_cuts('tr3_t')}), '0|0',
	'b1p: no leaf ends inside a run');
is($node->safe_psql('postgres', q{SELECT tagged FROM tag_bounds('tr3_t')}), '0',
	'b1p: no high key has a tag');
check_index('tr3_t', 'b1p');

# (b2) A long run: 100 rows, of which only row 100 (TID (0,100), the
# highest key) is tagged, with 500 more entries.  The leaf that holds rows
# 1-99 ends with over a quarter of a page of the run, and keeps it; the
# next is all run.  Both high keys keep a tag, and the leaves after them
# are flagged.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tr2 (id int, k int) USING heap_versioned;
	INSERT INTO tr2 SELECT g, g FROM generate_series(1, 100) g;
	SET test_versioned_tableam.build_tag_percent = 1;
	SET test_versioned_tableam.build_history = 500;
	CREATE INDEX tr2_k ON tr2 USING bark (k);
});
is($node->safe_psql('postgres', q{SELECT ctid FROM tr2 WHERE id = 100}),
	'(0,100)', 'b2: row 100 is the one tagged');
is(history_right('tr2_k', 'tr2 WHERE id = 100', 500), '1/1',
	'b2: row 100 has its 501 entries, tagged xmin - 500 .. xmin');
is( $node->safe_psql(
		'postgres', q{
		SELECT count(DISTINCT htid) FILTER (WHERE tag IS NULL) || ' ' || count(*)
		  FROM bark_entries('tr2_k') WHERE htid <> '(0,100)'}),
	'99 99', 'b2: the other rows have one untagged entry each');
my $leaves = q{
	SELECT string_agg(ftid || '-' || ltid || ' ' || (hitag IS NOT NULL) || ' ' ||
	                  ('lowbound_tagged' = ANY (flags)) || ' ' || (tailn = ndata),
	                  ', ' ORDER BY blk)
	  FROM leaf_runs('tr2_k')};
like($node->safe_psql('postgres', $leaves),
	qr/^\(0,1\)-\(0,100\) true false false, \(0,100\)-\(0,100\) true true true, \(0,100\)-\(0,100\) /,
	'b2: the first leaf keeps its part of the run, the second is all run; both high keys have a tag, the leaves after them are flagged');
is( $node->safe_psql(
		'postgres', q{
		SELECT tailbytes > current_setting('block_size')::int / 4
		  FROM leaf_runs('tr2_k') ORDER BY blk LIMIT 1}),
	't', 'b2: the first leaf\'s part is over a quarter of a page');
is($node->safe_psql('postgres', q{SELECT bad FROM run_cuts('tr2_k')}),
	'0', 'b2: no leaf ends inside a run that it could have moved');
is($node->safe_psql('postgres', q{SELECT * FROM tag_bounds('tr2_k')}),
	'2|t', 'b2: two high keys have a tag, and the leaves after them are flagged');
check_index('tr2_k', 'b2');
$node->safe_psql('postgres',
	'RESET test_versioned_tableam.build_tag_percent; RESET test_versioned_tableam.build_history');

# (d) The builds' WAL, replayed by crash recovery: the standby replayed it
# with wal_consistency_checking in check_index.
my %before = map { $_ => $node->safe_psql('postgres', "SELECT * FROM bark_entries('$_')") }
  qw(tr1_k tr2_k);
$node->stop('immediate');
$node->start;
for my $idx (qw(tr1_k tr2_k))
{
	is($node->safe_psql('postgres', "SELECT * FROM bark_entries('$idx')"),
		$before{$idx}, "b, crash recovery: $idx has its entries");
	check_index($idx, "b, crash recovery: $idx");
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

# A plain multikey BARK index built on a versioned table has
# BARK_META_MKTAGS and stores tags on its keys (amindexcanversion): an
# update that affects it keeps the locator (TU_Affected) and tags its new
# key entries, as an update that affects only a scalar BARK index tags
# that index's entry.  A GIN-interface multikey index stores no tags: an
# update that affects it moves the row (TU_All) and its entries stay
# untagged.  A version 2 index on a versioned table stores no tags either:
# the update moves the row, with no error.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tmk (id int, s int, a int4[]) USING heap_versioned;
	CREATE INDEX tmk_s ON tmk USING bark (s);
	CREATE INDEX tmk_a ON tmk USING bark (a bark_int4_array_ops);
	INSERT INTO tmk SELECT g, g, ARRAY[g, g + 1] FROM generate_series(1, 50) g;
});
is( $node->safe_psql(
		'postgres', q{
		SELECT string_agg(i::text || '=' || pg_indexam_has_property(a.oid, 'can_version'), ',')
		  FROM unnest(ARRAY['tmk_s', 'tmk_a']::regclass[]) i
		  JOIN pg_class c ON c.oid = i JOIN pg_am a ON a.oid = c.relam}),
	'tmk_s=true,tmk_a=true', 'mk: the AM can version');
my $kept = q{SELECT n FROM test_versioned_counts() WHERE counter = 'update_kept'};
sub update_kept
{
	my ($sql) = @_;
	return $node->safe_psql(
		'postgres', qq{
		DO \$\$BEGIN PERFORM test_versioned_reset(); END\$\$;
		SET test_versioned_tableam.keep_locator = pretend;
		$sql;
		$kept});
}
is(update_kept(q{UPDATE tmk SET s = s + 100, a = a || 0 WHERE id = 1}),
	'1', 'mk: scalar and multikey both affected: the locator is kept');
is(update_kept(q{UPDATE tmk SET a = a || 1 WHERE id = 2}),
	'1', 'mk: multikey only affected: the locator is kept');
is(update_kept(q{UPDATE tmk SET s = s + 100 WHERE id = 3}),
	'1', 'mk: scalar only affected: the locator is kept');
# Each update tags the entries it makes in the indexes it affected: rows 1
# and 3 in tmk_s, rows 1 and 2 (three keys each) in tmk_a.
is( $node->safe_psql(
		'postgres', q{
		SELECT (SELECT count(*) FROM bark_entries('tmk_s') WHERE tag IS NOT NULL) || ' ' ||
		       (SELECT count(*) FROM bark_entries('tmk_a') WHERE tag IS NOT NULL)}),
	'2 6', 'mk: both indexes have tagged entries');
is(tags_right('tmk_s', 'tmk'), '2/2', 'mk: the tags are the xmins');
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tmg (id int, a int4[]) USING heap_versioned;
	CREATE INDEX tmg_a ON tmg USING bark (a bark_gin_int4_array_ops);
	INSERT INTO tmg SELECT g, ARRAY[g, g + 1] FROM generate_series(1, 50) g;
});
is(update_kept(q{UPDATE tmg SET a = a || 1 WHERE id = 2}),
	'0', 'mk: GIN-interface affected: the row moves');
is( $node->safe_psql(
		'postgres', q{SELECT count(*) FROM bark_entries('tmg_a') WHERE tag IS NOT NULL}),
	'0', 'mk: the GIN-interface index has no tagged entry');
# keep_locator = pretend leaves the moved row without a tmk_a entry, so no
# heapallindexed here
for my $idx (qw(tmk_s tmk_a tmg_a))
{
	is($node->safe_psql('postgres', "SELECT bark_index_parent_check('$idx')"),
		'', "mk: $idx checks");
}

# An exclusion constraint on a multikey column stores no tags either.  An
# update that keeps the column's keys (barkcomparedatums) and changes a
# partial scalar index's column keeps the locator (TU_Affected): the
# constraint's index keeps its entries, and core checks the constraint for
# the new value after the update (ExecCheckHotExclusion).  Under = values
# with the same keys need not conflict alike: {2,1} has {1,2,2}'s keys and
# equals row 2.  (The scalar index is partial so that core can tell it is
# affected without making the update move the row.)
$node->safe_psql(
	'postgres', q{
	CREATE TABLE tmx (id int, s int, a int4[],
	  CONSTRAINT tmx_a EXCLUDE USING bark (a bark_int4_array_ops WITH =))
	  USING heap_versioned;
	CREATE INDEX tmx_s ON tmx USING bark (s) WHERE s > 0;
	INSERT INTO tmx VALUES (1, 1, '{1,2}'), (2, 2, '{2,1}');
});
is(update_kept(q{UPDATE tmx SET s = 10, a = '{1,2,2}' WHERE id = 1}),
	'1', 'mx: same keys, scalar changed: the locator is kept');
my ($mxret, $mxout, $mxerr) = $node->psql(
	'postgres', q{
	SET test_versioned_tableam.keep_locator = pretend;
	UPDATE tmx SET s = 11, a = '{2,1}' WHERE id = 1});
like(
	$mxerr,
	qr/conflicting key value violates exclusion constraint "tmx_a"/,
	'mx: the constraint is checked after an update that kept the locator');
# tmx_s's tagged entries: the update's, and the failed update's, made
# before the check failed
is( $node->safe_psql(
		'postgres', q{
		SELECT (SELECT count(*) FROM bark_entries('tmx_s') WHERE tag IS NOT NULL) || ' ' ||
		       (SELECT count(*) FROM bark_entries('tmx_a') WHERE tag IS NOT NULL)}),
	'2 0', 'mx: the scalar index has tagged entries, the constraint\'s none');
is($node->safe_psql('postgres', q{SELECT bark_index_parent_check('tmx_a')}),
	'', 'mx: tmx_a checks');

$node->safe_psql(
	'postgres', q{
	CREATE TABLE tv2 (id int, k int) USING heap_versioned;
	CREATE INDEX tv2_k ON tv2 USING bark (k);
	INSERT INTO tv2 SELECT g, g FROM generate_series(1, 100) g;
});
my $tv2path = $node->data_dir . '/'
  . $node->safe_psql('postgres', q{SELECT pg_relation_filepath('tv2_k')});
$node->stop;
open(my $fh, '+<:raw', $tv2path) or die "open $tv2path: $!";
seek($fh, 24 + 4, 0) or die "seek: $!";
print $fh pack('L', 2);		# version 2
seek($fh, 24 + 20, 0) or die "seek: $!";
print $fh pack('L', 0);		# bark_flags: no BARK_META_VERSIONED
close $fh;
$node->start;
is( $node->safe_psql('postgres', q{SELECT version, flags FROM bark_metap('tv2_k')}),
	'2|0', 'v2: a version 2 index on a versioned table');
is(update_kept(q{UPDATE tv2 SET k = k + 1000 WHERE id <= 10}),
	'0', 'v2: an update affecting it moves the rows, with no error');
is( $node->safe_psql(
		'postgres', q{
		SELECT count(*) FILTER (WHERE tag IS NOT NULL) || ' ' || count(*) FROM bark_entries('tv2_k')}),
	'0 110', 'v2: its entries are untagged');
is( $node->safe_psql(
		'postgres', q{
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		SELECT count(*), sum(k) FROM tv2 WHERE k > 0}),
	'100|15050', 'v2: index scan');
is($node->safe_psql('postgres', q{SELECT bark_index_parent_check('tv2_k', true)}),
	'', 'v2: checks');

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

# a clean stop, so that backends exit normally (coverage counts)
$standby->stop;
$node->stop;
done_testing();
