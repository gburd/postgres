
# Copyright (c) 2026, PostgreSQL Global Development Group

# Probe (temporary): the layout of a run of one row's tagged entries.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf', "autovacuum = off\nwal_consistency_checking = 'bark'\n");
$node->start;
$node->safe_psql('postgres', q{
	CREATE EXTENSION test_versioned_tableam;
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pageinspect;
	DO $$ BEGIN FOR i IN 1..2000 LOOP PERFORM pg_current_xact_id(); COMMIT; END LOOP; END $$;
	CREATE TABLE tl (id int, k int) USING heap_versioned;
	INSERT INTO tl SELECT g, CASE WHEN g = 100 THEN 150 ELSE g END FROM generate_series(1, 300) g;
	SET test_versioned_tableam.build_tag_percent = 100;
	CREATE INDEX tl_k ON tl USING bark (k);
	SET test_versioned_tableam.stale_tags = live;
	SELECT count(test_versioned_insert_entry('tl_k', ctid, 2 * g)) FROM tl, generate_series(1, 600) g WHERE id = 150;
});
print STDERR $node->safe_psql('postgres', q{
	SELECT b, s.type, s.bark_prev, s.bark_next, s.flags, s.live_items, s.bark_level,
	       (SELECT string_agg(i.itemoffset || ':' || i.shape || ':' || coalesce(i.htid::text,'') || ':' || coalesce(i.tag::text,'') || ':' || coalesce(i.downlink::text, '') || ':' || i.data, ' | ' ORDER BY i.itemoffset)
	          FROM bark_page_items('tl_k', b) i WHERE i.itemoffset <= 3 OR i.itemoffset >= s.live_items - 1)
	  FROM generate_series(1, pg_relation_size('tl_k') / 8192 - 1) b, LATERAL bark_page_stats('tl_k', b) s ORDER BY b});
print STDERR $node->safe_psql('postgres', q{SELECT xmin, ctid, * FROM tl WHERE k = 150});
ok(1);
done_testing();
