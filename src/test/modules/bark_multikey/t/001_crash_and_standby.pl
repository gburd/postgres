
# Copyright (c) 2026, PostgreSQL Global Development Group

# A multikey BARK index through a crash and on a standby.
#
# 1. A crash between two keys of a row: the insert stops after placing its
#    first key, before descending again for the next, and the server is
#    killed.  After recovery the row's keys already written are entries of
#    a dead tuple; bark_index_check accepts the index, VACUUM removes them,
#    and the multikey flag the row set stays set.
# 2. A standby replays inserts, a CREATE INDEX, VACUUM, the flag's and the
#    count's page images, and markers, with wal_consistency_checking
#    comparing every page it rebuilds with the primary's, and its index
#    holds the primary's entries.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', qq[
autovacuum = off
wal_consistency_checking = 'Bark'
wal_keep_size = 1GB
]);
$node->start;

$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE EXTENSION bark_multikey;
CREATE TABLE t (id int, a int4[]);
INSERT INTO t SELECT g, ARRAY[g] FROM generate_series(1, 1000) g;
CREATE INDEX t_a ON t USING bark (a bark_int4_array_ops);
]);

## 1: a crash between two keys of a row
SKIP:
{
	skip 'Injection points not supported by this build', 8
	  unless $ENV{enable_injection_points} eq 'yes';

	$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');

	# Keys 5 and 900 are on different leaves, so the second key descends
	# again, where the injection point waits.
	my $s1 = $node->background_psql('postgres', on_error_stop => 0);
	$s1->query_safe(
		q[
SELECT injection_points_set_local();
SELECT injection_points_attach('bark-insert-redescend', 'wait');
]);
	$s1->query_until(
		qr/starting_insert/, q[
\echo starting_insert
INSERT INTO t VALUES (2000, '{5,900}');
]);
	$node->wait_for_event('client backend', 'bark-insert-redescend');

	is( $node->safe_psql(
			'postgres',
			"SELECT count(*) FROM bark_multikey_entries('t_a') WHERE key = 5"),
		'2',
		'the row\'s first key is in the index before the crash');

	# kill -9 the waiting backend.  TAP clusters run with restart_after_crash
	# off, so the postmaster shuts down, and the start below recovers.
	my $pid = $node->safe_psql('postgres',
		"SELECT pid FROM pg_stat_activity WHERE wait_event = 'bark-insert-redescend'"
	);
	is(PostgreSQL::Test::Utils::system_log('pg_ctl', 'kill', 'KILL', $pid),
		0, 'killed the inserting backend with SIGKILL');
	$s1->{run}->finish;
	$node->stop('immediate', fail_ok => 1);
	$node->start;
	ok($node->log_contains('database system was interrupted'),
		'the server went through crash recovery');

	is($node->safe_psql('postgres', "SELECT bark_index_check('t_a')"),
		'', 'bark_index_check accepts the index after the crash');
	is( $node->safe_psql(
			'postgres',
			"SELECT count(*) FROM bark_multikey_entries('t_a') WHERE key IN (5, 900)"),
		'3',
		'the first key survived the crash, the second was never written');
	is( $node->safe_psql(
			'postgres', "SELECT multikey FROM bark_multikey_meta('t_a')"),
		't',
		'the flag the row set before its first key survived the crash');
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t WHERE id = 2000'),
		'0', 'the row did not commit');

	$node->safe_psql('postgres', 'VACUUM t');
	is( $node->safe_psql(
			'postgres',
			"SELECT bark_index_check('t_a'), (SELECT count(*) FROM bark_multikey_entries('t_a') WHERE key = 5), nkeys FROM bark_multikey_meta('t_a')"
		),
		'|1|1000',
		'VACUUM removes the dead row\'s entry and counts the members left');
}

## 2: a standby
$node->backup('backup');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'backup', has_streaming => 1);
$standby->start;

$node->safe_psql(
	'postgres', q[
CREATE TABLE s (id int, a int4[]);
CREATE INDEX s_a ON s USING bark (a bark_int4_array_marked_ops);
INSERT INTO s VALUES (1, '{1}'), (2, NULL), (3, '{}');
INSERT INTO s SELECT g, ARRAY(SELECT (g * 7 + i * 13) % 5000
                              FROM generate_series(1, 20) i)
  FROM generate_series(1, 1500) g;
INSERT INTO s VALUES (4, '{NULL,3,3}');
CREATE INDEX s_id_a ON s USING bark (id, a bark_int4_array_marked_ops);
CREATE INDEX s_a_scan ON s USING bark (a bark_int4_array_ops);
DELETE FROM s WHERE id % 4 = 0;
VACUUM s;
INSERT INTO t VALUES (3000, '{1,2,3}');
]);
$node->wait_for_replay_catchup($standby);

my $entries = q[
SELECT count(*), count(*) FILTER (WHERE marker), sum(hashtext(key::text || tid::text))
  FROM bark_multikey_entries('%s')];
foreach my $idx ('s_a', 's_id_a', 't_a')
{
	my $q = sprintf($entries, $idx);
	my $primary = $node->safe_psql('postgres', $q);
	is($standby->safe_psql('postgres', $q),
		$primary, "standby index $idx holds the primary's entries");
	is($standby->safe_psql('postgres', "SELECT bark_index_check('$idx')"),
		'', "standby index $idx passes bark_index_check");
	is( $standby->safe_psql(
			'postgres', "SELECT * FROM bark_multikey_meta('$idx')"),
		$node->safe_psql('postgres', "SELECT * FROM bark_multikey_meta('$idx')"),
		"standby index $idx has the primary's meta page");
}
is( $standby->safe_psql(
		'postgres', "SELECT bark_multikey_has_marker('s_a', -2147483648)"),
	't', 'the standby reads the marker');
is( $standby->safe_psql(
		'postgres',
		"SELECT multikey, nkeys > 0 FROM bark_multikey_meta('s_a')"),
	"t|t", 'the standby has the flag and the count');

# Scans of a multikey index on the standby return the primary's rows, each
# once, by index and by bitmap scan.  (The marked class has no procedure 8,
# so s_a and s_id_a cannot be scanned on the multikey column.)
foreach my $qual (
	"a && '{7,13,20,33}'", "a @> '{}'", "a <@ '{3}'",
	"id < 500 AND a && '{7,13,20,33}'")
{
	my $q =
	  "SELECT count(*) || ':' || count(DISTINCT id) || ':' || coalesce(sum(id), 0) FROM s WHERE $qual";
	my $want = $node->safe_psql('postgres',
		"SET enable_indexscan = off; SET enable_bitmapscan = off; $q");
	foreach my $mode ('enable_bitmapscan', 'enable_indexscan')
	{
		is( $standby->safe_psql(
				'postgres',
				"SET enable_seqscan = off; SET enable_indexonlyscan = off; "
				  . "SET enable_bitmapscan = off; SET enable_indexscan = off; "
				  . "SET $mode = on; $q"),
			$want,
			"standby $mode scan of s WHERE $qual matches the primary");
	}
}

# wal_consistency_checking would have stopped the standby on a mismatch.
ok($standby->safe_psql('postgres', 'SELECT 1') eq '1',
	'standby is alive after replaying with wal_consistency_checking');

$standby->stop;
$node->stop;

done_testing();
