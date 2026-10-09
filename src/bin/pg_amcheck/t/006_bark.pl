
# Copyright (c) 2026, PostgreSQL Global Development Group

# pg_amcheck checks bark indexes with bark_index_check and
# bark_index_parent_check, given an amcheck that has them (1.7).
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('test');
$node->init;
$node->start;
my $port = $node->port;

$node->safe_psql(
	'postgres', q(
	CREATE EXTENSION amcheck;
	CREATE TABLE barktbl (i int4, t text);
	INSERT INTO barktbl SELECT g, 'row ' || g FROM generate_series(1, 5000) g;
	CREATE INDEX barkidx ON barktbl USING bark (i);
	CREATE INDEX barkidx_t ON barktbl USING bark (t) INCLUDE (i);
	CREATE INDEX btidx ON barktbl USING btree (i);
));

# Every check passes on a sound index, and the bark indexes are among the
# ones checked, by their own functions.
for my $opts ([], ['--heapallindexed'], ['--parent-check'],
	['--parent-check', '--heapallindexed'])
{
	$node->command_like(
		[ 'pg_amcheck', '--port' => $port, @$opts, 'postgres' ],
		qr/^$/,
		"pg_amcheck @$opts reports no corruption");
}
$node->command_checks_all(
	[
		'pg_amcheck', '--port' => $port,
		'--echo', '--heapallindexed', '--index' => 'barkidx', 'postgres'
	],
	0,
	[qr/bark_index_check\(index := c\.oid, heapallindexed := true\)/],
	[],
	'pg_amcheck checks a bark index with bark_index_check');
$node->command_checks_all(
	[
		'pg_amcheck', '--port' => $port,
		'--echo', '--parent-check', '--table' => 'barktbl', 'postgres'
	],
	0,
	[
		qr/bark_index_parent_check\(index := c\.oid, heapallindexed := false\)/,
		qr/bt_index_parent_check\(index := c\.oid/
	],
	[],
	'pg_amcheck checks a table\'s bark and btree indexes, each with its own function');

# Rows inserted while the indexes are not ready have no entries; only
# heapallindexed finds them.  The btree index is damaged the same way, for
# comparison.
$node->safe_psql(
	'postgres', q(
	UPDATE pg_index SET indisready = false
	  WHERE indexrelid IN ('barkidx'::regclass, 'btidx'::regclass);
	INSERT INTO barktbl VALUES (5001, 'row 5001');
	UPDATE pg_index SET indisready = true
	  WHERE indexrelid IN ('barkidx'::regclass, 'btidx'::regclass);
));
$node->command_like([ 'pg_amcheck', '--port' => $port, 'postgres' ],
	qr/^$/, 'pg_amcheck without heapallindexed reports no corruption');
for my $opts (['--heapallindexed'], ['--parent-check', '--heapallindexed'])
{
	$node->command_checks_all(
		[ 'pg_amcheck', '--port' => $port, @$opts, 'postgres' ],
		2,
		[
			qr/bark index "postgres\.public\.barkidx":/,
			qr/lacks matching index tuple within index "barkidx"/,
			qr/btree index "postgres\.public\.btidx":/,
			qr/lacks matching index tuple within index "btidx"/
		],
		[],
		"pg_amcheck @$opts reports the missing entries");
}

# With amcheck 1.6, which has no heapallindexed for bark, bark indexes are
# not selected, as indexes of other access methods are not.
$node->safe_psql(
	'postgres', q(
	DROP EXTENSION amcheck;
	CREATE EXTENSION amcheck VERSION '1.6';
));
$node->command_checks_all(
	[
		'pg_amcheck', '--port' => $port,
		'--echo', '--heapallindexed', 'postgres'
	],
	2,
	[qr/lacks matching index tuple within index "btidx"/],
	[],
	'pg_amcheck with amcheck 1.6 checks btree indexes only');
$node->command_checks_all(
	[ 'pg_amcheck', '--port' => $port, '--index' => 'barkidx', 'postgres' ],
	1,
	[qr/^$/],
	[qr/no btree indexes to check matching "barkidx"/],
	'pg_amcheck with amcheck 1.6 does not select a bark index');

$node->stop;
done_testing();
