
# Copyright (c) 2026, PostgreSQL Global Development Group

# pg_amcheck checks bark indexes with bark_index_check and
# bark_index_parent_check, given an amcheck that has them (1.7), and so
# btree indexes that bark implements (pg_index.indimplam) too.
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
	SET default_btree_implementation = bark;
	CREATE INDEX implidx ON barktbl USING btree (i);
	CREATE UNIQUE INDEX implidx_u ON barktbl (i) INCLUDE (t);
	RESET default_btree_implementation;
));
is( $node->safe_psql(
		'postgres', q(
	SELECT string_agg(c.relname || ':' || a.amname || ':' ||
	                  coalesce(ia.amname, '-'), ' ' ORDER BY c.relname)
	FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
	  JOIN pg_am a ON a.oid = c.relam LEFT JOIN pg_am ia ON ia.oid = i.indimplam
	WHERE i.indrelid = 'barktbl'::regclass)),
	'barkidx:bark:- barkidx_t:bark:- btidx:btree:- implidx:btree:bark implidx_u:btree:bark',
	'implidx and implidx_u are btree indexes that bark implements');

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

# A btree index that bark implements is checked as a bark index, whether
# named or found through its table, and bt_index_check() is never called on
# it (it would refuse it).
for my $sel (['--index' => 'implidx'], ['--index' => 'implidx_u'],
	['--table' => 'barktbl'])
{
	my ($stdout, $stderr) = run_command(
		[
			'pg_amcheck', '--port' => $port, '--echo', '--heapallindexed',
			'--checkunique', @$sel, 'postgres'
		]);
	my @bark = $stdout =~ /bark_index_check\(index := c\.oid[^\n]*\n[^\n]*WHERE c\.oid = (\d+)/g;
	my @bt = $stdout =~ /bt_index_check\(index := c\.oid[^\n]*\n[^\n]*WHERE c\.oid = (\d+)/g;
	my $names = sub {
		my @oids = @_;
		return '' unless @oids;
		return $node->safe_psql('postgres',
			    "SELECT string_agg(relname, ' ' ORDER BY relname) FROM pg_class"
			  . " WHERE oid IN (" . join(', ', @oids) . ")"
			  . " AND relnamespace <> 'pg_toast'::regnamespace");
	};
	is($stderr, '', "pg_amcheck @$sel: no errors");
	if ($sel->[0] eq '--table')
	{
		is($names->(@bark), 'barkidx barkidx_t implidx implidx_u',
			'pg_amcheck checks a table\'s bark-implemented indexes with bark_index_check');
		is($names->(@bt), 'btidx',
			'pg_amcheck checks only the nbtree index (and TOAST\'s) with bt_index_check');
	}
	else
	{
		is($names->(@bark), $sel->[1],
			"pg_amcheck checks $sel->[1] with bark_index_check");
		is($names->(@bt), '', "pg_amcheck does not call bt_index_check on $sel->[1]");
	}
}
$node->command_checks_all(
	[
		'pg_amcheck', '--port' => $port,
		'--verbose', '--index' => 'implidx', 'postgres'
	],
	0,
	[qr/^$/],
	[qr/checking bark index "postgres\.public\.implidx"/],
	'pg_amcheck reports a bark-implemented btree index as a bark index');

# Rows inserted while the indexes are not ready have no entries; only
# heapallindexed finds them.  The btree index is damaged the same way, for
# comparison.
$node->safe_psql(
	'postgres', q(
	UPDATE pg_index SET indisready = false
	  WHERE indexrelid IN ('barkidx'::regclass, 'btidx'::regclass,
	                       'implidx'::regclass);
	INSERT INTO barktbl VALUES (5001, 'row 5001');
	UPDATE pg_index SET indisready = true
	  WHERE indexrelid IN ('barkidx'::regclass, 'btidx'::regclass,
	                       'implidx'::regclass);
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
			qr/lacks matching index tuple within index "btidx"/,
			qr/bark index "postgres\.public\.implidx":/,
			qr/lacks matching index tuple within index "implidx"/
		],
		[],
		"pg_amcheck @$opts reports the missing entries");
}

# A duplicate in the unique bark-implemented index, inserted while the
# catalog says it is not unique: only checkunique finds it.  The INCLUDE
# column keeps the two entries apart (no LIST), as in a unique index.
$node->safe_psql(
	'postgres', q(
	UPDATE pg_index SET indisunique = false
	  WHERE indexrelid = 'implidx_u'::regclass;
	INSERT INTO barktbl VALUES (1, 'row 1 again');
	UPDATE pg_index SET indisunique = true
	  WHERE indexrelid = 'implidx_u'::regclass;
));
$node->command_like(
	[ 'pg_amcheck', '--port' => $port, '--index' => 'implidx_u', 'postgres' ],
	qr/^$/, 'pg_amcheck without checkunique reports no corruption in implidx_u');
$node->command_checks_all(
	[
		'pg_amcheck', '--port' => $port,
		'--checkunique', '--index' => 'implidx_u', 'postgres'
	],
	2,
	[
		qr/bark index "postgres\.public\.implidx_u":/,
		qr/index uniqueness is violated for index "implidx_u"/
	],
	[],
	'pg_amcheck --checkunique reports the duplicate in implidx_u');

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
$node->command_checks_all(
	[ 'pg_amcheck', '--port' => $port, '--index' => 'implidx', 'postgres' ],
	1,
	[qr/^$/],
	[qr/no btree indexes to check matching "implidx"/],
	'pg_amcheck with amcheck 1.6 does not select a bark-implemented btree index');

$node->stop;
done_testing();
