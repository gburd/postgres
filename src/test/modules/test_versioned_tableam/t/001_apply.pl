# Copyright (c) 2026, PostgreSQL Global Development Group

# Logical replication apply into a heap_versioned table: apply's updates go
# through the same index rule as UPDATE, and the subscriber's indexes carry
# the tags heap_versioned hands out.
#
# The publisher's table is plain heap; the subscriber's is heap_versioned,
# whose btree indexes are implemented by test_versioned_btree.  heap_versioned
# raises an error if any index scan, constraint fetch or VACUUM pass hands
# it a (TID, tag) whose tag does not name the tuple at that TID, so the
# checks below fail loudly if apply mistags an entry.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $pub = PostgreSQL::Test::Cluster->new('publisher');
$pub->init(allows_streaming => 'logical');
$pub->start;

my $sub = PostgreSQL::Test::Cluster->new('subscriber');
$sub->init;
$sub->start;

$pub->safe_psql('postgres',
	'CREATE TABLE t (id int PRIMARY KEY, a int, b int)');
$sub->safe_psql(
	'postgres', q{
	CREATE EXTENSION test_versioned_tableam;
	CREATE TABLE t (id int PRIMARY KEY, a int, b int) USING heap_versioned;
	CREATE INDEX t_a ON t (a);
});

$pub->safe_psql('postgres', 'CREATE PUBLICATION p FOR TABLE t');
my $connstr = $pub->connstr . ' dbname=postgres';
$sub->safe_psql('postgres',
	"CREATE SUBSCRIPTION s CONNECTION '$connstr' PUBLICATION p");
$sub->wait_for_subscription_sync($pub, 's');

$pub->safe_psql('postgres',
	'INSERT INTO t SELECT g, g, g FROM generate_series(1, 30) g');
$pub->wait_for_catchup('s');

$sub->safe_psql('postgres', 'SELECT test_versioned_reset()');

# changes an indexed column: the row moves, and its new entries are tagged
$pub->safe_psql('postgres', 'UPDATE t SET a = a + 100 WHERE id <= 10');
# changes no indexed column: HOT, no new entries
$pub->safe_psql('postgres', 'UPDATE t SET b = b + 100 WHERE id > 20');
$pub->safe_psql('postgres', 'DELETE FROM t WHERE id = 15');
$pub->wait_for_catchup('s');

is( $sub->safe_psql(
		'postgres', q{
	SELECT count(*) FROM t
	 WHERE id <= 10 AND test_versioned_tag('t_a', ctid) = xmin::text::bigint
	   AND test_versioned_tag('t_pkey', ctid) = xmin::text::bigint}),
	'10',
	'apply tags the entries of rows whose indexed columns changed');

is( $sub->safe_psql(
		'postgres',
		q{SELECT n FROM test_versioned_counts() WHERE counter = 'insert_tagged'}),
	'20',
	'apply made tagged entries in both indexes, and only for moved rows');

# scans through both indexes see the tags and agree with a seqscan
is( $sub->safe_psql(
		'postgres', q{
	SET enable_seqscan = off; SET enable_bitmapscan = off;
	SELECT count(*) FROM t WHERE a > 100}),
	'10',
	'index scan through the tagged index finds the updated rows');
is( $sub->safe_psql(
		'postgres', q{
	SET enable_seqscan = off; SET enable_bitmapscan = off;
	SELECT count(*), sum(b) FROM t WHERE id > 0}),
	$sub->safe_psql('postgres', 'SELECT count(*), sum(b) FROM t'),
	'primary key scan agrees with a sequential scan');
ok( $sub->safe_psql(
		'postgres',
		q{SELECT n > 0 FROM test_versioned_counts() WHERE counter = 'scan_tagged'}
	) eq 't',
	'tagged entries reached the table AM');

# VACUUM untags the live tagged entries
$sub->safe_psql('postgres', 'VACUUM t');
is( $sub->safe_psql(
		'postgres',
		q{SELECT count(*) FROM t WHERE test_versioned_tag('t_a', ctid) <> 0}),
	'0',
	'VACUUM version callback untagged the live entries');

# a conflicting insert on the subscriber is reported, not silently indexed
$sub->safe_psql('postgres', 'INSERT INTO t VALUES (100, 0, 0)');
$pub->safe_psql('postgres', 'INSERT INTO t VALUES (100, 1, 1)');
$sub->wait_for_log(qr/conflict detected on relation "public.t"/);
$sub->safe_psql('postgres', 'DELETE FROM t WHERE id = 100');
$pub->wait_for_catchup('s');
is($sub->safe_psql('postgres', 'SELECT a FROM t WHERE id = 100'),
	'1', 'apply resumed after the conflict was resolved');

$sub->safe_psql('postgres', 'DROP SUBSCRIPTION s');
$sub->stop;
$pub->stop;

done_testing();
