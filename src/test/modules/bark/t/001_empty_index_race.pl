
# Copyright (c) 2026, PostgreSQL Global Development Group

# Concurrent first inserts into an empty unique BARK index.
#
# CREATE INDEX always writes a root page, so the only BARK index with no root
# is one reset to its init fork: an unlogged index after a crash.  Two backends
# can then both find the index empty.  One creates the root; the other must not
# insert behind the uniqueness check because of that, but restart the insert
# and see the first backend's row.
#
# s1 stops at bark-create-root-leaf after finding the index empty.  s2 then
# inserts the same key (creating the root) and commits.  Woken, s1 must fail
# with a unique violation.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

$node->safe_psql(
	'postgres', q[
CREATE EXTENSION injection_points;
CREATE EXTENSION amcheck;
CREATE EXTENSION bark_multikey;
CREATE UNLOGGED TABLE bark_empty (i int4);
CREATE UNIQUE INDEX bark_empty_idx ON bark_empty USING bark (i);
CREATE UNLOGGED TABLE bark_empty_knn (i int4);
CREATE INDEX bark_empty_knn_idx ON bark_empty_knn USING bark (i);
]);

# Crash, so that recovery resets the unlogged index to its init fork.
$node->stop('immediate');
$node->start;
is( $node->safe_psql(
		'postgres', "SELECT pg_relation_size('bark_empty_idx') / current_setting('block_size')::int"),
	'1',
	'index reset to a meta page with no root');

# A KNN scan of an index with no root finds nothing.  In a serializable
# transaction it locks the whole index, as a plain scan does, and driven
# through the AM's routine alone, with no snapshot, it takes no lock.
is( $node->safe_psql(
		'postgres', q[
BEGIN ISOLATION LEVEL SERIALIZABLE;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM (SELECT i FROM bark_empty_knn ORDER BY i <~> 5 LIMIT 3) s;
SELECT count(*) FROM pg_locks
 WHERE mode = 'SIReadLock' AND locktype = 'relation'
   AND relation = 'bark_empty_knn_idx'::regclass AND pid = pg_backend_pid();
SELECT bark_multikey_bare_scan('bark_empty_knn_idx', '<~>(int4,int4)', 5);
COMMIT;
]),
	"0\n1\n0",
	'a KNN scan of an index with no root locks the index, and a bare one runs');
is( $node->safe_psql(
		'postgres', q[
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT i FROM bark_empty_knn ORDER BY i <~> 5 LIMIT 3;
]) =~ /Index Only Scan using bark_empty_knn_idx|Index Scan using bark_empty_knn_idx/ ? 1 : 0,
	1, 'the KNN query is an index scan');

my $s1 = $node->background_psql('postgres', on_error_stop => 0);
$s1->query_safe(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('bark-create-root-leaf', 'wait');
]);
$s1->query_until(
	qr/starting_insert/, q[
\echo starting_insert
INSERT INTO bark_empty VALUES (1);
]);
$node->wait_for_event('client backend', 'bark-create-root-leaf');

$node->safe_psql('postgres', 'INSERT INTO bark_empty VALUES (1)');

$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('bark-create-root-leaf')");

# Wait for s1's INSERT to finish; its error is left in $s1->{stderr}.
$s1->query('SELECT 1');
like(
	$s1->{stderr},
	qr/duplicate key value violates unique constraint "bark_empty_idx"/,
	'second first-insert of the same key fails');
$s1->quit;

is($node->safe_psql('postgres', 'SELECT count(*) FROM bark_empty'),
	'1', 'one row in the table');
is( $node->safe_psql(
		'postgres', q[
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM bark_empty WHERE i = 1;
]),
	'1',
	'one entry in the index');
is($node->safe_psql('postgres', "SELECT bark_index_check('bark_empty_idx')"),
	'', 'bark_index_check passes');

# Stop in fast mode, so the backends exit normally.
$node->stop;
done_testing();
