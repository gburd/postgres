
# Copyright (c) 2026, PostgreSQL Global Development Group

# Logical replication into a table that has an incrementally maintained
# materialized view on the subscriber: the apply worker's inserts, updates,
# deletes and truncates are maintained into the view at each commit.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->start;

my $ddl = "CREATE TABLE tab_ivm (id int PRIMARY KEY, k int, v int)";
$node_publisher->safe_psql('postgres', $ddl);
$node_subscriber->safe_psql('postgres', $ddl);
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_ivm SELECT g, g % 5, g FROM generate_series(1, 100) g");

# The view on the subscriber, and a query that is true when it matches
# its definition.
$node_subscriber->safe_psql(
	'postgres', q{
	CREATE MATERIALIZED VIEW tab_ivm_v WITH (incremental_view_maintenance) AS
	  SELECT k, count(*) AS n, sum(v) AS s, max(v) AS hi
	  FROM tab_ivm GROUP BY k;
	CREATE FUNCTION tab_ivm_ok() RETURNS bool LANGUAGE sql AS $$
	  SELECT NOT EXISTS (
	    (SELECT * FROM tab_ivm_v EXCEPT ALL
	     SELECT k, count(*), sum(v), max(v) FROM tab_ivm GROUP BY k)
	    UNION ALL
	    (SELECT k, count(*), sum(v), max(v) FROM tab_ivm GROUP BY k
	     EXCEPT ALL SELECT * FROM tab_ivm_v))
	$$;
});

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE tab_ivm");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"
);
$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub');

# The initial table sync copies the rows with COPY.
is( $node_subscriber->safe_psql(
		'postgres', "SELECT tab_ivm_ok(), sum(n) FROM tab_ivm_v"),
	't|100',
	'view maintained through the initial table sync');

$node_publisher->safe_psql(
	'postgres', q{
	INSERT INTO tab_ivm SELECT g, g % 7, g FROM generate_series(101, 150) g;
	UPDATE tab_ivm SET k = k + 10 WHERE id % 3 = 0;
	UPDATE tab_ivm SET v = v * 2 WHERE k = 1;
	DELETE FROM tab_ivm WHERE id % 4 = 0;
});
$node_publisher->wait_for_catchup('tap_sub');
is( $node_subscriber->safe_psql(
		'postgres',
		"SELECT tab_ivm_ok(), sum(n) = (SELECT count(*) FROM tab_ivm) FROM tab_ivm_v"),
	't|t',
	'view maintained through applied INSERT, UPDATE and DELETE');

# Several statements in one transaction, applied as one transaction.
$node_publisher->safe_psql(
	'postgres', q{
	BEGIN;
	INSERT INTO tab_ivm VALUES (1000, 42, 1);
	UPDATE tab_ivm SET v = 2 WHERE id = 1000;
	DELETE FROM tab_ivm WHERE k = 2;
	COMMIT;
});
$node_publisher->wait_for_catchup('tap_sub');
is( $node_subscriber->safe_psql(
		'postgres', "SELECT tab_ivm_ok(), (SELECT s FROM tab_ivm_v WHERE k = 42)"),
	't|2',
	'view maintained through an applied multi-statement transaction');

$node_publisher->safe_psql('postgres', "TRUNCATE tab_ivm");
$node_publisher->wait_for_catchup('tap_sub');
is( $node_subscriber->safe_psql(
		'postgres', "SELECT tab_ivm_ok(), count(*) FROM tab_ivm_v"),
	't|0',
	'view maintained through an applied TRUNCATE');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');

done_testing();
