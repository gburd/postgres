
# Copyright (c) 2026, PostgreSQL Global Development Group

# btree indexes implemented by BARK keep BARK as their implementation through
# pg_dump/pg_restore and pg_upgrade, and are served by it afterwards: plans
# use them (including a KNN ordering that only BARK can serve), their answers
# agree with a sequential scan, and bark_index_check accepts them.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $tempdir = PostgreSQL::Test::Utils::tempdir;

my $old = PostgreSQL::Test::Cluster->new('old');
$old->init;
$old->start;

$old->safe_psql(
	'postgres', q{
CREATE EXTENSION amcheck;
CREATE TABLE t (a int, b text, c int);
SET default_btree_implementation = bark;
CREATE INDEX t_a ON t (a) WITH (deduplicate_items = off);
ALTER TABLE t ADD CONSTRAINT t_b_key UNIQUE (b);
ALTER TABLE t ADD PRIMARY KEY (c);
RESET default_btree_implementation;
CREATE INDEX t_plain ON t (b, a);
CREATE INDEX t_bark ON t USING bark (c, a);
INSERT INTO t SELECT g % 1000, 'v' || g, g FROM generate_series(1, 5000) g;
CREATE TABLE fk (c int REFERENCES t (c));
INSERT INTO fk SELECT generate_series(1, 100);
ANALYZE t;
});

# The access method and implementation of each index of t, by index name.
my $implq = q{
SELECT string_agg(c.relname || '=' || m.amname || '/' || coalesce(a.amname, '-'),
                  ',' ORDER BY c.relname)
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_am m ON m.oid = c.relam
LEFT JOIN pg_am a ON a.oid = i.indimplam
WHERE i.indrelid = 't'::regclass};
my $expected_impl =
  't_a=btree/bark,t_b_key=btree/bark,t_bark=bark/-,t_pkey=btree/bark,t_plain=btree/-';
is($old->safe_psql('postgres', $implq),
	$expected_impl, 'implementations before');

# The plans, and the answers by index against a seqscan.
my $plans = q{
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT a FROM t WHERE a = 7;
EXPLAIN (COSTS OFF) SELECT a FROM t ORDER BY a <~> 500 LIMIT 3;
EXPLAIN (COSTS OFF) SELECT b FROM t WHERE b = 'v77';
};
my $answers = q{
SELECT count(*), sum(c) FROM t WHERE a BETWEEN 100 AND 199;
SELECT string_agg(a::text, ',') FROM (SELECT a FROM t ORDER BY a <~> 500, c LIMIT 7) s;
SELECT count(*) FROM t WHERE b = 'v500';
SELECT count(*) FROM t WHERE c = ANY ('{1,50,4999,7000}');
};

sub check_served
{
	my ($node, $db, $name, $plan_before) = @_;

	my $plan = $node->safe_psql($db, $plans);
	like($plan, qr/Index Only Scan using t_a on t/,
		"$name: equality uses the BARK-implemented index");
	like($plan, qr/Order By: \(a <~> 500\)/,
		"$name: KNN ordering served by the BARK-implemented index");
	is($plan, $plan_before, "$name: plans unchanged") if defined $plan_before;

	my $byindex = $node->safe_psql($db,
		"SET enable_seqscan = off; SET enable_bitmapscan = off; $answers");
	my $byseq = $node->safe_psql($db,
		"SET enable_indexscan = off; SET enable_indexonlyscan = off; SET enable_bitmapscan = off; $answers"
	);
	is($byindex, $byseq, "$name: index answers agree with a seqscan");

	is( $node->safe_psql(
			$db, q{
SELECT count(bark_index_check(i.indexrelid, true))
FROM pg_index i WHERE i.indrelid = 't'::regclass AND i.indimplam <> 0}),
		'3',
		"$name: bark_index_check passes the BARK-implemented indexes");

	my ($ret, $out, $err) =
	  $node->psql($db, q{INSERT INTO t VALUES (1, 'v1', 999999)});
	like(
		$err,
		qr/duplicate key value violates unique constraint "t_b_key"/,
		"$name: unique constraint still enforced");
	($ret, $out, $err) = $node->psql($db, q{INSERT INTO fk VALUES (999999)});
	like($err, qr/violates foreign key constraint/,
		"$name: foreign key still enforced");
	return $plan;
}

my $plan_orig = check_served($old, 'postgres', 'original');

# pg_dump states the implementation; a restore reproduces it.
command_ok(
	[
		'pg_dump', '--no-sync',
		'--format' => 'custom',
		'--file' => "$tempdir/dump.custom",
		'--dbname' => $old->connstr('postgres'),
	],
	'pg_dump');
my ($stdout, $stderr) = run_command(
	[
		'pg_restore', '--schema-only',
		'--file' => '-',
		"$tempdir/dump.custom"
	]);
like(
	$stdout,
	qr/SET default_btree_implementation = bark;\nCREATE INDEX t_a ON public.t USING btree \(a\) WITH \(deduplicate_items=off\);\nSET default_btree_implementation = '';/,
	'dump sets the implementation around CREATE INDEX, with its reloptions');
like(
	$stdout,
	qr/SET default_btree_implementation = bark;\nALTER TABLE ONLY public.t\n    ADD CONSTRAINT t_pkey PRIMARY KEY \(c\);\nSET default_btree_implementation = '';/,
	'dump sets the implementation around ADD CONSTRAINT');
unlike(
	$stdout,
	qr/SET default_btree_implementation = [^']*;\nCREATE INDEX t_(plain|bark)/,
	'dump sets nothing for indexes without an implementation');

$old->safe_psql('postgres', 'CREATE DATABASE restored');
command_ok(
	[
		'pg_restore', '--exit-on-error',
		'--dbname' => $old->connstr('restored'),
		"$tempdir/dump.custom"
	],
	'pg_restore');
$old->safe_psql('restored', 'ANALYZE t');
is($old->safe_psql('restored', $implq),
	$expected_impl, 'implementations after pg_restore');
check_served($old, 'restored', 'restored', $plan_orig);

# pg_upgrade keeps the index files, so the implementations must be kept
# exactly, with the setting in force on both clusters pointing elsewhere.
$old->safe_psql('postgres', 'DROP DATABASE restored');
$old->stop;

my $new = PostgreSQL::Test::Cluster->new('new');
$new->init;
$new->append_conf('postgresql.conf', "default_btree_implementation = bark");
$old->append_conf('postgresql.conf', "default_btree_implementation = bark");

chdir ${PostgreSQL::Test::Utils::tmp_check};
command_ok(
	[
		'pg_upgrade', '--no-sync',
		'--old-datadir' => $old->data_dir,
		'--new-datadir' => $new->data_dir,
		'--old-bindir' => $old->config_data('--bindir'),
		'--new-bindir' => $new->config_data('--bindir'),
		'--socketdir' => $new->host,
		'--old-port' => $old->port,
		'--new-port' => $new->port,
		'--copy',
	],
	'pg_upgrade');

$new->start;
is($new->safe_psql('postgres', $implq),
	$expected_impl,
	'implementations after pg_upgrade, t_plain still nbtree');
$new->safe_psql('postgres', 'ANALYZE t');
check_served($new, 'postgres', 'upgraded', $plan_orig);
$new->stop;

done_testing();
