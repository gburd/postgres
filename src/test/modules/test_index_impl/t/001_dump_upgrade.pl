
# Copyright (c) 2026, PostgreSQL Global Development Group

# Indexes implemented by another index AM survive pg_dump/pg_restore and
# pg_upgrade, and are still served by their implementation afterwards.

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
CREATE EXTENSION test_index_impl;
CREATE TABLE t (a int, b text) USING heap;
CREATE TABLE ta (a int, b text) USING test_impl_heap;
SET default_btree_implementation = test_btree_impl;
CREATE INDEX t_a ON t (a);
ALTER TABLE t ADD CONSTRAINT t_b_key UNIQUE (b);
RESET default_btree_implementation;
CREATE INDEX t_plain ON t (b, a);
-- ta's table AM picks test_btree_impl; this one overrides it with nothing
CREATE INDEX ta_a ON ta (a);
INSERT INTO t SELECT g, 'v' || g FROM generate_series(1, 1000) g;
INSERT INTO ta SELECT g, 'v' || g FROM generate_series(1, 1000) g;
});

# The implementation of each index of t and ta, by index name.
my $implq = q{
SELECT string_agg(c.relname || '=' || coalesce(a.amname, '-'), ',' ORDER BY c.relname)
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
LEFT JOIN pg_am a ON a.oid = i.indimplam
WHERE i.indrelid IN ('t'::regclass, 'ta'::regclass)};
my $expected_impl =
  't_a=test_btree_impl,t_b_key=test_btree_impl,t_plain=-,ta_a=test_btree_impl';
is($old->safe_psql('postgres', $implq),
	$expected_impl, 'implementations before');

# Each index's answers through its implementation agree with a seqscan, and
# the implementation's code runs.
sub check_served
{
	my ($node, $name) = @_;
	my $r = $node->safe_psql(
		'postgres', q{
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT test_index_impl_reset();
SELECT count(*), sum(a) FROM t WHERE a BETWEEN 100 AND 199;
SELECT test_index_impl_scans();
SELECT count(*) FROM t WHERE b = 'v500';
SELECT count(*), sum(a) FROM ta WHERE a BETWEEN 100 AND 199;
SELECT test_index_impl_scans() >= 2;
SELECT test_index_impl_reset();
INSERT INTO t VALUES (2001, 'v2001');
SELECT test_index_impl_inserts();
DELETE FROM t WHERE a = 2001;
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*), sum(a) FROM t WHERE a BETWEEN 100 AND 199;
SELECT count(*) FROM t WHERE b = 'v500';
SELECT count(*), sum(a) FROM ta WHERE a BETWEEN 100 AND 199;
});
	is( $r,
		"\n100|14950\n1\n1\n100|14950\nt\n\n2\n100|14950\n1\n100|14950",
		"$name: implemented indexes are used and agree with a seqscan");
	my ($ret, $out, $err) =
	  $node->psql('postgres', q{INSERT INTO t VALUES (3000, 'v1')});
	like(
		$err,
		qr/duplicate key value violates unique constraint "t_b_key"/,
		"$name: unique constraint still enforced");
}

check_served($old, 'original');

# pg_dump shows the setting, and a restore reproduces the implementations.
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
	qr/SET default_btree_implementation = test_btree_impl;\nCREATE INDEX t_a ON public.t USING btree \(a\);\nSET default_btree_implementation = '';/,
	'dump sets the implementation around CREATE INDEX');
like(
	$stdout,
	qr/SET default_btree_implementation = test_btree_impl;\nALTER TABLE ONLY public.t\n    ADD CONSTRAINT t_b_key UNIQUE \(b\);\nSET default_btree_implementation = '';/,
	'dump sets the implementation around ADD CONSTRAINT');
unlike(
	$stdout,
	qr/SET default_btree_implementation = [^']*;\nCREATE INDEX t_plain/,
	'dump sets nothing for an index without an implementation');

$old->safe_psql('postgres', 'CREATE DATABASE restored');
command_ok(
	[
		'pg_restore', '--exit-on-error',
		'--dbname' => $old->connstr('restored'),
		"$tempdir/dump.custom"
	],
	'pg_restore');
is($old->safe_psql('restored', $implq),
	$expected_impl, 'implementations after pg_restore');

# pg_upgrade keeps the index files, so the implementations must be kept
# exactly, whatever the table AM or the setting would choose now.
$old->safe_psql('postgres', 'DROP DATABASE restored');
$old->append_conf('postgresql.conf',
	"default_btree_implementation = test_btree_impl_nounique");
$old->restart;
$old->stop;

my $new = PostgreSQL::Test::Cluster->new('new');
$new->init;
$new->append_conf('postgresql.conf',
	"default_btree_implementation = test_btree_impl_nounique");

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
	$expected_impl, 'implementations after pg_upgrade');
check_served($new, 'upgraded');
$new->stop;

done_testing();
