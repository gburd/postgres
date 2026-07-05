# Copyright (c) 2026, PostgreSQL Global Development Group
#
# QUEL extension TAP test.  Spins up a postmaster with the quel
# extension in shared_preload_libraries, exercises the
# introspection functions, and asserts:
#
#   1. quel registers cleanly at postmaster start and the grammar is
#      composed in-process (no subprocess, no C compiler).
#   2. quel_extension_status() reports the registration summary.
#   3. quel_serialized_lime() returns a fragment that contains every
#      token, type, and rule we registered.
#   4. The base SQL grammar is invariant: all standard SQL still parses
#      correctly through the composed parser.
#   5. QUEL statements -- typed with their REAL keywords (retrieve,
#      append, replace, delete, range, of, is, to, by) -- build real PG
#      parse trees and produce identical results to equivalent SQL.
#      Keyword collisions with base SQL (range/of/is/to/by, and delete)
#      are resolved by the admissibility oracle / one-token lookahead,
#      so no mangled lexemes are needed.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

sub log_text
{
	my ($node) = @_;
	my $logfile = $node->logfile;
	open(my $fh, '<', $logfile) or die "cannot read $logfile: $!";
	local $/;
	my $text = <$fh>;
	close $fh;
	return $text;
}

# ---------------------------------------------------------------
# Spin up a postmaster with quel preloaded and CREATE EXTENSION.
# ---------------------------------------------------------------
my $node = PostgreSQL::Test::Cluster->new('quel_node');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'quel'\n"
	. "log_min_messages = debug1\n"
	. "client_min_messages = notice\n");
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION quel');

# ---------------------------------------------------------------
# 1. Registration succeeded; the grammar was composed in-process.
# ---------------------------------------------------------------
my $logs = log_text($node);
like($logs,
	qr/quel: registered/,
	'quel registered at _PG_init()');

# The in-process compose must NOT shell out to a C compiler: there is
# no pg_parser_cache directory and no "running lime" / cc invocation.
unlike($logs, qr/running lime to rebuild parser/,
	'no subprocess lime rebuild (in-process compose)');
unlike($logs, qr{/pg_parser_cache/},
	'no on-disk .so cache (in-process compose, no cc)');

# ---------------------------------------------------------------
# 2. Status function reports the expected summary.
# ---------------------------------------------------------------
my $status = $node->safe_psql('postgres', 'SELECT quel_extension_status()');
like($status,
	qr/quel registered: 10 tokens/,
	'quel_extension_status() reports registered token count');

# ---------------------------------------------------------------
# 3. Serialized .lime fragment contains every registered piece.
# ---------------------------------------------------------------
my $frag = $node->safe_psql('postgres', 'SELECT quel_serialized_lime()');
like($frag, qr/%token K_QUEL_RETRIEVE/, 'fragment: K_QUEL_RETRIEVE');
like($frag, qr/%token K_QUEL_REPLACE/,  'fragment: K_QUEL_REPLACE');
like($frag, qr/%token K_QUEL_APPEND/,   'fragment: K_QUEL_APPEND');
like($frag, qr/%token K_QUEL_RANGE/,    'fragment: K_QUEL_RANGE');
like($frag, qr/%type quel_stmt \{Node \*\}/, 'fragment: quel_stmt type');
like($frag, qr/%type quel_retrieve_stmt/,    'fragment: retrieve type');
like($frag, qr/stmt\(A\) ::= quel_stmt/,
	'fragment: stmt -> quel_stmt forwarder');
like($frag, qr/quel_range_stmt\(A\) ::= K_QUEL_RANGE.*K_QUEL_OF.*IDENT.*K_QUEL_IS.*IDENT/,
	'fragment: range-of-IDENT-is-IDENT rule');

# ---------------------------------------------------------------
# 4. Base SQL grammar invariant under the composed parser.
# ---------------------------------------------------------------
is($node->safe_psql('postgres', 'SELECT 1+1'), '2',
	'base SQL: SELECT 1+1');
is($node->safe_psql('postgres', "SELECT 'hello' || ' world'"),
	'hello world', 'base SQL: string concat');
is(
	$node->safe_psql(
		'postgres',
		'CREATE TABLE quel_demo (id int, dept text); '
			. 'INSERT INTO quel_demo VALUES (1, \'shoe\'), (2, \'toy\'); '
			. 'SELECT count(*) FROM quel_demo WHERE dept = \'shoe\''),
	'1', 'base SQL: DDL+DML+SELECT round-trip');

# Window function and CTE -- deep grammar paths.
is(
	$node->safe_psql(
		'postgres',
		'WITH ranked AS (SELECT id, row_number() OVER (ORDER BY id) AS rn '
			. 'FROM quel_demo) SELECT max(rn) FROM ranked'),
	'2', 'base SQL: CTE + window');

# Base SQL using the colliding keywords in BASE contexts must keep
# their base meaning (the oracle must not steal them for QUEL).
is($node->safe_psql('postgres', 'SELECT 1 IS NULL'), 'f',
	'base SQL: IS in base context (not QUEL)');
is($node->safe_psql('postgres',
		'SELECT x FROM (VALUES(2),(1)) v(x) ORDER BY x'),
	"1\n2", 'base SQL: ORDER BY in base context (not QUEL)');
is($node->safe_psql('postgres',
		"SELECT sum(x) OVER (ORDER BY x RANGE UNBOUNDED PRECEDING) "
			. "FROM (VALUES(1),(2)) v(x)"),
	"1\n3", 'base SQL: RANGE window frame in base context (not QUEL)');

# ---------------------------------------------------------------
# 5. QUEL end-to-end with REAL keywords: bind tuple variable, run a
#    real QUEL retrieve, verify the result matches an equivalent SQL
#    SELECT.  No mangled lexemes -- range/of/is/to/by/delete are the
#    real spellings, disambiguated from base SQL by the oracle.
# ---------------------------------------------------------------
$node->safe_psql('postgres', q{
	CREATE TABLE qb_emp (name text, salary numeric, dept text);
	INSERT INTO qb_emp VALUES
		('alice', 50000, 'shoe'),
		('bob',   60000, 'shoe'),
		('carol', 80000, 'toy');
});

# Bind tuple var + run QUEL retrieve in ONE session.  RANGE bindings
# are session-scoped so we drive both statements through the same psql
# connection.
my $quel_out = $node->safe_psql('postgres',
	q{range of e is qb_emp;
	  retrieve (e.name) where e.dept = 'shoe';});

my $sql_out = $node->safe_psql('postgres',
	q{SELECT name FROM qb_emp WHERE dept = 'shoe' ORDER BY name;});

# QUEL output may be unordered; sort both for comparison.
my @quel_rows = sort split /\n/, $quel_out;
my @sql_rows  = sort split /\n/, $sql_out;
is_deeply(\@quel_rows, \@sql_rows,
	'QUEL retrieve and SQL SELECT return identical results');

# Equivalent test for retrieve (...) without WHERE -- full table scan.
$quel_out = $node->safe_psql('postgres',
	q{range of e is qb_emp;
	  retrieve (e.name, e.salary);});
$sql_out = $node->safe_psql('postgres',
	q{SELECT name, salary FROM qb_emp ORDER BY name;});
@quel_rows = sort split /\n/, $quel_out;
@sql_rows  = sort split /\n/, $sql_out;
is_deeply(\@quel_rows, \@sql_rows,
	'QUEL retrieve target_list matches SQL SELECT target_list');

# QUEL APPEND vs SQL INSERT round-trip equivalence.
$node->safe_psql('postgres', q{
	CREATE TABLE qb_append_quel AS SELECT * FROM qb_emp WITH NO DATA;
	CREATE TABLE qb_append_sql  AS SELECT * FROM qb_emp WITH NO DATA;
});
$node->safe_psql('postgres',
	q{append to qb_append_quel (name='dave', salary=70000, dept='shoe')});
$node->safe_psql('postgres',
	q{INSERT INTO qb_append_sql (name, salary, dept) VALUES ('dave', 70000, 'shoe')});
is(
	$node->safe_psql('postgres', 'SELECT * FROM qb_append_quel'),
	$node->safe_psql('postgres', 'SELECT * FROM qb_append_sql'),
	'QUEL append produces identical row to SQL INSERT');

# QUEL REPLACE vs SQL UPDATE round-trip equivalence.
$node->safe_psql('postgres', q{
	CREATE TABLE qb_replace_quel AS SELECT * FROM qb_emp;
	CREATE TABLE qb_replace_sql  AS SELECT * FROM qb_emp;
});
$node->safe_psql('postgres',
	q{replace qb_replace_quel (salary = 99000) where dept = 'shoe'});
$node->safe_psql('postgres',
	q{UPDATE qb_replace_sql SET salary = 99000 WHERE dept = 'shoe'});
is(
	$node->safe_psql('postgres',
		'SELECT name, salary FROM qb_replace_quel ORDER BY name'),
	$node->safe_psql('postgres',
		'SELECT name, salary FROM qb_replace_sql ORDER BY name'),
	'QUEL replace produces identical updates to SQL UPDATE');

# QUEL DELETE vs SQL DELETE round-trip equivalence.  `delete` is the
# verb that leads a statement in BOTH grammars; the one-token peek
# (next == FROM -> base DELETE, else QUEL) keeps them distinct.
$node->safe_psql('postgres', q{
	CREATE TABLE qb_delete_quel AS SELECT * FROM qb_emp;
	CREATE TABLE qb_delete_sql  AS SELECT * FROM qb_emp;
});
$node->safe_psql('postgres',
	q{range of e is qb_delete_quel;
	  delete e where e.salary < 70000});
$node->safe_psql('postgres',
	q{DELETE FROM qb_delete_sql WHERE salary < 70000});
is(
	$node->safe_psql('postgres',
		'SELECT count(*) FROM qb_delete_quel'),
	$node->safe_psql('postgres',
		'SELECT count(*) FROM qb_delete_sql'),
	'QUEL delete removes the same row count as SQL DELETE');

# Base SQL DELETE FROM must still work (the peek picks base on FROM).
$node->safe_psql('postgres', q{
	CREATE TABLE qb_basedel AS SELECT * FROM qb_emp;
	DELETE FROM qb_basedel WHERE salary >= 80000;
});
is($node->safe_psql('postgres', 'SELECT count(*) FROM qb_basedel'),
	'2', 'base SQL DELETE FROM still works under the composed parser');

# QUEL retrieve+BY vs SQL SELECT+ORDER BY (asc + desc).
is(
	$node->safe_psql('postgres',
		q{range of e is qb_emp;
		  retrieve (e.name) by e.salary;}),
	$node->safe_psql('postgres',
		q{SELECT name FROM qb_emp ORDER BY salary;}),
	'QUEL retrieve+BY ASC matches SQL SELECT+ORDER BY ASC');
is(
	$node->safe_psql('postgres',
		q{range of e is qb_emp;
		  retrieve (e.name) by e.salary desc;}),
	$node->safe_psql('postgres',
		q{SELECT name FROM qb_emp ORDER BY salary DESC;}),
	'QUEL retrieve+BY DESC matches SQL SELECT+ORDER BY DESC');

# QUEL multi-tuple-variable join vs SQL FROM-list.
$node->safe_psql('postgres', q{
	CREATE TABLE qb_dept (name text, budget numeric);
	INSERT INTO qb_dept VALUES ('shoe', 1000000), ('toy', 500000);
});
is(
	$node->safe_psql('postgres',
		q{range of e is qb_emp;
		  range of d is qb_dept;
		  retrieve (e.name) where e.dept = d.name;}),
	$node->safe_psql('postgres',
		q{SELECT e.name FROM qb_emp e, qb_dept d WHERE e.dept = d.name;}),
	'QUEL multi-tuple-variable join matches SQL FROM-list join');

# QUEL EXPLAIN passes through to SQL plan structure.
$node->safe_psql('postgres', q{ANALYZE qb_emp;});
my $quel_plan = $node->safe_psql('postgres',
	q{range of e is qb_emp;
	  EXPLAIN (COSTS OFF) retrieve (e.name) where e.dept = 'shoe';});
my $sql_plan = $node->safe_psql('postgres',
	q{EXPLAIN (COSTS OFF) SELECT name FROM qb_emp e WHERE e.dept = 'shoe';});
is($quel_plan, $sql_plan,
	'EXPLAIN QUEL retrieve produces identical plan to EXPLAIN SQL SELECT');

note('QUEL complete: all four DML statements (RETRIEVE / REPLACE / '
	. 'APPEND / DELETE) build real PG parse trees from REAL keywords '
	. 'and produce identical results to equivalent SQL, composed '
	. 'in-process with no C compiler.');

$node->stop;

done_testing();
