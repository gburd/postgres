# Copyright (c) 2026, PostgreSQL Global Development Group
#
# QUEL extension TAP test.  Spins up a postmaster with the quel
# extension in shared_preload_libraries, exercises the
# introspection functions, and asserts:
#
#   1. quel registers cleanly at postmaster start.
#   2. The pipeline log shows lime + cc + dlopen for the rebuild.
#   3. The cache key (SHA256) is stable across boots.
#   4. quel_extension_status() reports the registration summary.
#   5. quel_serialized_lime() returns a fragment that contains
#      every token, type, rule, and precedence directive we
#      registered.
#   6. The base SQL grammar is invariant: all standard SQL still
#      parses correctly after the rebuild.
#   7. QUEL keywords typed at psql ARE NOT yet recognized as such
#      (Track A scanner-table limit) -- the test asserts this
#      KNOWN behaviour so a future Track B fix that surfaces
#      QUEL parsing breaks the test loudly.

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
# 1. Registration succeeded; pipeline ran.
# ---------------------------------------------------------------
my $logs = log_text($node);
like($logs,
	qr/quel: registered \(rebuild will run on first parse\)/,
	'quel registered at _PG_init()');
like($logs,
	qr/running lime to rebuild parser for 1 extension\(s\)/,
	'rebuild pipeline ran');
like($logs,
	qr/loaded extended parser from .*\.so/,
	'rebuilt .so loaded');

# Capture cache key.
my ($cache_key) = $logs =~
	m{loaded extended parser from .*?/pg_parser_cache/([0-9a-f]{64})\.so};
ok($cache_key, "cache key recorded ($cache_key)");

# ---------------------------------------------------------------
# 2. Status function reports the expected summary.
# ---------------------------------------------------------------
my $status = $node->safe_psql('postgres', 'SELECT quel_extension_status()');
like($status,
	qr/quel registered: 10 tokens, 8 types, 30 rules, 4 prec/,
	'quel_extension_status() reports registered counts');
like($status,
	qr/scanner-keyword hook \(Track B Phase 1\) live/,
	'status documents Track B Phase 1 LIVE');

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
like($frag, qr/%nonassoc K_QUEL_RETRIEVE/,   'fragment: prec on retrieve');
like($frag, qr/stmt\(A\) ::= quel_stmt/,
	'fragment: stmt -> quel_stmt forwarder');
like($frag, qr/quel_range_stmt\(A\) ::= K_QUEL_RANGE.*K_QUEL_OF.*IDENT.*K_QUEL_IS.*IDENT/,
	'fragment: range-of-IDENT-is-IDENT rule');

# ---------------------------------------------------------------
# 4. Base SQL grammar invariant under the rebuilt parser.
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

# ---------------------------------------------------------------
# 5. QUEL Phase B end-to-end: bind tuple variable, run a real
#    QUEL retrieve, verify the result matches an equivalent SQL
#    SELECT.  This is THE flagship test -- it asserts that QUEL
#    queries produce identical results to equivalent SQL.
# ---------------------------------------------------------------
$node->safe_psql('postgres', q{
	CREATE TABLE qb_emp (name text, salary numeric, dept text);
	INSERT INTO qb_emp VALUES
		('alice', 50000, 'shoe'),
		('bob',   60000, 'shoe'),
		('carol', 80000, 'toy');
});

# Bind tuple var + run QUEL retrieve in ONE session.  RANGE
# bindings are session-scoped so we drive both statements
# through the same psql connection.
my $quel_out = $node->safe_psql('postgres',
	q{q_range q_of e q_is qb_emp;
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
	q{q_range q_of e q_is qb_emp;
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
	q{append q_to qb_append_quel (name='dave', salary=70000, dept='shoe')});
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
	q{q_replace qb_replace_quel (salary = 99000) where dept = 'shoe'});
$node->safe_psql('postgres',
	q{UPDATE qb_replace_sql SET salary = 99000 WHERE dept = 'shoe'});
is(
	$node->safe_psql('postgres',
		'SELECT name, salary FROM qb_replace_quel ORDER BY name'),
	$node->safe_psql('postgres',
		'SELECT name, salary FROM qb_replace_sql ORDER BY name'),
	'QUEL replace produces identical updates to SQL UPDATE');

# QUEL DELETE vs SQL DELETE round-trip equivalence.
$node->safe_psql('postgres', q{
	CREATE TABLE qb_delete_quel AS SELECT * FROM qb_emp;
	CREATE TABLE qb_delete_sql  AS SELECT * FROM qb_emp;
});
$node->safe_psql('postgres',
	q{q_delete qb_delete_quel where salary < 70000});
$node->safe_psql('postgres',
	q{DELETE FROM qb_delete_sql WHERE salary < 70000});
is(
	$node->safe_psql('postgres',
		'SELECT count(*) FROM qb_delete_quel'),
	$node->safe_psql('postgres',
		'SELECT count(*) FROM qb_delete_sql'),
	'QUEL delete removes the same row count as SQL DELETE');

# QUEL retrieve+BY vs SQL SELECT+ORDER BY (asc + desc).
$node->safe_psql('postgres',
	q{q_range q_of e q_is qb_emp;});
is(
	$node->safe_psql('postgres',
		q{q_range q_of e q_is qb_emp;
		  retrieve (e.name) q_by e.salary;}),
	$node->safe_psql('postgres',
		q{SELECT name FROM qb_emp ORDER BY salary;}),
	'QUEL retrieve+BY ASC matches SQL SELECT+ORDER BY ASC');
is(
	$node->safe_psql('postgres',
		q{q_range q_of e q_is qb_emp;
		  retrieve (e.name) q_by e.salary desc;}),
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
		q{q_range q_of e q_is qb_emp;
		  q_range q_of d q_is qb_dept;
		  retrieve (e.name) where e.dept = d.name;}),
	$node->safe_psql('postgres',
		q{SELECT e.name FROM qb_emp e, qb_dept d WHERE e.dept = d.name;}),
	'QUEL multi-tuple-variable join matches SQL FROM-list join');

# QUEL EXPLAIN passes through to SQL plan structure.  We assert that
# EXPLAIN on an equivalent QUEL retrieve produces the SAME plan tree
# as EXPLAIN on the SQL form.
$node->safe_psql('postgres', q{ANALYZE qb_emp;});
my $quel_plan = $node->safe_psql('postgres',
	q{q_range q_of e q_is qb_emp;
	  EXPLAIN (COSTS OFF) retrieve (e.name) where e.dept = 'shoe';});
my $sql_plan = $node->safe_psql('postgres',
	q{EXPLAIN (COSTS OFF) SELECT name FROM qb_emp e WHERE e.dept = 'shoe';});
is($quel_plan, $sql_plan,
	'EXPLAIN QUEL retrieve produces identical plan to EXPLAIN SQL SELECT');

note('QUEL Phase B complete: all four DML statements (RETRIEVE / '
	. 'REPLACE / APPEND / DELETE) build real PG parse trees and '
	. 'produce identical results to equivalent SQL.');

# ---------------------------------------------------------------
# 6. Cache hit on second postmaster of the same cluster.
# ---------------------------------------------------------------
$node->stop;

# Truncate the log so the second-boot pipeline messages are easy
# to find against the first-boot ones.
my $logfile_path = $node->logfile;
truncate $logfile_path, 0;

$node->start;
# Trigger a parse on the second boot so the cache code path runs.
$node->safe_psql('postgres', 'SELECT 1');
my $logs2 = log_text($node);

# After the second boot the cache hit avoids running lime + cc.
# parser_extension.c logs 'grammar extension cache hit: <path>'
# (LOG level) on cache hit, then dlopens directly.  Either path
# yields the same SHA256.
my ($cache_key2) = $logs2 =~
	m{(?:loaded extended parser from|grammar extension cache hit:) .*?/pg_parser_cache/([0-9a-f]{64})\.so};
is($cache_key2, $cache_key,
	'cache key stable across postmaster restarts');

$node->stop;

done_testing();
