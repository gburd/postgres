# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Per-session grammar dialect selection.  Proves that two backends of the
# SAME postmaster, alive at the same time, parse with DIFFERENT grammars,
# selected per session by the grammar_dialect GUC:
#
#   Session A: SET grammar_dialect = 'quel'  -> parses QUEL
#   Session B: SET grammar_dialect = 'none'  -> parses base SQL only, and
#              REJECTS the very QUEL statement A accepts.
#
# Both sessions are held open concurrently (persistent background psql
# connections) and their queries are interleaved, so this is genuine
# concurrent use of two distinct composed grammar snapshots from one
# server -- not two sequential runs.
#
# The dialect snapshots are composed once at postmaster start (prewarm)
# and shared read-only across every backend; each backend pins the one
# its GUC selects.  This is Option B ("per-session dialect selection")
# from the Track B plan.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('quel_per_session');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'quel'\n"
	  . "client_min_messages = warning\n");
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION quel');

# Seed a table both grammars can reference.
$node->safe_psql('postgres',
	q{CREATE TABLE qb_emp (name text, salary numeric, dept text);
	  INSERT INTO qb_emp VALUES ('ann', 100, 'shoe'), ('bob', 200, 'toy');});

# A QUEL statement using real QUEL keywords (range/of/is, retrieve/where),
# and the equivalent base SQL.  The QUEL form parses ONLY under the quel
# dialect; base SQL parses everywhere.
my $quel_stmt =
  q{range of e is qb_emp; retrieve (e.name) where e.dept = 'shoe';};
my $sql_stmt = q{SELECT name FROM qb_emp WHERE dept = 'shoe';};

# ---------------------------------------------------------------
# Open TWO concurrent backends.  on_error_stop => 0 so a syntax
# error in one session does not tear the session down -- we want to
# keep it alive and prove the other session is unaffected.
# ---------------------------------------------------------------
my $sess_quel = $node->background_psql('postgres', on_error_stop => 0);
my $sess_base = $node->background_psql('postgres', on_error_stop => 0);

$sess_quel->query_safe("SET grammar_dialect = 'quel'");
$sess_base->query_safe("SET grammar_dialect = 'none'");

is($sess_quel->query_safe('SHOW grammar_dialect'),
	'quel', 'session A: grammar_dialect = quel');
is($sess_base->query_safe('SHOW grammar_dialect'),
	'none', 'session B: grammar_dialect = none');

# ---------------------------------------------------------------
# Interleave queries on the two live sessions to demonstrate they are
# concurrent, each with its own grammar.
# ---------------------------------------------------------------

# 1. Session A parses QUEL and returns the shoe employee.
my ($a_out, $a_err) = $sess_quel->query($quel_stmt);
is($a_err, 0, 'session A (quel): QUEL statement parses without error');
is($a_out, 'ann', 'session A (quel): QUEL retrieve returns the shoe row');

# 2. Meanwhile session B parses base SQL fine.
my ($b_sql_out, $b_sql_err) = $sess_base->query($sql_stmt);
is($b_sql_err, 0, 'session B (none): base SQL parses without error');
is($b_sql_out, 'ann', 'session B (none): base SQL SELECT returns the shoe row');

# 3. Session B REJECTS the QUEL statement A just accepted -- its grammar
#    is base SQL only.  This is the crux: same postmaster, same instant,
#    different parser per session.
my ($b_quel_out, $b_quel_err) = $sess_base->query($quel_stmt);
is($b_quel_err, 1,
	'session B (none): the SAME QUEL statement is a syntax error');
like($sess_base->{stderr}, qr/syntax error at or near "range"/,
	'session B (none): error is a base-SQL syntax error on the QUEL verb');

# 4. Session A is still alive and still speaks QUEL after B errored --
#    B's failure did not disturb A's grammar.
($a_out, $a_err) = $sess_quel->query($quel_stmt);
is($a_err, 0, 'session A (quel): still parses QUEL after B errored');
is($a_out, 'ann', 'session A (quel): still returns the shoe row');

# 5. And session A cannot be confused into base-only: a base SQL SELECT
#    also works under the quel dialect (quel is base SQL + QUEL, a superset).
is($sess_quel->query_safe($sql_stmt),
	'ann', 'session A (quel): base SQL still works under the quel dialect');

$sess_quel->quit;
$sess_base->quit;

# ---------------------------------------------------------------
# Backward-compatibility: a session that never sets grammar_dialect
# uses the default "all" grammar (base SQL + every loaded extension),
# so QUEL still parses -- exactly as before per-session selection.
# ---------------------------------------------------------------
{
	my $sess_default = $node->background_psql('postgres', on_error_stop => 0);

	is($sess_default->query_safe('SHOW grammar_dialect'),
		'', 'default session: grammar_dialect is empty (the "all" grammar)');

	my ($d_out, $d_err) = $sess_default->query($quel_stmt);
	is($d_err, 0, 'default session: QUEL parses under the default "all" grammar');
	is($d_out, 'ann', 'default session: QUEL retrieve returns the shoe row');

	# 'all' is an explicit synonym for the default.
	$sess_default->query_safe("SET grammar_dialect = 'all'");
	($d_out, $d_err) = $sess_default->query($quel_stmt);
	is($d_err, 0, "default session: QUEL parses under grammar_dialect = 'all'");
	is($d_out, 'ann', "default session: 'all' also returns the shoe row");

	$sess_default->quit;
}

# ---------------------------------------------------------------
# An unknown dialect name is rejected at SET time (typo protection).
# ---------------------------------------------------------------
{
	my ($rc, $out, $err) = $node->psql('postgres',
		"SET grammar_dialect = 'nosuchdialect'");
	isnt($rc, 0, 'unknown dialect name is rejected at SET time');
	like($err, qr/No loaded grammar dialect is named/,
		'rejection names the offending dialect');
}

$node->stop;

done_testing();
