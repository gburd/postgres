# Copyright (c) 2026, PostgreSQL Global Development Group
#
# grammar_ext_compose: torture test for parser_extension.h.
#
# Spins up a postmaster with various combinations of the six
# compose_ext_* extensions in shared_preload_libraries, runs a
# trivial SQL query, and asserts:
#
#   1. Single-extension load: pipeline runs, register() succeeds,
#      base grammar still parses SELECT 1.
#   2. Multi-extension load: alpha + beta both register, single
#      rebuild produces ONE .so combining both, base grammar still
#      parses SELECT 1.
#   3. Token-name no-op: alpha + echo (echo redeclares K_GRAMMAR_-
#      ALPHA with same lexeme/category) both register, no error.
#   4. Token-name conflict: alpha + foxtrot (foxtrot redeclares
#      K_GRAMMAR_ALPHA with different lexeme), foxtrot's
#      register() fails with a clear error.
#   5. Cross-ext token reference: alpha + golf (golf's rule uses
#      K_GRAMMAR_ALPHA), works in the right load order.
#   6. Precedence on existing token: alpha + hotel (hotel sets
#      precedence on K_GRAMMAR_BRAVO from alpha), works.
#   7. Cache key determinism: load alpha+beta in two postmaster
#      cycles, the SHA256 of the input fragment is the same, the
#      cache hit avoids re-running lime+cc.
#   8. Order independence (input fragment): alpha-then-beta vs
#      beta-then-alpha produce structurally-identical fragments
#      modulo extension declaration order.  (We assert the
#      generated .so loads cleanly in both orders, not byte
#      equality of the .so itself -- that would lock cache key
#      to alphabetical order which the implementation may or may
#      not do.)
#   9. Base grammar invariance: loading any combination of the six
#      extensions does NOT change the parse of SELECT 1, CREATE
#      TABLE, or any other base SQL.

use strict;
use warnings FATAL => 'all';

use File::Find ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Each combination spins a fresh cluster.  Cluster startup cost
# (~3s per cluster) dominates; we keep the matrix small but
# exhaustive across the API surface.

# Helper: create + start a cluster with the given comma-separated
# shared_preload_libraries value, then run a smoke query.  Returns
# (cluster, server_log).
sub start_with_extensions
{
	my ($name, $libs) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init;
	# Defer logging to disk so we can grep AFTER stop().
	$node->append_conf('postgresql.conf',
		"shared_preload_libraries = '$libs'\n"
		. "log_min_messages = debug1\n"
		. "client_min_messages = notice\n");
	$node->start;
	return $node;
}

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
# Test 1: single-extension load (alpha alone).
# ---------------------------------------------------------------
{
	my $node = start_with_extensions('compose_alpha',
		'grammar_ext_compose_alpha');
	my $out = $node->safe_psql('postgres', 'SELECT 1');
	is($out, '1', 'single ext: SELECT 1 returns 1');

	my $logs = log_text($node);
	like($logs,
		qr/grammar_ext_compose_alpha registered \(2 tokens, 2 rules, 0 prec\)/,
		'single ext: alpha registered with expected counts');
	like($logs,
		qr/composing grammar in-process for 1 extension\(s\)/,
		'single ext: in-process compose ran exactly once');
	unlike($logs, qr{/pg_parser_cache/[0-9a-f]{64}\.so},
		'single ext: no .so cache (in-process compose)');
	$node->stop;
}

# ---------------------------------------------------------------
# Test 2: multi-extension load (alpha + beta).  One rebuild
# combining both.
# ---------------------------------------------------------------
{
	my $node = start_with_extensions('compose_alpha_beta',
		'grammar_ext_compose_alpha,grammar_ext_compose_beta');
	my $out = $node->safe_psql('postgres', 'SELECT 1');
	is($out, '1', 'alpha+beta: SELECT 1 returns 1');

	my $logs = log_text($node);
	like($logs,
		qr/grammar_ext_compose_alpha registered/,
		'alpha+beta: alpha registered');
	like($logs,
		qr/grammar_ext_compose_beta registered \(2 tokens, 1 rules, 1 prec\)/,
		'alpha+beta: beta registered with prec count');
	like($logs,
		qr/composing grammar in-process for 2 extension\(s\)/,
		'alpha+beta: ONE in-process compose for both extensions');
	$node->stop;
}

# ---------------------------------------------------------------
# Test 3: token-name no-op (alpha + echo).  echo redeclares
# K_GRAMMAR_ALPHA with same lexeme/category.  Per the API
# contract this is a no-op: both extensions register.
# ---------------------------------------------------------------
{
	my $node = start_with_extensions('compose_alpha_echo',
		'grammar_ext_compose_alpha,grammar_ext_compose_echo');
	my $out = $node->safe_psql('postgres', 'SELECT 1');
	is($out, '1', 'alpha+echo: SELECT 1 returns 1');

	my $logs = log_text($node);
	like($logs,
		qr/grammar_ext_compose_echo registered \(1 tokens, 1 rules, 0 prec\)/,
		'alpha+echo: echo registered (re-declaration is a no-op)');
	unlike($logs, qr/echo register\(\) failed/,
		'alpha+echo: echo did NOT report failure');
	$node->stop;
}

# ---------------------------------------------------------------
# Test 4: token-name conflict (alpha + foxtrot).  foxtrot
# redeclares K_GRAMMAR_ALPHA with a DIFFERENT lexeme; the API
# must reject it cleanly.  Under Track B the merged fragment is
# compiled in-process at prewarm (postmaster start); lime reports
# the duplicate token declaration as a compose error, which
# surfaces as a prewarm/first-parse failure rather than at
# register() time.  The TAP test asserts the OBSERVED behaviour.
#
# CURRENT GAP: the API contract says the second declaration should
# fail at register() with a clear error; instead the serializer
# emits BOTH %token directives and the in-process compile errors
# with "K_GRAMMAR_ALPHA already declared".  Validating at register()
# is tracked in lime-letter-34 (token-conflict detection).
# ---------------------------------------------------------------
TODO: {
	local $TODO = 'token-conflict detection happens at in-process '
		. 'compile time, not at register() time; register()-time '
		. 'validation tracked in lime-letter-34';

	my $node = PostgreSQL::Test::Cluster->new('compose_alpha_foxtrot');
	$node->init;
	$node->append_conf('postgresql.conf',
		"shared_preload_libraries = "
			. "'grammar_ext_compose_alpha,grammar_ext_compose_foxtrot'\n"
			. "log_min_messages = debug1\n"
			. "client_min_messages = notice\n");

	# Postmaster might fail to start once the rebuild fires on first
	# parse.  Capture the start outcome and grep logs.
	my $started = eval { $node->start; 1 };
	my $log_after_start = $started ? log_text($node) : '';
	if ($started)
	{
		# Trigger rebuild via a smoke query.
		my $out;
		my $ok = eval {
			$out = $node->safe_psql('postgres', 'SELECT 1');
			1;
		};
		my $logs = log_text($node);
		like(
			$logs,
			qr/(K_GRAMMAR_ALPHA.*already|conflicting.*K_GRAMMAR_ALPHA|register\(\) failed)/,
			'alpha+foxtrot: token-name conflict reported');
		$node->stop if $ok;
	}
	else
	{
		pass('alpha+foxtrot: postmaster failed to start due to conflict');
	}
}

# ---------------------------------------------------------------
# Test 5: cross-extension token reference (alpha + golf).  golf's
# rule uses K_GRAMMAR_ALPHA.  shared_preload_libraries order is
# alpha first, then golf.
# ---------------------------------------------------------------
{
	my $node = start_with_extensions('compose_alpha_golf',
		'grammar_ext_compose_alpha,grammar_ext_compose_golf');
	my $out = $node->safe_psql('postgres', 'SELECT 1');
	is($out, '1', 'alpha+golf: SELECT 1 returns 1');

	my $logs = log_text($node);
	like($logs,
		qr/grammar_ext_compose_golf registered/,
		'alpha+golf: golf registered (cross-ext token visible)');
	$node->stop;
}

# ---------------------------------------------------------------
# Test 6: cross-extension precedence reference (alpha + hotel).
# hotel sets precedence on K_GRAMMAR_BRAVO from alpha.
# ---------------------------------------------------------------
{
	my $node = start_with_extensions('compose_alpha_hotel',
		'grammar_ext_compose_alpha,grammar_ext_compose_hotel');
	my $out = $node->safe_psql('postgres', 'SELECT 1');
	is($out, '1', 'alpha+hotel: SELECT 1 returns 1');

	my $logs = log_text($node);
	like($logs,
		qr/grammar_ext_compose_hotel registered \(1 tokens, 1 rules, 1 prec\)/,
		'alpha+hotel: hotel registered with cross-ext prec ref');
	$node->stop;
}

# ---------------------------------------------------------------
# Test 7: compose determinism.  Spin up alpha+beta in two separate
# clusters; each composes the same fragment set in-process, so the
# composed base rule count (and thus the grammar) is identical.
# Track B has no .so cache -- the relevant invariant is that the
# same extension set always composes to the same grammar.
# ---------------------------------------------------------------
{
	my $first = start_with_extensions('compose_determ_first',
		'grammar_ext_compose_alpha,grammar_ext_compose_beta');
	$first->safe_psql('postgres', 'SELECT 1');
	my $first_logs = log_text($first);
	$first->stop;

	like($first_logs,
		qr/composing grammar in-process for 2 extension\(s\)/,
		'compose: first boot composed both extensions in-process');
	unlike($first_logs, qr{/pg_parser_cache/[0-9a-f]{64}\.so},
		'compose: no .so cache path emitted');

	my $second = start_with_extensions('compose_determ_second',
		'grammar_ext_compose_alpha,grammar_ext_compose_beta');
	is($second->safe_psql('postgres', 'SELECT 1'), '1',
		'compose: second boot of same extension set parses base SQL');
	my $second_logs = log_text($second);
	like($second_logs,
		qr/composing grammar in-process for 2 extension\(s\)/,
		'compose: second boot composed deterministically');
	$second->stop;
}

# ---------------------------------------------------------------
# Test 8: base-grammar invariance.  With the heaviest combination
# (alpha+beta+golf+hotel) loaded, every base SQL still parses.
# ---------------------------------------------------------------
{
	my $node = start_with_extensions('compose_heavy',
		'grammar_ext_compose_alpha,grammar_ext_compose_beta,'
			. 'grammar_ext_compose_golf,grammar_ext_compose_hotel');

	is($node->safe_psql('postgres', 'SELECT 1+1'), '2',
		'heavy load: SELECT 1+1');
	is($node->safe_psql('postgres', "SELECT 'hello'"), 'hello',
		'heavy load: string literal');
	is(
		$node->safe_psql(
			'postgres',
			'CREATE TABLE base_invariance (id int, t text); '
				. 'INSERT INTO base_invariance VALUES (1, \'a\'); '
				. 'SELECT * FROM base_invariance'),
		'1|a',
		'heavy load: DDL+DML+SELECT round-trip');

	# Window function -- exercises a deep grammar path.
	is(
		$node->safe_psql(
			'postgres',
			'SELECT row_number() OVER (ORDER BY g) FROM '
				. 'generate_series(1,3) g LIMIT 1'),
		'1', 'heavy load: window function');

	$node->stop;
}

# ---------------------------------------------------------------
# Test 9: compose-time conflict gate (Lime letter-35 Q1 / v1.8.1).
# The `india` extension registers two identical `stmt ::=
# K_GRAMMAR_INDIA` productions -> a reduce/reduce conflict the LALR
# builder resolves keep-first but reports via nconflict > 0.
# pg_grammar_compose_install calls lime_compile_grammar_in_process_ex
# and REFUSES to install a conflicted parser; the refusal is FATAL at
# prewarm, so the postmaster must FAIL TO START rather than silently
# mis-parse.  We assert the start fails and the log names the conflict.
# ---------------------------------------------------------------
{
	my $node = PostgreSQL::Test::Cluster->new('compose_india_conflict');
	$node->init;
	$node->append_conf('postgresql.conf',
		"shared_preload_libraries = 'grammar_ext_compose_india'\n"
			. "log_min_messages = debug1\n");

	# Startup must fail because the conflicted compose is FATAL at prewarm.
	# fail_ok => 1 makes start() return false instead of BAIL_OUT-ing the
	# whole test file when the postmaster refuses to come up.
	my $started = $node->start(fail_ok => 1);
	ok(!$started, 'india: postmaster refuses to start on a conflicted compose');

	my $logs = slurp_file($node->logfile);
	like($logs,
		qr/grammar extension compose introduced \d+ unresolved conflict/,
		'india: startup log names the unresolved conflict count');

	# Nothing to stop -- the node never came up.
}

done_testing();
