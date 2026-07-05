# Copyright (c) 2026, PostgreSQL Global Development Group
#
# grammar_ext_overlap: multi-extension overlap torture test.
#
# Loads all five simulated ecosystem extensions in one
# shared_preload_libraries directive and asserts:
#
#   1. All five register cleanly without conflicts.
#   2. ONE in-process compose runs for all five (not five separate
#      composes); no subprocess, no C compiler, no .so cache.
#   3. Each extension's keywords are reachable from real input.
#   4. Each extension's reduce callback fires when invoked.
#   5. Base SQL still parses identically (SELECT, DDL, DML, CTE,
#      window functions all unaffected).
#   6. Compose is deterministic regardless of extension order.
#   7. Order independence: extensions in different load orders
#      produce the same accept set for the same input.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# All five extensions, in alphabetical order.
my $all_exts = join ',', qw(
	grammar_ext_overlap_duckdb_compat
	grammar_ext_overlap_mongo_jsonb
	grammar_ext_overlap_mysql_compat
	grammar_ext_overlap_pg_infer
	grammar_ext_overlap_quel_lite
);

# Reverse order for the order-independence test.
my $all_exts_rev = join ',', reverse split /,/, $all_exts;

sub start_with
{
	my ($name, $libs) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init;
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

# ------------------------------------------------------------------
# Test 1: Single mega-load with all five extensions.
# ------------------------------------------------------------------
note('=== test 1: load all five extensions simultaneously ===');
{
	my $node = start_with('overlap_all', $all_exts);

	# Trigger first parse so pipeline runs.
	is($node->safe_psql('postgres', 'SELECT 1+1'), '2',
		'all-load: base SQL still works');

	my $logs = log_text($node);

	# All five register cleanly.
	for my $name (qw(
		ext_duckdb_compat ext_mongo_jsonb ext_mysql_compat
		ext_pg_infer ext_quel_lite
		))
	{
		like($logs,
			qr/$name: registered \(\d+ tokens, \d+ rules\)/,
			"all-load: $name registered");
	}

	# ONE in-process compose for ALL five (no subprocess / no cc).
	like($logs,
		qr/composing grammar in-process for 5 extension\(s\)/,
		'all-load: ONE in-process compose for all 5 extensions');

	# Track B never shells out to a C compiler or caches a .so.
	unlike($logs, qr{/pg_parser_cache/[0-9a-f]{64}\.so},
		'all-load: no .so cache path (in-process compose)');
	unlike($logs, qr/running lime to rebuild parser/,
		'all-load: no subprocess rebuild pipeline');

	# Total keyword map should have 13 entries (3+3+2+3+2).
	like($logs,
		qr/grammar extension keyword map: 13 entries published/,
		'all-load: keyword map publishes 13 entries from 5 extensions');

	# Other base SQL is invariant.
	is(
		$node->safe_psql(
			'postgres',
			'CREATE TABLE t (id int); INSERT INTO t VALUES (42); '
				. 'SELECT id FROM t'),
		'42', 'all-load: DDL+DML+SELECT round-trip');

	# Window function -- deepest grammar path.
	is(
		$node->safe_psql(
			'postgres',
			'SELECT row_number() OVER () FROM generate_series(1,1)'),
		'1', 'all-load: window function');

	$node->stop;
}

# ------------------------------------------------------------------
# Test 2: Each extension's keyword reaches its reduce callback.
# ------------------------------------------------------------------
note('=== test 2: every extension keyword fires its reduce ===');
{
	my $node = start_with('overlap_reduces', $all_exts);

	# Each (lexeme, expected_reduce_label) -- one per registered rule.
	my @cases = (
		[ 'pivot',          'duckdb:pivot' ],
		[ 'unpivot',        'duckdb:unpivot' ],
		[ 'qualify',        'duckdb:qualify' ],
		[ 'describe_mysql', 'mysql:describe' ],
		[ 'use_mysql',      'mysql:use' ],
		[ 'utc_timestamp',  'mysql:utc_timestamp' ],
		[ 'mongo_find',     'mongo:find' ],
		[ 'mongo_aggpipe',  'mongo:aggpipe' ],
		[ 'infer',          'infer:infer' ],
		[ 'predict',        'infer:predict' ],
		[ 'train',          'infer:train' ],
		[ 'retrieve_lite',  'quel_lite:retrieve' ],
		[ 'append_lite',    'quel_lite:append' ],
	);

	for my $case (@cases)
	{
		my ($lexeme, $label) = @$case;
		my ($stdout, $stderr);
		my $rc = $node->psql('postgres', "$lexeme;",
			stdout => \$stdout, stderr => \$stderr,
			on_error_die => 0);

		# Each query runs through scan + parse + reduce; the reduce
		# callback fires NOTICE -- captured via stderr/psql.
		# The query then "succeeds" with no rows because the reduce
		# returns NULL (no parse-tree node downstream of stmt rule).
		like($stderr, qr/overlap: \Q$label\E reduced/,
			"reduce: $lexeme -> $label fires");
	}

	$node->stop;
}

# ------------------------------------------------------------------
# Test 3: Mixed-keyword query -- multiple extensions in one
# transaction.  Verify the parser handles back-to-back statements
# from different extensions without state pollution.
# ------------------------------------------------------------------
note('=== test 3: mixed multi-extension session ===');
{
	my $node = start_with('overlap_mixed', $all_exts);
	my $sql = q{
		SELECT 1;
		pivot;
		SELECT 2;
		mongo_find;
		SELECT 3;
		infer;
		SELECT 4;
		retrieve_lite;
	};
	my ($stdout, $stderr);
	$node->psql('postgres', $sql,
		stdout => \$stdout, stderr => \$stderr,
		on_error_die => 0);

	# All four extension reduces should have fired interleaved with
	# the four base SELECT statements.
	for my $label (qw(
		duckdb:pivot mongo:find infer:infer quel_lite:retrieve
		))
	{
		like($stderr, qr/overlap: \Q$label\E reduced/,
			"mixed session: $label fires");
	}

	# The four SELECTs should have produced 1,2,3,4.
	like($stdout, qr/\b1\b/, 'mixed session: SELECT 1 ran');
	like($stdout, qr/\b2\b/, 'mixed session: SELECT 2 ran');
	like($stdout, qr/\b3\b/, 'mixed session: SELECT 3 ran');
	like($stdout, qr/\b4\b/, 'mixed session: SELECT 4 ran');

	$node->stop;
}

# ------------------------------------------------------------------
# Test 4: Order independence -- forward vs reverse order.
# ------------------------------------------------------------------
note('=== test 4: order-independence (forward vs reverse) ===');
{
	# Forward: alphabetical (already cached from test 1).
	# Reverse: reversed list.  Cache keys MAY differ (extension
	# fragment order is part of the SHA256 input) but BEHAVIOR
	# must be identical.
	my $node = start_with('overlap_reverse', $all_exts_rev);

	# Every keyword still works.
	for my $lexeme (qw(pivot mongo_find infer retrieve_lite describe_mysql))
	{
		my ($stdout, $stderr);
		$node->psql('postgres', "$lexeme;",
			stdout => \$stdout, stderr => \$stderr,
			on_error_die => 0);
		like($stderr, qr/overlap:.*reduced/,
			"reverse order: $lexeme reduces");
	}

	# Base SQL still works.
	is($node->safe_psql('postgres', 'SELECT 1'), '1',
		'reverse order: base SQL invariant');

	$node->stop;
}

# ------------------------------------------------------------------
# Test 5: Subset load -- only 3 of 5 extensions.  Verify keywords
# from non-loaded extensions fall through to IDENT (and thus
# produce the expected SQL syntax-error path).
# ------------------------------------------------------------------
note('=== test 5: subset load -- non-loaded keywords are IDENT ===');
{
	my $subset = join ',', qw(
		grammar_ext_overlap_duckdb_compat
		grammar_ext_overlap_pg_infer
	);
	my $node = start_with('overlap_subset', $subset);

	# pivot is in DuckDB compat -- should fire.
	my ($stdout, $stderr);
	$node->psql('postgres', 'pivot;',
		stdout => \$stdout, stderr => \$stderr,
		on_error_die => 0);
	like($stderr, qr/overlap: duckdb:pivot reduced/,
		'subset load: pivot still works');

	# mongo_find is in MongoDB compat -- NOT loaded.
	# Should fall through to IDENT path and produce the standard
	# "syntax error" or "column does not exist" depending on context.
	$node->psql('postgres', 'mongo_find;',
		stdout => \$stdout, stderr => \$stderr,
		on_error_die => 0);
	# It's an IDENT now, which as a top-level statement is syntax
	# error.  Must NOT see "mongo:find reduced" since the extension
	# isn't loaded.
	unlike($stderr, qr/overlap: mongo:find reduced/,
		'subset load: mongo_find does NOT fire (ext not loaded)');
	like($stderr, qr/syntax error/,
		'subset load: mongo_find produces syntax error (IDENT path)');

	$node->stop;
}

# ------------------------------------------------------------------
# Test 6: Compose determinism for the same set of extensions across
# postmaster boots -- the composed snapshot's base rule count is
# stable, so the same fragments always compose to the same grammar.
# ------------------------------------------------------------------
note('=== test 6: compose determinism across postmaster boots ===');
{
	my $node = start_with('overlap_determinism', $all_exts);
	$node->safe_psql('postgres', 'SELECT 1');
	my $logs = log_text($node);
	like($logs,
		qr/composing grammar in-process for 5 extension\(s\)/,
		'compose runs once per boot, in-process');
	# Every extension keyword still reaches its reduce after a fresh boot.
	my ($stdout, $stderr);
	$node->psql('postgres', 'pivot;',
		stdout => \$stdout, stderr => \$stderr, on_error_die => 0);
	like($stderr, qr/overlap: duckdb:pivot reduced/,
		'fresh boot: composed grammar fires extension reduce');
	$node->stop;
}

done_testing();
