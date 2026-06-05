# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Verifies that Lime's in-process compile API
# (lime_compile_grammar_in_process) is callable from PG.
#
# This is the smallest unit of progress on Phase 4 Track B Phase 2:
# proves liblime_parser.a is linked, the public header is found, and
# the entry point produces a non-NULL ParserSnapshot for a trivial
# grammar.  No changes to parser.c or parser_extension.c yet.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('lime_smoke');
$node->init;
$node->start;

$node->safe_psql('postgres', q{
	CREATE FUNCTION lime_in_process_compile(text) RETURNS text
	AS 'lime_in_process_smoke', 'lime_in_process_compile'
	LANGUAGE C STRICT;
});

# Trivial grammar.  The in-process compile API
# (lime_compile_grammar_text) requires access to lime's own source
# files at runtime (limpar.c, snapshot_build.c) which aren't shipped
# by the standard Nix package.  Without LIME_TEMPLATE /
# LIME_SNAPSHOT_BUILD_C / etc.  set, this returns a structured error
# rather than a snapshot.
#
# This is the smallest unit of forward progress on Phase 4 Track B
# Phase 2: the API is reachable, the public symbol resolves, the
# function returns a structured response.  The full in-process
# integration (zero subprocess overhead) requires either:
#
#   (a) Lime upstream packaging share/lime/limpar.c,
#       snapshot_build.c, etc.  (we'd need them at runtime).
#   (b) PG-side bundling those files in tarballs (vendor copies).
#   (c) Acknowledging that the 'in-process' label is misleading --
#       Lime's lime_compile_grammar_text shells out to the lime
#       CLI itself, just from inside our process.  This would
#       still cut the lime+cc subprocess pipeline (which does
#       fork+exec twice, plus dlopen), but it's not the
#       order-of-magnitude win the design doc predicted.
#
# Tracked: .agent/notes/track-b-phase2-design.md will be updated
# to reflect the actual in-process semantics.
my $trivial_grammar = <<'EOG';
%name TestParse
%token_type {int}
%extra_argument {void *extra}
%token NUMBER.
%start_symbol stmt

stmt(A) ::= NUMBER(B). {
	A = B;
}
EOG

my $result = $node->safe_psql('postgres',
	"SELECT lime_in_process_compile(\$\$$trivial_grammar\$\$)");
diag("in-process compile result: $result");

like($result, qr/^ok: snapshot built \(snap=0x[0-9a-f]+\)/,
	'lime_compile_grammar_in_process returns a non-NULL ParserSnapshot');

# Malformed grammar: unmatched brace.
my $bad_grammar = <<'EOG';
%name TestParse
%include {
incomplete
EOG

my $bad_result = $node->safe_psql('postgres',
	"SELECT lime_in_process_compile(\$\$$bad_grammar\$\$)");
diag("malformed grammar result: $bad_result");

like($bad_result, qr/^error: rc=-?\d+ msg=/,
	'malformed grammar returns a structured rc + msg error');

$node->stop;
done_testing();
