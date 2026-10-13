# Copyright (c) 2026, PostgreSQL Global Development Group
#
# TAP driver for the SQL-oracle (Tier B) property tests.  These exercise
# backend code -- the planner, executor, and type machinery -- which cannot be
# linked into a frontend binary, so each property drives SQL against a live
# node and compares the answer to an oracle (a reference query, or the same
# query with an optimisation disabled).  This script starts one node, exports
# its PGHOST/PGPORT, and runs each Tier-B area of pg_prop against it.
#
# Controls (optional) are read from the environment and forwarded, so a run is
# reproducible and a buildfarm failure can be replayed:
#   PG_PROP_ITERATIONS   test cases per property (default: pg_prop's own 100)
#   PG_PROP_SEED         fixed RNG seed (default: pg_prop picks and prints one)
#   PG_PROP_TESTS        whitespace-separated property names to run
#
# To debug one failure interactively, start a node, point PGHOST/PGPORT at it,
# then run e.g.  pg_prop partprune pruning_preserves_rows -n 100000 -s 42
# See README and DESIGN.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Keep in step with the Tier-B entries in prop_area_sources (meson.build).
my @areas = qw(numeric numcast numscale numround numfmt numericalg numericcmp numericdiv decimalfmt mathfn mathround powexp float floatspecial floatcast intops intbit bitint bitlogic bitstringops safemath arithcmp cmpops text textfns textpos textcase textlength textnum textsplit strfunc strpos substring padtrim trimfns like likepattern stringmatch regexp regexfns regexmatch regexpreplace char charconv charfuncs varchartypmod cash bool boolcast booltest boolexpr casewhen condexpr coalesce coalescefn nullsemantics nulldistinct oid uuid uuidops bytea byteaops bit record enum jsonb jsonbops jsonbpath jsonbagg2 jsonbnum jsonbtype jsonbsub jsonbkeys jsonbcontain jsonbdelete jsonbexists jsonbmerge jsonbarray jsonbroundtrip jsonnest jsonbuild jsonconstruct jsontext jsonformat jsonpath jsonpathq jsonpathmath jsoneach array arrayops arrayslice arraycmp arrayconstruct arrayreduce arraysort arraynull anyall inlist range rangeops rangeagg multirange inet inetarith);

# No Tier-B areas wired in yet (e.g. the initial harness-only commit): nothing
# to run, which is a pass.  Skip before starting a node, so we don't pay for a
# cluster we won't use.
if (!@areas)
{
	plan skip_all => 'no SQL-oracle property areas registered';
}

my $node = PostgreSQL::Test::Cluster->new('prop');
$node->init;
$node->start;

# Point libpq (used by pg_prop's Tier-B helpers) at this node.
$ENV{PGHOST} = $node->host;
$ENV{PGPORT} = $node->port;
$ENV{PGDATABASE} = 'postgres';

my @common_args = ('--no-tap');
push @common_args, ('--iterations', $ENV{PG_PROP_ITERATIONS})
  if defined $ENV{PG_PROP_ITERATIONS} && $ENV{PG_PROP_ITERATIONS} ne '';
push @common_args, ('--seed', $ENV{PG_PROP_SEED})
  if defined $ENV{PG_PROP_SEED} && $ENV{PG_PROP_SEED} ne '';

my @select;
@select = split /\s+/, $ENV{PG_PROP_TESTS}
  if defined $ENV{PG_PROP_TESTS} && $ENV{PG_PROP_TESTS} ne '';

my $list = '';
run_log([ 'pg_prop', '--list' ], '>', \$list);
my %known_in_area;
for my $qualified (split /\n/, $list)
{
	my ($a, $n) = split m{/}, $qualified, 2;
	push @{ $known_in_area{$a} }, $n if defined $n;
}

foreach my $area (@areas)
{
	my @args = @common_args;
	if (@select)
	{
		my %have = map { $_ => 1 } @{ $known_in_area{$area} // [] };
		my @mine = grep { $have{$_} } @select;
	  SKIP:
		{
			skip "no selected properties in area $area", 1 unless @mine;
			run_area($area, [ @args, @mine ]);
		}
		next;
	}
	run_area($area, \@args);
}

$node->stop;
done_testing();

sub run_area
{
	my ($area, $args) = @_;
	my ($stdout, $stderr) = ('', '');
	my $ok = run_log([ 'pg_prop', $area, @$args ],
		'>', \$stdout, '2>', \$stderr);

	note($stdout) if $stdout ne '';
	note($stderr) if $stderr ne '';

	ok($ok, "pg_prop $area: all properties held");
}
