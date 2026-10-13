# Copyright (c) 2026, PostgreSQL Global Development Group
#
# TAP driver for the property-based tests.  Runs each area of pg_prop and
# reports one test point per area; pg_prop's own per-property output is echoed
# as TAP diagnostics so a failure shows the counterexample and replay blob.
#
# Controls (all optional) come from the environment and are forwarded, so one
# `meson test` run is reproducible and a buildfarm failure can be replayed:
#   PG_PROP_ITERATIONS   test cases per property (default: pg_prop's own 100)
#   PG_PROP_SEED         fixed RNG seed (default: pg_prop picks and prints one)
#   PG_PROP_TESTS        whitespace-separated property names to run
#
# To debug a single property, run pg_prop directly, e.g.
#   pg_prop path canonicalize_idempotent --iterations 100000 --seed 42
# See README and DESIGN.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;

# TAP driver for the in-process (Tier A) property tests.  These exercise
# src/common and src/port code linked directly into pg_prop, so they need no
# cluster.  The SQL-oracle (Tier B) areas run from t/002_sql_props.pl instead.
#
# Keep in step with the Tier-A entries in prop_area_sources (meson.build).
my @areas = qw(path jsonapi encoding unicode_case unicode_norm);

# No Tier-A areas wired in yet (e.g. the initial harness-only commit): nothing
# to run, which is a pass, not a failure.
if (!@areas)
{
	plan skip_all => 'no in-process property areas registered';
}

# Shared arguments from the environment.
my @common_args = ('--no-tap');
push @common_args, ('--iterations', $ENV{PG_PROP_ITERATIONS})
  if defined $ENV{PG_PROP_ITERATIONS} && $ENV{PG_PROP_ITERATIONS} ne '';
push @common_args, ('--seed', $ENV{PG_PROP_SEED})
  if defined $ENV{PG_PROP_SEED} && $ENV{PG_PROP_SEED} ne '';

# PG_PROP_TESTS selects individual properties by name (unqualified).
my @select;
@select = split /\s+/, $ENV{PG_PROP_TESTS}
  if defined $ENV{PG_PROP_TESTS} && $ENV{PG_PROP_TESTS} ne '';

# All areas are known to one binary; list once.
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
		# A selection that this area has none of: skip it with a passing TODO
		# so the plan still accounts for every area.
	  SKIP:
		{
			skip "no selected properties in area $area", 1 unless @mine;
			run_area($area, [ @args, @mine ]);
		}
		next;
	}
	run_area($area, \@args);
}

done_testing();

# Run one area of pg_prop; emit one test point plus diagnostics.
sub run_area
{
	my ($area, $args) = @_;
	my ($stdout, $stderr) = ('', '');
	my $ok = run_log([ 'pg_prop', $area, @$args ],
		'>', \$stdout, '2>', \$stderr);

	# Echo pg_prop's plain output as TAP diagnostics (note() prefixes "# ").
	note($stdout) if $stdout ne '';
	note($stderr) if $stderr ne '';

	ok($ok, "pg_prop $area: all properties held");
}
