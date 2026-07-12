# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Phase 16: verify plperl is pooled-protocol-affine-safe.
#
# plperl keeps a per-OS-thread "current interpreter" (my_perl).  Under the
# pooled protocol several sessions multiplex onto one carrier OS thread, so a
# sibling session running a plperl function moves my_perl.  activate_interpreter()
# must re-assert PERL_SET_CONTEXT whenever the thread's actual current
# interpreter (PERL_GET_CONTEXT) no longer matches the resuming session's
# interpreter, or a resumed session silently runs against the wrong interpreter.
#
# This test forces that interleaving: MANY concurrent sessions (more than the
# carrier count) each stamp a session-unique value into a plperl-visible spot
# and then repeatedly read it back through plperl.  If interpreter state leaked
# across sessions on a shared carrier, a session would read back another
# session's value (or crash).  Correct affine behavior: each session only ever
# sees its own value, across many interleaved round trips.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('phase16_pooled_plperl_affine');
my $init_ok = eval { $node->init; 1; };

# Fewer carriers than sessions => guaranteed multiplexing of plperl sessions
# onto shared carrier OS threads.
$node->append_conf(
	'postgresql.conf', qq(
multithreaded = on
pooled_protocol_carriers = 2
));
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'threaded runtime active');
is($node->safe_psql('postgres', 'SHOW pooled_protocol_carriers'),
	'2', 'pooled protocol runtime with 2 carriers');

# Skip cleanly if this build has no plperl.
my ($rc, $out, $err);
$rc = $node->psql('postgres', 'CREATE EXTENSION plperl;',
	stderr => \$err);
if ($rc != 0)
{
	$node->stop;
	plan skip_all => "plperl not available: $err";
}

# A plperl function that stashes a session-unique tag in a Perl package
# (interpreter-global) variable, and another that reads it back.  In a shared
# interpreter the writes would clobber each other; in a correctly per-session
# interpreter each session sees only its own tag.  Under pooled-affine, the
# interpreters are per session, and this exercises that activate_interpreter()
# re-points my_perl at the RIGHT one on every resume.
$node->safe_psql(
	'postgres', q{
CREATE FUNCTION perl_set_tag(t text) RETURNS void LANGUAGE plperl AS $$
    $PLperl_affine::tag = $_[0];
    return undef;
$$;
CREATE FUNCTION perl_get_tag() RETURNS text LANGUAGE plperl AS $$
    return $PLperl_affine::tag;
$$;
});

# Number of concurrent sessions, deliberately > carriers.
my $NSESS = 8;
my $ROUNDS = 25;

my @sess;
for my $i (0 .. $NSESS - 1)
{
	my $s = $node->background_psql('postgres', timeout => 60);
	$s->query_safe("SELECT perl_set_tag('sess-$i');");
	push @sess, $s;
}

# Interleave reads: round-robin over the sessions many times.  Each read must
# return this session's own tag.  Between rounds the carriers definitely served
# other sessions (there are more sessions than carriers), so my_perl drifted and
# must have been re-asserted per session.
my $bad = 0;
for my $r (1 .. $ROUNDS)
{
	for my $i (0 .. $NSESS - 1)
	{
		my $got = $sess[$i]->query_safe('SELECT perl_get_tag();');
		if ($got ne "sess-$i")
		{
			$bad++;
			diag("round $r session $i: expected sess-$i, got '$got'");
		}
	}
}
is($bad, 0,
	"plperl interpreter state stays per-session across $ROUNDS interleaved rounds ($NSESS sessions, 2 carriers)");

# Re-stamp mid-flight (forces a fresh write on an interpreter that has since been
# used by siblings), then confirm isolation still holds.
for my $i (0 .. $NSESS - 1)
{
	$sess[$i]->query_safe("SELECT perl_set_tag('re-$i');");
}
my $bad2 = 0;
for my $r (1 .. $ROUNDS)
{
	for my $i (0 .. $NSESS - 1)
	{
		my $got = $sess[$i]->query_safe('SELECT perl_get_tag();');
		$bad2++ if $got ne "re-$i";
	}
}
is($bad2, 0, 'plperl per-session isolation holds after mid-flight re-stamp');

# Nested plperl (SPI calling another plperl function) must also re-point the
# interpreter correctly.
$node->safe_psql(
	'postgres', q{
CREATE FUNCTION perl_get_tag_via_spi() RETURNS text LANGUAGE plperl AS $$
    my $rv = spi_exec_query('SELECT perl_get_tag() AS t');
    return $rv->{rows}[0]->{t};
$$;
});
my $bad3 = 0;
for my $i (0 .. $NSESS - 1)
{
	my $got = $sess[$i]->query_safe('SELECT perl_get_tag_via_spi();');
	$bad3++ if $got ne "re-$i";
}
is($bad3, 0, 'plperl per-session isolation holds through nested SPI plperl calls');

$_->quit for @sess;
$node->stop;
done_testing();
