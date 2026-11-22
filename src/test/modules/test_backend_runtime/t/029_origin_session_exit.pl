# Copyright (c) 2026, PostgreSQL Global Development Group

# Each backend must register its own origin cleanup, including after another
# backend exits.  Public DROP is NOWAIT for the origin reference; bounded
# retries allow asynchronous teardown without treating client exit as a barrier.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC usleep);

sub connect_session
{
	my ($node, $name) = @_;
	local $ENV{PGAPPNAME} = $name;
	local $ENV{PGOPTIONS} = '';
	my $session = $node->background_psql('postgres',
		timeout => 5, on_error_stop => 0,
		extra_params => ['-v', 'VERBOSITY=verbose']);
	$session->set_query_timer_restart;
	$session->query_safe("SET statement_timeout = '3s'");
	$session->query_safe("SET lock_timeout = '1s'");
	return $session;
}

# Only the exact object-in-use error may be retried.  A lock timeout, query
# timeout, missing origin, or unexpected diagnostic is a different failure.
sub drop_after_exit
{
	my ($keeper, $name, $oid, $label) = @_;
	my $deadline = clock_gettime(CLOCK_MONOTONIC) + 10;
	my ($state, $stderr);
	while (1)
	{
		$state = $keeper->query(
			"SELECT pg_replication_origin_drop('$name');\n\\echo :SQLSTATE",
			verbose => 0);
		$state =~ s/\A\s+|\s+\z//g;
		$stderr = $keeper->{stderr};
		$keeper->{stderr} = '';
		last if $state eq '00000' && $stderr eq '';
		die "$label: unexpected DROP result: SQLSTATE=$state stderr=$stderr"
		  unless $state eq '55006' && $stderr =~
		  /\Apsql:<stdin>:[0-9]+: ERROR:  55006: could not drop replication origin with ID $oid, in use by PID [0-9]+\r?\nLOCATION:  replorigin_state_clear, origin\.c:[0-9]+(?:\r?\n)?\z/;
		last if clock_gettime(CLOCK_MONOTONIC) >= $deadline;
		usleep(20_000);
	}

	# A permanently retained old-code refcount is an assertion failure, not an
	# expected error.  The final query has its own five-second client bound.
	is($state, '00000', "$label: origin droppable after owner exit")
	  or diag("origin=$name oid=$oid stderr=$stderr");
	return 0 unless $state eq '00000';
	is($keeper->query_safe(
		"SELECT pg_replication_origin_oid('$name') IS NULL"),
		't', "$label: origin removed");
	return 1;
}

for my $lane ('fiber1', 'fork', 'fiber2')
{
	my $mode = $lane eq 'fork' ? 'off' : 'on';
	my $loops = $lane eq 'fiber2' ? 2 : 1;

	my @variants = ('auto', 'reset');
	push @variants, 'reuse' if $lane eq 'fiber1';
	for my $variant (@variants)
	{
		my $case = "${lane}_$variant";
		my $node = PostgreSQL::Test::Cluster->new("origin_$case");
		$node->init;
		$node->append_conf('postgresql.conf', qq{
multithreaded = $mode
carrier_loops = $loops
io_method = sync
summarize_wal = off
autovacuum = off
wal_level = logical
max_replication_slots = 4
max_active_replication_origins = 4
max_logical_replication_workers = 0
shared_preload_libraries = ''
session_preload_libraries = ''
local_preload_libraries = ''
});
		$node->start;
		my ($keeper, $owner);
		my $completed = eval {
			$keeper = connect_session($node, 'origin_keeper');
			is($keeper->query_safe('SHOW multithreaded'), $mode,
				"$case: backend mode");
			is($keeper->query_safe('SHOW max_active_replication_origins'), '4',
				"$case: origin capacity");
			is($keeper->query_safe('SELECT pg_is_in_recovery()'), 'f',
				"$case: not in recovery");
			is($keeper->query_safe('SELECT pg_replication_origin_session_is_setup()'),
				'f', "$case: keeper owns no origin");
			if ($lane eq 'fiber1' || $lane eq 'fiber2')
			{
				like(slurp_file($node->logfile),
					qr/xtc: carrier up \($loops loops?, /,
					"$case: native loop count");
			}
			my $keeper_pid = $keeper->query_safe('SELECT pg_backend_pid()');
			my $previous_pid = $keeper_pid;
			for my $which ('a', 'b')
			{
				my $name = "regress_origin_exit_$which";
				$owner = connect_session($node, "origin_owner_$which");
				my $pid = $owner->query_safe('SELECT pg_backend_pid()');
				isnt($pid, $keeper_pid, "$case/$which: owner differs from keeper");
				isnt($pid, $previous_pid, "$case/$which: distinct owner connection");
				$previous_pid = $pid;
				my $oid = $owner->query_safe("SELECT pg_replication_origin_create('$name')");
				note("$case/$which: backend=$pid origin=$name oid=$oid");
				if ($variant eq 'reuse' && $which eq 'b')
				{
					# The C setup registers cleanup before rejecting this PID.
					my $state = $owner->query(
						"SELECT pg_replication_origin_session_setup('$name', -1);\n\\echo :SQLSTATE");
					is($state, '55000', "$case/b: first setup fails after registration")
					  or die "unexpected setup SQLSTATE=$state stderr=$owner->{stderr}";
					like($owner->{stderr},
						qr/\Apsql:<stdin>:[0-9]+: ERROR:  55000: cannot use PID -1 for inactive replication origin with ID $oid\r?\nLOCATION:  replorigin_session_setup, origin\.c:[0-9]+(?:\r?\n)?\z/,
						"$case/b: exact setup diagnostic")
					  or die "unexpected setup diagnostic: $owner->{stderr}";
					$owner->{stderr} = '';
				}
				$owner->query_safe("SELECT pg_replication_origin_session_setup('$name', 0)");
				is($owner->query_safe('SELECT pg_replication_origin_session_is_setup()'),
					't', "$case/$which: origin setup");
				is($owner->query_safe('SELECT pg_replication_origin_session_progress(false) IS NULL'),
					't', "$case/$which: no origin advance");
				if ($variant eq 'reuse' && $which eq 'b')
				{
					# More than the exit-stack capacity catches re-registration
					# on explicit reset, ending with automatic exit cleanup.
					for (1 .. 32)
					{
						$owner->query_safe('SELECT pg_replication_origin_session_reset()');
						$owner->query_safe("SELECT pg_replication_origin_session_setup('$name', 0)");
					}
				}
				if ($variant eq 'reset' && $which eq 'b')
				{
					$owner->query_safe('SELECT pg_replication_origin_session_reset()');
					is($owner->query_safe('SELECT pg_replication_origin_session_is_setup()'),
						'f', "$case/b: explicit reset");
				}
				$owner->quit;
				$owner = undef;
				# Do not start B if A did not establish the first-owner control.
				last unless drop_after_exit($keeper, $name, $oid, "$case/$which");
			}
			$keeper->quit;
			$keeper = undef;
			1;
		};
		if (!$completed)
		{
			fail("$case: SQL contract completes");
			diag($@);
		}
		# Retained origins on old-code failures are left untouched until stop.
		my $stopped = $node->stop('fast', timeout => 10, fail_ok => 1);
		ok($stopped, "$case: bounded fast shutdown");
		$node->stop('immediate', timeout => 5) unless $stopped;
		for my $session ($owner, $keeper)
		{
			next unless defined $session;
			eval { $session->{timeout}->start; $session->{run}->finish; };
			diag($@) if $@;
		}
		unlike(slurp_file($node->logfile),
			qr/retained runtime-owned roots|PANIC:|segmentation fault|TRAP:/i,
			"$case: no retained roots or crash diagnostics");
	}
}

done_testing();
