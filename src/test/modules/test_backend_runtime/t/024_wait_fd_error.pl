# Copyright (c) 2026, PostgreSQL Global Development Group

# Focused Linux fixture: launch this TAP explicitly under
# prlimit --nofile=4096:4096, with PG_TEST_WAIT_FD_BACKEND=uring (or epoll).
# Do not lower the host/ordinary test runner's limit.  The real invalid positive
# FD is outside the server's hard limit, so another thread cannot reuse it.
# A finite INVAL can follow a timer; it is not proof of a kernel completion.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Errno qw(ENOENT ESRCH);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

plan skip_all => 'requires Linux process identity and fd inspection'
  unless $^O eq 'linux';
my $backend = $ENV{PG_TEST_WAIT_FD_BACKEND};
plan skip_all => 'requires explicit limited FD-wait qualification run'
  unless defined $backend;
BAIL_OUT('PG_TEST_WAIT_FD_BACKEND must be uring or epoll')
  unless $backend eq 'uring' || $backend eq 'epoll';
like(slurp_file('/proc/self/limits'),
	qr/^Max open files\s+4096\s+4096\s+files[ \t]*$/m,
	'focused runner has the fixed soft and hard fd limit')
  or BAIL_OUT('launch only this test with prlimit --nofile=4096:4096');

local $ENV{PGOPTIONS} = '';

# Linux starttime distinguishes the original host process from a reused PID.
sub identity
{
	my ($pid) = @_;
	open my $fh, '<', "/proc/$pid/stat" or do {
		return undef if $!{ENOENT} || $!{ESRCH};
		die "cannot open /proc/$pid/stat: $!";
	};
	local $! = 0;
	my $stat = <$fh>;
	my $read_error = $!;
	close $fh or die "cannot close /proc/$pid/stat: $!";
	if (!defined $stat)
	{
		return undef if $read_error == ENOENT || $read_error == ESRCH;
		die "cannot read /proc/$pid/stat: $read_error" if $read_error;
		# Empty reads are not death evidence unless the process is gone too.
		if (!stat("/proc/$pid"))
		{
			return undef if $!{ENOENT} || $!{ESRCH};
			die "cannot stat /proc/$pid: $!";
		}
		die "empty /proc/$pid/stat for an existing process";
	}
	$stat =~ /^\d+ \(.*\) (.*)$/ or die "invalid process stat: $stat";
	my @fields = split / /, $1;
	return [$fields[0], $fields[19]];
}

sub dead
{
	my ($pid, $start) = @_;
	my $now = identity($pid);
	return !defined($now) || $now->[1] ne $start || $now->[0] eq 'Z';
}

sub wait_until
{
	my ($predicate, $seconds) = @_;
	my $end = clock_gettime(CLOCK_MONOTONIC) + $seconds;
	do
	{
		return 1 if $predicate->();
		sleep 0.02;
	} while (clock_gettime(CLOCK_MONOTONIC) < $end);
	return 0;
}

# Capture children of every thread, not SQL-visible logical backend PIDs.
sub remember_children
{
	my ($pid, $children) = @_;
	for my $path (glob("/proc/$pid/task/*/children"))
	{
		open my $fh, '<', $path or next;
		my $line = <$fh> // '';
		close $fh;
		for my $child (split /\s+/, $line)
		{
			my $id = identity($child);
			$children->{$child} = $id->[1] if defined $id;
		}
	}
}

sub connect_session
{
	my ($node) = @_;
	my $session = $node->background_psql('postgres', timeout => 60);
	$session->set_query_timer_restart;
	return $session;
}

for my $case (['fork', 'off', 20, 'true'],
	['finite', 'on', 20, 'false'],
	['infinite', 'on', -1, 'false'],
	['held', 'on', -1, 'true'])
{
	my ($name, $mode, $timeout, $held) = @$case;
	my $node = PostgreSQL::Test::Cluster->new("wait_fd_$name");
	$node->init;
	$node->append_conf('postgresql.conf', qq{
multithreaded = $mode
carrier_loops = 1
io_method = sync
autovacuum = off
summarize_wal = off
max_connections = 10
max_worker_processes = 2
max_logical_replication_workers = 0
max_parallel_workers = 0
logging_collector = off
statement_timeout = 0
lock_timeout = 0
transaction_timeout = 0
idle_session_timeout = 0
idle_in_transaction_session_timeout = 0
synchronous_commit = on
});
	$node->start;
	# This module has no custom-GUC preload accessor; load it dynamically.
	$node->safe_psql('postgres', 'CREATE EXTENSION test_backend_runtime_threaded');
	is($node->safe_psql('postgres', 'SHOW multithreaded'), $mode,
		"$name: requested backend model");

	my $victim = connect_session($node);
	my $sibling = connect_session($node);
	if ($mode eq 'off')
	{
		for my $lock ('false', 'true')
		{
			is($victim->query_safe(
				"SELECT test_backend_runtime_wait_fd_error(20, $lock)"),
				'process wait returned', "fork: supported WaitLatch, held=$lock");
		}
		is($sibling->query_safe('SELECT 1'), '1', 'fork: sibling remains usable');
		$victim->quit;
		$sibling->quit;
		$node->stop;
		next;
	}

	like(slurp_file($node->logfile),
		qr/xtc: carrier up \(1 loop, /,
		"$name: one fiber carrier loop");
	is($victim->query_safe('SELECT test_backend_runtime_wait_fd_control()'),
		"fiber|$backend|timeout0=1|timeout20=1|readable=1",
		"$name: actual libxtc backend and normal FD wait controls")
	  or BAIL_OUT("$name: FD wait controls did not qualify the fixture");
	is($victim->query_safe('SELECT test_backend_runtime_hold_lwlock(20)'),
		't', "$name: ordinary held-lock wait releases its lock");
	is($victim->query_safe('SELECT test_backend_runtime_wait_on_lwlock()'),
		't', "$name: lock can be reacquired");

	# Separate transactions put the second durable row after the checkpoint.
	$victim->query_safe('CREATE TABLE sentinel (id int PRIMARY KEY)');
	$victim->query_safe('INSERT INTO sentinel VALUES (1)');
	$victim->query_safe('CHECKPOINT');
	$victim->query_safe('INSERT INTO sentinel VALUES (2)');
	is($victim->query_safe('SELECT string_agg(id::text, \',\' ORDER BY id) FROM sentinel'),
		'1,2', "$name: both committed generations visible before failure");

	my $pid = $node->{_pid};
	my $id = identity($pid);
	BAIL_OUT("$name: missing original postmaster identity") unless defined $id;
	my $start = $id->[1];
	note "$name: original postmaster pid=$pid starttime=$start";
	my @logstat = stat($node->logfile);
	my @fdstat = stat("/proc/$pid/fd/2");
	ok(-f "/proc/$pid/fd/2" && @fdstat && @logstat
		&& $fdstat[0] == $logstat[0] && $fdstat[1] == $logstat[1],
		"$name: raw stderr is the regular pg_ctl logfile")
	  or BAIL_OUT('uncontrolled diagnostic sink');

	my %children;
	remember_children($pid, \%children);
	# No new workers are requested during the fixture.  Remember any startup
	# process still finishing its handoff, and require its exit before restart.
	$sibling->query_safe("SET application_name = 'wait_fd_sibling'");
	$sibling->query_until(qr/sibling waiting/,
		"\\echo sibling waiting\nSELECT pg_sleep(60);\n");
	ok($node->poll_query_until('postgres', q{
		SELECT count(*) = 1 FROM pg_stat_activity
		WHERE application_name = 'wait_fd_sibling' AND wait_event = 'PgSleep'
	}), "$name: connected sibling is waiting before failure")
	  or BAIL_OUT("$name: sibling wait fixture not established");
	my $early_output = $victim->query_until(qr/victim starting/,
		"\\echo victim starting\nSELECT test_backend_runtime_wait_fd_error($timeout, $held);\n");

	my $died = wait_until(sub {
		$victim->{run}->pump_nb if $victim->{run}->pumpable;
		$sibling->{run}->pump_nb if $sibling->{run}->pumpable;
		remember_children($pid, \%children) unless dead($pid, $start);
		return dead($pid, $start);
	}, 10);

	# This assertion precedes ALL stop/kill/cleanup actions, including on old
	# policy.  Connection loss alone must never satisfy it.
	ok($died, "$name: original postmaster exits before deadline and cleanup");
	my $log = slurp_file($node->logfile);
	like($log,
		qr/wait-fd-test: calibration fiber=1 backend=\Q$backend\E fd=4096 timeout=$timeout rc=-1 revents=0 roots=ok/,
		"$name: real INVAL calibration with restored roots");
	unlike($log, qr/wait-fd-test: PG unwind reached/,
		"$name: no PostgreSQL error unwind");

	if (!$died)
	{
		# The tests-only baseline must reach this observation, not abort in the
		# helper.  Do not turn its expected red result into a success-policy mode.
		like($early_output . $victim->{stdout}, qr/old_wakes=[1-3] mailbox_clear=1/,
			"$name: old wrapper converts hard failure to a mailbox-free wake");
		note "$name: victim stdout=$victim->{stdout}, stderr=$victim->{stderr}";
		my ($out, $err, $timed_out);
		my $ret = $node->psql('postgres', 'SELECT 1', stdout => \$out,
			stderr => \$err, timeout => 5, timed_out => \$timed_out);
		ok(!$timed_out && defined($ret) && $ret == 0 && $out eq '1'
			&& !dead($pid, $start), "$name: old postmaster remains alive and usable");
		note "$name: deadline expired; cleanup begins now, not a fail-stop event";
		$node->stop('immediate', timeout => 5);
		$victim->{run}->finish;
		$sibling->{run}->finish;
		next;
	}

	my $held_bit = $held eq 'true' ? 1 : 0;
	like($log, qr/wait-fd-test: wrapper entering held=$held_bit mailbox_empty=1/,
		"$name: real wrapper entered with the requested lock state");
	like($log, qr/xtc: xtc_proc_wait_fd failed fd=4096 rc=-1; terminating process/,
		"$name: production hard-return diagnostic");
	for my $client ([$victim, 'victim'], [$sibling, 'sibling'])
	{
		my ($session, $label) = @$client;
		ok(wait_until(sub {
			$session->{run}->pump_nb if $session->{run}->pumpable;
			return !$session->{run}->pumpable;
		}, 5), "$name: $label client exits after connection loss")
		  or BAIL_OUT("$name: client did not quiesce; no restart");
		$session->{run}->finish;
		like($session->{stderr},
			qr/server closed the connection unexpectedly|connection to server was lost/,
			"$name: $label observes connection loss");
	}
	ok(wait_until(sub {
		for my $child (keys %children)
		{
			return 0 unless dead($child, $children{$child});
		}
		return 1;
	}, 10), "$name: test-owned children quiesce before restart")
	  or BAIL_OUT("$name: surviving children; no restart or stale-file removal");

	$node->{_pid} = undef;
	# Only this node's exact stale files, after identity-checked process death.
	for my $path ($node->data_dir . '/postmaster.pid',
		$node->host . '/.s.PGSQL.' . $node->port . '.lock')
	{
		unlink $path or die "cannot remove $path: $!" if -e $path;
	}
	$node->start;
	is($node->safe_psql('postgres',
		q{SELECT string_agg(id::text, ',' ORDER BY id) FROM sentinel}),
		'1,2', "$name: pre/post-checkpoint commits survive manual restart");
	$node->safe_psql('postgres', 'INSERT INTO sentinel VALUES (3)');
	is($node->safe_psql('postgres', 'SELECT count(*) FROM sentinel'),
		'3', "$name: new committed write after recovery");
	$node->stop;
}

done_testing();
