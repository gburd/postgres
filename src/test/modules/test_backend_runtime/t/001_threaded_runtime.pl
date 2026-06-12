# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use IPC::Run ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('threaded_runtime');

sub start_psql_script
{
	my ($sql, $timeout) = @_;
	my $stdin = $sql;
	my $stdout = '';
	my $stderr = '';
	my $timer = IPC::Run::timer($timeout);
	my @cmd = (
		'psql',
		'--no-psqlrc',
		'--no-align',
		'--tuples-only',
		'--quiet',
		'--dbname' => $node->connstr('postgres'),
		'--file' => '-');
	my $run = IPC::Run::start(\@cmd, '<', \$stdin, '>', \$stdout, '2>',
		\$stderr, $timer);

	return {
		run => $run,
		timer => $timer,
		stdout => \$stdout,
		stderr => \$stderr,
	};
}

sub postmaster_child_count
{
	my $postmaster_pid = slurp_file($node->data_dir . '/postmaster.pid');
	$postmaster_pid =~ s/\n.*//s;

	my $stdout = '';
	my $stderr = '';
	my $result = IPC::Run::run [ 'ps', '-Ao', 'ppid=' ], '>', \$stdout, '2>',
	  \$stderr;
	die "could not count postmaster children: $stderr" unless $result;

	my $count = 0;
	foreach my $ppid (split /\n/, $stdout)
	{
		$ppid =~ s/^\s+|\s+$//g;
		$count++ if $ppid eq $postmaster_pid;
	}
	return $count;
}

sub postmaster_child_command_count
{
	my ($pattern) = @_;
	my $postmaster_pid = slurp_file($node->data_dir . '/postmaster.pid');
	$postmaster_pid =~ s/\n.*//s;

	my $stdout = '';
	my $stderr = '';
	my $result = IPC::Run::run [ 'ps', '-Ao', 'ppid=,command=' ], '>',
	  \$stdout, '2>', \$stderr;
	die "could not inspect postmaster children: $stderr" unless $result;

	my $count = 0;
	foreach my $line (split /\n/, $stdout)
	{
		$line =~ s/^\s+//;
		my ($ppid, $command) = split /\s+/, $line, 2;
		next unless defined $command;
		$count++ if $ppid eq $postmaster_pid && $command =~ $pattern;
	}
	return $count;
}

$node->init;
$node->append_conf(
	'postgresql.conf', q{
multithreaded = on
autovacuum = on
autovacuum_naptime = '1h'
io_method = worker
io_min_workers = 2
io_max_workers = 4
io_worker_launch_interval = 0
io_worker_idle_timeout = '60s'
log_min_messages = debug1
});
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'threaded runtime enabled');

$node->safe_psql(
	'postgres',
	q{
CREATE FUNCTION test_backend_runtime_request_autovacuum_worker()
RETURNS bool
AS 'test_backend_runtime_threaded',
   'test_backend_runtime_request_autovacuum_worker'
LANGUAGE C;
CREATE FUNCTION test_backend_runtime_rejects_process_bgworker()
RETURNS bool
AS 'test_backend_runtime_threaded',
   'test_backend_runtime_rejects_process_bgworker'
LANGUAGE C;
CREATE FUNCTION test_backend_runtime_launch_thread_bgworker()
RETURNS int4
AS 'test_backend_runtime_threaded',
   'test_backend_runtime_launch_thread_bgworker'
LANGUAGE C;
CREATE FUNCTION test_backend_runtime_restart_thread_bgworker()
RETURNS bool
AS 'test_backend_runtime_threaded',
   'test_backend_runtime_restart_thread_bgworker'
LANGUAGE C;
SELECT test_backend_runtime_request_autovacuum_worker();
});

for (1 .. 50)
{
	last
	  if slurp_file($node->logfile) =~
	  qr/autovacuum worker started without a worker entry/;
	usleep(100_000);
}

like(slurp_file($node->logfile),
	qr/autovacuum worker started without a worker entry/,
	'deterministic autovacuum worker thread reached worker main');

SKIP:
{
	skip 'postmaster child counting smoke is Unix-specific', 8
	  if $^O eq 'MSWin32';

	$node->poll_query_until(
		'postgres',
		q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'autovacuum launcher';},
		't') || die "timed out waiting for autovacuum launcher";
	is($node->safe_psql(
			'postgres',
			q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'autovacuum launcher';}),
		'1', 'autovacuum launcher is visible as a logical threaded backend');
	is(postmaster_child_command_count(qr/autovacuum launcher/), 0,
		'autovacuum launcher did not fork a postmaster child process');

	$node->poll_query_until(
		'postgres',
		q{SELECT count(*) = 2 FROM pg_stat_activity WHERE backend_type = 'io worker';},
		't') || die "timed out waiting for startup IO workers";
	my $io_workers_before = $node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'io worker'});
	my $children_before = postmaster_child_count();

	is($io_workers_before, '2',
		'threaded runtime starts with two logical IO workers');
	is(postmaster_child_command_count(qr/io worker|ioworker/), 0,
		'startup IO workers were handed off to thread carriers');
	is($children_before, 0,
		'threaded runtime has no postmaster child processes after startup handoff');

	$node->safe_psql('postgres', q{ALTER SYSTEM SET io_min_workers = 3});
	$node->safe_psql('postgres', q{SELECT pg_reload_conf()});

	my $io_workers_after = 0;
	for (1 .. 50)
	{
		$io_workers_after = $node->safe_psql('postgres',
			q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'io worker'});
		last if $io_workers_after >= $io_workers_before + 1;
		usleep(100_000);
	}

	is($io_workers_after, '3', 'threaded runtime launched a late IO worker');
	is(postmaster_child_count(), $children_before,
		'late IO worker used a thread carrier, not a new process');
}

$node->safe_psql(
	'postgres',
	q{
CREATE TABLE threaded_runtime_stress(id int primary key, payload text);
INSERT INTO threaded_runtime_stress
SELECT g, repeat('x', 100) FROM generate_series(1, 2000) g;
});
$node->safe_psql(
	'postgres',
	q{
UPDATE threaded_runtime_stress SET payload = payload || 'y';
DELETE FROM threaded_runtime_stress WHERE id = 1;
SELECT pg_stat_force_next_flush();
});
pass('threaded DDL and primary-key index build completed');

my @sessions;
my %signal_pids;
for my $i (1 .. 5)
{
	my $session = $node->background_psql('postgres', timeout => 20);
	my $pid = $session->query_safe('SELECT pg_backend_pid();',
		verbose => 0);
	$signal_pids{$pid} = 1;
	push @sessions, $session;
}
is(scalar(keys %signal_pids), 5,
	'concurrent threaded sessions have distinct SQL-visible backend ids');

foreach my $session (@sessions)
{
	$session->quit;
}

my $cancel_psql = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT pg_sleep(30);\n", 30);
ok( pump_until($cancel_psql->{run}, $cancel_psql->{timer},
		$cancel_psql->{stdout}, qr/^\d+\s*$/m),
	'active threaded backend reported logical backend id');
my ($cancel_pid) = ${ $cancel_psql->{stdout} } =~ /^(\d+)\s*$/m;
is($node->safe_psql('postgres', "SELECT pg_cancel_backend($cancel_pid);"),
	't', 'cancel request accepted for active threaded backend');
ok( pump_until($cancel_psql->{run}, $cancel_psql->{timer},
		$cancel_psql->{stderr}, qr/canceling statement due to user request/),
	'active threaded backend observed query cancel');
eval { $cancel_psql->{run}->finish; };

my $victim = $node->background_psql('postgres', on_error_stop => 0,
	timeout => 20);
my $victim_pid = $victim->query_safe('SELECT pg_backend_pid();',
	verbose => 0);
is($node->safe_psql('postgres',
		"SELECT pg_terminate_backend($victim_pid, 5000);"),
	't', 'terminate request accepted for idle threaded backend');
$node->poll_query_until(
	'postgres',
	"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $victim_pid);",
	't') || die "timed out waiting for terminated threaded backend";
pass('idle threaded backend left pg_stat_activity after terminate');
eval { $victim->{run}->finish; };

$node->psql(
	'postgres',
	'SELECT 1 / 0;',
	on_error_stop => 1,
	stdout => \my $error_stdout,
	stderr => \my $error_stderr);
like($error_stderr, qr/division by zero/,
	'threaded backend reported SQL ERROR');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after SQL ERROR');

my ($abort_ret, $abort_stdout, $abort_stderr) = $node->psql(
	'postgres',
	'BEGIN; SELECT pg_advisory_xact_lock(987655); SELECT 1 / 0;',
	on_error_stop => 1);
isnt($abort_ret, 0, 'threaded transaction abort fixture failed as expected');
like($abort_stderr, qr/division by zero/,
	'threaded transaction abort fixture reported SQL ERROR');
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted;"),
	'0', 'threaded transaction abort released advisory locks');

$node->safe_psql(
	'postgres',
	q{
CREATE EXTENSION IF NOT EXISTS plpgsql;
CREATE FUNCTION threaded_plpgsql_add(a int, b int)
RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  RETURN a + b;
END
$$;
});
is($node->safe_psql('postgres', 'SELECT threaded_plpgsql_add(20, 22);'),
	'42', 'PL/pgSQL runs in threaded runtime');

my ($load_ret, $load_stdout, $load_stderr) =
  $node->psql('postgres', "LOAD 'test_backend_runtime';",
	on_error_stop => 1);
isnt($load_ret, 0,
	'process-only test module is rejected in threaded runtime');
like($load_stderr, qr/backend model mismatch/,
	'process-only module rejection reports backend model mismatch');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after process-only module rejection');

is($node->safe_psql(
		'postgres',
		'SELECT test_backend_runtime_rejects_process_bgworker();'),
	't', 'process-model background worker is rejected in threaded runtime');
like(
	slurp_file($node->logfile),
	qr/background worker "test_backend_runtime process bgworker" is not supported in threaded mode/,
	'process-model background worker rejection is logged explicitly');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after process-model bgworker rejection');

like(
	$node->safe_psql(
		'postgres',
		'SELECT test_backend_runtime_launch_thread_bgworker();'),
	qr/^\d+$/,
	'thread-model background worker starts and stops in threaded runtime');
like(
	slurp_file($node->logfile),
	qr/starting background worker thread carrier/,
	'thread-model background worker used a thread carrier');

is($node->safe_psql(
		'postgres',
		'SELECT test_backend_runtime_restart_thread_bgworker();'),
	't', 'restartable thread-model background worker restarted and stopped');
like(
	slurp_file($node->logfile),
	qr/test_backend_runtime restart bgworker run 2/,
	'restartable thread-model background worker reached its second run');

is($node->safe_psql(
		'postgres',
		q{
SET debug_parallel_query = on;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 4;
CREATE TEMP TABLE threaded_parallel AS
SELECT i, i % 10 AS g FROM generate_series(1, 20000) AS i;
ALTER TABLE threaded_parallel SET (parallel_workers = 4);
ANALYZE threaded_parallel;
SELECT sum(i)::text || '|' || count(*)::text
FROM threaded_parallel
WHERE g >= 0;
}),
	'200010000|20000',
	'parallel query returns expected result in threaded runtime');
like(
	slurp_file($node->logfile),
	qr/starting background worker thread carrier "parallel worker/,
	'parallel query used background worker thread carriers');

my $abandoned = $node->background_psql('postgres',
	on_error_stop => 0, timeout => 20);
$abandoned->query_safe('BEGIN; SELECT pg_advisory_lock(987654);',
	verbose => 0);
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted;"),
	'1', 'advisory lock is visible before client abandon');
$abandoned->{run}->kill_kill;
eval { $abandoned->{run}->finish; };

for (1 .. 100)
{
	last
	  if $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted;") eq '0';
	usleep(100_000);
}
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted;"),
	'0', 'abandoned threaded backend released advisory lock');

my $reconnect_ok = 1;
for my $i (1 .. 30)
{
	my $result = eval {
		$node->safe_psql('postgres', "SELECT $i;");
	};

	if (!defined $result || $result ne "$i")
	{
		diag("reconnect iteration $i returned "
		  . (defined $result ? qq{"$result"} : 'undef'));
		$reconnect_ok = 0;
		last;
	}
}
ok($reconnect_ok, 'repeated threaded connect/disconnect loop completed');

is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after Gate D smoke');

SKIP:
{
	skip 'postmaster child counting smoke is Unix-specific', 1
	  if $^O eq 'MSWin32';

	is(postmaster_child_count(), 0,
		'threaded runtime still has no postmaster child processes after worker activity');
}

unlike(
	slurp_file($node->logfile),
	qr/PANIC|segmentation|unsupported byval|could not find tuple|server process .* was terminated|was terminated by signal/,
	'server log has no threaded-runtime crash/corruption signatures');

$node->stop('fast');

done_testing();
