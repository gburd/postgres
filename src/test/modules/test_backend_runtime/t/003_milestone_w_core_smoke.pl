# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use IPC::Run ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('threaded_milestone_w');
my $top_reclaim_re =
  qr/(?:thread-backed child|pooled logical backend) \d+ reclaimed [1-9]\d* bytes from TopMemoryContext at exit/;

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

sub wait_for_pids_to_leave_pg_stat_activity
{
	my ($pids, $label) = @_;
	my $pid_list = join(',', map { int($_) } @$pids);

	$node->poll_query_until(
		'postgres',
		"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid IN ($pid_list));",
		't') || die "timed out waiting for $label";
	pass($label);
}

sub reclaimed_top_memory_context_log_count
{
	my ($log) = @_;

	return scalar(() = $log =~ /$top_reclaim_re/g);
}

sub wait_for_reclaimed_top_memory_context_logs
{
	my ($minimum, $label) = @_;
	my $log;
	my $count = 0;

	for (1 .. 100)
	{
		$log = slurp_file($node->logfile);
		$count = reclaimed_top_memory_context_log_count($log);
		last if $count >= $minimum;
		usleep(100_000);
	}

	ok($count >= $minimum, $label)
	  || diag("observed $count reclaimed TopMemoryContext log entries, expected at least $minimum");

	return $log;
}

$node->init;
$node->append_conf(
	'postgresql.conf', q{
multithreaded = on
pooled_protocol_carriers = 0
autovacuum = off
io_method = sync
summarize_wal = off
log_min_messages = debug1
});
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'Milestone W smoke starts threaded runtime');

SKIP:
{
	skip 'postmaster child counting smoke is Unix-specific', 1
	  if $^O eq 'MSWin32';

	is(postmaster_child_count(), 0,
		'Milestone W smoke starts regular sessions without postmaster children');
}

$node->safe_psql(
	'postgres',
	q{
CREATE EXTENSION test_backend_runtime_threaded;
CREATE TABLE threaded_w_core(id int primary key, payload text);
INSERT INTO threaded_w_core
SELECT g, repeat('w', 16) FROM generate_series(1, 128) g;
UPDATE threaded_w_core SET payload = payload || id::text;
DELETE FROM threaded_w_core WHERE id % 17 = 0;
});
is($node->safe_psql('postgres', 'SELECT count(*) FROM threaded_w_core;'),
	'121', 'Milestone W smoke runs catalog-writing SQL');

is($node->safe_psql(
		'postgres',
		q{
BEGIN;
INSERT INTO threaded_w_core VALUES (10000, 'rollback-me');
ROLLBACK;
SELECT count(*) FROM threaded_w_core WHERE id = 10000;
}),
	'0', 'Milestone W smoke preserves transaction rollback');

$node->psql(
	'postgres',
	'SELECT 1 / 0;',
	on_error_stop => 1,
	stdout => \my $error_stdout,
	stderr => \my $error_stderr);
like($error_stderr, qr/division by zero/,
	'Milestone W smoke reports SQL ERROR');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'Milestone W smoke remains usable after SQL ERROR');

my ($abort_ret, $abort_stdout, $abort_stderr) = $node->psql(
	'postgres',
	'BEGIN; SELECT pg_advisory_xact_lock(991000); SELECT 1 / 0;',
	on_error_stop => 1);
isnt($abort_ret, 0,
	'Milestone W smoke transaction abort fixture failed as expected');
like($abort_stderr, qr/division by zero/,
	'Milestone W smoke transaction abort fixture reported SQL ERROR');
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid = 991000;"),
	'0', 'Milestone W smoke transaction abort released advisory lock');

$node->safe_psql(
	'postgres',
	q{
CREATE EXTENSION IF NOT EXISTS plpgsql;
CREATE FUNCTION threaded_w_plpgsql_add(a int, b int)
RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  RETURN a + b;
END
$$;
});
is($node->safe_psql('postgres', 'SELECT threaded_w_plpgsql_add(20, 22);'),
	'42', 'Milestone W smoke runs PL/pgSQL');

$node->safe_psql(
	'postgres',
	q{
ALTER DATABASE postgres SET work_mem = '3MB';
CREATE ROLE threaded_w_role LOGIN;
ALTER ROLE threaded_w_role SET statement_timeout = '7s';
ALTER ROLE threaded_w_role SET default_statistics_target = 77;
});
my $threaded_w_role_connstr =
  $node->connstr('postgres') . " user=threaded_w_role";
is($node->safe_psql(
		'postgres',
		q{
SHOW work_mem;
SHOW statement_timeout;
SHOW default_statistics_target;
},
		connstr => $threaded_w_role_connstr),
	"3MB\n7s\n77",
	'Milestone W smoke applies database and role GUC defaults');
is($node->safe_psql(
		'postgres',
		'SHOW lock_timeout;',
		connstr => $threaded_w_role_connstr . " options='-c lock_timeout=8s'"),
	'8s', 'Milestone W smoke applies startup packet GUC options');
is($node->safe_psql(
		'postgres',
		q{
SET work_mem = '4MB';
SHOW work_mem;
BEGIN;
SET LOCAL work_mem = '5MB';
SHOW work_mem;
COMMIT;
SHOW work_mem;
RESET work_mem;
SHOW work_mem;
},
		connstr => $threaded_w_role_connstr),
	"4MB\n5MB\n4MB\n3MB",
	'Milestone W smoke preserves core GUC stack semantics');

my ($load_ret, $load_stdout, $load_stderr) =
  $node->psql('postgres', "LOAD 'test_backend_runtime';",
	on_error_stop => 1);
isnt($load_ret, 0,
	'Milestone W smoke rejects process-only module in threaded runtime');
like($load_stderr, qr/is not supported in the threaded backend runtime/,
	'Milestone W smoke reports process-only module mismatch');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'Milestone W smoke remains usable after module rejection');

is($node->safe_psql(
		'postgres',
		'SELECT test_backend_runtime_rejects_process_bgworker();'),
	't', 'Milestone W smoke rejects process-model background worker');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'Milestone W smoke remains usable after background-worker rejection');

like(
	$node->safe_psql(
		'postgres',
		'SELECT test_backend_runtime_launch_thread_bgworker();'),
	qr/^\d+$/,
	'Milestone W smoke starts and stops a thread-model background worker');
like(
	slurp_file($node->logfile),
	qr/starting background worker thread carrier/,
	'Milestone W smoke uses a thread carrier for worker handoff');

is($node->safe_psql(
		'postgres',
		q{
SET debug_parallel_query = on;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 4;
CREATE TEMP TABLE threaded_w_parallel AS
SELECT i, i % 10 AS g FROM generate_series(1, 20000) AS i;
ALTER TABLE threaded_w_parallel SET (parallel_workers = 4);
ANALYZE threaded_w_parallel;
SELECT sum(i)::text || '|' || count(*)::text
FROM threaded_w_parallel
WHERE g >= 0;
}),
	'200010000|20000',
	'Milestone W smoke runs representative parallel query');
like(
	slurp_file($node->logfile),
	qr/starting background worker thread carrier "parallel worker/,
	'Milestone W smoke uses thread carriers for parallel query workers');

my @sessions;
my @normal_pids;
my $teardown_reclaim_logs_before =
  reclaimed_top_memory_context_log_count(slurp_file($node->logfile));
for my $i (1 .. 3)
{
	my $session = $node->background_psql('postgres', timeout => 20);
	my $pid = $session->query_safe('SELECT pg_backend_pid();',
		verbose => 0);
	push @sessions, $session;
	push @normal_pids, $pid;
}
is(scalar(@normal_pids), 3,
	'Milestone W smoke opened concurrent threaded sessions');
foreach my $session (@sessions)
{
	$session->quit;
}
wait_for_pids_to_leave_pg_stat_activity(\@normal_pids,
	'Milestone W smoke cleaned up normal disconnects');

my $cancel_psql = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT pg_sleep(30);\n", 30);
ok(pump_until($cancel_psql->{run}, $cancel_psql->{timer},
		$cancel_psql->{stdout}, qr/^\d+\s*$/m),
	'Milestone W smoke active cancel backend reported logical backend id');
my ($cancel_pid) = ${ $cancel_psql->{stdout} } =~ /^(\d+)\s*$/m;
is($node->safe_psql('postgres', "SELECT pg_cancel_backend($cancel_pid);"),
	't', 'Milestone W smoke accepted active cancel request');
ok(pump_until($cancel_psql->{run}, $cancel_psql->{timer},
		$cancel_psql->{stderr}, qr/canceling statement due to user request/),
	'Milestone W smoke active backend observed query cancel');
eval { $cancel_psql->{run}->finish; };

my $abandoned = $node->background_psql('postgres',
	on_error_stop => 0, timeout => 20);
my $abandoned_pid = $abandoned->query_safe('SELECT pg_backend_pid();',
	verbose => 0);
$abandoned->query_safe('BEGIN; SELECT pg_advisory_lock(991001);',
	verbose => 0);
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid = 991001;"),
	'1', 'Milestone W smoke acquired abandoned-client advisory lock');
$abandoned->{run}->kill_kill;
eval { $abandoned->{run}->finish; };
wait_for_pids_to_leave_pg_stat_activity([$abandoned_pid],
	'Milestone W smoke removed abandoned backend from pg_stat_activity');
for (1 .. 100)
{
	last
	  if $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid = 991001;") eq '0';
	usleep(100_000);
}
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid = 991001;"),
	'0', 'Milestone W smoke released abandoned-client advisory lock');

my $terminate_psql = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT pg_sleep(30);\n", 30);
ok(pump_until($terminate_psql->{run}, $terminate_psql->{timer},
		$terminate_psql->{stdout}, qr/^\d+\s*$/m),
	'Milestone W smoke active terminate backend reported logical backend id');
my ($terminate_pid) = ${ $terminate_psql->{stdout} } =~ /^(\d+)\s*$/m;
is($node->safe_psql('postgres',
		"SELECT pg_terminate_backend($terminate_pid, 5000);"),
	't', 'Milestone W smoke accepted active terminate request');
eval { $terminate_psql->{run}->finish; };
wait_for_pids_to_leave_pg_stat_activity([$terminate_pid],
	'Milestone W smoke cleaned up terminated active backend');

my $fatal_psql = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT test_backend_runtime_emit_fatal();\n",
	30);
ok(pump_until($fatal_psql->{run}, $fatal_psql->{timer},
		$fatal_psql->{stdout}, qr/^\d+\s*$/m),
	'Milestone W smoke FATAL backend reported logical backend id');
my ($fatal_pid) = ${ $fatal_psql->{stdout} } =~ /^(\d+)\s*$/m;
ok(pump_until($fatal_psql->{run}, $fatal_psql->{timer},
		$fatal_psql->{stderr}, qr/test_backend_runtime requested FATAL/),
	'Milestone W smoke backend reported test FATAL');
eval { $fatal_psql->{run}->finish; };
wait_for_pids_to_leave_pg_stat_activity([$fatal_pid],
	'Milestone W smoke cleaned up FATAL backend');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'Milestone W smoke remains usable after teardown cases');
wait_for_reclaimed_top_memory_context_logs($teardown_reclaim_logs_before + 6,
	'Milestone W smoke log records reclaimed TopMemoryContext accounting for explicit teardown cases');

my $reconnect_ok = 1;
for my $i (1 .. 20)
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
ok($reconnect_ok, 'Milestone W smoke completed repeated reconnect loop');
is($node->safe_psql('postgres', 'SELECT count(*) FROM threaded_w_core;'),
	'121', 'Milestone W smoke preserved later SQL after reconnect loop');

SKIP:
{
	skip 'postmaster child counting smoke is Unix-specific', 1
	  if $^O eq 'MSWin32';

	is(postmaster_child_count(), 0,
		'Milestone W smoke did not leak postmaster child processes');
}

my $final_log = wait_for_reclaimed_top_memory_context_logs(1,
	'Milestone W smoke log records reclaimed TopMemoryContext accounting');

unlike(
	$final_log,
	qr/PANIC|segmentation|unsupported byval|could not find tuple|server process .* was terminated|was terminated by signal|retained \d+ bytes in TopMemoryContext at exit/,
	'Milestone W smoke log has no crash/corruption or retained TopMemoryContext signatures');

$node->stop('fast');

done_testing();
