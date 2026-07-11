# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use IPC::Run ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

use constant PARK_STATE => 0;
use constant QUEUE_STATE => 1;
use constant CARRIER_ATTACHED => 17;
use constant SESSION_PRESENT => 18;
use constant CONNECTION_PRESENT => 19;
use constant EXECUTION_PRESENT => 20;

my $node = PostgreSQL::Test::Cluster->new('phase13_wait_completion');

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

sub wait_for_completion_snapshot
{
	my ($pid, $pattern, $label) = @_;
	my $snapshot = '';

	for (1 .. 100)
	{
		$snapshot = $node->safe_psql(
			'postgres',
			"SELECT coalesce(test_backend_runtime_wait_completion_snapshot($pid), '');");
		if ($snapshot =~ $pattern)
		{
			pass($label);
			return $snapshot;
		}
		usleep(100_000);
	}

	fail($label);
	diag("last wait-completion snapshot for $pid: \"$snapshot\"");
	return $snapshot;
}

sub wait_for_carrier_pinned_non_protocol_park
{
	my ($pid, $label) = @_;
	my $snapshot = '';

	for (1 .. 100)
	{
		$snapshot = $node->safe_psql(
			'postgres',
			"SELECT coalesce(test_backend_runtime_protocol_park_snapshot($pid), '');");
		my @fields = split(/\|/, $snapshot);

		if (@fields >= 21 &&
			$fields[PARK_STATE] eq 'none' &&
			$fields[QUEUE_STATE] eq 'none' &&
			$fields[CARRIER_ATTACHED] == 1 &&
			$fields[SESSION_PRESENT] == 1 &&
			$fields[CONNECTION_PRESENT] == 1 &&
			$fields[EXECUTION_PRESENT] == 1)
		{
			pass($label);
			return $snapshot;
		}
		usleep(100_000);
	}

	fail($label);
	diag("last protocol-park snapshot for $pid: \"$snapshot\"");
	return $snapshot;
}

sub wait_for_protocol_parked
{
	my ($pid, $label) = @_;
	my $snapshot = '';

	for (1 .. 100)
	{
		$snapshot = $node->safe_psql(
			'postgres',
			"SELECT coalesce(test_backend_runtime_protocol_park_snapshot($pid), '');");
		my @fields = split(/\|/, $snapshot);

		if (@fields >= 21 &&
			$fields[PARK_STATE] eq 'committed' &&
			$fields[QUEUE_STATE] eq 'parked_protocol_read' &&
			$fields[CARRIER_ATTACHED] == 0 &&
			$fields[SESSION_PRESENT] == 1 &&
			$fields[CONNECTION_PRESENT] == 1 &&
			$fields[EXECUTION_PRESENT] == 1)
		{
			pass($label);
			return $snapshot;
		}
		usleep(100_000);
	}

	fail($label);
	diag("last protocol-park snapshot for $pid: \"$snapshot\"");
	return $snapshot;
}

sub wait_for_pid_to_leave_pg_stat_activity
{
	my ($pid, $label) = @_;

	$node->poll_query_until(
		'postgres',
		"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $pid);",
		't') || die "timed out waiting for $label";
	pass($label);
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

$node->safe_psql('postgres',
	'CREATE EXTENSION test_backend_runtime_threaded;');

if ($node->safe_psql('postgres',
		'SELECT test_backend_runtime_wait_completion_enabled();') ne 't')
{
	$node->stop('fast');
	plan skip_all => 'wait-completion publication is compiled out';
}

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'Phase 13 wait-completion TAP starts threaded runtime');

my $idle = $node->background_psql('postgres', timeout => 20);
my $idle_pid = $idle->query_safe('SELECT pg_backend_pid();', verbose => 0);

wait_for_protocol_parked($idle_pid,
	'idle threaded client parks at protocol read boundary');
like(
	$node->safe_psql(
		'postgres',
		"SELECT coalesce(test_backend_runtime_wait_completion_snapshot($idle_pid), '');"),
	qr/^inactive\|none\|/,
	'idle protocol park does not publish a generic wait-completion record');

is($node->safe_psql(
		'postgres',
		"SELECT wait_event FROM pg_stat_activity WHERE pid = $idle_pid;"),
	'ClientRead',
	'pg_stat_activity agrees idle threaded client is waiting on frontend input');

$idle->quit;
wait_for_pid_to_leave_pg_stat_activity($idle_pid,
	'idle threaded client exits cleanly after frontend input wait');

my $write_psql = start_psql_script(
	"SELECT pg_backend_pid();\nCOPY (SELECT repeat('x', 65536) FROM generate_series(1, 20000)) TO STDOUT;\n",
	120);
ok(pump_until($write_psql->{run}, $write_psql->{timer},
		$write_psql->{stdout}, qr/^\d+\s*$/m),
	'Phase 13 frontend-output wait backend reported logical backend id');
my ($write_pid) = ${ $write_psql->{stdout} } =~ /^(\d+)\s*$/m;

my $write_snapshot = wait_for_completion_snapshot(
	$write_pid,
	qr/^waiting\|event_set\|ClientWrite\|1\|.*\|1\|1\|1$/,
	'frontend output publishes client write wait completion for real threaded backend');
wait_for_carrier_pinned_non_protocol_park($write_pid,
	'frontend output wait remains carrier-pinned and non-protocol-parked');

is($node->safe_psql(
		'postgres',
		"SELECT wait_event FROM pg_stat_activity WHERE pid = $write_pid;"),
	'ClientWrite',
	'pg_stat_activity agrees active threaded backend is waiting on frontend output');

is($node->safe_psql('postgres', "SELECT pg_cancel_backend($write_pid);"),
	't', 'query cancel accepted while real backend is in published frontend-output wait');
ok(pump_until($write_psql->{run}, $write_psql->{timer},
		$write_psql->{stderr}, qr/canceling statement due to user request/),
	'published frontend-output wait observes query cancel');
eval { $write_psql->{run}->finish; };
wait_for_pid_to_leave_pg_stat_activity($write_pid,
	'canceled frontend-output-wait backend leaves pg_stat_activity');

my $sleep_psql = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT pg_sleep(30);\n",
	30);
ok(pump_until($sleep_psql->{run}, $sleep_psql->{timer},
		$sleep_psql->{stdout}, qr/^\d+\s*$/m),
	'Phase 13 latch wait backend reported logical backend id');
my ($sleep_pid) = ${ $sleep_psql->{stdout} } =~ /^(\d+)\s*$/m;

my $sleep_snapshot = wait_for_completion_snapshot(
	$sleep_pid,
	qr/^waiting\|event_set\|PgSleep\|1\|.*\|1\|1\|1$/,
	'pg_sleep publishes latch wait completion for real threaded backend');
wait_for_carrier_pinned_non_protocol_park($sleep_pid,
	'pg_sleep remains carrier-pinned and non-protocol-parked');

is($node->safe_psql(
		'postgres',
		"SELECT wait_event FROM pg_stat_activity WHERE pid = $sleep_pid;"),
	'PgSleep',
	'pg_stat_activity agrees active threaded backend is waiting in pg_sleep');

is($node->safe_psql('postgres', "SELECT pg_cancel_backend($sleep_pid);"),
	't', 'query cancel accepted while real backend is in published latch wait');
ok(pump_until($sleep_psql->{run}, $sleep_psql->{timer},
		$sleep_psql->{stderr}, qr/canceling statement due to user request/),
	'published latch wait observes query cancel');
eval { $sleep_psql->{run}->finish; };
wait_for_pid_to_leave_pg_stat_activity($sleep_pid,
	'canceled latch-wait backend leaves pg_stat_activity');

my $cv_psql = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT test_backend_runtime_wait_on_condition_variable(60000);\n",
	90);
ok(pump_until($cv_psql->{run}, $cv_psql->{timer},
		$cv_psql->{stdout}, qr/^\d+\s*$/m),
	'Phase 13 condition-variable wait backend reported logical backend id');
my ($cv_pid) = ${ $cv_psql->{stdout} } =~ /^(\d+)\s*$/m;

my $cv_snapshot = wait_for_completion_snapshot(
	$cv_pid,
	qr/^waiting\|event_set\|TestBackendRuntimeConditionVariable\|1\|.*\|1\|1\|1$/,
	'condition-variable sleep publishes wait completion for real threaded backend');
wait_for_carrier_pinned_non_protocol_park($cv_pid,
	'condition-variable wait remains carrier-pinned and non-protocol-parked');

is($node->safe_psql(
		'postgres',
		"SELECT wait_event FROM pg_stat_activity WHERE pid = $cv_pid;"),
	'TestBackendRuntimeConditionVariable',
	'pg_stat_activity agrees active threaded backend is waiting on condition variable');

is($node->safe_psql('postgres', "SELECT pg_cancel_backend($cv_pid);"),
	't', 'query cancel accepted while real backend is in published condition-variable wait');
ok(pump_until($cv_psql->{run}, $cv_psql->{timer},
		$cv_psql->{stderr}, qr/canceling statement due to user request/),
	'published condition-variable wait observes query cancel');
eval { $cv_psql->{run}->finish; };
wait_for_pid_to_leave_pg_stat_activity($cv_pid,
	'canceled condition-variable-wait backend leaves pg_stat_activity');

my $lock_holder = $node->background_psql('postgres', timeout => 20);
$lock_holder->query_safe('SELECT pg_advisory_lock(130013);', verbose => 0);

my $lock_psql = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT pg_advisory_lock(130013);\n",
	90);
ok(pump_until($lock_psql->{run}, $lock_psql->{timer},
		$lock_psql->{stdout}, qr/^\d+\s*$/m),
	'Phase 13 heavyweight-lock wait backend reported logical backend id');
my ($lock_pid) = ${ $lock_psql->{stdout} } =~ /^(\d+)\s*$/m;

my $lock_snapshot = wait_for_completion_snapshot(
	$lock_pid,
	qr/^waiting\|event_set\|advisory\|1\|.*\|1\|1\|1$/,
	'advisory lock wait publishes wait completion for real threaded backend');
wait_for_carrier_pinned_non_protocol_park($lock_pid,
	'advisory lock wait remains carrier-pinned and non-protocol-parked');

is($node->safe_psql(
		'postgres',
		"SELECT wait_event FROM pg_stat_activity WHERE pid = $lock_pid;"),
	'advisory',
	'pg_stat_activity agrees active threaded backend is waiting on advisory lock');

is($node->safe_psql('postgres', "SELECT pg_cancel_backend($lock_pid);"),
	't', 'query cancel accepted while real backend is in published lock wait');
ok(pump_until($lock_psql->{run}, $lock_psql->{timer},
		$lock_psql->{stderr}, qr/canceling statement due to user request/),
	'published lock wait observes query cancel');
eval { $lock_psql->{run}->finish; };
wait_for_pid_to_leave_pg_stat_activity($lock_pid,
	'canceled lock-wait backend leaves pg_stat_activity');

$lock_holder->query_safe('SELECT pg_advisory_unlock(130013);', verbose => 0);
$lock_holder->quit;

my $lw_holder = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT test_backend_runtime_hold_lwlock(60000);\n",
	90);
ok(pump_until($lw_holder->{run}, $lw_holder->{timer},
		$lw_holder->{stdout}, qr/^\d+\s*$/m),
	'Phase 13 LWLock holder backend reported logical backend id');
my ($lw_holder_pid) = ${ $lw_holder->{stdout} } =~ /^(\d+)\s*$/m;

my $lw_holder_snapshot = wait_for_completion_snapshot(
	$lw_holder_pid,
	qr/^waiting\|event_set\|TestBackendRuntimeHoldLWLock\|1\|.*\|1\|1\|1$/,
	'LWLock holder is active before testing semaphore-backed wait');
wait_for_carrier_pinned_non_protocol_park($lw_holder_pid,
	'LWLock holder wait remains carrier-pinned and non-protocol-parked');

my $lw_waiter = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT test_backend_runtime_wait_on_lwlock();\n",
	90);
ok(pump_until($lw_waiter->{run}, $lw_waiter->{timer},
		$lw_waiter->{stdout}, qr/^\d+\s*$/m),
	'Phase 13 LWLock waiter backend reported logical backend id');
my ($lw_waiter_pid) = ${ $lw_waiter->{stdout} } =~ /^(\d+)\s*$/m;

my $lw_waiter_snapshot = wait_for_completion_snapshot(
	$lw_waiter_pid,
	qr/^waiting\|semaphore\|TestBackendRuntimeLWLock\|1\|0\|0\|0\|1\|1\|1$/,
	'LWLock semaphore wait publishes wait completion for real threaded backend');
wait_for_carrier_pinned_non_protocol_park($lw_waiter_pid,
	'LWLock semaphore wait remains carrier-pinned and non-protocol-parked');

is($node->safe_psql(
		'postgres',
		"SELECT wait_event FROM pg_stat_activity WHERE pid = $lw_waiter_pid;"),
	'TestBackendRuntimeLWLock',
	'pg_stat_activity agrees active threaded backend is waiting on LWLock semaphore');

is($node->safe_psql('postgres', "SELECT pg_cancel_backend($lw_holder_pid);"),
	't', 'query cancel accepted for backend holding test LWLock');
ok(pump_until($lw_holder->{run}, $lw_holder->{timer},
		$lw_holder->{stderr}, qr/canceling statement due to user request/),
	'canceled LWLock holder releases test LWLock');
eval { $lw_holder->{run}->finish; };
wait_for_pid_to_leave_pg_stat_activity($lw_holder_pid,
	'canceled LWLock holder leaves pg_stat_activity');

ok(pump_until($lw_waiter->{run}, $lw_waiter->{timer},
		$lw_waiter->{stdout}, qr/^t\s*$/m),
	'published LWLock semaphore wait completes after holder releases');
eval { $lw_waiter->{run}->finish; };
wait_for_pid_to_leave_pg_stat_activity($lw_waiter_pid,
	'LWLock waiter leaves pg_stat_activity after acquiring test LWLock');

is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after Phase 13 wait-completion TAP');

$node->stop('fast');

done_testing();
