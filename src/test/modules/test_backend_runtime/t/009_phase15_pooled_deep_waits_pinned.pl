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

my $node = PostgreSQL::Test::Cluster->new('phase15_pooled_deep_waits_pinned');

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
pooled_protocol_carriers = 3
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
	'Phase 15 pooled deep-wait TAP starts threaded runtime');
is($node->safe_psql('postgres', 'SHOW pooled_protocol_carriers'), '3',
	'pooled deep-wait test uses a bounded carrier pool');

is($node->safe_psql('postgres',
		'SELECT test_backend_runtime_model_snapshot();'),
	'pooled_protocol|pooled-protocol-affine|3|1',
	'pooled deep-wait test runs in pooled protocol mode');

my $write_psql = start_psql_script(
	"SELECT pg_backend_pid();\nCOPY (SELECT repeat('x', 65536) FROM generate_series(1, 20000)) TO STDOUT;\n",
	120);
ok(pump_until($write_psql->{run}, $write_psql->{timer},
		$write_psql->{stdout}, qr/^\d+\s*$/m),
	'pooled frontend-output wait backend reported logical backend id');
my ($write_pid) = ${ $write_psql->{stdout} } =~ /^(\d+)\s*$/m;

wait_for_completion_snapshot(
	$write_pid,
	qr/^waiting\|event_set\|ClientWrite\|1\|.*\|1\|1\|1$/,
	'pooled frontend output publishes client write wait completion');
wait_for_carrier_pinned_non_protocol_park($write_pid,
	'pooled frontend output wait remains carrier-pinned and non-protocol-parked');

is($node->safe_psql('postgres', "SELECT pg_cancel_backend($write_pid);"),
	't', 'query cancel accepted for pooled frontend-output wait');
ok(pump_until($write_psql->{run}, $write_psql->{timer},
		$write_psql->{stderr}, qr/canceling statement due to user request/),
	'pooled frontend-output wait observes query cancel');
eval { $write_psql->{run}->finish; };
wait_for_pid_to_leave_pg_stat_activity($write_pid,
	'canceled pooled frontend-output backend leaves pg_stat_activity');

my $sleep_psql = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT pg_sleep(30);\n",
	30);
ok(pump_until($sleep_psql->{run}, $sleep_psql->{timer},
		$sleep_psql->{stdout}, qr/^\d+\s*$/m),
	'pooled pg_sleep backend reported logical backend id');
my ($sleep_pid) = ${ $sleep_psql->{stdout} } =~ /^(\d+)\s*$/m;

wait_for_completion_snapshot(
	$sleep_pid,
	qr/^waiting\|event_set\|PgSleep\|1\|.*\|1\|1\|1$/,
	'pooled pg_sleep publishes latch wait completion');
wait_for_carrier_pinned_non_protocol_park($sleep_pid,
	'pooled pg_sleep remains carrier-pinned and non-protocol-parked');

is($node->safe_psql('postgres', "SELECT pg_cancel_backend($sleep_pid);"),
	't', 'query cancel accepted for pooled pg_sleep');
ok(pump_until($sleep_psql->{run}, $sleep_psql->{timer},
		$sleep_psql->{stderr}, qr/canceling statement due to user request/),
	'pooled pg_sleep observes query cancel');
eval { $sleep_psql->{run}->finish; };
wait_for_pid_to_leave_pg_stat_activity($sleep_pid,
	'canceled pooled pg_sleep backend leaves pg_stat_activity');

my $lock_holder = $node->background_psql('postgres', timeout => 20);
$lock_holder->query_safe('SELECT pg_advisory_lock(150913);', verbose => 0);

my $lock_psql = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT pg_advisory_lock(150913);\n",
	90);
ok(pump_until($lock_psql->{run}, $lock_psql->{timer},
		$lock_psql->{stdout}, qr/^\d+\s*$/m),
	'pooled advisory-lock waiter reported logical backend id');
my ($lock_pid) = ${ $lock_psql->{stdout} } =~ /^(\d+)\s*$/m;

wait_for_completion_snapshot(
	$lock_pid,
	qr/^waiting\|event_set\|advisory\|1\|.*\|1\|1\|1$/,
	'pooled advisory lock wait publishes wait completion');
wait_for_carrier_pinned_non_protocol_park($lock_pid,
	'pooled advisory lock wait remains carrier-pinned and non-protocol-parked');

is($node->safe_psql('postgres', "SELECT pg_cancel_backend($lock_pid);"),
	't', 'query cancel accepted for pooled advisory-lock wait');
ok(pump_until($lock_psql->{run}, $lock_psql->{timer},
		$lock_psql->{stderr}, qr/canceling statement due to user request/),
	'pooled advisory-lock wait observes query cancel');
eval { $lock_psql->{run}->finish; };
wait_for_pid_to_leave_pg_stat_activity($lock_pid,
	'canceled pooled advisory-lock waiter leaves pg_stat_activity');

$lock_holder->query_safe('SELECT pg_advisory_unlock(150913);', verbose => 0);
$lock_holder->quit;

my $lw_holder = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT test_backend_runtime_hold_lwlock(60000);\n",
	90);
ok(pump_until($lw_holder->{run}, $lw_holder->{timer},
		$lw_holder->{stdout}, qr/^\d+\s*$/m),
	'pooled LWLock holder reported logical backend id');
my ($lw_holder_pid) = ${ $lw_holder->{stdout} } =~ /^(\d+)\s*$/m;

wait_for_completion_snapshot(
	$lw_holder_pid,
	qr/^waiting\|event_set\|TestBackendRuntimeHoldLWLock\|1\|.*\|1\|1\|1$/,
	'pooled LWLock holder publishes event-set wait completion');
wait_for_carrier_pinned_non_protocol_park($lw_holder_pid,
	'pooled LWLock holder remains carrier-pinned and non-protocol-parked');

my $lw_waiter = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT test_backend_runtime_wait_on_lwlock();\n",
	90);
ok(pump_until($lw_waiter->{run}, $lw_waiter->{timer},
		$lw_waiter->{stdout}, qr/^\d+\s*$/m),
	'pooled LWLock waiter reported logical backend id');
my ($lw_waiter_pid) = ${ $lw_waiter->{stdout} } =~ /^(\d+)\s*$/m;

wait_for_completion_snapshot(
	$lw_waiter_pid,
	qr/^waiting\|semaphore\|TestBackendRuntimeLWLock\|1\|0\|0\|0\|1\|1\|1$/,
	'pooled LWLock semaphore wait publishes wait completion');
wait_for_carrier_pinned_non_protocol_park($lw_waiter_pid,
	'pooled LWLock semaphore wait remains carrier-pinned and non-protocol-parked');

is($node->safe_psql('postgres', "SELECT pg_cancel_backend($lw_holder_pid);"),
	't', 'query cancel accepted for pooled LWLock holder');
ok(pump_until($lw_holder->{run}, $lw_holder->{timer},
		$lw_holder->{stderr}, qr/canceling statement due to user request/),
	'canceled pooled LWLock holder releases test LWLock');
eval { $lw_holder->{run}->finish; };
wait_for_pid_to_leave_pg_stat_activity($lw_holder_pid,
	'canceled pooled LWLock holder leaves pg_stat_activity');

ok(pump_until($lw_waiter->{run}, $lw_waiter->{timer},
		$lw_waiter->{stdout}, qr/^t\s*$/m),
	'pooled LWLock semaphore wait completes after holder releases');
eval { $lw_waiter->{run}->finish; };
wait_for_pid_to_leave_pg_stat_activity($lw_waiter_pid,
	'pooled LWLock waiter leaves pg_stat_activity after acquiring test LWLock');

is($node->safe_psql('postgres', 'SELECT 15019;'), '15019',
	'pooled protocol server remains usable after pinned deep-wait negatives');

$node->stop('fast');

done_testing();
