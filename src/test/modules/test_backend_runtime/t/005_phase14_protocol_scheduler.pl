# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

use constant PARK_STATE => 0;
use constant QUEUE_STATE => 1;
use constant LAST_WAKE_REASONS => 6;
use constant DEFERRED_NOTIFY_GENERATION => 10;
use constant PARKED_PROTOCOL_COUNT => 14;
use constant CARRIER_ATTACHED => 17;
use constant SESSION_PRESENT => 18;
use constant CONNECTION_PRESENT => 19;
use constant EXECUTION_PRESENT => 20;
use constant CARRIER_LIMIT => 21;
use constant SAME_CARRIER_RESUME_COUNT => 22;
use constant MIGRATED_RESUME_COUNT => 23;

use constant PROTOCOL_WAKE_NOTIFY => (1 << 8);

my $node = PostgreSQL::Test::Cluster->new('phase14_protocol_scheduler');

sub protocol_snapshot
{
	my ($pid) = @_;

	return $node->safe_psql(
		'postgres',
		"SELECT coalesce(test_backend_runtime_protocol_park_snapshot($pid), '');");
}

sub protocol_snapshot_fields
{
	my ($snapshot) = @_;

	return split(/\|/, $snapshot);
}

sub wait_for_protocol_snapshot
{
	my ($pid, $predicate, $label) = @_;
	my $snapshot = '';

	for (1 .. 100)
	{
		$snapshot = protocol_snapshot($pid);
		if ($predicate->($snapshot))
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

	return wait_for_protocol_snapshot(
		$pid,
		sub {
			my @fields = protocol_snapshot_fields(shift);

			return 0 unless @fields >= 24;
			return $fields[PARK_STATE] eq 'committed'
			  && $fields[QUEUE_STATE] eq 'parked_protocol_read'
			  && $fields[CARRIER_ATTACHED] == 0
			  && $fields[SESSION_PRESENT] == 1
			  && $fields[CONNECTION_PRESENT] == 1
			  && $fields[EXECUTION_PRESENT] == 1;
		},
		$label);
}

sub wait_for_protocol_field
{
	my ($pid, $field, $predicate, $label) = @_;

	return wait_for_protocol_snapshot(
		$pid,
		sub {
			my @fields = protocol_snapshot_fields(shift);

			return 0 unless @fields > $field;
			return $predicate->($fields[$field]);
		},
		$label);
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

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'Phase 14 protocol scheduler TAP starts threaded runtime');

$node->safe_psql('postgres',
	'CREATE EXTENSION test_backend_runtime_threaded;');

my @idle_clients;
my @idle_pids;
for my $i (1 .. 3)
{
	my $session = $node->background_psql('postgres', timeout => 20);
	my $pid = $session->query_safe('SELECT pg_backend_pid();',
		verbose => 0);

	push @idle_clients, $session;
	push @idle_pids, $pid;

	wait_for_protocol_parked($pid,
		"idle threaded client $i parks at protocol read boundary");
}

wait_for_protocol_field(
	$idle_pids[0],
	PARKED_PROTOCOL_COUNT,
	sub { return shift >= 3; },
	'protocol scheduler tracks multiple concurrently parked clients');

is($idle_clients[0]->query_safe('SELECT 1001;', verbose => 0), '1001',
	'frontend input wakes a parked protocol client');
wait_for_protocol_parked($idle_pids[0],
	'woken protocol client parks again after completing a full message');
wait_for_protocol_field(
	$idle_pids[0],
	SAME_CARRIER_RESUME_COUNT,
	sub { return shift >= 1; },
	'same-carrier protocol resume counter advances in staging mode');
wait_for_protocol_field(
	$idle_pids[0],
	MIGRATED_RESUME_COUNT,
	sub { return shift == 0; },
	'staging mode does not report migrated protocol resumes');

$idle_clients[1]->quit;
wait_for_pid_to_leave_pg_stat_activity($idle_pids[1],
	'disconnected parked protocol client exits cleanly');

is($node->safe_psql('postgres', "SELECT pg_cancel_backend($idle_pids[2]);"),
	't', 'query cancel request accepted for parked protocol client');
is($idle_clients[2]->query_safe('SELECT 1003;', verbose => 0), '1003',
	'cancelled parked protocol client remains usable for next frontend input');
wait_for_protocol_parked($idle_pids[2],
	'cancelled parked protocol client returns to protocol-read park');

my $terminated = $node->background_psql('postgres',
	on_error_stop => 0, timeout => 20);
my $terminated_pid = $terminated->query_safe('SELECT pg_backend_pid();',
	verbose => 0);
wait_for_protocol_parked($terminated_pid,
	'termination victim parks at protocol read boundary');
is($node->safe_psql('postgres',
		"SELECT pg_terminate_backend($terminated_pid, 5000);"),
	't', 'terminate request accepted for parked protocol client');
wait_for_pid_to_leave_pg_stat_activity($terminated_pid,
	'terminated parked protocol client leaves pg_stat_activity');
eval { $terminated->{run}->finish; };

my $listener = $node->background_psql('postgres', timeout => 20);
my $notify_pattern =
  qr/Asynchronous notification "phase14_notify".*"payload" received/;
my $listener_pid = $listener->query_safe(
	'LISTEN phase14_notify; SELECT pg_backend_pid();',
	verbose => 0);
wait_for_protocol_parked($listener_pid,
	'LISTEN client parks before asynchronous notification');
$node->safe_psql('postgres', "NOTIFY phase14_notify, 'payload';");
wait_for_protocol_field(
	$listener_pid,
	LAST_WAKE_REASONS,
	sub {
		my $reasons = shift;
		return ($reasons & PROTOCOL_WAKE_NOTIFY) != 0;
	},
	'asynchronous notification wakes parked protocol client');
like($listener->query_safe('SELECT 1004;', verbose => 0),
	$notify_pattern,
	'asynchronous notification is visible to listening client');
wait_for_protocol_parked($listener_pid,
	'LISTEN client parks again after notification visibility check');

my $deferred = $node->background_psql('postgres', timeout => 20);
my $deferred_notify_pattern =
  qr/Asynchronous notification "phase14_defer".*"payload" received/;
$deferred->query_safe('LISTEN phase14_defer;', verbose => 0);
my $deferred_pid = $deferred->query_safe(
	'BEGIN; SELECT pg_backend_pid();',
	verbose => 0);
$deferred->query_safe('SELECT pg_advisory_xact_lock(140014);', verbose => 0);
wait_for_protocol_parked($deferred_pid,
	'idle-in-transaction LISTEN client parks at protocol read boundary');
is($node->safe_psql(
		'postgres',
		"SELECT state FROM pg_stat_activity WHERE pid = $deferred_pid;"),
	'idle in transaction',
	'parked idle-in-transaction client preserves transaction state');
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE pid = $deferred_pid AND "
		  . "locktype = 'advisory' AND objid = 140014 AND granted;"),
	'1',
	'parked idle-in-transaction client preserves transaction advisory lock');
$node->safe_psql('postgres', "NOTIFY phase14_defer, 'payload';");
wait_for_protocol_field(
	$deferred_pid,
	DEFERRED_NOTIFY_GENERATION,
	sub { return shift > 0; },
	'deferred notification is recorded while client remains idle in transaction');
usleep(250_000);
$deferred->{run}->pump_nb();
unlike(
	$deferred->{stdout},
	$deferred_notify_pattern,
	'deferred notification is not delivered before transaction end');
$deferred->{stdout} = '';
like($deferred->query_safe('COMMIT;', verbose => 0),
	$deferred_notify_pattern,
	'deferred notification is delivered after COMMIT wakes the parked client');
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE pid = $deferred_pid AND "
		  . "locktype = 'advisory' AND objid = 140014 AND granted;"),
	'0',
	'idle-in-transaction advisory lock is released after COMMIT');
wait_for_protocol_parked($deferred_pid,
	'deferred notification client parks again after COMMIT');

my $idle_timeout = $node->background_psql('postgres',
	on_error_stop => 0, timeout => 20);
my $idle_timeout_pid = $idle_timeout->query_safe(
	"SET idle_session_timeout = '3s'; SELECT pg_backend_pid();",
	verbose => 0);
wait_for_protocol_parked($idle_timeout_pid,
	'idle-session-timeout client parks at protocol read boundary');
wait_for_pid_to_leave_pg_stat_activity($idle_timeout_pid,
	'idle_session_timeout wakes and exits parked protocol client');
eval { $idle_timeout->{run}->finish; };

my $xact_timeout = $node->background_psql('postgres',
	on_error_stop => 0, timeout => 20);
my $xact_timeout_sql =
  "SET idle_in_transaction_session_timeout = '3s'; "
  . "BEGIN; SELECT pg_backend_pid();";
my $xact_timeout_pid = $xact_timeout->query_safe(
	$xact_timeout_sql, verbose => 0);
wait_for_protocol_parked($xact_timeout_pid,
	'idle-in-transaction-timeout client parks at protocol read boundary');
wait_for_pid_to_leave_pg_stat_activity($xact_timeout_pid,
	'idle_in_transaction_session_timeout wakes and exits parked protocol client');
eval { $xact_timeout->{run}->finish; };

my $transaction_timeout = $node->background_psql('postgres',
	on_error_stop => 0, timeout => 20);
my $transaction_timeout_sql =
  "SET transaction_timeout = '3s'; "
  . "BEGIN; SELECT pg_backend_pid();";
my $transaction_timeout_pid = $transaction_timeout->query_safe(
	$transaction_timeout_sql, verbose => 0);
wait_for_protocol_parked($transaction_timeout_pid,
	'transaction-timeout client parks at protocol read boundary');
wait_for_pid_to_leave_pg_stat_activity($transaction_timeout_pid,
	'transaction_timeout wakes and exits parked protocol client');
eval { $transaction_timeout->{run}->finish; };

$listener->quit;
$deferred->quit;
$idle_clients[0]->quit;
$idle_clients[2]->quit;

is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after Phase 14 protocol scheduler TAP');

$node->stop('fast');

done_testing();
