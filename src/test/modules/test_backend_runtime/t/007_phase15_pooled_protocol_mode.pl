# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use Test::More;
use Time::HiRes qw(usleep);

use constant PARK_STATE => 0;
use constant QUEUE_STATE => 1;
use constant LAST_WAKE_REASONS => 6;
use constant PARKED_PROTOCOL_COUNT => 14;
use constant CARRIER_ATTACHED => 17;
use constant SESSION_PRESENT => 18;
use constant CONNECTION_PRESENT => 19;
use constant EXECUTION_PRESENT => 20;
use constant CARRIER_LIMIT => 21;
use constant SAME_CARRIER_RESUME_COUNT => 22;
use constant MIGRATED_RESUME_COUNT => 23;
use constant REGISTERED_CARRIER_COUNT => 24;
use constant IDLE_CARRIER_COUNT => 25;
use constant ACTIVE_CARRIER_COUNT => 26;
use constant LONG_LIVED_BACKGROUND_TIMEOUT => 60;

use constant PROTOCOL_WAKE_NOTIFY => (1 << 8);

my $node = PostgreSQL::Test::Cluster->new('phase15_pooled_protocol_mode');

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

	my $snapshot = wait_for_protocol_snapshot(
		$pid,
		sub {
			my @fields = protocol_snapshot_fields(shift);

			return 0 unless @fields >= 27;
			return $fields[PARK_STATE] eq 'committed'
			  && ($fields[QUEUE_STATE] eq 'parked_protocol_read'
				|| $fields[QUEUE_STATE] eq 'polling')
			  && $fields[CARRIER_ATTACHED] == 0
			  && $fields[SESSION_PRESENT] == 1
			  && $fields[CONNECTION_PRESENT] == 1
			  && $fields[EXECUTION_PRESENT] == 1;
		},
		$label);

	return protocol_snapshot_fields($snapshot);
}

sub wait_for_protocol_field
{
	my ($pid, $field, $predicate, $label) = @_;

	my $snapshot = wait_for_protocol_snapshot(
		$pid,
		sub {
			my @fields = protocol_snapshot_fields(shift);

			return 0 unless @fields > $field;
			return $predicate->($fields[$field]);
		},
		$label);

	return protocol_snapshot_fields($snapshot);
}

sub wait_for_carrier_pinned_non_protocol_park
{
	my ($pid, $label) = @_;

	my $snapshot = wait_for_protocol_snapshot(
		$pid,
		sub {
			my @fields = protocol_snapshot_fields(shift);

			return 0 unless @fields >= 21;
			return $fields[PARK_STATE] eq 'none'
			  && $fields[QUEUE_STATE] eq 'none'
			  && $fields[CARRIER_ATTACHED] == 1
			  && $fields[SESSION_PRESENT] == 1
			  && $fields[CONNECTION_PRESENT] == 1
			  && $fields[EXECUTION_PRESENT] == 1;
		},
		$label);

	return protocol_snapshot_fields($snapshot);
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
pooled_protocol_carriers = 2
autovacuum = off
io_method = sync
summarize_wal = off
log_min_messages = debug1
});
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'Phase 15 pooled protocol TAP starts threaded runtime');
is($node->safe_psql('postgres', 'SHOW pooled_protocol_carriers'), '2',
	'pooled protocol carrier limit is configured');

$node->safe_psql('postgres',
	'CREATE EXTENSION test_backend_runtime_threaded;');

is($node->safe_psql('postgres',
		'SELECT test_backend_runtime_model_snapshot();'),
	'pooled_protocol|pooled-protocol-affine|2|1',
	'positive pooled_protocol_carriers selects pooled protocol runtime model');

my @sessions;
my @pids;
for my $i (1 .. 5)
{
	my $session = $node->background_psql('postgres',
		timeout => LONG_LIVED_BACKGROUND_TIMEOUT);
	my $pid = $session->query_safe('SELECT pg_backend_pid();', verbose => 0);

	push @sessions, $session;
	push @pids, $pid;

	wait_for_protocol_parked($pid,
		"pooled protocol session $i parks at protocol read boundary");
}

my @fields = wait_for_protocol_field(
	$pids[0],
	PARKED_PROTOCOL_COUNT,
	sub { return shift >= scalar @sessions; },
	'pooled protocol scheduler tracks more parked sessions than carriers');

is($fields[CARRIER_LIMIT], '2',
	'protocol scheduler exposes configured pooled carrier limit');
ok($fields[REGISTERED_CARRIER_COUNT] >= 1
	  && $fields[REGISTERED_CARRIER_COUNT] <= $fields[CARRIER_LIMIT],
	'pooled protocol carriers are demand-started within configured carrier limit');
is($fields[IDLE_CARRIER_COUNT] + $fields[ACTIVE_CARRIER_COUNT],
	$fields[REGISTERED_CARRIER_COUNT],
	'protocol scheduler accounts for all registered carriers');
ok($fields[ACTIVE_CARRIER_COUNT] >= 1,
	'protocol scheduler accounts for active snapshot carrier');

my $resume_count_before =
  $fields[SAME_CARRIER_RESUME_COUNT] + $fields[MIGRATED_RESUME_COUNT];

is($sessions[0]->query_safe('SELECT 15015;', verbose => 0), '15015',
	'pooled protocol mode resumes a parked protocol session');
@fields = wait_for_protocol_parked($pids[0],
	'pooled protocol mode parks again after completing one message');
ok($fields[SAME_CARRIER_RESUME_COUNT] + $fields[MIGRATED_RESUME_COUNT] >
	  $resume_count_before,
	'pooled protocol resume counters record same-or-migrated carrier result');

$sessions[1]->query_safe('BEGIN;', verbose => 0);
$sessions[1]->query_safe('SELECT pg_advisory_xact_lock(150115);',
	verbose => 0);
wait_for_protocol_parked($pids[1],
	'idle-in-transaction pooled protocol session parks while holding a lock');
is($node->safe_psql(
		'postgres',
		"SELECT state FROM pg_stat_activity WHERE pid = $pids[1];"),
	'idle in transaction',
	'parked pooled idle-in-transaction session preserves transaction state');
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE pid = $pids[1] AND "
		  . "locktype = 'advisory' AND objid = 150115 AND granted;"),
	'1',
	'parked pooled idle-in-transaction session preserves advisory lock');

for my $i (2 .. 4)
{
	is($sessions[$i]->query_safe("SELECT 15015 + $i;", verbose => 0),
		15015 + $i,
		"carrier pool serves parked-session peer $i while transaction is idle");
	wait_for_protocol_parked($pids[$i],
		"pooled protocol peer $i parks again after carrier service");
}

is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE pid = $pids[1] AND "
		  . "locktype = 'advisory' AND objid = 150115 AND granted;"),
	'1',
	'idle-in-transaction lock survives while carriers serve other sessions');
is($sessions[1]->query_safe('COMMIT; SELECT 15115;', verbose => 0),
	'15115',
	'pooled idle-in-transaction session resumes and commits');
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE pid = $pids[1] AND "
		  . "locktype = 'advisory' AND objid = 150115 AND granted;"),
	'0',
	'pooled idle-in-transaction advisory lock is released after commit');
wait_for_protocol_parked($pids[1],
	'committed pooled idle-in-transaction session parks again');

wait_for_protocol_field(
	$pids[0],
	PARKED_PROTOCOL_COUNT,
	sub { return shift >= scalar @sessions; },
	'pooled protocol sessions still outnumber carriers after stress');

my $sleep_session = $node->background_psql('postgres', timeout => 30);
my $sleep_output = $sleep_session->query_until(qr/^\d+\s*$/m,
	"SELECT pg_backend_pid();\nSELECT pg_sleep(30);\n");
my ($sleep_pid) = $sleep_output =~ /^(\d+)\s*$/m;
wait_for_carrier_pinned_non_protocol_park($sleep_pid,
	'pooled pg_sleep remains carrier-pinned and non-protocol-parked');
is($node->safe_psql('postgres', "SELECT pg_cancel_backend($sleep_pid);"),
	't',
	'query cancel accepted for pooled carrier-pinned pg_sleep');
eval { $sleep_session->{run}->finish; };

$sessions[4]->quit;
wait_for_pid_to_leave_pg_stat_activity($pids[4],
	'disconnected pooled parked protocol session exits cleanly');
pop @sessions;
pop @pids;

is($node->safe_psql('postgres', "SELECT pg_cancel_backend($pids[0]);"),
	't',
	'query cancel request accepted for pooled parked protocol session');
is($sessions[0]->query_safe('SELECT 15016;', verbose => 0), '15016',
	'cancelled pooled parked protocol session remains usable');
wait_for_protocol_parked($pids[0],
	'cancelled pooled parked protocol session returns to protocol-read park');

my $terminated = $node->background_psql('postgres',
	on_error_stop => 0, timeout => 20);
my $terminated_pid = $terminated->query_safe('SELECT pg_backend_pid();',
	verbose => 0);
wait_for_protocol_parked($terminated_pid,
	'pooled termination victim parks at protocol read boundary');
is($node->safe_psql('postgres',
		"SELECT pg_terminate_backend($terminated_pid, 5000);"),
	't',
	'terminate request accepted for pooled parked protocol session');
wait_for_pid_to_leave_pg_stat_activity($terminated_pid,
	'terminated pooled parked protocol session leaves pg_stat_activity');
eval { $terminated->{run}->finish; };

my $listener = $node->background_psql('postgres',
	timeout => LONG_LIVED_BACKGROUND_TIMEOUT);
my $notify_pattern =
  qr/Asynchronous notification "phase15_notify".*"payload" received/;
my $listener_pid = $listener->query_safe(
	'LISTEN phase15_notify; SELECT pg_backend_pid();',
	verbose => 0);
wait_for_protocol_parked($listener_pid,
	'pooled LISTEN session parks before asynchronous notification');
$node->safe_psql('postgres', "NOTIFY phase15_notify, 'payload';");
wait_for_protocol_field(
	$listener_pid,
	LAST_WAKE_REASONS,
	sub {
		my $reasons = shift;
		return ($reasons & PROTOCOL_WAKE_NOTIFY) != 0;
	},
	'asynchronous notification wakes pooled parked protocol session');
like($listener->query_safe('SELECT 15017;', verbose => 0),
	$notify_pattern,
	'asynchronous notification is visible to pooled listening session');
wait_for_protocol_parked($listener_pid,
	'pooled LISTEN session parks again after notification visibility check');

my $idle_timeout = $node->background_psql('postgres',
	on_error_stop => 0, timeout => 20);
my $idle_timeout_pid = $idle_timeout->query_safe(
	"SET idle_session_timeout = '3s'; SELECT pg_backend_pid();",
	verbose => 0);
wait_for_protocol_parked($idle_timeout_pid,
	'pooled idle-session-timeout session parks at protocol read boundary');
wait_for_pid_to_leave_pg_stat_activity($idle_timeout_pid,
	'pooled idle_session_timeout wakes and exits parked protocol session');
eval { $idle_timeout->{run}->finish; };

is($node->safe_psql('postgres', 'SELECT 15018;'), '15018',
	'pooled protocol server remains usable after parked wake races');

$listener->quit;
for my $session (@sessions)
{
	$session->quit;
}
$node->stop;

done_testing();
