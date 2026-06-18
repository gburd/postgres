# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use IPC::Run ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

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
autovacuum = off
io_method = sync
summarize_wal = off
log_min_messages = debug1
});
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'Phase 13 wait-completion TAP starts threaded runtime');

$node->safe_psql('postgres',
	'CREATE EXTENSION test_backend_runtime_threaded;');

my $idle = $node->background_psql('postgres', timeout => 20);
my $idle_pid = $idle->query_safe('SELECT pg_backend_pid();', verbose => 0);

my $idle_snapshot = wait_for_completion_snapshot(
	$idle_pid,
	qr/^waiting\|event_set\|ClientRead\|1\|.*\|1\|1\|1$/,
	'idle threaded client publishes frontend input wait completion');

is($node->safe_psql(
		'postgres',
		"SELECT wait_event FROM pg_stat_activity WHERE pid = $idle_pid;"),
	'ClientRead',
	'pg_stat_activity agrees idle threaded client is waiting on frontend input');

$idle->quit;
wait_for_pid_to_leave_pg_stat_activity($idle_pid,
	'idle threaded client exits cleanly after frontend input wait');

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

is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after Phase 13 wait-completion TAP');

$node->stop('fast');

done_testing();
