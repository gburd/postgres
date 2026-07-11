# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

use constant PARK_STATE => 0;
use constant QUEUE_STATE => 1;
use constant PARKED_PROTOCOL_COUNT => 14;
use constant CARRIER_ATTACHED => 17;
use constant SESSION_PRESENT => 18;
use constant CONNECTION_PRESENT => 19;
use constant EXECUTION_PRESENT => 20;
use constant CARRIER_LIMIT => 21;
use constant REGISTERED_CARRIER_COUNT => 24;

my $node =
  PostgreSQL::Test::Cluster->new('phase15_pooled_protocol_postmaster_death');

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
			  && $fields[QUEUE_STATE] eq 'parked_protocol_read'
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
	'Phase 15 pooled postmaster-death TAP starts threaded runtime');
is($node->safe_psql('postgres', 'SHOW pooled_protocol_carriers'), '2',
	'pooled postmaster-death test uses a bounded carrier pool');

$node->safe_psql('postgres',
	'CREATE EXTENSION test_backend_runtime_threaded;');

my @sessions;
my @pids;
for my $i (1 .. 3)
{
	my $session = $node->background_psql('postgres', timeout => 20);
	my $pid = $session->query_safe('SELECT pg_backend_pid();', verbose => 0);

	push @sessions, $session;
	push @pids, $pid;

	wait_for_protocol_parked($pid,
		"pooled postmaster-death client $i parks before shutdown");
}

my @fields = wait_for_protocol_field(
	$pids[0],
	PARKED_PROTOCOL_COUNT,
	sub { return shift >= scalar @sessions; },
	'pooled postmaster-death test parks more sessions than carriers');

is($fields[CARRIER_LIMIT], '2',
	'pooled postmaster-death snapshot exposes carrier limit');
ok($fields[REGISTERED_CARRIER_COUNT] >= 1
	  && $fields[REGISTERED_CARRIER_COUNT] <= $fields[CARRIER_LIMIT],
	'pooled postmaster-death snapshot exposes bounded demand-started carriers');

$node->stop('immediate');

my $closed_connection = qr/
	  \Qserver closed the connection unexpectedly\E
	| \Qconnection to server was lost\E
	| \Qcould not send data to server\E
	| \Qterminating connection\E
/x;
for my $i (0 .. $#sessions)
{
	my $session = $sessions[$i];

	$session->{stdin} .= "SELECT 1;\n";
	ok(pump_until(
			$session->{run},
			$session->{timeout},
			\$session->{stderr},
			$closed_connection),
		"parked pooled protocol client " . ($i + 1) .
		  " observes postmaster shutdown");
	eval { $session->{run}->finish; };
}

done_testing();
