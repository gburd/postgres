# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

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

my $node = PostgreSQL::Test::Cluster->new('phase14_protocol_postmaster_death');

sub protocol_snapshot
{
	my ($pid) = @_;

	return $node->safe_psql(
		'postgres',
		"SELECT coalesce(test_backend_runtime_protocol_park_snapshot($pid), '');");
}

sub wait_for_protocol_parked
{
	my ($pid, $label) = @_;
	my $snapshot = '';

	for (1 .. 100)
	{
		$snapshot = protocol_snapshot($pid);
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
	'Phase 14 postmaster-death TAP starts threaded runtime');

$node->safe_psql('postgres',
	'CREATE EXTENSION test_backend_runtime_threaded;');

my $idle = $node->background_psql('postgres', timeout => 20);
my $idle_pid = $idle->query_safe('SELECT pg_backend_pid();',
	verbose => 0);
wait_for_protocol_parked($idle_pid,
	'idle threaded client parks before postmaster shutdown');

$node->stop('immediate');

my $closed_connection = qr/
	  \Qserver closed the connection unexpectedly\E
	| \Qconnection to server was lost\E
	| \Qcould not send data to server\E
	| \Qterminating connection\E
/x;
$idle->{stdin} .= "SELECT 1;\n";
ok(pump_until(
		$idle->{run},
		$idle->{timeout},
		\$idle->{stderr},
		$closed_connection),
	'parked protocol client observes postmaster shutdown');
eval { $idle->{run}->finish; };

done_testing();
