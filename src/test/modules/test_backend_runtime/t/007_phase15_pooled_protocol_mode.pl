# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use Test::More;
use Time::HiRes qw(usleep);

use constant PARK_STATE => 0;
use constant QUEUE_STATE => 1;
use constant PARKED_PROTOCOL_COUNT => 14;
use constant CARRIER_LIMIT => 21;

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

sub wait_for_protocol_parked
{
	my ($pid, $label) = @_;
	my $snapshot = '';

	for (1 .. 100)
	{
		$snapshot = protocol_snapshot($pid);
		my @fields = protocol_snapshot_fields($snapshot);

		if (@fields == 24 &&
			$fields[PARK_STATE] eq 'committed' &&
			$fields[QUEUE_STATE] eq 'parked_protocol_read')
		{
			pass($label);
			return @fields;
		}
		usleep(100_000);
	}

	fail($label);
	diag("last protocol-park snapshot for $pid: \"$snapshot\"");
	return ();
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
	'Phase 15 pooled protocol smoke starts threaded runtime');
is($node->safe_psql('postgres', 'SHOW pooled_protocol_carriers'), '2',
	'pooled protocol carrier limit is configured');

$node->safe_psql('postgres',
	'CREATE EXTENSION test_backend_runtime_threaded;');

is($node->safe_psql('postgres',
		'SELECT test_backend_runtime_model_snapshot();'),
	'pooled_protocol|pooled-protocol-affine|2|1',
	'positive pooled_protocol_carriers selects pooled protocol runtime model');

my $session = $node->background_psql('postgres', timeout => 20);
my $pid = $session->query_safe('SELECT pg_backend_pid();', verbose => 0);
my @fields = wait_for_protocol_parked($pid,
	'pooled protocol mode parks at protocol read boundary');

is($fields[CARRIER_LIMIT], '2',
	'protocol scheduler exposes configured pooled carrier limit');
ok($fields[PARKED_PROTOCOL_COUNT] >= 1,
	'protocol scheduler tracks parked session in pooled protocol mode');

is($session->query_safe('SELECT 15015;', verbose => 0), '15015',
	'pooled protocol mode resumes a parked protocol session');
wait_for_protocol_parked($pid,
	'pooled protocol mode parks again after completing one message');

$session->quit;
$node->stop;

done_testing();
