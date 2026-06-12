# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('threaded_runtime');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
multithreaded = on
autovacuum = on
autovacuum_naptime = '1s'
log_min_messages = debug1
});
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'threaded runtime enabled');

$node->safe_psql(
	'postgres',
	q{
CREATE TABLE threaded_runtime_stress(id int primary key, payload text);
INSERT INTO threaded_runtime_stress
SELECT g, repeat('x', 32) FROM generate_series(1, 5) g;
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

is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after Gate D smoke');

for (1 .. 50)
{
	last
	  if slurp_file($node->logfile) =~
	  qr/autovacuum workers are disabled in multithreaded mode/;
	usleep(100_000);
}

like(slurp_file($node->logfile),
	qr/autovacuum workers are disabled in multithreaded mode/,
	'autovacuum worker deferral was logged in threaded mode');
unlike(
	slurp_file($node->logfile),
	qr/PANIC|segmentation|unsupported byval|could not find tuple|server process .* was terminated|was terminated by signal/,
	'server log has no threaded-runtime crash/corruption signatures');

$node->stop('fast');

done_testing();
