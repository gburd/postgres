# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('threaded_bgworker_crash');

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
	'threaded runtime enabled for crash escalation fixture');

$node->safe_psql(
	'postgres',
	q{
CREATE FUNCTION test_backend_runtime_crash_thread_bgworker()
RETURNS bool
AS 'test_backend_runtime_threaded',
   'test_backend_runtime_crash_thread_bgworker'
LANGUAGE C;
});

my $postmaster_pid = slurp_file($node->data_dir . '/postmaster.pid');
$postmaster_pid =~ s/\n.*//s;
my $log_start = -s $node->logfile;

my ($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	'SELECT test_backend_runtime_crash_thread_bgworker();',
	timeout => 20);

isnt($ret, 0,
	'crashing thread-backed background worker terminates the SQL connection');
like($stderr,
	qr/server closed the connection unexpectedly|connection to server was lost|could not send data to server|terminating connection/,
	'client observed threaded runtime termination');

$node->wait_for_log(
	qr/terminating threaded server runtime after child crash/,
	$log_start);

my $postmaster_exited = 0;
for (1 .. 100)
{
	if (kill(0, $postmaster_pid) == 0)
	{
		$postmaster_exited = 1;
		last;
	}
	usleep(100_000);
}
ok($postmaster_exited,
	'threaded runtime postmaster exited after background worker crash');

my $log = slurp_file($node->logfile, $log_start);
like($log, qr/test_backend_runtime crash bgworker run 1/,
	'crashing background worker reached its thread entrypoint');
unlike($log, qr/issuing SIGKILL to recalcitrant children/,
	'threaded runtime did not wedge in process-mode crash recovery');

$node->stop('immediate', fail_ok => 1);

done_testing();
