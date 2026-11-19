# Copyright (c) 2026, PostgreSQL Global Development Group

# With xtc_carrier_use_orc = on, client backend fibers are TEMPORARY children
# of the per-loop xtc_orc supervisors, and on_child_down is the crash
# escalation path.  A clean disconnect must not escalate; a crashing backend
# must fail-stop the server through the hook; a forged DOWN must do neither.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('orc_supervision');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
multithreaded = on
pooled_protocol_carriers = 0
xtc_carrier_use_orc = on
autovacuum = off
io_method = sync
summarize_wal = off
});
$node->start;

like(slurp_file($node->logfile),
	qr/xtc_orc supervisors up .* TEMPORARY children/,
	'xtc_orc supervisors own the backend fibers');

$node->safe_psql('postgres', 'CREATE EXTENSION test_backend_runtime_threaded');

# Several short sessions: each is added to a supervisor and exits cleanly.
for my $i (1 .. 8)
{
	is($node->safe_psql('postgres', "SELECT $i"), $i, "clean session $i");
}
my $log = slurp_file($node->logfile);
like($log, qr/xtc: orc added backend fiber/, 'backends were added as children');
unlike($log, qr/orc xtc_sup_add_child failed/, 'no add_child failed');
unlike($log, qr/GENUINE-CRASH/, 'clean exits did not escalate');

is($node->safe_psql('postgres', 'SELECT test_backend_runtime_send_forged_down()'),
	't', 'forged DOWN delivered');
is($node->safe_psql('postgres', 'SELECT 1'), '1', 'server survives a forged DOWN');

my $postmaster_pid = slurp_file($node->data_dir . '/postmaster.pid');
$postmaster_pid =~ s/\n.*//s;
my $log_start = -s $node->logfile;
my ($ret, $out, $err) = $node->psql('postgres',
	'SELECT test_backend_runtime_crash_current_backend()');
isnt($ret, 0, 'crashing backend fails its query');

my $exited = 0;
for (1 .. 100)
{
	if (kill(0, $postmaster_pid) == 0)
	{
		$exited = 1;
		last;
	}
	usleep(100_000);
}
ok($exited, 'server fail-stopped after the backend crash');
like(slurp_file($node->logfile, $log_start),
	qr/ORC observed GENUINE-CRASH/,
	'the crash was escalated by on_child_down');

$node->stop('immediate', fail_ok => 1);

done_testing();
