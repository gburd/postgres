# Copyright (c) 2026, PostgreSQL Global Development Group

# Backend fibers are TEMPORARY children of the per-loop xtc_orc supervisors,
# and on_child_down is the crash escalation path.  A clean disconnect must not
# escalate; a crashing backend must fail-stop the server through the hook; a
# forged DOWN must do neither.  Each loop thread is pinned to its own CPU.
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
carrier_loops = 2
autovacuum = off
io_method = sync
summarize_wal = off
});
$node->start;

my $startlog = slurp_file($node->logfile);
like($startlog, qr/xtc: carrier up \(2 loops, each with an xtc_orc supervisor, pinned to a CPU\)/,
	'two loops, each supervised and pinned');

# Each loop reports the CPU it pinned to, and for each such CPU some thread of
# the server is restricted to that CPU alone.
my $pm = (split /\n/, slurp_file($node->data_dir . '/postmaster.pid'))[0];
SKIP:
{
	skip 'no /proc', 2 unless -d "/proc/$pm/task";
	my %pinned;
	while ($startlog =~ /xtc: loop (\d+) pinned to CPU (\d+)/g)
	{
		$pinned{$1} = $2;
	}
	is(scalar(keys %pinned), 2, 'both loops report a pinned CPU');
	my @single;
	opendir(my $dh, "/proc/$pm/task") or die "opendir: $!";
	for my $tid (grep { /^\d+$/ } readdir($dh))
	{
		my $status = slurp_file("/proc/$pm/task/$tid/status");
		push @single, $1
		  if $status =~ /^Cpus_allowed_list:\s*(\d+)\s*$/m;
	}
	closedir($dh);
	my %single = map { $_ => 1 } @single;
	ok(!(grep { !$single{$_} } values %pinned),
		'a thread is restricted to each CPU a loop reported');
}

$node->safe_psql('postgres', 'CREATE EXTENSION test_backend_runtime_threaded');

# Several short sessions: each is added to a supervisor and exits cleanly.
for my $i (1 .. 8)
{
	is($node->safe_psql('postgres', "SELECT $i"), $i, "clean session $i");
}
my $log = slurp_file($node->logfile);
like($log, qr/xtc: spawned backend fiber/, 'backends were added as children');
unlike($log, qr/could not add backend fiber/, 'no add_child failed');
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
	qr/supervisor observed GENUINE-CRASH/,
	'the crash was escalated by on_child_down');

$node->stop('immediate', fail_ok => 1);

done_testing();
